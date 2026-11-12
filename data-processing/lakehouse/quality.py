"""Pure-Python lakehouse rules shared by the Spark job and offline tests."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any


SOURCE_SPECS = {
    "customer_management": {
        "table": "customers",
        "mode": "snapshot",
        "primary_key": "customer_id",
        "required": (
            "customer_id",
            "full_name",
            "email",
            "phone",
            "city",
            "created_at",
            "updated_at",
        ),
    },
    "core_banking": {
        "table": "accounts",
        "mode": "snapshot",
        "primary_key": "account_id",
        "required": (
            "account_id",
            "customer_id",
            "branch_id",
            "currency",
            "account_type",
            "balance",
            "status",
            "created_at",
            "updated_at",
        ),
    },
    "payment_processing": {
        "table": "transactions",
        "mode": "incremental",
        "primary_key": "transaction_id",
        "required": (
            "transaction_id",
            "account_id",
            "customer_id",
            "transaction_type",
            "amount",
            "balance_after",
            "event_time",
            "event_id",
        ),
    },
    "card_management": {
        "table": "cards",
        "mode": "snapshot",
        "primary_key": "card_id",
        "required": (
            "card_id",
            "account_id",
            "customer_id",
            "card_token",
            "last_four",
            "card_type",
            "status",
            "created_at",
        ),
    },
    "loan_management": {
        "table": "loans",
        "mode": "snapshot",
        "primary_key": "loan_id",
        "required": (
            "loan_id",
            "customer_id",
            "principal",
            "outstanding",
            "interest_rate",
            "status",
            "created_at",
            "updated_at",
        ),
    },
    "branch_management": {
        "table": "branches",
        "mode": "snapshot",
        "primary_key": "branch_id",
        "required": ("branch_id", "branch_name", "city", "opened_at"),
    },
}

DOMAIN_RULES = {
    "core_banking": {
        "nonnegative": ("balance",),
    },
    "payment_processing": {
        "positive": ("amount",),
        "one_of": {"transaction_type": ("DEPOSIT", "WITHDRAWAL")},
    },
    "loan_management": {
        "positive": ("principal",),
        "nonnegative": ("outstanding",),
    },
}

REFERENCES = {
    "core_banking": {"customer_id": "customer_management", "branch_id": "branch_management"},
    "payment_processing": {
        "account_id": "core_banking",
        "customer_id": "customer_management",
    },
    "card_management": {
        "account_id": "core_banking",
        "customer_id": "customer_management",
    },
    "loan_management": {"customer_id": "customer_management"},
}


class MissingBronzeObjectError(RuntimeError):
    """Raised when a completed Bronze manifest references an unavailable object."""


def validate_manifest(manifest: Mapping[str, Any], source: str) -> None:
    """Reject incomplete or mismatched manifests before Spark reads their data."""
    spec = SOURCE_SPECS[source]
    if (
        manifest.get("source") != source
        or manifest.get("table") != spec["table"]
        or manifest.get("mode") != spec["mode"]
        or not manifest.get("batch_id")
        or not manifest.get("started_at")
        or not manifest.get("completed_at")
    ):
        raise ValueError(f"Invalid completed Bronze manifest for {source}.")

    row_count = manifest.get("row_count")
    objects = manifest.get("objects")
    if (
        not isinstance(row_count, int)
        or row_count < 0
        or not isinstance(objects, list)
        or (row_count > 0 and not objects)
    ):
        raise ValueError(f"Invalid row count or object list in {source} manifest.")
    expected_prefix = (
        f"bronze/{source}/{spec['table']}/mode={spec['mode']}/"
        f"batch_id={manifest['batch_id']}/"
    )
    if any(not isinstance(key, str) or not key.startswith(expected_prefix) for key in objects):
        raise ValueError(f"Invalid object path in {source} manifest.")


def check_manifest_objects(
    manifest: Mapping[str, Any],
    object_exists: Callable[[str], bool],
) -> None:
    """Ensure every manifest object is present without reading its contents."""
    for key in manifest["objects"]:
        if not isinstance(key, str) or not key:
            raise ValueError("A Bronze manifest contains an invalid object key.")
        if not object_exists(key):
            raise MissingBronzeObjectError(
                f"Bronze object referenced by batch {manifest['batch_id']} is missing."
            )


def select_manifests(source: str, manifests: Iterable[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    """Use one latest complete snapshot, or all complete incremental batches."""
    spec = SOURCE_SPECS[source]
    candidates = [manifest for manifest in manifests if manifest.get("source") == source]
    for manifest in candidates:
        validate_manifest(manifest, source)

    candidates.sort(key=lambda item: (item["started_at"], item["batch_id"]))
    if not candidates:
        raise FileNotFoundError(f"No completed Bronze manifests found for {source}.")
    if spec["mode"] == "snapshot":
        return [candidates[-1]]
    return candidates


def record_errors(
    source: str,
    record: Mapping[str, Any],
    reference_ids: Mapping[str, set[Any]] | None = None,
) -> list[str]:
    """Return stable, value-free quality codes for a source record."""
    errors = [
        f"null:{field}"
        for field in SOURCE_SPECS[source]["required"]
        if record.get(field) is None
    ]
    rules = DOMAIN_RULES.get(source, {})
    for field in rules.get("positive", ()):
        value = record.get(field)
        if value is not None and value <= 0:
            errors.append(f"not_positive:{field}")
    for field in rules.get("nonnegative", ()):
        value = record.get(field)
        if value is not None and value < 0:
            errors.append(f"negative:{field}")
    for field, allowed in rules.get("one_of", {}).items():
        value = record.get(field)
        if value is not None and value not in allowed:
            errors.append(f"unexpected_value:{field}")
    for field, reference_source in REFERENCES.get(source, {}).items():
        value = record.get(field)
        known_ids = (reference_ids or {}).get(reference_source)
        if value is not None and known_ids is not None and value not in known_ids:
            errors.append(f"missing_reference:{field}")
    return errors


def deduplicate_records(
    source: str, records: Sequence[Mapping[str, Any]]
) -> tuple[list[Mapping[str, Any]], int]:
    """Keep the most recently ingested row per key and report discarded duplicates."""
    primary_key = SOURCE_SPECS[source]["primary_key"]
    winners: dict[Any, Mapping[str, Any]] = {}
    duplicate_rows = 0
    for record in records:
        key = record.get(primary_key)
        if key is None:
            continue
        previous = winners.get(key)
        if previous is None:
            winners[key] = record
            continue
        duplicate_rows += 1
        current_order = (str(record.get("_ingested_at", "")), str(record.get("_batch_id", "")))
        previous_order = (str(previous.get("_ingested_at", "")), str(previous.get("_batch_id", "")))
        if current_order > previous_order:
            winners[key] = record
    return list(winners.values()), duplicate_rows


def run_before_publish(work: Callable[[], Any], publish: Callable[[], None]) -> Any:
    """Call the catalog publish operation only after all work returns successfully."""
    result = work()
    publish()
    return result
