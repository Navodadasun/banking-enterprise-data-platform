import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "data-processing" / "lakehouse"))

from quality import (
    MissingBronzeObjectError,
    check_manifest_objects,
    deduplicate_records,
    record_errors,
    run_before_publish,
    select_manifests,
)
from nessie import NessieClient
from unittest.mock import Mock, call


def manifest(source, batch_id, started_at, row_count=1, objects=None):
    source_table = {
        "payment_processing": ("transactions", "incremental"),
        "customer_management": ("customers", "snapshot"),
    }
    table, mode = source_table[source]
    return {
        "source": source,
        "table": table,
        "mode": mode,
        "batch_id": batch_id,
        "started_at": started_at,
        "completed_at": started_at,
        "row_count": row_count,
        "objects": (
            objects
            if objects is not None
            else [
                f"bronze/{source}/{table}/mode={mode}/"
                f"batch_id={batch_id}/part-00000.parquet"
            ]
        ),
    }


class LakehouseQualityTests(unittest.TestCase):
    def test_duplicate_transaction_batches_are_selected_and_deduplicated(self):
        batches = [
            manifest("payment_processing", "batch-1", "2026-01-01T00:00:00Z"),
            manifest("payment_processing", "batch-2", "2026-01-02T00:00:00Z"),
        ]
        selected = select_manifests("payment_processing", batches)
        self.assertEqual(["batch-1", "batch-2"], [item["batch_id"] for item in selected])

        rows = [
            {"transaction_id": 42, "_batch_id": "batch-1", "_ingested_at": "2026-01-01"},
            {"transaction_id": 42, "_batch_id": "batch-2", "_ingested_at": "2026-01-02"},
        ]
        winners, duplicate_count = deduplicate_records("payment_processing", rows)
        self.assertEqual(1, duplicate_count)
        self.assertEqual("batch-2", winners[0]["_batch_id"])

    def test_latest_complete_customer_snapshot_is_selected(self):
        batches = [
            manifest("customer_management", "old", "2026-01-01T00:00:00Z"),
            manifest("customer_management", "new", "2026-01-02T00:00:00Z"),
        ]
        self.assertEqual(
            ["new"],
            [item["batch_id"] for item in select_manifests("customer_management", batches)],
        )

    def test_invalid_records_are_reported_with_value_free_quality_codes(self):
        errors = record_errors(
            "payment_processing",
            {
                "transaction_id": 3,
                "account_id": 8,
                "customer_id": 5,
                "transaction_type": "TRANSFER",
                "amount": -4,
                "balance_after": 0,
                "event_time": "2026-01-01T00:00:00Z",
                "event_id": "synthetic-event",
            },
            {"core_banking": {9}, "customer_management": {5}},
        )
        self.assertEqual(
            [
                "not_positive:amount",
                "unexpected_value:transaction_type",
                "missing_reference:account_id",
            ],
            errors,
        )

    def test_missing_source_files_fail_validation(self):
        with self.assertRaises(FileNotFoundError):
            select_manifests("customer_management", [])
        incomplete = manifest(
            "customer_management",
            "missing",
            "2026-01-01T00:00:00Z",
            objects=[
                "bronze/customer_management/customers/mode=snapshot/"
                "batch_id=missing/not-present.parquet"
            ],
        )
        with self.assertRaises(MissingBronzeObjectError):
            check_manifest_objects(incomplete, lambda _key: False)

    def test_failure_before_publish_does_not_commit_a_branch(self):
        published = []

        def interrupted():
            raise RuntimeError("simulated failure")

        with self.assertRaisesRegex(RuntimeError, "simulated failure"):
            run_before_publish(interrupted, lambda: published.append(True))
        self.assertEqual([], published)

    def test_repeated_publish_of_same_transaction_is_idempotent_by_key(self):
        original = {
            "transaction_id": 1001,
            "_batch_id": "first-run",
            "_ingested_at": "2026-01-01T00:00:00Z",
        }
        replay = {
            "transaction_id": 1001,
            "_batch_id": "second-run",
            "_ingested_at": "2026-01-02T00:00:00Z",
        }
        winners, duplicates = deduplicate_records(
            "payment_processing", [original, replay, replay]
        )
        self.assertEqual(2, duplicates)
        self.assertEqual(1, len(winners))
        self.assertEqual("second-run", winners[0]["_batch_id"])

    def test_nessie_branch_requests_use_the_v2_reference_and_merge_contract(self):
        client = NessieClient("http://nessie:19120/api/v2")
        client.request = Mock(
            side_effect=[{}, {"reference": {"name": "pipeline-run-1", "hash": "source-hash"}}, {}]
        )

        client.create_branch("pipeline-run-1", "base-hash")
        self.assertEqual(
            (
                "POST",
                "/trees?name=pipeline-run-1&type=BRANCH",
                {"type": "BRANCH", "name": "main", "hash": "base-hash"},
            ),
            client.request.call_args.args,
        )

        client.publish_branch("pipeline-run-1", "base-hash")
        self.assertEqual(
            [
                call("GET", "/trees/pipeline-run-1"),
                call(
                    "POST",
                    "/trees/main@base-hash/history/merge",
                    {
                        "fromRefName": "pipeline-run-1",
                        "fromHash": "source-hash",
                        "dryRun": False,
                        "fetchAdditionalInfo": False,
                    },
                ),
            ],
            client.request.call_args_list[1:],
        )

    def test_nessie_reference_response_and_delete_use_current_hash(self):
        client = NessieClient("http://nessie:19120/api/v2")
        client.request = Mock(
            return_value={"reference": {"name": "pipeline-run-2", "hash": "head"}}
        )

        self.assertEqual("head", client.reference("pipeline-run-2")["hash"])
        client.delete_branch("pipeline-run-2")
        self.assertEqual(
            ("DELETE", "/trees/pipeline-run-2@head?type=BRANCH"),
            client.request.call_args.args[:2],
        )


if __name__ == "__main__":
    unittest.main()
