"""Small Nessie v2 REST client for atomic run-branch creation and publication."""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


class NessieApiError(RuntimeError):
    def __init__(self, status: int, method: str, path: str):
        super().__init__(f"Nessie API returned HTTP {status} for {method} {path}.")
        self.status = status


class NessieClient:
    def __init__(self, uri: str):
        self.base_uri = uri.rstrip("/")

    def request(
        self, method: str, path: str, body: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        data = json.dumps(body).encode("utf-8") if body is not None else None
        request = urllib.request.Request(
            f"{self.base_uri}{path}",
            data=data,
            headers={"Content-Type": "application/json"},
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                payload = response.read()
        except urllib.error.HTTPError as exc:
            raise NessieApiError(exc.code, method, path) from None
        if not payload:
            return {}
        return json.loads(payload)

    def reference(self, name: str) -> dict[str, Any]:
        path = f"/trees/{urllib.parse.quote(name, safe='')}"
        return self.request("GET", path)["reference"]

    def create_branch(self, name: str, source_hash: str) -> None:
        path = "/trees?" + urllib.parse.urlencode(
            {"name": name, "type": "BRANCH"}
        )
        self.request(
            "POST",
            path,
            {"type": "BRANCH", "name": "main", "hash": source_hash},
        )

    def publish_branch(self, name: str, expected_main_hash: str) -> None:
        reference = urllib.parse.quote(f"main@{expected_main_hash}", safe="@")
        source_hash = self.reference(name)["hash"]
        self.request(
            "POST",
            f"/trees/{reference}/history/merge",
            {
                "fromRefName": name,
                "fromHash": source_hash,
                "dryRun": False,
                "fetchAdditionalInfo": False,
            },
        )

    def delete_branch(self, name: str) -> None:
        branch_hash = self.reference(name)["hash"]
        reference = urllib.parse.quote(f"{name}@{branch_hash}", safe="@")
        self.request("DELETE", f"/trees/{reference}?type=BRANCH")
