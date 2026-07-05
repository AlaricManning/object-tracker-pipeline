"""Processed-clip state tracking.

`StateStore` is the interface the sync loop sees; `ManifestStateStore` is the
S3-backed implementation: an append-only ledger of JSONL objects under
`catalog/_manifest/`. The leading underscore keeps query engines (DuckDB,
Athena) from reading the ledger as table data.

Write path: each `mark_processed()` call puts one new manifest object — no
read-modify-write, so concurrent or crashed runs can never corrupt the ledger.
Read path: union of all manifest objects. Compacting many small manifest
objects into one is a future enhancement if listing ever gets slow; a
DynamoDB-backed store (per-clip retry/error state) would slot in behind the
same interface.
"""

import json
import uuid
from abc import ABC, abstractmethod
from collections.abc import Iterable
from datetime import UTC, datetime


class StateStore(ABC):
    """What the sync loop needs to know: which raw keys are already done."""

    @abstractmethod
    def processed_keys(self) -> set[str]:
        """Return every raw S3 key that has been fully processed."""

    @abstractmethod
    def mark_processed(self, keys: Iterable[str]) -> None:
        """Durably record that these raw keys are fully processed."""


class ManifestStateStore(StateStore):
    """Ledger of processed keys as JSONL objects under an S3 prefix."""

    def __init__(self, s3_client, bucket: str, prefix: str = "catalog/_manifest/"):
        self._s3 = s3_client
        self._bucket = bucket
        self._prefix = prefix if prefix.endswith("/") else prefix + "/"

    def processed_keys(self) -> set[str]:
        keys: set[str] = set()
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=self._prefix):
            for obj in page.get("Contents", []):
                body = self._s3.get_object(Bucket=self._bucket, Key=obj["Key"])
                for line in body["Body"].read().decode().splitlines():
                    if line.strip():
                        keys.add(json.loads(line)["key"])
        return keys

    def mark_processed(self, keys: Iterable[str]) -> None:
        keys = list(keys)
        if not keys:
            return
        now = datetime.now(UTC)
        lines = "".join(
            json.dumps({"key": key, "processed_at": now.isoformat()}) + "\n"
            for key in keys
        )
        # Timestamp for human-readable ordering, uuid suffix for uniqueness.
        name = f"{now.strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}.jsonl"
        self._s3.put_object(
            Bucket=self._bucket,
            Key=self._prefix + name,
            Body=lines.encode(),
        )
