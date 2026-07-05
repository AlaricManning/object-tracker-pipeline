"""Sync loop: discover new .klv clips in raw/, transform, upload to catalog/.

Per-clip ordering is the crash-safety contract:

    download .klv → transform → upload Parquet → mark processed

The manifest is written last, so a crash mid-clip means the next run simply
re-processes that clip — and the transform layer's deterministic Parquet
names make the re-run overwrite its own previous output, never duplicate it.

Run as:  python -m object_tracker_pipeline.sync --bucket <bucket> [--dry-run]
Credentials come from the environment (AWS_PROFILE); never from code/config.
"""

import argparse
import logging
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

import boto3

from object_tracker_pipeline import transform
from object_tracker_pipeline.keys import ClipRef, parse_clip_key
from object_tracker_pipeline.state import ManifestStateStore, StateStore

logger = logging.getLogger(__name__)

DETECTIONS_SUBDIR = "detections"


@dataclass
class SyncResult:
    processed: list[str] = field(default_factory=list)  # raw keys handled this run
    already_done: int = 0
    uploaded_files: list[str] = field(default_factory=list)  # catalog keys written


def list_clips(s3, bucket: str, raw_prefix: str = "raw/") -> list[ClipRef]:
    """Every .klv clip currently under the raw prefix (videos etc. filtered out)."""
    refs = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=raw_prefix):
        for obj in page.get("Contents", []):
            ref = parse_clip_key(obj["Key"], prefix=raw_prefix)
            if ref is not None:
                refs.append(ref)
    return refs


def process_clip(
    s3, bucket: str, ref: ClipRef, catalog_prefix: str, workdir: Path
) -> list[str]:
    """Download, transform, and upload one clip. Returns uploaded catalog keys.

    A clip whose KLV holds no packets uploads nothing but still counts as
    processed — the caller marks it so it isn't re-fetched forever.
    """
    data = s3.get_object(Bucket=bucket, Key=ref.key)["Body"].read()
    table = transform.klv_to_table(data, tier=ref.tier)

    uploaded = []
    for path in transform.write_clip_parquet(table, workdir / DETECTIONS_SUBDIR):
        key = catalog_prefix + str(path.relative_to(workdir))
        s3.upload_file(str(path), bucket, key)
        uploaded.append(key)
    return uploaded


def run_sync(
    s3,
    bucket: str,
    *,
    raw_prefix: str = "raw/",
    catalog_prefix: str = "catalog/",
    state: StateStore | None = None,
    dry_run: bool = False,
) -> SyncResult:
    """One incremental sync pass. Idempotent; safe to re-run any time."""
    if state is None:
        state = ManifestStateStore(s3, bucket, prefix=catalog_prefix + "_manifest/")

    result = SyncResult()
    done = state.processed_keys()
    for ref in list_clips(s3, bucket, raw_prefix):
        if ref.key in done:
            result.already_done += 1
            continue
        if dry_run:
            result.processed.append(ref.key)
            continue
        with tempfile.TemporaryDirectory() as tmp:
            uploaded = process_clip(s3, bucket, ref, catalog_prefix, Path(tmp))
        state.mark_processed([ref.key])
        result.processed.append(ref.key)
        result.uploaded_files.extend(uploaded)
        logger.info("processed %s -> %d file(s)", ref.key, len(uploaded))
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Sync raw KLV clips into the Parquet catalog."
    )
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--raw-prefix", default="raw/")
    parser.add_argument("--catalog-prefix", default="catalog/")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="list clips that would be processed; change nothing",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    s3 = boto3.client("s3")
    result = run_sync(
        s3,
        args.bucket,
        raw_prefix=args.raw_prefix,
        catalog_prefix=args.catalog_prefix,
        dry_run=args.dry_run,
    )
    verb = "would process" if args.dry_run else "processed"
    logger.info(
        "%s %d clip(s), %d already done",
        verb,
        len(result.processed),
        result.already_done,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
