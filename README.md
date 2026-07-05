# object-tracker-pipeline

ELT layer for the [object-tracker](https://github.com/amanning1080/object-tracker) edge capture system.

Edge devices upload clips (`.ts` video + binary `.klv` metadata) to `s3://object-tracker-am/raw/`. This pipeline unpacks the KLV detection metadata into a partitioned Parquet catalog under `s3://object-tracker-am/catalog/`, queryable with DuckDB or Athena — e.g. "show all near-miss clips from last week."

## Architecture

```
s3://object-tracker-am/raw/          s3://object-tracker-am/catalog/
  ├── <clip>.ts                        ├── detections/date=YYYY-MM-DD/*.parquet
  └── <clip>.klv        ── sync ──►    └── _manifest/   (processed-clip ledger)
```

- **Extract** — list `raw/` for clips not yet in the manifest, download `.klv` files
- **Transform** — parse KLV packets (vendored parser; the KLV contract is owned by the edge repo) into Arrow tables
- **Load** — write date-partitioned Parquet to `catalog/`, update the manifest
- **Query** — DuckDB directly over S3, or Athena via Glue tables

## Catalog schema

One row per detection per frame, hive-partitioned by capture date
(`detections/date=YYYY-MM-DD/`), one deterministically named file per clip so
re-processing a clip overwrites rather than duplicates.

| Column | Type | Notes |
|---|---|---|
| `session_id`, `clip_id` | string | identify the clip; map back to `raw/{session_id}/{tier}/{clip_id}.ts` |
| `tier` | string | `hits` / `near_misses` — from the S3 key path, not the KLV |
| `frame_id` | int32 | 0-based frame index within the clip's `.ts` (pre-roll included) |
| `timestamp` | timestamp (microseconds, UTC) | stamped at frame capture; naive values treated as UTC |
| `track_id` | int64 | Norfair track id |
| `class_name` | string | COCO class |
| `confidence`, `x1`, `y1`, `x2`, `y2` | float32 | matches the float32 wire format |
| `date` | date32 | partition key, derived per row from `timestamp` |

## Running

```bash
# See what a sync would do, without changing anything
AWS_PROFILE=<pipeline-profile> python -m object_tracker_pipeline.sync \
    --bucket object-tracker-am --dry-run

# Run it for real
AWS_PROFILE=<pipeline-profile> python -m object_tracker_pipeline.sync \
    --bucket object-tracker-am
```

Idempotent and crash-safe: Parquet is uploaded before a clip is marked
processed, and re-processing overwrites deterministic file names. The
pipeline's IAM identity (read `raw/*`, write `catalog/*`) is documented in a
later PR; credentials are never stored in this repo.

## Development

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
ruff check . && ruff format --check .
pytest
```

## Future enhancements

- Extract the vendored KLV parser into a shared package (git-tag pin or AWS CodeArtifact) consumed by both this repo and the edge repo
- DynamoDB-backed `StateStore` for per-clip retry/error tracking
- Scheduled sync (cron/systemd) with backfill support
