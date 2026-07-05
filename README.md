# object-tracker-pipeline

ELT layer for the [object-tracker](https://github.com/AlaricManning/object-tracker) edge capture system.

Edge devices upload clips (`.ts` video + binary `.klv` metadata) to `s3://object-tracker-am/raw/`. This pipeline unpacks the KLV detection metadata into a partitioned Parquet catalog under `s3://object-tracker-am/catalog/`, queryable with DuckDB or Athena — e.g. "show all near-miss clips from last week."

## Architecture

```
s3://object-tracker-am/raw/                  s3://object-tracker-am/catalog/
  └── {session_id}/                            ├── detections/date=YYYY-MM-DD/*.parquet
      ├── {tier}/clip_N.ts                     └── _manifest/   (processed-clip ledger)
      ├── {tier}/clip_N.klv    ── sync ──►
      └── session_summary.json     (tier = hits | near_misses)
```

Only the tiny `.klv` metadata files are ever downloaded; the videos stay put
and are addressed by the catalog's `session_id`/`clip_id`/`frame_id` columns.

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

All commands accept `OBJECT_TRACKER_BUCKET` as the fallback for their
`--bucket`/`--catalog`/`--location` flags — export it once and drop the flags:

```bash
export OBJECT_TRACKER_BUCKET=object-tracker-am
```

```bash
# See what a sync would do, without changing anything
AWS_PROFILE=object-tracker-pipeline python -m object_tracker_pipeline.sync --dry-run

# Run it for real
AWS_PROFILE=object-tracker-pipeline python -m object_tracker_pipeline.sync
```

Idempotent and crash-safe: Parquet is uploaded before a clip is marked
processed, and re-processing overwrites deterministic file names. The
`object-tracker-pipeline` profile setup — its IAM policy is read-only on
`raw/*`, read/write on `catalog/*` and Athena querying, no delete anywhere —
is documented in [docs/aws-iam-setup.md](docs/aws-iam-setup.md); credentials
are never stored in this repo.

## Querying

```bash
# All near-miss clips from the last 7 days
AWS_PROFILE=object-tracker-pipeline python -m object_tracker_pipeline.query \
    --tier near_misses --days 7
```

Each result line ends with the clip's `.ts` key, so anything the query finds
is one `aws s3 cp` away from watchable. Filters compose: `--tier`, `--class
person`, `--days N`. From Python, `query.connect()` + `query.clips()` return
Arrow tables; the `detections` view is plain SQL for anything custom:

```python
from object_tracker_pipeline import query

con = query.connect("s3://object-tracker-am/catalog")
con.execute("SELECT class_name, count(*) FROM detections GROUP BY 1").fetchall()
```

The same catalog is queryable from the AWS console via Athena (table DDL is
generated from the schema by `python -m object_tracker_pipeline.ddl`; one-time
setup in [docs/athena-setup.md](docs/athena-setup.md)).

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
- Scheduled sync (cron/systemd, or GitHub Actions with OIDC) with backfill support
- Manifest compaction (many small `_manifest/` objects → one) if ledger listing ever gets slow
- Catalog hygiene: if `raw/` ever gets retention-based deletion, remove catalog rows whose `.ts` no longer exists
- S3 lifecycle rule expiring `athena-results/*` after ~30 days (S3 does the deleting; no IAM delete permission needed)
