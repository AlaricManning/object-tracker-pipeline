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
