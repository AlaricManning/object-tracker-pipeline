# Athena setup

Athena queries the same `catalog/detections/` Parquet as DuckDB, but from the
AWS console (or anything else) — no laptop required. One-time setup, done as
your **admin** user; querying afterwards works for the pipeline user too.

## Why there's no partition registration

The table uses **partition projection**: instead of keeping a registry of
partitions in Glue (and re-registering after every sync with
`MSCK REPAIR TABLE`), the table's properties tell Athena the pattern the
partitions follow (`date=YYYY-MM-DD`, daily, from `projection.date.range`
until now). Athena computes the partition list at query time, so new dates
are queryable the instant the sync writes them.

## One-time setup (admin)

1. **Query result location.** Athena console → Settings → Manage → set the
   query result location to `s3://object-tracker-am/athena-results/`.
   (Athena writes every query's results as files; this is where.)

2. **Create the database.** In the query editor, run:

   ```sql
   CREATE DATABASE IF NOT EXISTS object_tracker;
   ```

3. **Create the table.** Generate the DDL and paste it into the editor:

   ```bash
   python -m object_tracker_pipeline.ddl
   ```

   The DDL is generated from `transform.SCHEMA`, so it always matches what
   the pipeline actually writes. If the schema ever changes, regenerate,
   `DROP TABLE object_tracker.detections`, and re-run (external table —
   dropping it never touches the Parquet data).

4. **Extend the pipeline user's policy.** IAM → Policies →
   `object-tracker-pipeline-policy` → Edit → replace the JSON with the
   current [`infra/iam/pipeline-policy.json`](../infra/iam/pipeline-policy.json)
   (adds Athena querying on the primary workgroup, read-only Glue access to
   this one table, and read/write on `athena-results/*`). Still no delete
   permissions anywhere.

## Example queries

```sql
-- Near-miss clips from the last 7 days
SELECT session_id, clip_id,
       count(*)        AS detections,
       max(confidence) AS peak_confidence,
       min("timestamp") AS started_at
FROM object_tracker.detections
WHERE tier = 'near_misses'
  AND "date" >= date_add('day', -7, current_date)
GROUP BY session_id, clip_id
ORDER BY started_at DESC;

-- What does the camera see most?
SELECT class_name, count(*) AS detections
FROM object_tracker.detections
GROUP BY class_name
ORDER BY detections DESC;
```

`"date"` and `"timestamp"` need quoting in Athena — both are reserved words.
Any query filtering on `"date"` gets partition pruning: only the matching
days' files are scanned (and billed — Athena charges per TB scanned, which at
this catalog's size rounds to zero).
