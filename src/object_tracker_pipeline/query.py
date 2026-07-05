"""Query layer: DuckDB over the Parquet catalog.

Works identically against a local directory or the real S3 catalog:

    con = connect("s3://object-tracker-am/catalog")
    near_miss_clips(con, days=7)

For S3, DuckDB's httpfs extension resolves credentials through the standard
AWS chain — run with AWS_PROFILE=object-tracker-pipeline, same as the sync.

Run as:  python -m object_tracker_pipeline.query --catalog <path-or-s3-url> \
             [--tier near_misses] [--class person] [--days 7]
"""

import argparse
from datetime import UTC, datetime, timedelta

import duckdb
import pyarrow as pa

_CLIPS_SQL = """
    SELECT
        session_id,
        clip_id,
        tier,
        min(timestamp)                      AS started_at,
        max(timestamp)                      AS ended_at,
        count(*)                            AS detections,
        max(confidence)                     AS peak_confidence,
        list_sort(list(DISTINCT class_name)) AS classes,
        'raw/' || session_id || '/' || tier || '/' || clip_id || '.ts' AS ts_key
    FROM detections
    WHERE {where}
    GROUP BY session_id, clip_id, tier
    ORDER BY started_at DESC
"""


def connect(catalog: str) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection with a `detections` view over the catalog.

    `catalog` is the catalog root — a local path or `s3://bucket/catalog`.
    """
    con = duckdb.connect()
    if catalog.startswith("s3://"):
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("CREATE SECRET (TYPE s3, PROVIDER credential_chain)")

    glob = f"{catalog.rstrip('/')}/detections/*/*.parquet"
    quoted = glob.replace("'", "''")  # parameters aren't allowed in CREATE VIEW
    try:
        con.execute(
            "CREATE VIEW detections AS "
            f"SELECT * FROM read_parquet('{quoted}', hive_partitioning = true)"
        )
    except duckdb.IOException as exc:
        raise ValueError(f"no readable parquet files under {glob!r}") from exc
    return con


def clips(
    con: duckdb.DuckDBPyConnection,
    *,
    tier: str | None = None,
    class_name: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
) -> pa.Table:
    """One row per clip matching the filters, newest first.

    Each row aggregates the clip's detections (count, peak confidence, class
    list, start/end timestamps) and carries `ts_key` — the S3 key of the
    paired video, so results always point back to something watchable.
    """
    conditions, params = ["true"], []
    if tier is not None:
        conditions.append("tier = ?")
        params.append(tier)
    if class_name is not None:
        conditions.append("class_name = ?")
        params.append(class_name)
    if since is not None:
        conditions.append("timestamp >= ?")
        params.append(since)
    if until is not None:
        conditions.append("timestamp < ?")
        params.append(until)

    sql = _CLIPS_SQL.format(where=" AND ".join(conditions))
    return con.execute(sql, params).to_arrow_table()


def near_miss_clips(con: duckdb.DuckDBPyConnection, days: int = 7) -> pa.Table:
    """The motivating query: all near-miss clips from the last `days` days."""
    since = datetime.now(UTC) - timedelta(days=days)
    return clips(con, tier="near_misses", since=since)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Query the detection catalog.")
    parser.add_argument(
        "--catalog",
        required=True,
        help="catalog root: local path or s3://bucket/catalog",
    )
    parser.add_argument("--tier", choices=["hits", "near_misses"])
    parser.add_argument("--class", dest="class_name", help="filter by COCO class name")
    parser.add_argument("--days", type=int, help="only clips from the last N days")
    args = parser.parse_args(argv)

    since = None
    if args.days is not None:
        since = datetime.now(UTC) - timedelta(days=args.days)

    con = connect(args.catalog)
    table = clips(con, tier=args.tier, class_name=args.class_name, since=since)

    for row in table.to_pylist():
        classes = ",".join(row["classes"])
        print(
            f"{row['started_at']:%Y-%m-%d %H:%M:%S}  "
            f"{row['tier']:<11}  "
            f"peak={row['peak_confidence']:.2f}  "
            f"n={row['detections']:<4d}  "
            f"[{classes}]  "
            f"{row['ts_key']}"
        )
    print(f"-- {table.num_rows} clip(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
