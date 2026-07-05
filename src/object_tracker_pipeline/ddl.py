"""Athena/Glue table DDL, generated from the catalog schema.

Generated rather than hand-written so the Athena table cannot drift from
`transform.SCHEMA` — a schema change here fails a test until the type
mapping (and the deployed table) are updated deliberately.

The table uses partition projection: Athena derives the `date` partitions
from the key layout instead of a partition registry, so new dates are
queryable the moment the sync writes them — no `MSCK REPAIR TABLE`, ever.

Run as:  python -m object_tracker_pipeline.ddl   (prints the DDL to paste
into the Athena query editor; see docs/athena-setup.md)
"""

import argparse

import pyarrow as pa

from object_tracker_pipeline.transform import SCHEMA

PARTITION_COLUMN = "date"

_ATHENA_TYPES = {
    "string": "string",
    "int32": "int",
    "int64": "bigint",
    "float": "float",
    "timestamp[us, tz=UTC]": "timestamp",
    "date32[day]": "date",
}


def athena_type(field: pa.Field) -> str:
    try:
        return _ATHENA_TYPES[str(field.type)]
    except KeyError:
        raise ValueError(
            f"no Athena type mapping for column {field.name!r} ({field.type})"
        ) from None


def generate_ddl(
    location: str = "s3://object-tracker-am/catalog/detections/",
    *,
    database: str = "object_tracker",
    table: str = "detections",
    projection_start: str = "2026-07-01",
) -> str:
    """The CREATE EXTERNAL TABLE statement for the detections catalog."""
    location = location.rstrip("/") + "/"
    columns = ",\n".join(
        f"    `{field.name}` {athena_type(field)}"
        for field in SCHEMA
        if field.name != PARTITION_COLUMN
    )
    partition = SCHEMA.field(PARTITION_COLUMN)
    p = PARTITION_COLUMN
    return f"""\
CREATE EXTERNAL TABLE IF NOT EXISTS `{database}`.`{table}` (
{columns}
)
PARTITIONED BY (`{p}` {athena_type(partition)})
STORED AS PARQUET
LOCATION '{location}'
TBLPROPERTIES (
    'projection.enabled' = 'true',
    'projection.{p}.type' = 'date',
    'projection.{p}.format' = 'yyyy-MM-dd',
    'projection.{p}.range' = '{projection_start},NOW',
    'projection.{p}.interval' = '1',
    'projection.{p}.interval.unit' = 'DAYS',
    'storage.location.template' = '{location}{p}=${{{p}}}'
);"""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Print the Athena DDL for the detections table."
    )
    parser.add_argument(
        "--location", default="s3://object-tracker-am/catalog/detections/"
    )
    parser.add_argument("--database", default="object_tracker")
    parser.add_argument("--table", default="detections")
    parser.add_argument(
        "--projection-start",
        default="2026-07-01",
        help="earliest date partition Athena should project",
    )
    args = parser.parse_args(argv)
    print(
        generate_ddl(
            args.location,
            database=args.database,
            table=args.table,
            projection_start=args.projection_start,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
