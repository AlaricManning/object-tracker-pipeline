"""KLV detection packets → Arrow table → date-partitioned Parquet.

Catalog layout (hive-partitioned by capture date):

    <root>/date=YYYY-MM-DD/<session_id>_<clip_id>-<i>.parquet

File names are deterministic per clip, so re-transforming the same clip
overwrites its previous output instead of duplicating rows — the sync layer
relies on this for crash-safe, idempotent re-runs.
"""

from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds

from object_tracker_pipeline import klv

SCHEMA = pa.schema(
    [
        pa.field("session_id", pa.string()),
        pa.field("clip_id", pa.string()),
        pa.field("tier", pa.string()),
        pa.field("frame_id", pa.int32()),
        pa.field("timestamp", pa.timestamp("us", tz="UTC")),
        pa.field("track_id", pa.int64()),
        pa.field("class_name", pa.string()),
        pa.field("confidence", pa.float32()),
        pa.field("x1", pa.float32()),
        pa.field("y1", pa.float32()),
        pa.field("x2", pa.float32()),
        pa.field("y2", pa.float32()),
        pa.field("date", pa.date32()),
    ]
)

_PARTITIONING = ds.partitioning(
    pa.schema([pa.field("date", pa.date32())]), flavor="hive"
)


def _parse_timestamp(value: str) -> datetime:
    # Edge devices stamp ISO-8601 at frame capture; treat naive values as UTC.
    ts = datetime.fromisoformat(value)
    if ts.tzinfo is None:
        return ts.replace(tzinfo=UTC)
    return ts.astimezone(UTC)


def klv_to_table(data: bytes, tier: str) -> pa.Table:
    """Decode one .klv file's bytes into an Arrow table matching SCHEMA.

    `tier` is "hits" or "near_misses" — it lives in the clip's S3 key path,
    not in the KLV packets, so the caller must supply it.
    """
    rows = []
    for packet in klv.iter_packets(data):
        ts = _parse_timestamp(packet["timestamp"])
        rows.append(
            {
                "session_id": packet["session_id"],
                "clip_id": packet["clip_id"],
                "tier": tier,
                "frame_id": packet["frame_id"],
                "timestamp": ts,
                "track_id": packet["track_id"],
                "class_name": packet["class_name"],
                "confidence": packet["confidence"],
                "x1": packet["x1"],
                "y1": packet["y1"],
                "x2": packet["x2"],
                "y2": packet["y2"],
                "date": ts.date(),
            }
        )
    return pa.Table.from_pylist(rows, schema=SCHEMA)


def write_clip_parquet(table: pa.Table, root: str | Path) -> list[Path]:
    """Write one clip's table under `root`, partitioned by date.

    Returns the paths actually written (reported by pyarrow's file_visitor).
    Writing the same clip again overwrites the same paths (deterministic
    basename), leaving other clips untouched.
    """
    if table.num_rows == 0:
        return []

    session_id = table["session_id"][0].as_py()
    clip_id = table["clip_id"][0].as_py()
    basename = f"{session_id}_{clip_id}-{{i}}.parquet"

    written: list[Path] = []
    ds.write_dataset(
        table,
        root,
        format="parquet",
        partitioning=_PARTITIONING,
        basename_template=basename,
        existing_data_behavior="overwrite_or_ignore",
        file_visitor=lambda f: written.append(Path(f.path)),
    )
    return sorted(written)
