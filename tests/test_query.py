"""Tests for the DuckDB query layer, against a locally built catalog."""

from datetime import UTC, datetime, timedelta

import pytest

from object_tracker_pipeline import klv, query, transform


def days_ago(n: float) -> str:
    return (datetime.now(UTC) - timedelta(days=n)).isoformat()


def add_clip(
    root,
    clip_id: str,
    tier: str = "hits",
    *,
    age_days: float = 1,
    class_names: tuple[str, ...] = ("person",),
    confidences: tuple[float, ...] = (0.9,),
    session_id: str = "2026-07-04T11-48-14",
):
    """Write one clip into the catalog: one detection per (class, confidence)."""
    data = b"".join(
        klv.encode_packet(
            frame_id=i,
            timestamp=days_ago(age_days),
            session_id=session_id,
            clip_id=clip_id,
            track_id=i,
            class_name=class_names[i % len(class_names)],
            confidence=confidences[i % len(confidences)],
            bbox=[1.0, 2.0, 3.0, 4.0],
        )
        for i in range(max(len(class_names), len(confidences)))
    )
    table = transform.klv_to_table(data, tier=tier)
    transform.write_clip_parquet(table, root / "detections")


@pytest.fixture
def catalog(tmp_path):
    add_clip(
        tmp_path,
        "clip_0001",
        tier="hits",
        age_days=1,
        class_names=("person", "bicycle"),
        confidences=(0.95, 0.60),
    )
    add_clip(tmp_path, "clip_0002", tier="near_misses", age_days=2, confidences=(0.55,))
    add_clip(
        tmp_path, "clip_0003", tier="near_misses", age_days=40, confidences=(0.58,)
    )
    return tmp_path


def test_clips_aggregates_one_row_per_clip(catalog):
    table = query.clips(query.connect(str(catalog)))

    assert table.num_rows == 3
    rows = {r["clip_id"]: r for r in table.to_pylist()}
    hit = rows["clip_0001"]
    assert hit["detections"] == 2
    assert hit["peak_confidence"] == pytest.approx(0.95, abs=1e-4)
    assert hit["classes"] == ["bicycle", "person"]
    assert hit["ts_key"] == "raw/2026-07-04T11-48-14/hits/clip_0001.ts"
    assert hit["started_at"] <= hit["ended_at"]


def test_results_ordered_newest_first(catalog):
    table = query.clips(query.connect(str(catalog)))
    starts = [r["started_at"] for r in table.to_pylist()]
    assert starts == sorted(starts, reverse=True)


def test_tier_filter(catalog):
    table = query.clips(query.connect(str(catalog)), tier="near_misses")
    assert sorted(table["clip_id"].to_pylist()) == ["clip_0002", "clip_0003"]


def test_class_filter(catalog):
    table = query.clips(query.connect(str(catalog)), class_name="bicycle")
    assert table["clip_id"].to_pylist() == ["clip_0001"]


def test_since_and_until_filters(catalog):
    con = query.connect(str(catalog))
    now = datetime.now(UTC)

    recent = query.clips(con, since=now - timedelta(days=7))
    assert sorted(recent["clip_id"].to_pylist()) == ["clip_0001", "clip_0002"]

    old = query.clips(con, until=now - timedelta(days=7))
    assert old["clip_id"].to_pylist() == ["clip_0003"]


def test_near_miss_clips_last_week(catalog):
    con = query.connect(str(catalog))

    table = query.near_miss_clips(con, days=7)

    # clip_0002 is a 2-day-old near miss; clip_0003 is 40 days old and
    # clip_0001 is a hit — both excluded.
    assert table["clip_id"].to_pylist() == ["clip_0002"]


def test_connect_on_empty_catalog_raises(tmp_path):
    with pytest.raises(ValueError, match="no readable parquet files"):
        query.connect(str(tmp_path))


def test_cli_prints_clips(catalog, capsys):
    assert query.main(["--catalog", str(catalog), "--tier", "near_misses"]) == 0
    out = capsys.readouterr().out
    assert "near_misses" in out
    assert "raw/2026-07-04T11-48-14/near_misses/clip_0002.ts" in out
    assert "-- 2 clip(s)" in out
