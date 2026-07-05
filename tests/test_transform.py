"""Tests for the KLV → Parquet transform layer."""

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow.dataset as ds
import pytest

from object_tracker_pipeline import klv, transform

FIXTURES = Path(__file__).parent / "fixtures"


def load_fixture(name: str):
    data = (FIXTURES / f"{name}.klv").read_bytes()
    expected = json.loads((FIXTURES / f"{name}.expected.json").read_text())
    return data, expected


def make_packet(frame_id: int, timestamp: str, clip_id: str = "clip_0001") -> bytes:
    return klv.encode_packet(
        frame_id=frame_id,
        timestamp=timestamp,
        session_id="session_test",
        clip_id=clip_id,
        track_id=1,
        class_name="person",
        confidence=0.5,
        bbox=[1.0, 2.0, 3.0, 4.0],
    )


def test_table_matches_golden_fixture():
    data, expected = load_fixture("multi_detection_clip")
    table = transform.klv_to_table(data, tier="near_misses")

    assert table.schema == transform.SCHEMA
    assert table.num_rows == len(expected)
    rows = table.to_pylist()
    for row, exp in zip(rows, expected, strict=True):
        assert row["session_id"] == exp["session_id"]
        assert row["clip_id"] == exp["clip_id"]
        assert row["tier"] == "near_misses"
        assert row["frame_id"] == exp["frame_id"]
        assert row["track_id"] == exp["track_id"]
        assert row["class_name"] == exp["class_name"]
        assert row["confidence"] == pytest.approx(exp["confidence"], abs=1e-4)
        assert row["x1"] == pytest.approx(exp["x1"], abs=1e-4)
        assert row["y2"] == pytest.approx(exp["y2"], abs=1e-4)
        assert row["timestamp"] == datetime.fromisoformat(exp["timestamp"])
        assert row["date"] == datetime.fromisoformat(exp["timestamp"]).date()


def test_naive_timestamp_treated_as_utc():
    data = make_packet(0, "2026-07-03T14:22:07.481332")
    table = transform.klv_to_table(data, tier="hits")
    assert table.to_pylist()[0]["timestamp"] == datetime(
        2026, 7, 3, 14, 22, 7, 481332, tzinfo=UTC
    )


def test_non_utc_timestamp_converted_to_utc():
    data = make_packet(0, "2026-07-03T09:22:07-05:00")
    table = transform.klv_to_table(data, tier="hits")
    row = table.to_pylist()[0]
    assert row["timestamp"] == datetime(2026, 7, 3, 14, 22, 7, tzinfo=UTC)
    assert row["date"] == date(2026, 7, 3)


def test_empty_klv_gives_empty_table_with_schema():
    table = transform.klv_to_table(b"", tier="hits")
    assert table.num_rows == 0
    assert table.schema == transform.SCHEMA


def test_write_creates_hive_partition_layout(tmp_path):
    data, _ = load_fixture("multi_detection_clip")
    table = transform.klv_to_table(data, tier="near_misses")

    written = transform.write_clip_parquet(table, tmp_path)

    assert written == [
        tmp_path / "date=2026-07-03" / "session_20260703_142200_clip_0002-0.parquet"
    ]


def test_written_data_reads_back_identically(tmp_path):
    data, _ = load_fixture("multi_detection_clip")
    table = transform.klv_to_table(data, tier="near_misses")
    transform.write_clip_parquet(table, tmp_path)

    order = [("frame_id", "ascending"), ("track_id", "ascending")]
    read_back = (
        ds.dataset(tmp_path, partitioning="hive")
        .to_table()
        .select(transform.SCHEMA.names)
        .cast(transform.SCHEMA)
        .sort_by(order)
    )
    assert read_back.equals(table.sort_by(order))


def test_rewrite_same_clip_is_idempotent(tmp_path):
    data, expected = load_fixture("multi_detection_clip")
    table = transform.klv_to_table(data, tier="near_misses")

    first = transform.write_clip_parquet(table, tmp_path)
    second = transform.write_clip_parquet(table, tmp_path)

    assert first == second
    total_rows = ds.dataset(tmp_path, partitioning="hive").to_table().num_rows
    assert total_rows == len(expected)


def test_different_clips_do_not_clobber_each_other(tmp_path):
    a = transform.klv_to_table(
        make_packet(0, "2026-07-03T10:00:00+00:00", clip_id="clip_0001"), tier="hits"
    )
    b = transform.klv_to_table(
        make_packet(0, "2026-07-03T11:00:00+00:00", clip_id="clip_0002"), tier="hits"
    )

    transform.write_clip_parquet(a, tmp_path)
    transform.write_clip_parquet(b, tmp_path)

    dataset = ds.dataset(tmp_path, partitioning="hive").to_table()
    assert dataset.num_rows == 2
    assert sorted(dataset["clip_id"].to_pylist()) == ["clip_0001", "clip_0002"]


def test_clip_straddling_midnight_lands_in_two_partitions(tmp_path):
    data = make_packet(0, "2026-07-03T23:59:59+00:00") + make_packet(
        1, "2026-07-04T00:00:01+00:00"
    )
    table = transform.klv_to_table(data, tier="hits")

    written = transform.write_clip_parquet(table, tmp_path)

    partitions = sorted(p.parent.name for p in written)
    assert partitions == ["date=2026-07-03", "date=2026-07-04"]


def test_write_empty_table_writes_nothing(tmp_path):
    table = transform.klv_to_table(b"", tier="hits")
    assert transform.write_clip_parquet(table, tmp_path) == []
    assert list(tmp_path.iterdir()) == []
