"""End-to-end sync loop tests against moto's fake S3."""

import pyarrow.dataset as ds
import pytest
from conftest import BUCKET

from object_tracker_pipeline import klv, sync


def make_klv(clip_id: str, frames: int = 2, ts: str = "2026-07-03T14:22:07+00:00"):
    return b"".join(
        klv.encode_packet(
            frame_id=i,
            timestamp=ts,
            session_id="session_test",
            clip_id=clip_id,
            track_id=1,
            class_name="person",
            confidence=0.9,
            bbox=[1.0, 2.0, 3.0, 4.0],
        )
        for i in range(frames)
    )


def put_clip(s3, clip_id: str, tier: str = "hits", data: bytes | None = None) -> str:
    key = f"raw/session_test/{tier}/{clip_id}.klv"
    body = make_klv(clip_id) if data is None else data
    s3.put_object(Bucket=BUCKET, Key=key, Body=body)
    return key


def put_noise(s3):
    """Objects the sync must ignore: video + session summary."""
    s3.put_object(Bucket=BUCKET, Key="raw/session_test/hits/clip_0001.ts", Body=b"tv")
    s3.put_object(
        Bucket=BUCKET, Key="raw/session_test/session_summary.json", Body=b"{}"
    )


def catalog_keys(s3) -> list[str]:
    listing = s3.list_objects_v2(Bucket=BUCKET, Prefix="catalog/detections/")
    return sorted(obj["Key"] for obj in listing.get("Contents", []))


def read_catalog(s3, tmp_path):
    """Download the catalog's parquet area and open it as one dataset."""
    for key in catalog_keys(s3):
        local = tmp_path / key
        local.parent.mkdir(parents=True, exist_ok=True)
        s3.download_file(BUCKET, key, str(local))
    return ds.dataset(tmp_path / "catalog/detections", partitioning="hive").to_table()


def test_first_run_processes_all_clips(s3, tmp_path):
    k1 = put_clip(s3, "clip_0001", tier="hits")
    k2 = put_clip(s3, "clip_0002", tier="near_misses")
    put_noise(s3)

    result = sync.run_sync(s3, BUCKET)

    assert sorted(result.processed) == sorted([k1, k2])
    assert result.already_done == 0
    assert catalog_keys(s3) == [
        "catalog/detections/date=2026-07-03/session_test_clip_0001-0.parquet",
        "catalog/detections/date=2026-07-03/session_test_clip_0002-0.parquet",
    ]
    table = read_catalog(s3, tmp_path)
    assert table.num_rows == 4  # 2 clips x 2 frames
    assert sorted(set(table["tier"].to_pylist())) == ["hits", "near_misses"]


def test_second_run_is_a_noop(s3):
    put_clip(s3, "clip_0001")
    sync.run_sync(s3, BUCKET)

    result = sync.run_sync(s3, BUCKET)

    assert result.processed == []
    assert result.already_done == 1
    assert len(catalog_keys(s3)) == 1


def test_new_clip_picked_up_incrementally(s3):
    put_clip(s3, "clip_0001")
    sync.run_sync(s3, BUCKET)

    new_key = put_clip(s3, "clip_0002")
    result = sync.run_sync(s3, BUCKET)

    assert result.processed == [new_key]
    assert result.already_done == 1
    assert len(catalog_keys(s3)) == 2


def test_crash_before_mark_reprocesses_without_duplicates(s3, tmp_path):
    key = put_clip(s3, "clip_0001")
    # Simulate a run that died after uploading Parquet but before marking:
    # catalog output exists, manifest says nothing.
    sync.run_sync(s3, BUCKET, catalog_prefix="catalog/")
    s3_objects = s3.list_objects_v2(Bucket=BUCKET, Prefix="catalog/_manifest/")
    for obj in s3_objects["Contents"]:
        s3.delete_object(Bucket=BUCKET, Key=obj["Key"])

    result = sync.run_sync(s3, BUCKET)

    assert result.processed == [key]  # picked up again
    assert len(catalog_keys(s3)) == 1  # overwritten, not duplicated
    assert read_catalog(s3, tmp_path).num_rows == 2


def test_dry_run_changes_nothing(s3):
    key = put_clip(s3, "clip_0001")

    result = sync.run_sync(s3, BUCKET, dry_run=True)

    assert result.processed == [key]
    assert catalog_keys(s3) == []
    manifest = s3.list_objects_v2(Bucket=BUCKET, Prefix="catalog/_manifest/")
    assert manifest["KeyCount"] == 0


def test_empty_bucket_is_a_noop(s3):
    result = sync.run_sync(s3, BUCKET)
    assert result.processed == []
    assert result.already_done == 0


def test_empty_klv_marked_processed_without_output(s3):
    key = put_clip(s3, "clip_0001", data=b"")

    result = sync.run_sync(s3, BUCKET)

    assert result.processed == [key]
    assert catalog_keys(s3) == []
    # ...and it is not re-fetched next run.
    assert sync.run_sync(s3, BUCKET).already_done == 1


def test_custom_prefixes(s3):
    key = "staging/session_test/hits/clip_0001.klv"
    s3.put_object(Bucket=BUCKET, Key=key, Body=make_klv("clip_0001"))

    result = sync.run_sync(s3, BUCKET, raw_prefix="staging/", catalog_prefix="out/")

    assert result.processed == [key]
    listing = s3.list_objects_v2(Bucket=BUCKET, Prefix="out/detections/")
    assert listing["KeyCount"] == 1


def test_cli_dry_run(s3):
    put_clip(s3, "clip_0001")
    assert sync.main(["--bucket", BUCKET, "--dry-run"]) == 0
    assert catalog_keys(s3) == []


def test_cli_bucket_falls_back_to_env(s3, monkeypatch):
    monkeypatch.setenv("OBJECT_TRACKER_BUCKET", BUCKET)
    put_clip(s3, "clip_0001")
    assert sync.main(["--dry-run"]) == 0
    assert catalog_keys(s3) == []


def test_cli_errors_without_bucket_or_env(monkeypatch, capsys):
    monkeypatch.delenv("OBJECT_TRACKER_BUCKET", raising=False)
    with pytest.raises(SystemExit):
        sync.main([])
    assert "OBJECT_TRACKER_BUCKET" in capsys.readouterr().err
