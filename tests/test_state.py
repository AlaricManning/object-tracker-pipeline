"""Tests for the manifest-backed StateStore, against moto's fake S3."""

import json

from conftest import BUCKET

from object_tracker_pipeline.state import ManifestStateStore


def make_store(s3):
    return ManifestStateStore(s3, BUCKET)


def test_empty_store_has_no_processed_keys(s3):
    assert make_store(s3).processed_keys() == set()


def test_mark_then_read_round_trip(s3):
    store = make_store(s3)
    store.mark_processed(["raw/s1/hits/clip_0001.klv"])
    assert store.processed_keys() == {"raw/s1/hits/clip_0001.klv"}


def test_marks_accumulate_across_calls(s3):
    store = make_store(s3)
    store.mark_processed(["raw/s1/hits/clip_0001.klv"])
    store.mark_processed(
        ["raw/s1/hits/clip_0002.klv", "raw/s1/near_misses/clip_0003.klv"]
    )
    assert store.processed_keys() == {
        "raw/s1/hits/clip_0001.klv",
        "raw/s1/hits/clip_0002.klv",
        "raw/s1/near_misses/clip_0003.klv",
    }


def test_state_persists_across_store_instances(s3):
    make_store(s3).mark_processed(["raw/s1/hits/clip_0001.klv"])
    # A fresh instance (e.g. the next sync run) sees the same ledger.
    assert make_store(s3).processed_keys() == {"raw/s1/hits/clip_0001.klv"}


def test_mark_processed_with_no_keys_writes_nothing(s3):
    store = make_store(s3)
    store.mark_processed([])
    listing = s3.list_objects_v2(Bucket=BUCKET, Prefix="catalog/_manifest/")
    assert listing["KeyCount"] == 0


def test_manifest_objects_live_under_underscore_prefix(s3):
    make_store(s3).mark_processed(["raw/s1/hits/clip_0001.klv"])
    listing = s3.list_objects_v2(Bucket=BUCKET, Prefix="catalog/_manifest/")
    assert listing["KeyCount"] == 1
    assert listing["Contents"][0]["Key"].endswith(".jsonl")


def test_manifest_lines_record_key_and_processed_at(s3):
    make_store(s3).mark_processed(["raw/s1/hits/clip_0001.klv"])
    listing = s3.list_objects_v2(Bucket=BUCKET, Prefix="catalog/_manifest/")
    body = s3.get_object(Bucket=BUCKET, Key=listing["Contents"][0]["Key"])
    record = json.loads(body["Body"].read().decode().strip())
    assert record["key"] == "raw/s1/hits/clip_0001.klv"
    assert "processed_at" in record


def test_duplicate_marks_do_not_duplicate_keys(s3):
    store = make_store(s3)
    store.mark_processed(["raw/s1/hits/clip_0001.klv"])
    store.mark_processed(["raw/s1/hits/clip_0001.klv"])
    assert store.processed_keys() == {"raw/s1/hits/clip_0001.klv"}


def test_custom_prefix_is_respected(s3):
    store = ManifestStateStore(s3, BUCKET, prefix="catalog/_manifest_v2")
    store.mark_processed(["raw/s1/hits/clip_0001.klv"])
    listing = s3.list_objects_v2(Bucket=BUCKET, Prefix="catalog/_manifest_v2/")
    assert listing["KeyCount"] == 1
    assert store.processed_keys() == {"raw/s1/hits/clip_0001.klv"}
