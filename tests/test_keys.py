"""Tests for raw-area S3 key parsing."""

import pytest

from object_tracker_pipeline.keys import ClipRef, parse_clip_key


def test_parses_hits_clip_key():
    ref = parse_clip_key("raw/session_20260703_142200/hits/clip_0001.klv")
    assert ref == ClipRef(
        key="raw/session_20260703_142200/hits/clip_0001.klv",
        session_id="session_20260703_142200",
        tier="hits",
        clip_stem="clip_0001",
    )


def test_parses_near_misses_clip_key():
    ref = parse_clip_key("raw/session_x/near_misses/clip_0042.klv")
    assert ref is not None
    assert ref.tier == "near_misses"
    assert ref.clip_stem == "clip_0042"


@pytest.mark.parametrize(
    "key",
    [
        "raw/session_x/hits/clip_0001.ts",  # video, not metadata
        "raw/session_x/session_summary.json",  # summary, wrong depth anyway
        "raw/session_x/clip_0001.klv",  # missing tier level
        "raw/session_x/hits/nested/clip_0001.klv",  # too deep
        "raw/session_x/hits/",  # directory marker
        "raw//hits/clip_0001.klv",  # empty session segment
        "catalog/detections/date=2026-07-03/f.parquet",  # not under raw/
        "other/session_x/hits/clip_0001.klv",  # wrong prefix
    ],
)
def test_non_clip_keys_return_none(key):
    assert parse_clip_key(key) is None


def test_custom_prefix():
    ref = parse_clip_key("staging/session_x/hits/clip_0001.klv", prefix="staging/")
    assert ref is not None
    assert ref.session_id == "session_x"


def test_tier_is_not_validated_against_a_fixed_set():
    # If the edge repo ever adds a tier, we ingest it rather than drop data.
    ref = parse_clip_key("raw/session_x/some_new_tier/clip_0001.klv")
    assert ref is not None
    assert ref.tier == "some_new_tier"
