"""Tests for the vendored KLV parser.

The golden fixtures (tests/fixtures/*.klv) are the frozen reference for the
wire contract: if a parser change breaks these, the contract broke.
The handcrafted-bytes tests pin the wire format independently of the encoder,
so an accidental change to encoder and decoder together still fails loudly.
"""

import json
import struct
from pathlib import Path

import pytest

from object_tracker_pipeline import klv

FIXTURES = Path(__file__).parent / "fixtures"

GOLDEN_NAMES = ["single_detection", "multi_detection_clip"]


@pytest.mark.parametrize("name", GOLDEN_NAMES)
def test_golden_fixture_decodes_to_expected(name):
    data = (FIXTURES / f"{name}.klv").read_bytes()
    expected = json.loads((FIXTURES / f"{name}.expected.json").read_text())
    assert list(klv.iter_packets(data)) == expected


def test_wire_format_pinned_by_hand():
    # Build a packet byte-by-byte, bypassing encode_packet entirely:
    # [2-byte payload length BE] then [tag][len][value] triplets.
    payload = (
        bytes([klv.TAG_FRAME_ID, 4])
        + struct.pack(">I", 7)
        + bytes([klv.TAG_CLASS_NAME, 6])
        + b"person"
        + bytes([klv.TAG_CONFIDENCE, 4])
        + struct.pack(">f", 0.5)
    )
    data = struct.pack(">H", len(payload)) + payload
    assert list(klv.iter_packets(data)) == [
        {"frame_id": 7, "class_name": "person", "confidence": 0.5}
    ]


def test_unknown_tag_is_skipped():
    payload = (
        bytes([0x7F, 3])
        + b"xyz"  # tag 0x7F is not in the schema
        + bytes([klv.TAG_FRAME_ID, 4])
        + struct.pack(">I", 1)
    )
    data = struct.pack(">H", len(payload)) + payload
    assert list(klv.iter_packets(data)) == [{"frame_id": 1}]


def test_multiple_packets_decode_sequentially():
    one = klv.encode_packet(
        frame_id=0,
        timestamp="2026-07-03T14:22:07+00:00",
        session_id="s",
        clip_id="c",
        track_id=1,
        class_name="person",
        confidence=0.5,
        bbox=[1.0, 2.0, 3.0, 4.0],
    )
    two = klv.encode_packet(
        frame_id=1,
        timestamp="2026-07-03T14:22:07+00:00",
        session_id="s",
        clip_id="c",
        track_id=1,
        class_name="person",
        confidence=0.75,
        bbox=[1.5, 2.5, 3.5, 4.5],
    )
    packets = list(klv.iter_packets(one + two))
    assert [p["frame_id"] for p in packets] == [0, 1]
    assert [p["confidence"] for p in packets] == [0.5, 0.75]


def test_empty_data_yields_no_packets():
    assert list(klv.iter_packets(b"")) == []


def test_truncated_header_raises():
    with pytest.raises(ValueError, match="Truncated KLV packet header"):
        list(klv.iter_packets(b"\x00"))


def test_truncated_payload_raises():
    data = struct.pack(">H", 10) + b"\x01\x02"  # claims 10 bytes, has 2
    with pytest.raises(ValueError, match="Truncated KLV packet payload"):
        list(klv.iter_packets(data))


def test_oversized_value_rejected_on_encode():
    with pytest.raises(ValueError, match="too long"):
        klv.encode_packet(
            frame_id=0,
            timestamp="t",
            session_id="x" * 256,
            clip_id="c",
            track_id=1,
            class_name="person",
            confidence=0.5,
            bbox=[1.0, 2.0, 3.0, 4.0],
        )
