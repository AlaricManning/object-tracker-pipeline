"""Regenerate the golden KLV fixtures and their expected-output JSON.

The .klv files committed here are the frozen reference for the KLV wire
contract. Do NOT regenerate them casually: if regeneration changes any
committed fixture, that is a contract change and needs a migration plan
coordinated with the edge repo.

Usage:  python tests/fixtures/generate_fixtures.py
"""

import json
import struct
from pathlib import Path

from object_tracker_pipeline import klv

HERE = Path(__file__).parent


def f32(value: float) -> float:
    """Quantize through float32 and round like the decoder does."""
    return round(struct.unpack(">f", struct.pack(">f", value))[0], 4)


# One clip, one detection.
SINGLE_DETECTION = [
    dict(
        session_id="session_20260703_142200",
        clip_id="clip_0001",
        frame_id=0,
        timestamp="2026-07-03T14:22:07.481332+00:00",
        track_id=1,
        class_name="person",
        confidence=0.9134,
        bbox=[412.5, 233.0, 611.25, 588.5],
    ),
]

# One clip spanning several frames, including two detections in the same frame
# and a frame gap (frames with no detections emit no packets).
MULTI_DETECTION_CLIP = [
    dict(
        session_id="session_20260703_142200",
        clip_id="clip_0002",
        frame_id=0,
        timestamp="2026-07-03T14:25:11.102847+00:00",
        track_id=4,
        class_name="person",
        confidence=0.6712,
        bbox=[102.0, 88.5, 240.75, 402.0],
    ),
    dict(
        session_id="session_20260703_142200",
        clip_id="clip_0002",
        frame_id=0,
        timestamp="2026-07-03T14:25:11.102847+00:00",
        track_id=5,
        class_name="bicycle",
        confidence=0.5521,
        bbox=[300.0, 150.25, 520.5, 460.0],
    ),
    dict(
        session_id="session_20260703_142200",
        clip_id="clip_0002",
        frame_id=1,
        timestamp="2026-07-03T14:25:11.136180+00:00",
        track_id=4,
        class_name="person",
        confidence=0.7003,
        bbox=[104.5, 90.0, 243.0, 405.5],
    ),
    dict(
        session_id="session_20260703_142200",
        clip_id="clip_0002",
        frame_id=14,
        timestamp="2026-07-03T14:25:11.569902+00:00",
        track_id=4,
        class_name="person",
        confidence=0.9688,
        bbox=[130.0, 95.5, 268.25, 411.0],
    ),
]

FIXTURES = {
    "single_detection": SINGLE_DETECTION,
    "multi_detection_clip": MULTI_DETECTION_CLIP,
}


def expected_packet(spec: dict) -> dict:
    """Expected decoder output for one encoded detection (float32-quantized)."""
    return {
        "session_id": spec["session_id"],
        "clip_id": spec["clip_id"],
        "frame_id": spec["frame_id"],
        "timestamp": spec["timestamp"],
        "track_id": spec["track_id"],
        "class_name": spec["class_name"],
        "confidence": f32(spec["confidence"]),
        "x1": f32(spec["bbox"][0]),
        "y1": f32(spec["bbox"][1]),
        "x2": f32(spec["bbox"][2]),
        "y2": f32(spec["bbox"][3]),
    }


def main() -> None:
    for name, specs in FIXTURES.items():
        data = b"".join(klv.encode_packet(**spec) for spec in specs)
        (HERE / f"{name}.klv").write_bytes(data)
        expected = [expected_packet(spec) for spec in specs]
        (HERE / f"{name}.expected.json").write_text(
            json.dumps(expected, indent=2) + "\n"
        )
        print(f"wrote {name}.klv ({len(data)} bytes, {len(specs)} packets)")


if __name__ == "__main__":
    main()
