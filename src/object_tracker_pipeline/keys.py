"""Parsing of edge-repo S3 keys.

The edge capture layout (owned by the edge repo, like the KLV contract):

    raw/{session_id}/{tier}/{clip}.klv     ← detection metadata (we process these)
    raw/{session_id}/{tier}/{clip}.ts      ← video (never downloaded)
    raw/{session_id}/session_summary.json  ← per-session summary (ignored for now)
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class ClipRef:
    """One .klv object in the raw area, with the metadata encoded in its key."""

    key: str  # full S3 key, e.g. "raw/session_20260703_142200/hits/clip_0001.klv"
    session_id: str
    tier: str  # "hits" / "near_misses"
    clip_stem: str  # file name without extension, e.g. "clip_0001"


def parse_clip_key(key: str, prefix: str = "raw/") -> ClipRef | None:
    """Parse an S3 key into a ClipRef, or None if it isn't a clip .klv.

    Returns None (rather than raising) for everything else that legitimately
    lives under the prefix — .ts videos, session summaries — and for key
    shapes that don't match the layout, so callers can filter a full listing
    with this one function.
    """
    if not key.startswith(prefix) or not key.endswith(".klv"):
        return None

    parts = key[len(prefix) :].split("/")
    if len(parts) != 3 or not all(parts):
        return None

    session_id, tier, filename = parts
    return ClipRef(
        key=key,
        session_id=session_id,
        tier=tier,
        clip_stem=filename.removesuffix(".klv"),
    )
