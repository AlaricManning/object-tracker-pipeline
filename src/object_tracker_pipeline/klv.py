"""
Simplified KLV (Key-Length-Value) encoder/decoder for object tracking metadata.

Wire format per packet:
    [2-byte total payload length, big-endian]
    followed by N triplets: [1-byte tag][1-byte length][N bytes value]

Custom schema — not MISB ST 0601. Migrate to ST 0601 Universal Keys when
interoperability with ATAK / Falcon View is required.

FRAME_ID is the 0-based frame index within the clip's video file (pre-roll
frames included), so frame N in the KLV addresses frame N of the paired .ts —
no external offset table needed. TIMESTAMP is stamped at frame capture, not
at encode time.
"""
import struct

TAG_FRAME_ID    = 0x01  # uint32 BE — 0-based frame index within the clip video
TAG_TIMESTAMP   = 0x02  # UTF-8 string
TAG_TRACK_ID    = 0x03  # uint32 BE
TAG_CLASS_NAME  = 0x04  # UTF-8 string
TAG_CONFIDENCE  = 0x05  # float32 BE
TAG_BBOX_X1     = 0x06  # float32 BE
TAG_BBOX_Y1     = 0x07  # float32 BE
TAG_BBOX_X2     = 0x08  # float32 BE
TAG_BBOX_Y2     = 0x09  # float32 BE
TAG_SESSION_ID  = 0x0A  # UTF-8 string
TAG_CLIP_ID     = 0x0B  # UTF-8 string

_TAG_NAMES = {
    TAG_FRAME_ID:   ('frame_id',    '>I'),
    TAG_TRACK_ID:   ('track_id',    '>I'),
    TAG_CONFIDENCE: ('confidence',  '>f'),
    TAG_BBOX_X1:    ('x1',          '>f'),
    TAG_BBOX_Y1:    ('y1',          '>f'),
    TAG_BBOX_X2:    ('x2',          '>f'),
    TAG_BBOX_Y2:    ('y2',          '>f'),
}
_TAG_STRINGS = {TAG_TIMESTAMP, TAG_CLASS_NAME, TAG_SESSION_ID, TAG_CLIP_ID}
_TAG_STRING_NAMES = {
    TAG_TIMESTAMP:  'timestamp',
    TAG_CLASS_NAME: 'class_name',
    TAG_SESSION_ID: 'session_id',
    TAG_CLIP_ID:    'clip_id',
}


def _triplet(tag: int, value: bytes) -> bytes:
    if len(value) > 255:
        raise ValueError(f"KLV value too long for tag 0x{tag:02X}: {len(value)} bytes")
    return struct.pack('BB', tag, len(value)) + value


def encode_packet(
    frame_id: int,
    timestamp: str,
    session_id: str,
    clip_id: str,
    track_id: int,
    class_name: str,
    confidence: float,
    bbox: list,
) -> bytes:
    """
    Encode one detection event as a KLV packet.
    Returns bytes with a 2-byte length prefix followed by the payload.
    Write multiple packets sequentially for multiple detections per frame.
    """
    payload = (
        _triplet(TAG_SESSION_ID,   session_id.encode())
        + _triplet(TAG_CLIP_ID,    clip_id.encode())
        + _triplet(TAG_FRAME_ID,   struct.pack('>I', frame_id))
        + _triplet(TAG_TIMESTAMP,  timestamp.encode())
        + _triplet(TAG_TRACK_ID,   struct.pack('>I', track_id))
        + _triplet(TAG_CLASS_NAME, class_name.encode())
        + _triplet(TAG_CONFIDENCE, struct.pack('>f', confidence))
        + _triplet(TAG_BBOX_X1,    struct.pack('>f', bbox[0]))
        + _triplet(TAG_BBOX_Y1,    struct.pack('>f', bbox[1]))
        + _triplet(TAG_BBOX_X2,    struct.pack('>f', bbox[2]))
        + _triplet(TAG_BBOX_Y2,    struct.pack('>f', bbox[3]))
    )
    return struct.pack('>H', len(payload)) + payload


def decode_packet(data: bytes, offset: int = 0) -> tuple[dict, int]:
    """Decode one KLV packet from a byte buffer. Returns (fields, next_offset)."""
    if offset + 2 > len(data):
        raise ValueError("Truncated KLV packet header")

    payload_len = struct.unpack_from('>H', data, offset)[0]
    offset += 2
    end = offset + payload_len

    if end > len(data):
        raise ValueError("Truncated KLV packet payload")

    fields = {}
    while offset < end:
        tag, length = struct.unpack_from('BB', data, offset)
        offset += 2
        value = data[offset:offset + length]
        offset += length

        if tag in _TAG_NAMES:
            name, fmt = _TAG_NAMES[tag]
            fields[name] = round(struct.unpack(fmt, value)[0], 4)
        elif tag in _TAG_STRINGS:
            fields[_TAG_STRING_NAMES[tag]] = value.decode()

    return fields, end


def iter_packets(data: bytes):
    """Yield all decoded detection packets from a .klv file's contents."""
    offset = 0
    while offset < len(data):
        packet, offset = decode_packet(data, offset)
        yield packet
