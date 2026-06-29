"""Unit tests for tools.dlq_replay.envelope (strict TDD — RED phase first)."""

from __future__ import annotations

import base64
import json

import pytest

from tools.dlq_replay.envelope import CorruptEnvelope, DLQEnvelope, decode


def _make_raw(overrides: dict | None = None) -> bytes:
    """Build a minimal valid DLQ envelope JSON payload."""
    payload = {
        "original_topic": "raw.strength",
        "original_key": "A1",
        "original_value": base64.b64encode(b'{"event_id":"e1","athlete_id":"A1"}').decode(),
        "error_type": "VALIDATION_FAILURE",
        "error_message": "bad field",
        "error_stack": None,
        "timestamp": 1719619200000,
    }
    if overrides:
        payload.update(overrides)
    return json.dumps(payload).encode()


# sc-4: valid envelope with base64 value → decoded bytes
def test_decode_valid_envelope_returns_dataclass():
    raw = _make_raw()
    env = decode(raw)
    assert isinstance(env, DLQEnvelope)
    assert env.original_topic == "raw.strength"
    assert env.original_key == "A1"
    assert env.original_value == b'{"event_id":"e1","athlete_id":"A1"}'
    assert env.error_type == "VALIDATION_FAILURE"
    assert env.timestamp == 1719619200000


# Triangulate: canonical-origin envelope
def test_decode_canonical_origin_topic():
    raw = _make_raw({"original_topic": "canonical.training_event"})
    env = decode(raw)
    assert env.original_topic == "canonical.training_event"


# sc-4: original_value is base64-decoded to bytes (not the base64 string)
def test_decode_original_value_is_bytes_not_b64_string():
    inner = b'{"event_id":"e1","athlete_id":"A1"}'
    raw = _make_raw({"original_value": base64.b64encode(inner).decode()})
    env = decode(raw)
    assert env.original_value == inner
    assert isinstance(env.original_value, bytes)


# sc-19: non-JSON bytes → CorruptEnvelope
def test_decode_non_json_raises_corrupt_envelope():
    with pytest.raises(CorruptEnvelope):
        decode(b"this is not json at all")


# sc-20: valid JSON but missing original_value → CorruptEnvelope
def test_decode_missing_original_value_raises_corrupt_envelope():
    payload = {
        "original_topic": "raw.strength",
        "original_key": "A1",
        # original_value intentionally absent
        "error_type": "VALIDATION_FAILURE",
    }
    with pytest.raises(CorruptEnvelope):
        decode(json.dumps(payload).encode())


# sc-20: valid JSON but missing original_topic → CorruptEnvelope
def test_decode_missing_original_topic_raises_corrupt_envelope():
    payload = {
        # original_topic intentionally absent
        "original_key": "A1",
        "original_value": base64.b64encode(b"data").decode(),
        "error_type": "VALIDATION_FAILURE",
    }
    with pytest.raises(CorruptEnvelope):
        decode(json.dumps(payload).encode())


# Null original_key is valid (sc-16 precondition)
def test_decode_null_original_key_is_valid():
    raw = _make_raw({"original_key": None})
    env = decode(raw)
    assert env.original_key is None


# Null original_topic is also decoded (the replay layer handles unrecoverable gate)
def test_decode_null_original_topic_is_valid():
    raw = _make_raw({"original_topic": None})
    env = decode(raw)
    assert env.original_topic is None
