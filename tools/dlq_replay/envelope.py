"""DLQ envelope decoding — inverse of build_dlq_envelope().

The DLQ envelope is a JSON object produced by canonicalize jobs and the metrics
job. ``decode()`` parses the raw bytes, validates required fields, and
base64-decodes ``original_value`` back to the original raw bytes.
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass


class CorruptEnvelope(Exception):
    """Raised when a DLQ message cannot be decoded (non-JSON, missing fields)."""


@dataclass
class DLQEnvelope:
    """Decoded DLQ envelope representing a single failed message.

    Attributes:
        original_topic: The Kafka topic the message was originally destined for.
            May be None if the field is present but null in the envelope.
        original_key: The original Kafka message key. May be None (null key).
        original_value: The original raw bytes (already base64-decoded).
        error_type: One of VALIDATION_FAILURE, SCHEMA_INCOMPATIBILITY,
            DESERIALIZATION_ERROR, TRANSFORM_ERROR, LATE_DATA. May be None.
        timestamp: Epoch-millisecond timestamp from the envelope. May be None.
    """

    original_topic: str | None
    original_key: str | None
    original_value: bytes
    error_type: str | None
    timestamp: int | None
    original_value_truncated: bool = False  # NEW — additive; default keeps legacy decoders valid


# Required fields that MUST be present (even if their value can be null).
_REQUIRED_FIELDS = ("original_topic", "original_value")


def decode(raw_bytes: bytes) -> DLQEnvelope:
    """Decode raw DLQ message bytes into a DLQEnvelope.

    Args:
        raw_bytes: The raw bytes of the Kafka DLQ message value.

    Returns:
        A populated DLQEnvelope with ``original_value`` as decoded bytes.

    Raises:
        CorruptEnvelope: If ``raw_bytes`` are not valid JSON, or if any
            required field (original_topic, original_value) is absent.
    """
    try:
        envelope = json.loads(raw_bytes)
    except (json.JSONDecodeError, ValueError) as exc:
        raise CorruptEnvelope(f"DLQ message is not valid JSON: {exc}") from exc

    for field in _REQUIRED_FIELDS:
        if field not in envelope:
            raise CorruptEnvelope(
                f"DLQ envelope missing required field '{field}'"
            )

    # base64-decode original_value back to raw bytes (inverse of build_dlq_envelope).
    raw_value_b64: str | None = envelope.get("original_value")
    if raw_value_b64 is None:
        # original_value key present but value is null — treat as corrupt.
        raise CorruptEnvelope("DLQ envelope has null 'original_value'")

    try:
        original_value = base64.b64decode(raw_value_b64)
    except Exception as exc:
        raise CorruptEnvelope(
            f"DLQ envelope 'original_value' is not valid base64: {exc}"
        ) from exc

    return DLQEnvelope(
        original_topic=envelope.get("original_topic"),
        original_key=envelope.get("original_key"),
        original_value=original_value,
        error_type=envelope.get("error_type"),
        timestamp=envelope.get("timestamp"),
        original_value_truncated=bool(envelope.get("original_value_truncated", False)),
    )
