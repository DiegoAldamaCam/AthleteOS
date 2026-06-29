"""DLQ quality reports — aggregators, dataclasses, and renderers.

Three in-memory aggregators feed from a single consumer pass:
  - ErrorTypeAgg: counts per (dlq_topic, error_type); NULL for None; raw for unknown.
  - AgeAgg: 7-bucket age distribution with oldest/newest tracking; fixed now_ms arg.
  - TriageAgg: fixability classification + origin counts + bounded error_type samples.

Renderers emit table (human-readable) or JSON to a string; retention_warning emits
to stderr. original_value bytes are NEVER stored anywhere (ADR-6/ADR-9).
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from bootstrap._topology import SEVEN_DAYS

# ---------------------------------------------------------------------------
# Age-bucket boundary constants (ADR-4)
# ---------------------------------------------------------------------------

_1D_MS = 86_400_000
_3D_MS = 259_200_000
_6D_MS = 518_400_000
# SEVEN_DAYS is the retention boundary (imported from _topology)

# ---------------------------------------------------------------------------
# Fixability mapping (ADR-5)
# ---------------------------------------------------------------------------

FIXABILITY_MAP: dict[str | None, str] = {
    "VALIDATION_FAILURE": "DATA_FIX",
    "TRANSFORM_ERROR": "DATA_FIX",
    "SCHEMA_INCOMPATIBILITY": "INFRA_FIX",
    "DESERIALIZATION_ERROR": "INFRA_FIX",
    "LATE_DATA": "LATE_ARRIVAL",
}


# ---------------------------------------------------------------------------
# Aggregators
# ---------------------------------------------------------------------------


class ErrorTypeAgg:
    """Count DLQ messages per (dlq_topic, error_type) and maintain cross-topic totals.

    None error_type is bucketed as 'NULL'.
    Unrecognized strings are bucketed by their raw value (no crash).
    """

    def __init__(self) -> None:
        # counts[topic][error_type_label] = int
        self.counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        # totals[error_type_label] = int (across all topics)
        self.totals: dict[str, int] = defaultdict(int)

    def add(self, topic: str, error_type: str | None) -> None:
        """Record one message for (topic, error_type).

        Args:
            topic: DLQ topic name.
            error_type: error_type value from the envelope; None → 'NULL'.
        """
        label = error_type if error_type is not None else "NULL"
        self.counts[topic][label] += 1
        self.totals[label] += 1


class AgeAgg:
    """7-bucket age distribution with oldest/newest non-null timestamp tracking.

    Bucket order (ADR-4):
      null_ts   → timestamp is None
      clock_skew → age < 0 (future timestamp)
      <1d        → [0, _1D_MS)
      1-3d       → [_1D_MS, _3D_MS)
      3-6d       → [_3D_MS, _6D_MS)
      >6d        → [_6D_MS, SEVEN_DAYS)
      expired    → [SEVEN_DAYS, ∞)

    Args:
        now_ms: Epoch-ms reference time, snapshotted once before the scan loop (ADR-3).
                Threaded as an explicit arg so age bucketing is a pure function.
    """

    def __init__(self) -> None:
        # counts[topic][bucket] = int
        self.counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        # extremes[topic] = {"oldest": int|None, "newest": int|None}
        self.extremes: dict[str, dict[str, int | None]] = defaultdict(
            lambda: {"oldest": None, "newest": None}
        )

    def add(self, topic: str, timestamp: int | None, now_ms: int) -> None:
        """Classify one message into an age bucket and update extremes.

        Args:
            topic: DLQ topic name.
            timestamp: Epoch-ms from the envelope; None → null_ts bucket.
            now_ms: Reference time snapshotted before the scan loop (ADR-3).
        """
        if timestamp is None:
            self.counts[topic]["null_ts"] += 1
            return

        age = now_ms - timestamp

        if age < 0:
            bucket = "clock_skew"
        elif age < _1D_MS:
            bucket = "<1d"
        elif age < _3D_MS:
            bucket = "1-3d"
        elif age < _6D_MS:
            bucket = "3-6d"
        elif age < SEVEN_DAYS:
            bucket = ">6d"
        else:
            bucket = "expired"

        self.counts[topic][bucket] += 1

        # Track oldest/newest non-null timestamp per topic.
        ex = self.extremes[topic]
        if ex["oldest"] is None or timestamp < ex["oldest"]:
            ex["oldest"] = timestamp
        if ex["newest"] is None or timestamp > ex["newest"]:
            ex["newest"] = timestamp


class TriageAgg:
    """Schema triage: fixability classification, original_topic counts, error_type samples.

    Samples are bounded lists of error_type strings per
    (dlq_topic, error_type, original_topic) key.
    original_value bytes are NEVER stored (ADR-6).
    """

    def __init__(self) -> None:
        # fix_counts[topic][fixability] = int
        self.fix_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        # origin_counts[topic][original_topic] = int
        self.origin_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        # samples["topic|error_type|original_topic"] = [error_type_str, ...]
        # Each sample stores the error_type label as the "error message" —
        # original_value is never stored (ADR-6).
        self.samples: dict[str, list[str]] = defaultdict(list)

    def add(self, topic: str, envelope: Any, sample_count: int) -> None:
        """Record one message: classify fixability, count origin, maybe add sample.

        Args:
            topic: DLQ topic name.
            envelope: DLQEnvelope (original_value never read — ADR-6).
            sample_count: Max samples per (topic, error_type, original_topic).
        """
        error_type: str | None = envelope.error_type
        original_topic: str | None = envelope.original_topic or "UNKNOWN_ORIGIN"

        fixability = FIXABILITY_MAP.get(error_type, "UNKNOWN")
        self.fix_counts[topic][fixability] += 1
        self.origin_counts[topic][original_topic] += 1

        # Sample bounded list of error_type labels (no original_value — ADR-6).
        key = f"{topic}|{error_type or 'NULL'}|{original_topic}"
        if len(self.samples[key]) < sample_count:
            # Store error_type label as the sample "error message".
            self.samples[key].append(error_type or "NULL")


# ---------------------------------------------------------------------------
# QualityResult dataclass
# ---------------------------------------------------------------------------


@dataclass
class QualityResult:
    """Aggregated result from a single quality scan pass.

    All fields are plain scalars or nested dicts/lists of scalars.
    original_value bytes are NEVER stored here (ADR-6/ADR-9).

    Attributes:
        error_type: {topic: {error_type_label: count}}
        age: {topic: {bucket: count}}
        age_extremes: {topic: {oldest: int|None, newest: int|None}}
        triage_fix: {topic: {fixability: count}}
        triage_origin: {topic: {original_topic: count}}
        samples: {"topic|error_type|origin": [error_type_str, ...]}
        corrupt: Count of messages that raised CorruptEnvelope.
        scanned: Total messages consumed (including corrupt).
    """

    error_type: dict[str, dict[str, int]]
    age: dict[str, dict[str, int]]
    age_extremes: dict[str, dict[str, int | None]]
    triage_fix: dict[str, dict[str, int]]
    triage_origin: dict[str, dict[str, int]]
    samples: dict[str, list[str]]
    corrupt: int = 0
    scanned: int = 0

    def to_dict(self) -> dict:
        """Serialize to a plain dict (no bytes — ADR-9).

        Returns:
            Nested dict safe for json.dumps().
        """
        return {
            "error_type": {t: dict(c) for t, c in self.error_type.items()},
            "age": {t: dict(c) for t, c in self.age.items()},
            "age_extremes": {t: dict(ex) for t, ex in self.age_extremes.items()},
            "triage_fix": {t: dict(c) for t, c in self.triage_fix.items()},
            "triage_origin": {t: dict(c) for t, c in self.triage_origin.items()},
            "samples": {k: list(v) for k, v in self.samples.items()},
            "corrupt": self.corrupt,
            "scanned": self.scanned,
        }


# ---------------------------------------------------------------------------
# Renderers
# ---------------------------------------------------------------------------


def render_table(result: QualityResult) -> str:
    """Render a QualityResult as a human-readable table string.

    Args:
        result: Populated QualityResult from scan().

    Returns:
        Multi-line string with labeled sections and counts.
    """
    lines: list[str] = []

    if result.error_type:
        lines.append("=== Error-Type Distribution ===")
        for topic, counts in sorted(result.error_type.items()):
            lines.append(f"  Topic: {topic}")
            for label, count in sorted(counts.items()):
                lines.append(f"    {label}: {count}")
        lines.append("")

    if result.age:
        lines.append("=== Age Distribution ===")
        for topic, counts in sorted(result.age.items()):
            lines.append(f"  Topic: {topic}")
            for bucket in ["<1d", "1-3d", "3-6d", ">6d", "expired", "null_ts", "clock_skew"]:
                c = counts.get(bucket, 0)
                if c:
                    lines.append(f"    {bucket}: {c}")
            ex = result.age_extremes.get(topic, {})
            if ex.get("oldest") is not None:
                lines.append(f"    oldest_ts: {ex['oldest']}")
            if ex.get("newest") is not None:
                lines.append(f"    newest_ts: {ex['newest']}")
        lines.append("")

    if result.triage_fix or result.triage_origin:
        lines.append("=== Schema Triage ===")
        for topic in sorted(set(list(result.triage_fix) + list(result.triage_origin))):
            lines.append(f"  Topic: {topic}")
            fix_counts = result.triage_fix.get(topic, {})
            for fix, count in sorted(fix_counts.items()):
                lines.append(f"    {fix}: {count}")
            origin_counts = result.triage_origin.get(topic, {})
            for orig, count in sorted(origin_counts.items()):
                lines.append(f"    origin:{orig}: {count}")
        if result.samples:
            lines.append("  Samples:")
            for key, msgs in sorted(result.samples.items()):
                lines.append(f"    [{key}]:")
                for msg in msgs:
                    lines.append(f"      - {msg}")
        lines.append("")

    if not lines:
        lines.append("(no data)")

    lines.append(f"Scanned: {result.scanned}  Corrupt: {result.corrupt}")
    return "\n".join(lines)


def render_json(result: QualityResult) -> str:
    """Render a QualityResult as a JSON string.

    Args:
        result: Populated QualityResult from scan().

    Returns:
        JSON string parseable by json.loads() (ADR-9 — no bytes).
    """
    return json.dumps(result.to_dict(), indent=2)


# ---------------------------------------------------------------------------
# Retention warning
# ---------------------------------------------------------------------------


def retention_warning(result: QualityResult) -> None:
    """Emit a WARNING to stderr if any message is near or past retention.

    Emits a WARNING when the '>6d' or 'expired' bucket is non-empty for
    any topic. Silent when both are empty for all topics (sc-27, sc-28, sc-29).

    Args:
        result: Populated QualityResult from scan().
    """
    danger_topics: list[str] = []
    for topic, counts in result.age.items():
        if counts.get(">6d", 0) > 0 or counts.get("expired", 0) > 0:
            danger_topics.append(topic)

    if danger_topics:
        print(
            "WARNING: Messages approaching or past 7-day retention detected "
            f"in topics: {', '.join(sorted(danger_topics))}. "
            "Review or replay before data expires.",
            file=sys.stderr,
        )
