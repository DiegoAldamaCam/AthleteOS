"""Unit tests for tools.dlq_quality.reports (strict TDD — RED phase first).

Covers: sc-1..18, sc-21, sc-22, sc-27, sc-28, sc-29, S1.
No Kafka connection required — pure aggregator logic with fixed now_ms.
"""

from __future__ import annotations

import json
import sys

import pytest

from bootstrap._topology import SEVEN_DAYS


# ---------------------------------------------------------------------------
# Helpers — build DLQEnvelope-like objects without hitting Kafka
# ---------------------------------------------------------------------------

def _make_envelope(
    error_type=None,
    timestamp=None,
    original_topic="raw.strength",
    original_value=b"PAYLOAD",
    error_message=None,  # ignored: DLQEnvelope has no error_message field
):
    """Build a minimal DLQEnvelope for testing without importing the real decode().

    Note: error_message is not a field on DLQEnvelope. TriageAgg uses error_type
    as the sample text when no richer field is available. The error_message param
    here is accepted but ignored so test call-sites read naturally.
    """
    from tools.dlq_replay.envelope import DLQEnvelope

    return DLQEnvelope(
        original_topic=original_topic,
        original_key=None,
        original_value=original_value,
        error_type=error_type,
        timestamp=timestamp,
    )


# ---------------------------------------------------------------------------
# sc-1: 5 known error types counted per topic
# ---------------------------------------------------------------------------

def test_error_type_agg_five_known_types():
    """sc-1: 5 known types each counted once → TOTALS row shows 5."""
    from tools.dlq_quality.reports import ErrorTypeAgg

    agg = ErrorTypeAgg()
    known = [
        "VALIDATION_FAILURE",
        "SCHEMA_INCOMPATIBILITY",
        "DESERIALIZATION_ERROR",
        "TRANSFORM_ERROR",
        "LATE_DATA",
    ]
    topic = "dlq.canonical.training_event"
    for et in known:
        agg.add(topic, et)

    counts = agg.counts[topic]
    for et in known:
        assert counts[et] == 1, f"Expected count 1 for {et}"
    assert agg.totals["VALIDATION_FAILURE"] == 1
    assert sum(agg.totals.values()) == 5


# ---------------------------------------------------------------------------
# sc-2: NULL error_type bucketed as "NULL"
# ---------------------------------------------------------------------------

def test_error_type_agg_none_bucketed_as_null():
    """sc-2: error_type=None → bucketed under 'NULL', no crash."""
    from tools.dlq_quality.reports import ErrorTypeAgg

    agg = ErrorTypeAgg()
    topic = "dlq.canonical.training_event"
    agg.add(topic, None)
    assert agg.counts[topic]["NULL"] == 1


# ---------------------------------------------------------------------------
# sc-3: Unrecognized raw string bucketed without crash
# ---------------------------------------------------------------------------

def test_error_type_agg_raw_string_bucketed():
    """sc-3: error_type='FUTURE_ERROR_TYPE' → bucketed by raw string, no exception."""
    from tools.dlq_quality.reports import ErrorTypeAgg

    agg = ErrorTypeAgg()
    topic = "dlq.canonical.training_event"
    agg.add(topic, "FUTURE_ERROR_TYPE")
    assert agg.counts[topic]["FUTURE_ERROR_TYPE"] == 1


# ---------------------------------------------------------------------------
# sc-4: Cross-topic totals
# ---------------------------------------------------------------------------

def test_error_type_agg_cross_topic_totals():
    """sc-4: two topics with 3 VALIDATION_FAILURE each → totals['VALIDATION_FAILURE'] == 6."""
    from tools.dlq_quality.reports import ErrorTypeAgg

    agg = ErrorTypeAgg()
    for topic in ["dlq.canonical.training_event", "dlq.canonical.wellness_event"]:
        for _ in range(3):
            agg.add(topic, "VALIDATION_FAILURE")
    assert agg.totals["VALIDATION_FAILURE"] == 6


# ---------------------------------------------------------------------------
# sc-5: All 7 age buckets classified correctly with fixed now_ms
# ---------------------------------------------------------------------------

_NOW_MS = 10_000_000_000  # fixed reference for deterministic tests

_AGE_BUCKET_CASES = [
    # (timestamp, expected_bucket)
    (_NOW_MS - 3_600_000, "<1d"),          # 1 hour ago → <1d
    (_NOW_MS - 172_800_000, "1-3d"),       # 2 days ago → 1-3d
    (_NOW_MS - 432_000_000, "3-6d"),       # 5 days ago → 3-6d
    (_NOW_MS - 550_000_000, ">6d"),        # ~6.4 days → >6d (below SEVEN_DAYS)
    (_NOW_MS - SEVEN_DAYS - 1, "expired"),  # just past 7 days → expired
    (None, "null_ts"),                     # None timestamp
    (_NOW_MS + 60_000, "clock_skew"),      # 1 minute in future → clock_skew
]


@pytest.mark.parametrize("ts,expected", _AGE_BUCKET_CASES)
def test_age_agg_bucket_classification(ts, expected):
    """sc-5: each timestamp maps to the correct age bucket with fixed now_ms."""
    from tools.dlq_quality.reports import AgeAgg

    agg = AgeAgg()
    topic = "dlq.canonical.training_event"
    agg.add(topic, ts, _NOW_MS)
    assert agg.counts[topic][expected] == 1, (
        f"ts={ts} expected bucket={expected!r}, "
        f"got counts={dict(agg.counts[topic])}"
    )


# ---------------------------------------------------------------------------
# sc-9 / S1: SEVEN_DAYS boundary — parametrized fencepost
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("age_delta,expected_bucket", [
    (SEVEN_DAYS - 1, ">6d"),   # S1: age == SEVEN_DAYS - 1 → >6d
    (SEVEN_DAYS,     "expired"),  # S1: age == SEVEN_DAYS → expired
])
def test_age_agg_seven_days_boundary(age_delta, expected_bucket):
    """sc-9/S1: SEVEN_DAYS boundary fencepost — age==SEVEN_DAYS-1→>6d, ==SEVEN_DAYS→expired."""
    from tools.dlq_quality.reports import AgeAgg

    agg = AgeAgg()
    topic = "dlq.canonical.training_event"
    ts = _NOW_MS - age_delta
    agg.add(topic, ts, _NOW_MS)
    assert agg.counts[topic][expected_bucket] == 1, (
        f"age_delta={age_delta} expected {expected_bucket!r}, "
        f"got counts={dict(agg.counts[topic])}"
    )


# ---------------------------------------------------------------------------
# sc-7: null_ts for None timestamp
# ---------------------------------------------------------------------------

def test_age_agg_none_timestamp_null_ts_bucket():
    """sc-7: timestamp=None → null_ts bucket, no crash."""
    from tools.dlq_quality.reports import AgeAgg

    agg = AgeAgg()
    agg.add("dlq.canonical.training_event", None, _NOW_MS)
    assert agg.counts["dlq.canonical.training_event"]["null_ts"] == 1


# ---------------------------------------------------------------------------
# sc-8: clock_skew for future timestamp
# ---------------------------------------------------------------------------

def test_age_agg_future_timestamp_clock_skew():
    """sc-8: timestamp 60 s in the future → clock_skew bucket."""
    from tools.dlq_quality.reports import AgeAgg

    agg = AgeAgg()
    ts = _NOW_MS + 60_000  # 60 seconds in the future
    agg.add("dlq.canonical.training_event", ts, _NOW_MS)
    assert agg.counts["dlq.canonical.training_event"]["clock_skew"] == 1


# ---------------------------------------------------------------------------
# sc-10: Oldest and newest per topic
# ---------------------------------------------------------------------------

def test_age_agg_oldest_newest_tracking():
    """sc-10: oldest and newest non-null ts tracked per topic."""
    from tools.dlq_quality.reports import AgeAgg

    agg = AgeAgg()
    topic = "dlq.canonical.training_event"
    ts_old = _NOW_MS - 200_000_000
    ts_new = _NOW_MS - 10_000
    agg.add(topic, ts_old, _NOW_MS)
    agg.add(topic, ts_new, _NOW_MS)
    agg.add(topic, None, _NOW_MS)  # null_ts should not affect extremes

    assert agg.extremes[topic]["oldest"] == ts_old
    assert agg.extremes[topic]["newest"] == ts_new


# ---------------------------------------------------------------------------
# sc-11: DATA_FIX classification
# ---------------------------------------------------------------------------

def test_triage_agg_data_fix_validation_failure():
    """sc-11: VALIDATION_FAILURE → DATA_FIX."""
    from tools.dlq_quality.reports import TriageAgg

    agg = TriageAgg()
    env = _make_envelope(error_type="VALIDATION_FAILURE")
    topic = "dlq.canonical.training_event"
    agg.add(topic, env, sample_count=3)
    assert agg.fix_counts[topic]["DATA_FIX"] == 1


def test_triage_agg_data_fix_transform_error():
    """sc-11: TRANSFORM_ERROR → DATA_FIX."""
    from tools.dlq_quality.reports import TriageAgg

    agg = TriageAgg()
    env = _make_envelope(error_type="TRANSFORM_ERROR")
    agg.add("dlq.canonical.training_event", env, sample_count=3)
    assert agg.fix_counts["dlq.canonical.training_event"]["DATA_FIX"] == 1


# ---------------------------------------------------------------------------
# sc-12: INFRA_FIX classification
# ---------------------------------------------------------------------------

def test_triage_agg_infra_fix_schema_incompatibility():
    """sc-12: SCHEMA_INCOMPATIBILITY → INFRA_FIX."""
    from tools.dlq_quality.reports import TriageAgg

    agg = TriageAgg()
    env = _make_envelope(error_type="SCHEMA_INCOMPATIBILITY")
    agg.add("dlq.canonical.training_event", env, sample_count=3)
    assert agg.fix_counts["dlq.canonical.training_event"]["INFRA_FIX"] == 1


def test_triage_agg_infra_fix_deserialization_error():
    """sc-12: DESERIALIZATION_ERROR → INFRA_FIX."""
    from tools.dlq_quality.reports import TriageAgg

    agg = TriageAgg()
    env = _make_envelope(error_type="DESERIALIZATION_ERROR")
    agg.add("dlq.canonical.training_event", env, sample_count=3)
    assert agg.fix_counts["dlq.canonical.training_event"]["INFRA_FIX"] == 1


# ---------------------------------------------------------------------------
# sc-13: LATE_ARRIVAL classification
# ---------------------------------------------------------------------------

def test_triage_agg_late_arrival():
    """sc-13: LATE_DATA → LATE_ARRIVAL."""
    from tools.dlq_quality.reports import TriageAgg

    agg = TriageAgg()
    env = _make_envelope(error_type="LATE_DATA")
    agg.add("dlq.canonical.training_event", env, sample_count=3)
    assert agg.fix_counts["dlq.canonical.training_event"]["LATE_ARRIVAL"] == 1


# ---------------------------------------------------------------------------
# sc-14: UNKNOWN for None and unrecognized string
# ---------------------------------------------------------------------------

def test_triage_agg_unknown_for_none():
    """sc-14: error_type=None → UNKNOWN."""
    from tools.dlq_quality.reports import TriageAgg

    agg = TriageAgg()
    env = _make_envelope(error_type=None)
    agg.add("dlq.canonical.training_event", env, sample_count=3)
    assert agg.fix_counts["dlq.canonical.training_event"]["UNKNOWN"] == 1


def test_triage_agg_unknown_for_unrecognized_string():
    """sc-14: error_type='FUTURE_TYPE' → UNKNOWN."""
    from tools.dlq_quality.reports import TriageAgg

    agg = TriageAgg()
    env = _make_envelope(error_type="FUTURE_TYPE")
    agg.add("dlq.canonical.training_event", env, sample_count=3)
    assert agg.fix_counts["dlq.canonical.training_event"]["UNKNOWN"] == 1


# ---------------------------------------------------------------------------
# sc-15: original_topic distribution counted
# ---------------------------------------------------------------------------

def test_triage_agg_original_topic_distribution():
    """sc-15: 5 from raw.strength + 3 from canonical.training_event counted per dlq topic."""
    from tools.dlq_quality.reports import TriageAgg

    agg = TriageAgg()
    dlq_topic = "dlq.canonical.training_event"
    for _ in range(5):
        env = _make_envelope(
            error_type="VALIDATION_FAILURE", original_topic="raw.strength"
        )
        agg.add(dlq_topic, env, sample_count=3)
    for _ in range(3):
        env = _make_envelope(
            error_type="VALIDATION_FAILURE",
            original_topic="canonical.training_event",
        )
        agg.add(dlq_topic, env, sample_count=3)

    origin_counts = agg.origin_counts[dlq_topic]
    assert origin_counts["raw.strength"] == 5
    assert origin_counts["canonical.training_event"] == 3


# ---------------------------------------------------------------------------
# sc-16: sample_count cap enforced at default 3
# ---------------------------------------------------------------------------

def test_triage_agg_sample_cap_default_3():
    """sc-16: 10 messages with same (dlq_topic, error_type, original_topic) → exactly 3 samples."""
    from tools.dlq_quality.reports import TriageAgg

    agg = TriageAgg()
    dlq_topic = "dlq.canonical.training_event"
    for i in range(10):
        env = _make_envelope(
            error_type="VALIDATION_FAILURE",
            original_topic="raw.strength",
            error_message=f"msg {i}",
        )
        agg.add(dlq_topic, env, sample_count=3)

    key = "dlq.canonical.training_event|VALIDATION_FAILURE|raw.strength"
    assert len(agg.samples[key]) == 3


# ---------------------------------------------------------------------------
# sc-17: sample_count configurable up to 10
# ---------------------------------------------------------------------------

def test_triage_agg_sample_cap_configurable_10():
    """sc-17: 10 messages with --sample-count 10 → exactly 10 samples."""
    from tools.dlq_quality.reports import TriageAgg

    agg = TriageAgg()
    dlq_topic = "dlq.canonical.training_event"
    for i in range(10):
        env = _make_envelope(
            error_type="VALIDATION_FAILURE",
            original_topic="raw.strength",
            error_message=f"msg {i}",
        )
        agg.add(dlq_topic, env, sample_count=10)

    key = "dlq.canonical.training_event|VALIDATION_FAILURE|raw.strength"
    assert len(agg.samples[key]) == 10


# ---------------------------------------------------------------------------
# sc-18: original_value NEVER appears in any output (render_table or render_json)
# ---------------------------------------------------------------------------

def test_original_value_absent_from_table_output():
    """sc-18: original_value bytes never appear in render_table() output."""
    from tools.dlq_quality.reports import TriageAgg, QualityResult, render_table

    agg = TriageAgg()
    dlq_topic = "dlq.canonical.training_event"
    secret = b"SECRET_PAYLOAD_BYTES_XYZ"
    env = _make_envelope(
        error_type="VALIDATION_FAILURE",
        original_topic="raw.strength",
        original_value=secret,
    )
    agg.add(dlq_topic, env, sample_count=3)

    result = QualityResult(
        error_type={},
        age={},
        age_extremes={},
        triage_fix=agg.fix_counts,
        triage_origin=agg.origin_counts,
        samples=agg.samples,
    )
    output = render_table(result)
    assert secret not in output.encode("utf-8"), "original_value bytes must not appear in table output"


def test_original_value_absent_from_json_output():
    """sc-18: original_value bytes never appear in render_json() output."""
    from tools.dlq_quality.reports import TriageAgg, QualityResult, render_json

    agg = TriageAgg()
    dlq_topic = "dlq.canonical.training_event"
    secret = b"SECRET_PAYLOAD_BYTES_ABC"
    env = _make_envelope(
        error_type="VALIDATION_FAILURE",
        original_topic="raw.strength",
        original_value=secret,
    )
    agg.add(dlq_topic, env, sample_count=3)

    result = QualityResult(
        error_type={},
        age={},
        age_extremes={},
        triage_fix=agg.fix_counts,
        triage_origin=agg.origin_counts,
        samples=agg.samples,
    )
    output = render_json(result)
    # Bytes cannot appear in JSON (json.dumps would raise on bytes) — also check string repr
    assert secret not in output.encode("utf-8")
    # Verify it's actually valid JSON
    json.loads(output)


# ---------------------------------------------------------------------------
# sc-21: render_table() returns non-empty human-readable string
# ---------------------------------------------------------------------------

def test_render_table_returns_human_readable_string():
    """sc-21: render_table() produces a non-empty string with labeled content."""
    from tools.dlq_quality.reports import ErrorTypeAgg, QualityResult, render_table

    agg = ErrorTypeAgg()
    agg.add("dlq.canonical.training_event", "VALIDATION_FAILURE")
    result = QualityResult(
        error_type=agg.counts,
        age={},
        age_extremes={},
        triage_fix={},
        triage_origin={},
        samples={},
    )
    output = render_table(result)
    assert isinstance(output, str)
    assert len(output) > 0
    assert "VALIDATION_FAILURE" in output


# ---------------------------------------------------------------------------
# sc-22: render_json() output is valid JSON parseable by json.loads()
# ---------------------------------------------------------------------------

def test_render_json_is_valid_json():
    """sc-22: render_json() output parses with json.loads() without exception."""
    from tools.dlq_quality.reports import AgeAgg, QualityResult, render_json

    agg = AgeAgg()
    agg.add("dlq.canonical.training_event", _NOW_MS - 3_600_000, _NOW_MS)
    result = QualityResult(
        error_type={},
        age=agg.counts,
        age_extremes=agg.extremes,
        triage_fix={},
        triage_origin={},
        samples={},
    )
    output = render_json(result)
    parsed = json.loads(output)
    assert isinstance(parsed, dict)
    assert "age" in parsed


# ---------------------------------------------------------------------------
# sc-27: WARNING emitted when >6d bucket non-empty
# ---------------------------------------------------------------------------

def test_retention_warning_when_gt6d_nonempty(capsys):
    """sc-27: >6d bucket non-empty → WARNING line on stderr."""
    from tools.dlq_quality.reports import AgeAgg, QualityResult, retention_warning

    agg = AgeAgg()
    ts_gt6d = _NOW_MS - 550_000_000  # ~6.4 days
    agg.add("dlq.canonical.training_event", ts_gt6d, _NOW_MS)

    result = QualityResult(
        error_type={},
        age=agg.counts,
        age_extremes=agg.extremes,
        triage_fix={},
        triage_origin={},
        samples={},
    )
    retention_warning(result)
    captured = capsys.readouterr()
    assert "WARNING" in captured.err


# ---------------------------------------------------------------------------
# sc-28: WARNING emitted when expired bucket non-empty
# ---------------------------------------------------------------------------

def test_retention_warning_when_expired_nonempty(capsys):
    """sc-28: expired bucket non-empty → WARNING line on stderr."""
    from tools.dlq_quality.reports import AgeAgg, QualityResult, retention_warning

    agg = AgeAgg()
    ts_expired = _NOW_MS - (SEVEN_DAYS + 1000)  # past 7 days
    agg.add("dlq.canonical.training_event", ts_expired, _NOW_MS)

    result = QualityResult(
        error_type={},
        age=agg.counts,
        age_extremes=agg.extremes,
        triage_fix={},
        triage_origin={},
        samples={},
    )
    retention_warning(result)
    captured = capsys.readouterr()
    assert "WARNING" in captured.err


# ---------------------------------------------------------------------------
# sc-29: No WARNING when both >6d and expired buckets empty
# ---------------------------------------------------------------------------

def test_retention_warning_absent_when_both_empty(capsys):
    """sc-29: all messages in safe buckets → no WARNING on stderr."""
    from tools.dlq_quality.reports import AgeAgg, QualityResult, retention_warning

    agg = AgeAgg()
    ts_safe = _NOW_MS - 3_600_000  # 1 hour ago → <1d
    agg.add("dlq.canonical.training_event", ts_safe, _NOW_MS)

    result = QualityResult(
        error_type={},
        age=agg.counts,
        age_extremes=agg.extremes,
        triage_fix={},
        triage_origin={},
        samples={},
    )
    retention_warning(result)
    captured = capsys.readouterr()
    assert "WARNING" not in captured.err
