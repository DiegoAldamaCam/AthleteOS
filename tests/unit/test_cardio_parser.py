"""Unit tests for the cardio CSV parser (PR-C1).

Mirrors tests/unit/test_wellness_parser.py structure exactly.

Covers ``parse_row`` (single-row) and ``parse_csv`` (skip-and-collect batch).
Spec scenarios: sc-1..sc-8.

Cardio CSV columns:
  Required: athlete_id (str), activity_type (str), duration_sec (int), timestamp (ISO datetime str)
  Optional/nullable: distance_km (float | None), avg_hr (int | None), tss (float | None)
"""

from __future__ import annotations

import pytest

from ingestion.cardio.parser import (
    CardioRecord,
    MalformedRowError,
    ParseResult,
    parse_csv,
    parse_row,
)


def _valid_row() -> dict[str, str]:
    """A well-formed cardio CSV row with all fields present."""
    return {
        "athlete_id": "A1",
        "activity_type": "Run",
        "duration_sec": "3600",
        "timestamp": "2025-06-01T10:00:00",
        "distance_km": "10.5",
        "avg_hr": "145",
        "tss": "85.0",
    }


# --- sc-1: Happy path — full row parsed ---


def test_parse_row_valid_returns_typed_record():
    """sc-1: A valid row is coerced to a CardioRecord with correct types."""
    record = parse_row(_valid_row())

    assert isinstance(record, CardioRecord)
    assert record.athlete_id == "A1"
    assert record.activity_type == "Run"
    assert record.duration_sec == 3600
    assert record.timestamp == "2025-06-01T10:00:00"
    assert record.distance_km == 10.5      # float
    assert record.avg_hr == 145            # int
    assert record.tss == 85.0             # float


def test_parse_row_full_row_types_are_correct():
    """sc-1 triangulation: numeric coercions are correct for all typed fields."""
    row = {
        "athlete_id": "B2",
        "activity_type": "Ride",
        "duration_sec": "7200",
        "timestamp": "2025-07-15T08:30:00",
        "distance_km": "50.0",
        "avg_hr": "160",
        "tss": "120.5",
    }
    record = parse_row(row)

    assert record.duration_sec == 7200
    assert isinstance(record.duration_sec, int)
    assert record.distance_km == 50.0
    assert isinstance(record.distance_km, float)
    assert record.avg_hr == 160
    assert isinstance(record.avg_hr, int)
    assert record.tss == 120.5
    assert isinstance(record.tss, float)


# --- sc-2: Nullable fields absent → None, no error ---


def test_parse_row_nullable_fields_empty_become_none():
    """sc-2: Empty nullable fields -> None, no MalformedRowError."""
    row = {
        "athlete_id": "A1",
        "activity_type": "Run",
        "duration_sec": "3600",
        "timestamp": "2025-06-01T10:00:00",
        "distance_km": "",
        "avg_hr": "",
        "tss": "",
    }

    record = parse_row(row)

    assert record.distance_km is None
    assert record.avg_hr is None
    assert record.tss is None
    # required fields still populated
    assert record.athlete_id == "A1"
    assert record.activity_type == "Run"
    assert record.duration_sec == 3600


def test_parse_row_nullable_fields_entirely_absent_become_none():
    """sc-2 triangulation: nullable fields completely absent from dict -> None."""
    row = {
        "athlete_id": "A1",
        "activity_type": "Swim",
        "duration_sec": "1800",
        "timestamp": "2025-06-01T09:00:00",
    }

    record = parse_row(row)

    assert record.distance_km is None
    assert record.avg_hr is None
    assert record.tss is None


# --- sc-3: Missing athlete_id → collect error, skip row ---


def test_parse_csv_missing_athlete_id_collects_error():
    """sc-3: A row with missing athlete_id goes to errors, not records."""
    good = _valid_row()
    bad = _valid_row()
    del bad["athlete_id"]

    result = parse_csv([good, bad])

    assert len(result.records) == 1
    assert result.records[0].athlete_id == "A1"
    assert len(result.errors) == 1
    assert isinstance(result.errors[0], MalformedRowError)


def test_parse_csv_empty_athlete_id_collects_error():
    """sc-3 triangulation: empty athlete_id treated as missing."""
    bad = _valid_row()
    bad["athlete_id"] = ""

    result = parse_csv([bad])

    assert len(result.records) == 0
    assert len(result.errors) == 1


# --- sc-4: Missing activity_type → collect error, skip row ---


def test_parse_csv_missing_activity_type_collects_error():
    """sc-4: A row with missing activity_type goes to errors."""
    good = _valid_row()
    bad = _valid_row()
    del bad["activity_type"]

    result = parse_csv([good, bad])

    assert len(result.records) == 1
    assert len(result.errors) == 1


def test_parse_csv_empty_activity_type_collects_error():
    """sc-4 triangulation: empty activity_type treated as missing."""
    bad = _valid_row()
    bad["activity_type"] = ""

    result = parse_csv([bad])

    assert len(result.errors) == 1
    assert len(result.records) == 0


# --- sc-5: Missing duration_sec → collect error, skip row ---


def test_parse_csv_missing_duration_sec_collects_error():
    """sc-5: A row with missing duration_sec goes to errors."""
    good = _valid_row()
    bad = _valid_row()
    del bad["duration_sec"]

    result = parse_csv([good, bad])

    assert len(result.records) == 1
    assert len(result.errors) == 1
    assert isinstance(result.errors[0], MalformedRowError)


def test_parse_csv_empty_duration_sec_collects_error():
    """sc-5 triangulation: empty duration_sec treated as missing."""
    bad = _valid_row()
    bad["duration_sec"] = ""

    result = parse_csv([bad])

    assert len(result.errors) == 1


# --- sc-6: Missing timestamp → collect error, skip row ---


def test_parse_csv_missing_timestamp_collects_error():
    """sc-6: A row with missing timestamp goes to errors."""
    good = _valid_row()
    bad = _valid_row()
    del bad["timestamp"]

    result = parse_csv([good, bad])

    assert len(result.records) == 1
    assert len(result.errors) == 1


def test_parse_csv_start_date_alias_accepted():
    """sc-6 / design: Strava export variant 'start_date' column is accepted as timestamp."""
    row = {
        "athlete_id": "A1",
        "activity_type": "Run",
        "duration_sec": "3600",
        "start_date": "2025-06-01T10:00:00",
        "distance_km": "10.0",
        "avg_hr": "145",
        "tss": "85.0",
    }

    record = parse_row(row)

    assert record.timestamp == "2025-06-01T10:00:00"


# --- sc-7: Empty CSV → zero records, zero errors ---


def test_parse_csv_empty_input_yields_empty_result():
    """sc-7: An empty row stream produces zero records and zero errors."""
    result = parse_csv([])

    assert result.records == []
    assert result.errors == []


def test_parse_csv_collects_valid_and_skips_errors_mixed_batch():
    """sc-7 triangulation: multi-row batch with mix of good and bad rows."""
    good1 = _valid_row()
    good2 = _valid_row()
    good2["athlete_id"] = "A2"
    bad_no_athlete = _valid_row()
    del bad_no_athlete["athlete_id"]
    bad_no_ts = _valid_row()
    del bad_no_ts["timestamp"]

    result = parse_csv([good1, bad_no_athlete, good2, bad_no_ts])

    assert isinstance(result, ParseResult)
    assert len(result.records) == 2
    assert {r.athlete_id for r in result.records} == {"A1", "A2"}
    assert len(result.errors) == 2
    assert all(isinstance(e, MalformedRowError) for e in result.errors)


# --- sc-8: Unknown activity_type accepted — no DLQ ---


def test_parse_row_unknown_activity_type_accepted():
    """sc-8: Unknown activity_type 'UltraMarathon' is accepted as-is, no error."""
    row = _valid_row()
    row["activity_type"] = "UltraMarathon"

    record = parse_row(row)

    assert record.activity_type == "UltraMarathon"


def test_parse_row_another_unknown_activity_type_accepted():
    """sc-8 triangulation: 'KettlebellSwing' is also accepted without error."""
    row = _valid_row()
    row["activity_type"] = "KettlebellSwing"

    record = parse_row(row)

    assert record.activity_type == "KettlebellSwing"
