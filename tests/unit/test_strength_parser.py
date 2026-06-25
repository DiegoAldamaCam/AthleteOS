"""Unit tests for the Strong CSV parser (PR2, task 3.1/3.2).

The parser converts a raw Strong CSV row (a column-name -> string-cell mapping)
into a typed ``StrengthSetRecord``. Per the event-contracts spec "Raw Topic JSON
Shape", the raw layer carries source fields verbatim - so the parser only
coeres types and validates required fields; it does NOT normalize, derive, or
compute anything (in particular session_load is a canonicalization-layer concern,
not the ingestion connector's job).

Column names follow the spec's Strong CSV source-field mapping table:
  athlete_id, workout_id, exercise_id, set_number, reps, weight_kg, rpe, rir,
  timestamp
"""

from __future__ import annotations

import pytest

from ingestion.strength.parser import (
    MalformedRowError,
    ParseResult,
    StrengthSetRecord,
    parse_csv,
    parse_row,
)


def _valid_row() -> dict[str, str]:
    """A well-formed Strong CSV row using the spec source-field column names."""
    return {
        "athlete_id": "athlete-123",
        "workout_id": "w-001",
        "exercise_id": "bench-press",
        "set_number": "1",
        "reps": "8",
        "weight_kg": "100",
        "rpe": "8.5",
        "rir": "2",
        "timestamp": "2024-01-15T10:30:00",
    }


# --- parse_row: happy path + triangulation ---


def test_parse_row_valid_returns_typed_record():
    """A valid row is coerced to a StrengthSetRecord with correct types."""
    record = parse_row(_valid_row())

    assert isinstance(record, StrengthSetRecord)
    assert record.athlete_id == "athlete-123"
    assert record.workout_id == "w-001"
    assert record.exercise_id == "bench-press"
    assert record.set_number == 1          # int
    assert record.reps == 8                # int
    assert record.weight_kg == 100.0       # float
    assert record.rpe == 8.5               # float (nullable but present)
    assert record.rir == 2.0               # float (nullable but present)
    # timestamp is kept verbatim as the source string (raw layer = source fidelity)
    assert record.timestamp == "2024-01-15T10:30:00"


def test_parse_row_optional_rpe_rir_absent_become_none():
    """rpe and rir are nullable; a row omitting them yields None, not an error."""
    row = _valid_row()
    del row["rpe"]
    del row["rir"]

    record = parse_row(row)

    assert record.rpe is None
    assert record.rir is None
    # required fields still populated
    assert record.reps == 8
    assert record.weight_kg == 100.0


def test_parse_row_timestamp_with_space_separator_is_accepted():
    """Strong exports often use 'YYYY-MM-DD HH:MM:SS'; the parser accepts it
    and keeps the original string verbatim in the record."""
    row = _valid_row()
    row["timestamp"] = "2024-01-15 10:30:00"

    record = parse_row(row)

    assert record.timestamp == "2024-01-15 10:30:00"


# --- parse_row: malformed rows raise ---


def test_parse_row_missing_required_reps_raises():
    """A row missing a required field raises MalformedRowError."""
    row = _valid_row()
    del row["reps"]

    with pytest.raises(MalformedRowError):
        parse_row(row)


def test_parse_row_unparseable_weight_raises():
    """A non-numeric weight_kg raises MalformedRowError."""
    row = _valid_row()
    row["weight_kg"] = "heavy"

    with pytest.raises(MalformedRowError):
        parse_row(row)


def test_parse_row_unparseable_timestamp_raises():
    """An unparseable timestamp raises MalformedRowError (required field)."""
    row = _valid_row()
    row["timestamp"] = "not-a-date"

    with pytest.raises(MalformedRowError):
        parse_row(row)


def test_parse_row_empty_athlete_id_raises():
    """An empty athlete_id (partition key) is malformed and must raise."""
    row = _valid_row()
    row["athlete_id"] = ""

    with pytest.raises(MalformedRowError):
        parse_row(row)


# --- parse_csv: skip-and-collect over a stream of rows ---


def test_parse_csv_collects_valid_records_and_errors():
    """parse_csv walks an iterable of rows, collecting valid records and
    recording MalformedRowError for each bad row without aborting the batch."""
    good = _valid_row()
    good2 = _valid_row()
    good2["set_number"] = "2"
    bad_missing_reps = _valid_row()
    del bad_missing_reps["reps"]
    bad_weight = _valid_row()
    bad_weight["weight_kg"] = "nope"

    result = parse_csv([good, bad_missing_reps, good2, bad_weight])

    assert isinstance(result, ParseResult)
    assert len(result.records) == 2
    assert {r.set_number for r in result.records} == {1, 2}
    assert len(result.errors) == 2
    assert all(isinstance(e, MalformedRowError) for e in result.errors)


def test_parse_csv_empty_input_yields_empty_result():
    """An empty row stream produces an empty ParseResult (no errors, no records)."""
    result = parse_csv([])

    assert result.records == []
    assert result.errors == []
