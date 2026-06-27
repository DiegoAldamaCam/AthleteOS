"""Unit tests for the wellness CSV parser (PR-W1).

Mirrors tests/unit/test_strength_parser.py structure exactly.

Covers ``parse_row`` (single-row) and ``parse_csv`` (skip-and-collect batch).
Spec scenarios: W1-1..W1-4.

Wellness CSV columns (all except athlete_id and date are optional/nullable):
  athlete_id, date, hrv, sleep_hours, resting_hr, steps, body_weight_kg,
  energy, soreness, mood, stress, perceived_recovery
"""

from __future__ import annotations

import pytest

from ingestion.wellness.parser import (
    MalformedRowError,
    ParseResult,
    WellnessRecord,
    parse_csv,
    parse_row,
)


def _valid_row() -> dict[str, str]:
    """A well-formed wellness CSV row with all fields present."""
    return {
        "athlete_id": "A1",
        "date": "2025-03-01",
        "hrv": "65.0",
        "sleep_hours": "7.5",
        "resting_hr": "52",
        "steps": "9000",
        "body_weight_kg": "78.5",
        "energy": "7",
        "soreness": "3",
        "mood": "8",
        "stress": "4",
        "perceived_recovery": "8",
    }


# --- parse_row: happy path (W1-1) ---


def test_parse_row_valid_returns_typed_record():
    """W1-1: A valid row is coerced to a WellnessRecord with correct types."""
    record = parse_row(_valid_row())

    assert isinstance(record, WellnessRecord)
    assert record.athlete_id == "A1"
    assert record.date == "2025-03-01"
    assert record.hrv == 65.0            # float
    assert record.sleep_hours == 7.5     # float
    assert record.resting_hr == 52       # int
    assert record.steps == 9000          # int
    assert record.body_weight_kg == 78.5  # float
    assert record.energy == 7            # int
    assert record.soreness == 3          # int
    assert record.mood == 8              # int
    assert record.stress == 4            # int
    assert record.perceived_recovery == 8  # int


# --- parse_row: optional fields null (W1-2) ---


def test_parse_row_all_optional_fields_absent_become_none():
    """W1-2: All nullable fields absent/empty -> None, no error raised."""
    row = {
        "athlete_id": "A1",
        "date": "2025-03-01",
        "hrv": "",
        "sleep_hours": "",
        "resting_hr": "",
        "steps": "",
        "body_weight_kg": "",
        "energy": "",
        "soreness": "",
        "mood": "",
        "stress": "",
        "perceived_recovery": "",
    }

    record = parse_row(row)

    assert record.hrv is None
    assert record.sleep_hours is None
    assert record.resting_hr is None
    assert record.steps is None
    assert record.body_weight_kg is None
    assert record.energy is None
    assert record.soreness is None
    assert record.mood is None
    assert record.stress is None
    assert record.perceived_recovery is None
    # required fields still populated
    assert record.athlete_id == "A1"
    assert record.date == "2025-03-01"


def test_parse_row_optional_fields_entirely_missing_become_none():
    """W1-2 triangulation: fields completely absent from dict -> None."""
    row = {"athlete_id": "A1", "date": "2025-03-01"}

    record = parse_row(row)

    assert record.hrv is None
    assert record.sleep_hours is None
    assert record.perceived_recovery is None


# --- parse_row: malformed rows raise ---


def test_parse_row_missing_athlete_id_raises():
    """W1-3: A row missing athlete_id raises MalformedRowError."""
    row = _valid_row()
    del row["athlete_id"]

    with pytest.raises(MalformedRowError):
        parse_row(row)


def test_parse_row_empty_athlete_id_raises():
    """W1-3: An empty athlete_id raises MalformedRowError."""
    row = _valid_row()
    row["athlete_id"] = ""

    with pytest.raises(MalformedRowError):
        parse_row(row)


def test_parse_row_missing_date_raises():
    """W1-3: A row missing date raises MalformedRowError."""
    row = _valid_row()
    del row["date"]

    with pytest.raises(MalformedRowError):
        parse_row(row)


def test_parse_row_unparseable_hrv_raises():
    """A non-numeric hrv value raises MalformedRowError."""
    row = _valid_row()
    row["hrv"] = "not-a-number"

    with pytest.raises(MalformedRowError):
        parse_row(row)


def test_parse_row_unparseable_resting_hr_raises():
    """A non-integer resting_hr value raises MalformedRowError."""
    row = _valid_row()
    row["resting_hr"] = "52.5"  # float not valid for int field

    with pytest.raises(MalformedRowError):
        parse_row(row)


# --- parse_csv: skip-and-collect (W1-3, W1-4) ---


def test_parse_csv_collects_valid_records_and_skips_errors():
    """W1-3: parse_csv skips malformed rows without aborting the batch."""
    good = _valid_row()
    good2 = _valid_row()
    good2["athlete_id"] = "A2"
    bad_missing_athlete = _valid_row()
    del bad_missing_athlete["athlete_id"]
    bad_hrv = _valid_row()
    bad_hrv["hrv"] = "bad"

    result = parse_csv([good, bad_missing_athlete, good2, bad_hrv])

    assert isinstance(result, ParseResult)
    assert len(result.records) == 2
    assert {r.athlete_id for r in result.records} == {"A1", "A2"}
    assert len(result.errors) == 2
    assert all(isinstance(e, MalformedRowError) for e in result.errors)


def test_parse_csv_empty_input_yields_empty_result():
    """W1-4: An empty row stream produces zero records and zero errors."""
    result = parse_csv([])

    assert result.records == []
    assert result.errors == []
