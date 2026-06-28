"""Unit tests for the nutrition CSV parser (PR-N1).

Mirrors tests/unit/test_recovery_parser.py structure.

Covers ``parse_row`` (single-row) and ``parse_csv`` (skip-and-collect batch).
Spec scenarios: sc-1..sc-7.

Nutrition CSV columns:
  Required: athlete_id (str), date (ISO date str)
  Optional/nullable: calories (int), protein_g (float), carbs_g (float),
                     fat_g (float), adherence_score (float)

Note: ``calories`` uses ``_to_optional_int`` — non-integer float strings raise
MalformedRowError (sc-7). All other nullable fields are float.
"""

from __future__ import annotations

import pytest

from ingestion.nutrition.parser import (
    MalformedRowError,
    ParseResult,
    NutritionRecord,
    parse_csv,
    parse_row,
)


def _valid_row() -> dict[str, str]:
    """A well-formed nutrition CSV row with all fields present."""
    return {
        "athlete_id": "A1",
        "date": "2025-06-01",
        "calories": "2400",
        "protein_g": "150.0",
        "carbs_g": "300.0",
        "fat_g": "80.0",
        "adherence_score": "0.85",
    }


# --- parse_row: happy path (sc-1) ---


def test_parse_row_valid_returns_typed_record():
    """sc-1: A valid row is coerced to a NutritionRecord with all fields correctly typed."""
    record = parse_row(_valid_row())

    assert isinstance(record, NutritionRecord)
    assert record.athlete_id == "A1"
    assert record.date == "2025-06-01"
    assert record.calories == 2400          # int
    assert record.protein_g == 150.0        # float
    assert record.carbs_g == 300.0          # float
    assert record.fat_g == 80.0             # float
    assert record.adherence_score == 0.85   # float


def test_parse_row_returns_frozen_record():
    """sc-1 triangulation: NutritionRecord is frozen (immutable dataclass)."""
    record = parse_row(_valid_row())
    with pytest.raises((AttributeError, TypeError)):
        record.athlete_id = "mutated"  # type: ignore[misc]


def test_parse_row_calories_is_int_not_float():
    """sc-1 triangulation: calories must be typed as int, not float."""
    record = parse_row(_valid_row())
    assert isinstance(record.calories, int)


# --- parse_row: all nullable fields absent -> None, no error (sc-2) ---


def test_parse_row_all_nullable_fields_empty_become_none():
    """sc-2: All nullable fields empty strings -> None, no error raised."""
    row = {
        "athlete_id": "A1",
        "date": "2025-06-01",
        "calories": "",
        "protein_g": "",
        "carbs_g": "",
        "fat_g": "",
        "adherence_score": "",
    }

    record = parse_row(row)

    assert record.calories is None
    assert record.protein_g is None
    assert record.carbs_g is None
    assert record.fat_g is None
    assert record.adherence_score is None
    # required fields still populated
    assert record.athlete_id == "A1"
    assert record.date == "2025-06-01"


def test_parse_row_nullable_fields_entirely_missing_become_none():
    """sc-2 triangulation: nullable fields completely absent from dict -> None."""
    row = {"athlete_id": "A1", "date": "2025-06-01"}

    record = parse_row(row)

    assert record.calories is None
    assert record.protein_g is None
    assert record.carbs_g is None
    assert record.fat_g is None
    assert record.adherence_score is None


# --- parse_csv: missing athlete_id -> collect error, skip row (sc-3) ---


def test_parse_csv_missing_athlete_id_collected_not_raised():
    """sc-3: Row missing athlete_id is added to errors; parsing continues."""
    good = _valid_row()
    bad_no_id = _valid_row()
    del bad_no_id["athlete_id"]

    result = parse_csv([good, bad_no_id])

    assert len(result.records) == 1
    assert result.records[0].athlete_id == "A1"
    assert len(result.errors) == 1
    assert isinstance(result.errors[0], MalformedRowError)


def test_parse_csv_empty_athlete_id_collected_not_raised():
    """sc-3 triangulation: empty athlete_id is also collected as error."""
    good = _valid_row()
    bad_empty_id = _valid_row()
    bad_empty_id["athlete_id"] = ""

    result = parse_csv([good, bad_empty_id])

    assert len(result.records) == 1
    assert len(result.errors) == 1


# --- parse_csv: missing date -> collect error, skip row (sc-4) ---


def test_parse_csv_missing_date_collected_not_raised():
    """sc-4: Row missing date is added to errors; parsing continues."""
    good = _valid_row()
    bad_no_date = _valid_row()
    del bad_no_date["date"]

    result = parse_csv([good, bad_no_date])

    assert len(result.records) == 1
    assert len(result.errors) == 1
    assert isinstance(result.errors[0], MalformedRowError)


def test_parse_csv_empty_date_collected_not_raised():
    """sc-4 triangulation: empty date string is also collected as error."""
    good = _valid_row()
    bad_empty_date = _valid_row()
    bad_empty_date["date"] = ""
    bad_empty_date["athlete_id"] = "A2"

    result = parse_csv([good, bad_empty_date])

    assert len(result.records) == 1
    assert len(result.errors) == 1


# --- parse_csv: all-null data row accepted (sc-5) ---


def test_parse_csv_all_null_data_fields_accepted():
    """sc-5: Row with valid athlete_id/date but all 5 data fields empty is accepted."""
    null_data_row = {
        "athlete_id": "A1",
        "date": "2025-06-01",
        "calories": "",
        "protein_g": "",
        "carbs_g": "",
        "fat_g": "",
        "adherence_score": "",
    }

    result = parse_csv([null_data_row])

    assert len(result.records) == 1
    assert len(result.errors) == 0
    record = result.records[0]
    assert record.calories is None
    assert record.protein_g is None
    assert record.carbs_g is None
    assert record.fat_g is None
    assert record.adherence_score is None


def test_parse_csv_all_null_data_fields_not_in_errors():
    """sc-5 triangulation: all-null row goes to records, not errors."""
    null_data_row = {"athlete_id": "A2", "date": "2025-06-02"}

    result = parse_csv([null_data_row])

    assert len(result.errors) == 0
    assert len(result.records) == 1
    assert result.records[0].athlete_id == "A2"


# --- parse_csv: empty CSV -> zero records, zero errors (sc-6) ---


def test_parse_csv_empty_input_yields_empty_result():
    """sc-6: Empty row stream produces zero records and zero errors."""
    result = parse_csv([])

    assert result.records == []
    assert result.errors == []


def test_parse_csv_returns_parse_result_instance():
    """sc-6 triangulation: parse_csv always returns a ParseResult."""
    result = parse_csv([])

    assert isinstance(result, ParseResult)


# --- parse_row: fractional calories -> MalformedRowError (sc-7) ---


def test_parse_row_fractional_calories_raises():
    """sc-7: calories='2400.5' (non-integer float string) MUST raise MalformedRowError."""
    row = _valid_row()
    row["calories"] = "2400.5"

    with pytest.raises(MalformedRowError):
        parse_row(row)


def test_parse_csv_fractional_calories_collected_as_error():
    """sc-7 triangulation: fractional calories is collected in errors, not records."""
    good = _valid_row()
    bad_frac = _valid_row()
    bad_frac["athlete_id"] = "A2"
    bad_frac["calories"] = "1999.9"

    result = parse_csv([good, bad_frac])

    assert len(result.records) == 1
    assert result.records[0].athlete_id == "A1"
    assert len(result.errors) == 1
    assert isinstance(result.errors[0], MalformedRowError)


# --- parse_row: error propagation (additional coverage) ---


def test_parse_row_missing_athlete_id_raises():
    """parse_row itself raises MalformedRowError for missing athlete_id."""
    row = _valid_row()
    del row["athlete_id"]

    with pytest.raises(MalformedRowError):
        parse_row(row)


def test_parse_row_missing_date_raises():
    """parse_row itself raises MalformedRowError for missing date."""
    row = _valid_row()
    del row["date"]

    with pytest.raises(MalformedRowError):
        parse_row(row)


def test_parse_csv_collects_multiple_errors_continues_parsing():
    """Skip-and-collect: multiple bad rows don't abort the batch."""
    good1 = _valid_row()
    good2 = _valid_row()
    good2["athlete_id"] = "A2"
    bad_no_id = _valid_row()
    del bad_no_id["athlete_id"]
    bad_no_date = _valid_row()
    bad_no_date["athlete_id"] = "A3"
    del bad_no_date["date"]

    result = parse_csv([good1, bad_no_id, good2, bad_no_date])

    assert len(result.records) == 2
    assert {r.athlete_id for r in result.records} == {"A1", "A2"}
    assert len(result.errors) == 2
    assert all(isinstance(e, MalformedRowError) for e in result.errors)
