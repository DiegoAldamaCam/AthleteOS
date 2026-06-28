"""Unit tests for the planning multi-format parser (PR-PL1).

Mirrors tests/unit/test_wellness_parser.py structure.

Covers parse_yaml (PL1-1), parse_json (PL1-2), parse_csv (PL1-3),
skip-and-collect validation (PL1-4, PL1-5, PL1-6), and wvt round-trip (PL1-7).

Spec scenarios: PL1-1..PL1-7.
"""

from __future__ import annotations

import json

import pytest

from ingestion.planning.parser import (
    MalformedRowError,
    ParseResult,
    PlanningRecord,
    parse_csv,
    parse_json,
    parse_yaml,
)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_VALID_YAML = """\
athlete_id: A1
block_id: BLK-001
goal: Build aerobic base
start_date: "2025-06-01"
end_date: "2025-08-31"
planned_sessions_per_week: 5
weekly_volume_targets:
  strength: 3
  cardio: 2
"""

_VALID_JSON = json.dumps(
    {
        "athlete_id": "A1",
        "block_id": "BLK-001",
        "goal": "Build aerobic base",
        "start_date": "2025-06-01",
        "end_date": "2025-08-31",
        "planned_sessions_per_week": 5,
        "weekly_volume_targets": {"strength": 3, "cardio": 2},
    }
)

# CSV header + one valid row
_CSV_HEADER = "athlete_id,block_id,goal,start_date,end_date,planned_sessions_per_week,weekly_volume_targets"
_CSV_VALID_ROW = (
    'A1,BLK-001,Build aerobic base,2025-06-01,2025-08-31,5,'
    '"{""strength"": 3, ""cardio"": 2}"'
)

# Expected wvt dict for all format tests
_WVT_DICT = {"strength": 3, "cardio": 2}
_WVT_STR = json.dumps(_WVT_DICT)


# ---------------------------------------------------------------------------
# Task 1.1 — PlanningRecord dataclass structure
# ---------------------------------------------------------------------------


def test_planning_record_dataclass_has_all_required_fields():
    """PlanningRecord must be constructible with all 7 required fields."""
    record = PlanningRecord(
        athlete_id="A1",
        block_id="BLK-001",
        goal="Build aerobic base",
        start_date="2025-06-01",
        end_date="2025-08-31",
        planned_sessions_per_week=5,
        weekly_volume_targets='{"strength": 3, "cardio": 2}',
    )

    assert record.athlete_id == "A1"
    assert record.block_id == "BLK-001"
    assert record.goal == "Build aerobic base"
    assert record.start_date == "2025-06-01"
    assert record.end_date == "2025-08-31"
    assert record.planned_sessions_per_week == 5
    assert record.weekly_volume_targets == '{"strength": 3, "cardio": 2}'


def test_planning_record_is_frozen():
    """PlanningRecord must be immutable (frozen dataclass)."""
    record = PlanningRecord(
        athlete_id="A1",
        block_id="BLK-001",
        goal="Build aerobic base",
        start_date="2025-06-01",
        end_date="2025-08-31",
        planned_sessions_per_week=5,
        weekly_volume_targets="{}",
    )

    with pytest.raises((AttributeError, TypeError)):
        record.athlete_id = "X"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Task 1.3 — parse_yaml happy path (PL1-1)
# ---------------------------------------------------------------------------


def test_parse_yaml_happy_path_returns_planning_record():
    """PL1-1: parse_yaml of a valid YAML block returns a PlanningRecord."""
    result = parse_yaml(_VALID_YAML)

    assert isinstance(result, ParseResult)
    assert len(result.records) == 1
    assert len(result.errors) == 0

    record = result.records[0]
    assert isinstance(record, PlanningRecord)
    assert record.athlete_id == "A1"
    assert record.block_id == "BLK-001"
    assert record.goal == "Build aerobic base"
    assert record.start_date == "2025-06-01"
    assert record.end_date == "2025-08-31"
    assert record.planned_sessions_per_week == 5


def test_parse_yaml_weekly_volume_targets_is_json_string():
    """PL1-1: weekly_volume_targets in YAML is dict at source -> JSON string in record."""
    result = parse_yaml(_VALID_YAML)

    record = result.records[0]
    # Must be a string, not a dict
    assert isinstance(record.weekly_volume_targets, str)
    # Must round-trip to the original dict
    assert json.loads(record.weekly_volume_targets) == _WVT_DICT


# ---------------------------------------------------------------------------
# Task 1.3 — parse_json happy path (PL1-2)
# ---------------------------------------------------------------------------


def test_parse_json_happy_path_returns_planning_record():
    """PL1-2: parse_json of a valid JSON string returns a PlanningRecord."""
    result = parse_json(_VALID_JSON)

    assert isinstance(result, ParseResult)
    assert len(result.records) == 1
    assert len(result.errors) == 0

    record = result.records[0]
    assert record.athlete_id == "A1"
    assert record.block_id == "BLK-001"
    assert record.planned_sessions_per_week == 5


def test_parse_json_semantically_equivalent_to_yaml():
    """PL1-2: JSON parse result must be semantically equivalent to YAML parse."""
    yaml_result = parse_yaml(_VALID_YAML)
    json_result = parse_json(_VALID_JSON)

    yaml_record = yaml_result.records[0]
    json_record = json_result.records[0]

    assert yaml_record.athlete_id == json_record.athlete_id
    assert yaml_record.block_id == json_record.block_id
    assert yaml_record.start_date == json_record.start_date
    assert yaml_record.end_date == json_record.end_date
    assert yaml_record.planned_sessions_per_week == json_record.planned_sessions_per_week
    # wvt must be the same serialized form
    assert json.loads(yaml_record.weekly_volume_targets) == json.loads(
        json_record.weekly_volume_targets
    )


# ---------------------------------------------------------------------------
# Task 1.5 — parse_csv happy path (PL1-3)
# ---------------------------------------------------------------------------


def test_parse_csv_happy_path_returns_planning_record():
    """PL1-3: parse_csv of a valid CSV row returns a PlanningRecord."""
    rows = [
        {
            "athlete_id": "A1",
            "block_id": "BLK-001",
            "goal": "Build aerobic base",
            "start_date": "2025-06-01",
            "end_date": "2025-08-31",
            "planned_sessions_per_week": "5",
            "weekly_volume_targets": '{"strength": 3, "cardio": 2}',
        }
    ]
    result = parse_csv(rows)

    assert isinstance(result, ParseResult)
    assert len(result.records) == 1
    assert len(result.errors) == 0

    record = result.records[0]
    assert record.athlete_id == "A1"
    assert record.planned_sessions_per_week == 5


def test_parse_csv_wvt_json_string_validated():
    """PL1-3: wvt as a valid JSON string in CSV is validated and stored as-is (or re-serialized)."""
    rows = [
        {
            "athlete_id": "A1",
            "block_id": "BLK-001",
            "goal": "Goal",
            "start_date": "2025-06-01",
            "end_date": "2025-08-31",
            "planned_sessions_per_week": "5",
            "weekly_volume_targets": '{"strength": 3, "cardio": 2}',
        }
    ]
    result = parse_csv(rows)

    record = result.records[0]
    assert isinstance(record.weekly_volume_targets, str)
    assert json.loads(record.weekly_volume_targets) == _WVT_DICT


def test_parse_csv_semantically_equivalent_to_yaml():
    """PL1-3: CSV parse result is semantically equivalent to YAML parse."""
    yaml_result = parse_yaml(_VALID_YAML)
    csv_rows = [
        {
            "athlete_id": "A1",
            "block_id": "BLK-001",
            "goal": "Build aerobic base",
            "start_date": "2025-06-01",
            "end_date": "2025-08-31",
            "planned_sessions_per_week": "5",
            "weekly_volume_targets": '{"strength": 3, "cardio": 2}',
        }
    ]
    csv_result = parse_csv(csv_rows)

    yaml_record = yaml_result.records[0]
    csv_record = csv_result.records[0]

    assert yaml_record.athlete_id == csv_record.athlete_id
    assert yaml_record.block_id == csv_record.block_id
    assert yaml_record.planned_sessions_per_week == csv_record.planned_sessions_per_week
    assert json.loads(yaml_record.weekly_volume_targets) == json.loads(
        csv_record.weekly_volume_targets
    )


# ---------------------------------------------------------------------------
# Task 1.7 — skip-and-collect validation errors (PL1-4, PL1-5, PL1-6)
# ---------------------------------------------------------------------------


def test_parse_yaml_end_date_before_start_date_skip_and_collect():
    """PL1-4: end_date < start_date is collected as MalformedRowError."""
    bad_yaml = """\
athlete_id: A1
block_id: BLK-001
goal: Goal
start_date: "2025-08-31"
end_date: "2025-06-01"
planned_sessions_per_week: 5
weekly_volume_targets:
  strength: 3
"""
    result = parse_yaml(bad_yaml)

    assert len(result.records) == 0
    assert len(result.errors) == 1
    assert isinstance(result.errors[0], MalformedRowError)


def test_parse_json_end_date_before_start_date_skip_and_collect():
    """PL1-4 triangulation: same rule applies for parse_json."""
    bad_json = json.dumps(
        {
            "athlete_id": "A1",
            "block_id": "BLK-001",
            "goal": "Goal",
            "start_date": "2025-08-31",
            "end_date": "2025-06-01",
            "planned_sessions_per_week": 5,
            "weekly_volume_targets": {"strength": 3},
        }
    )
    result = parse_json(bad_json)

    assert len(result.records) == 0
    assert len(result.errors) == 1


def test_parse_csv_planned_sessions_zero_skip_and_collect():
    """PL1-5: planned_sessions_per_week == 0 is collected as MalformedRowError."""
    rows = [
        {
            "athlete_id": "A1",
            "block_id": "BLK-001",
            "goal": "Goal",
            "start_date": "2025-06-01",
            "end_date": "2025-08-31",
            "planned_sessions_per_week": "0",
            "weekly_volume_targets": '{"strength": 3}',
        }
    ]
    result = parse_csv(rows)

    assert len(result.records) == 0
    assert len(result.errors) == 1
    assert isinstance(result.errors[0], MalformedRowError)


def test_parse_csv_planned_sessions_negative_skip_and_collect():
    """PL1-5 triangulation: negative sessions also collected as error."""
    rows = [
        {
            "athlete_id": "A1",
            "block_id": "BLK-001",
            "goal": "Goal",
            "start_date": "2025-06-01",
            "end_date": "2025-08-31",
            "planned_sessions_per_week": "-1",
            "weekly_volume_targets": '{"strength": 3}',
        }
    ]
    result = parse_csv(rows)

    assert len(result.records) == 0
    assert len(result.errors) == 1


def test_parse_csv_wvt_not_valid_json_skip_and_collect():
    """PL1-6: wvt that cannot be parsed as JSON is collected as MalformedRowError."""
    rows = [
        {
            "athlete_id": "A1",
            "block_id": "BLK-001",
            "goal": "Goal",
            "start_date": "2025-06-01",
            "end_date": "2025-08-31",
            "planned_sessions_per_week": "5",
            "weekly_volume_targets": "not-valid-json",
        }
    ]
    result = parse_csv(rows)

    assert len(result.records) == 0
    assert len(result.errors) == 1
    assert isinstance(result.errors[0], MalformedRowError)


def test_parse_csv_batch_continues_after_invalid_row():
    """PL1-4/PL1-5: parsing continues for subsequent records after a bad one."""
    rows = [
        # valid
        {
            "athlete_id": "A1",
            "block_id": "BLK-001",
            "goal": "Goal",
            "start_date": "2025-06-01",
            "end_date": "2025-08-31",
            "planned_sessions_per_week": "5",
            "weekly_volume_targets": '{"strength": 3}',
        },
        # invalid — end_date before start_date
        {
            "athlete_id": "A2",
            "block_id": "BLK-002",
            "goal": "Goal",
            "start_date": "2025-08-31",
            "end_date": "2025-06-01",
            "planned_sessions_per_week": "5",
            "weekly_volume_targets": '{"strength": 3}',
        },
        # valid
        {
            "athlete_id": "A3",
            "block_id": "BLK-003",
            "goal": "Goal",
            "start_date": "2025-06-01",
            "end_date": "2025-08-31",
            "planned_sessions_per_week": "3",
            "weekly_volume_targets": '{"cardio": 2}',
        },
    ]
    result = parse_csv(rows)

    assert len(result.records) == 2
    assert len(result.errors) == 1
    assert {r.athlete_id for r in result.records} == {"A1", "A3"}


# ---------------------------------------------------------------------------
# Task 1.9 — wvt round-trip fidelity (PL1-7)
# ---------------------------------------------------------------------------


def test_parse_yaml_wvt_round_trip_fidelity():
    """PL1-7: json.loads(record.weekly_volume_targets) must equal original dict."""
    yaml_content = """\
athlete_id: A1
block_id: BLK-001
goal: Build aerobic base
start_date: "2025-06-01"
end_date: "2025-08-31"
planned_sessions_per_week: 5
weekly_volume_targets:
  strength: 3
  cardio: 2
  endurance: 1
"""
    result = parse_yaml(yaml_content)
    record = result.records[0]

    original_dict = {"strength": 3, "cardio": 2, "endurance": 1}
    roundtripped = json.loads(record.weekly_volume_targets)

    assert roundtripped == original_dict
    # Re-serializing must produce the same JSON string that is in the Avro field
    assert json.dumps(roundtripped) == json.dumps(original_dict)


def test_parse_csv_empty_yields_empty_result():
    """Empty CSV row stream returns zero records and zero errors."""
    result = parse_csv([])

    assert result.records == []
    assert result.errors == []
