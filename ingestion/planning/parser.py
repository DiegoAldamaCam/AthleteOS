"""Multi-format planning parser for the planning ingestion connector (PR-PL1).

Converts planning input (YAML, JSON, CSV) into typed ``PlanningRecord``
frozen dataclasses. Follows the skip-and-collect pattern: malformed records
are captured in ``ParseResult.errors`` without aborting the batch.

Validation rules (applied uniformly across all three formats):
- ``end_date`` must be >= ``start_date`` (ISO date comparison)
- ``planned_sessions_per_week`` must be > 0
- ``weekly_volume_targets`` must be JSON-serializable; serialized to a JSON
  string at parse time.

Mirrors ``ingestion/wellness/parser.py`` structure; extends it with multi-format
dispatch (YAML/JSON/CSV) as specified by the planning connector design.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterable, Mapping


# ---------------------------------------------------------------------------
# Domain types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PlanningRecord:
    """A typed, validated representation of one planning block.

    ``weekly_volume_targets`` is always a JSON string (serialized at parse
    time from whatever source representation was provided).
    """

    athlete_id: str
    block_id: str
    goal: str
    start_date: str  # ISO date string YYYY-MM-DD
    end_date: str    # ISO date string YYYY-MM-DD
    planned_sessions_per_week: int
    weekly_volume_targets: str  # JSON-serialized object


class MalformedRowError(ValueError):
    """Raised when a planning record cannot be parsed or fails validation."""


@dataclass
class ParseResult:
    """Outcome of parsing a planning source: valid records plus per-record errors."""

    records: list[PlanningRecord]
    errors: list[MalformedRowError]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _serialize_wvt(raw_wvt: Any) -> str:
    """Serialize weekly_volume_targets to a JSON string.

    Accepts a dict (YAML/JSON input) or a JSON string literal (CSV input).
    Raises MalformedRowError if the value is not JSON-serializable.
    """
    if isinstance(raw_wvt, str):
        # CSV path: validate it is valid JSON, then re-serialize for consistency
        try:
            parsed = json.loads(raw_wvt)
        except (json.JSONDecodeError, ValueError) as exc:
            raise MalformedRowError(
                f"weekly_volume_targets is not valid JSON: {raw_wvt!r}"
            ) from exc
        # Re-serialize to normalize key order / whitespace
        try:
            return json.dumps(parsed)
        except (TypeError, ValueError) as exc:
            raise MalformedRowError(
                f"weekly_volume_targets cannot be re-serialized: {raw_wvt!r}"
            ) from exc
    else:
        # YAML/JSON path: value is already a Python object (dict, list, …)
        try:
            return json.dumps(raw_wvt)
        except (TypeError, ValueError) as exc:
            raise MalformedRowError(
                f"weekly_volume_targets is not JSON-serializable: {raw_wvt!r}"
            ) from exc


def _validate(data: Mapping[str, Any]) -> None:
    """Raise MalformedRowError if business rules are violated.

    Checks:
    - required fields present and non-empty
    - end_date >= start_date
    - planned_sessions_per_week > 0
    """
    required = (
        "athlete_id", "block_id", "goal",
        "start_date", "end_date",
        "planned_sessions_per_week", "weekly_volume_targets",
    )
    for field in required:
        if not data.get(field) and data.get(field) != 0:
            raise MalformedRowError(f"missing required field: {field!r}")

    start = str(data["start_date"])
    end = str(data["end_date"])
    if end < start:
        raise MalformedRowError(
            f"end_date ({end!r}) must not be before start_date ({start!r})"
        )

    sessions = int(data["planned_sessions_per_week"])
    if sessions <= 0:
        raise MalformedRowError(
            f"planned_sessions_per_week must be > 0, got {sessions}"
        )


def _build_record(data: Mapping[str, Any]) -> PlanningRecord:
    """Build a validated PlanningRecord from a parsed data mapping.

    Shared path for all three format parsers. Raises MalformedRowError on
    any validation failure.
    """
    _validate(data)

    wvt = _serialize_wvt(data["weekly_volume_targets"])

    return PlanningRecord(
        athlete_id=str(data["athlete_id"]),
        block_id=str(data["block_id"]),
        goal=str(data["goal"]),
        start_date=str(data["start_date"]),
        end_date=str(data["end_date"]),
        planned_sessions_per_week=int(data["planned_sessions_per_week"]),
        weekly_volume_targets=wvt,
    )


def _collect(data: Mapping[str, Any]) -> tuple[PlanningRecord | None, MalformedRowError | None]:
    """Try to build a record; return (record, None) or (None, error)."""
    try:
        return _build_record(data), None
    except MalformedRowError as exc:
        return None, exc


# ---------------------------------------------------------------------------
# Public format dispatch
# ---------------------------------------------------------------------------


def parse_yaml(content: str) -> ParseResult:
    """Parse a YAML string containing one planning block.

    The YAML document is expected to represent a single dict with all 7 required
    fields. ``weekly_volume_targets`` should be a YAML mapping (dict).

    Returns a ParseResult with 0 or 1 records and 0 or 1 errors.
    """
    import yaml  # lazy import — keeps module importable without PyYAML installed

    try:
        data = yaml.safe_load(content)
    except yaml.YAMLError as exc:
        err = MalformedRowError(f"YAML parse error: {exc}")
        return ParseResult(records=[], errors=[err])

    if not isinstance(data, dict):
        err = MalformedRowError(f"expected a YAML mapping, got {type(data).__name__}")
        return ParseResult(records=[], errors=[err])

    record, error = _collect(data)
    if record is not None:
        return ParseResult(records=[record], errors=[])
    return ParseResult(records=[], errors=[error])  # type: ignore[list-item]


def parse_json(content: str) -> ParseResult:
    """Parse a JSON string containing one planning block.

    Returns a ParseResult with 0 or 1 records and 0 or 1 errors.
    """
    try:
        data = json.loads(content)
    except (json.JSONDecodeError, ValueError) as exc:
        err = MalformedRowError(f"JSON parse error: {exc}")
        return ParseResult(records=[], errors=[err])

    if not isinstance(data, dict):
        err = MalformedRowError(f"expected a JSON object, got {type(data).__name__}")
        return ParseResult(records=[], errors=[err])

    record, error = _collect(data)
    if record is not None:
        return ParseResult(records=[record], errors=[])
    return ParseResult(records=[], errors=[error])  # type: ignore[list-item]


def parse_csv(rows: Iterable[Mapping[str, str]]) -> ParseResult:
    """Parse a stream of CSV rows (dict per row) into PlanningRecords.

    Each row mapping is expected to have all 7 required fields as strings.
    ``weekly_volume_targets`` must be a JSON string literal in the CSV cell.
    ``planned_sessions_per_week`` is coerced from string to int.

    Uses skip-and-collect: malformed rows are captured in ParseResult.errors.
    """
    records: list[PlanningRecord] = []
    errors: list[MalformedRowError] = []

    for row in rows:
        # Coerce sessions to int for the shared _build_record path
        coerced: dict[str, Any] = dict(row)
        sessions_raw = coerced.get("planned_sessions_per_week", "")
        try:
            coerced["planned_sessions_per_week"] = int(str(sessions_raw))
        except (ValueError, TypeError) as exc:
            errors.append(
                MalformedRowError(
                    f"planned_sessions_per_week is not an integer: {sessions_raw!r}"
                )
            )
            continue

        record, error = _collect(coerced)
        if record is not None:
            records.append(record)
        else:
            errors.append(error)  # type: ignore[arg-type]

    return ParseResult(records=records, errors=errors)
