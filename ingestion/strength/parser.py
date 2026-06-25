"""Strong CSV parser for the strength ingestion connector (PR2, task 3.1).

Converts raw Strong CSV rows (column-name -> string-cell mappings) into typed
``StrengthSetRecord`` values. Per the event-contracts spec "Raw Topic JSON
Shape", the raw layer carries source fields **verbatim** - the parser therefore
only validates required fields and coerces CSV strings to their typed values. It
does NOT normalize, derive, or compute anything.

In particular ``session_load`` is a **canonicalization-layer** concern (spec:
"session_load derivation (computed at canonicalization, required field)"), so it
is intentionally NOT computed here. It will be derived in the PR3 canonicalize
job from the verbatim payload fields this connector emits.

Column names follow the spec's Strong CSV source-field mapping table:
  athlete_id, workout_id, exercise_id, set_number, reps, weight_kg, rpe, rir,
  timestamp
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Mapping


@dataclass(frozen=True)
class StrengthSetRecord:
    """A typed, source-faithful representation of one Strong CSV set row.

    ``timestamp`` is kept as the original source string (raw-layer fidelity);
    the producer maps it to the envelope ``event_time`` (ISO-8601) while also
    preserving it verbatim inside ``payload``.
    """

    athlete_id: str
    workout_id: str
    exercise_id: str
    set_number: int
    reps: int
    weight_kg: float
    rpe: float | None
    rir: float | None
    timestamp: str


# Columns that MUST be present and parseable. rpe/rir are nullable (optional).
_REQUIRED_FIELDS: tuple[str, ...] = (
    "athlete_id",
    "workout_id",
    "exercise_id",
    "set_number",
    "reps",
    "weight_kg",
    "timestamp",
)
_OPTIONAL_FLOAT_FIELDS: tuple[str, ...] = ("rpe", "rir")


class MalformedRowError(ValueError):
    """Raised when a Strong CSV row cannot be parsed into a StrengthSetRecord."""


@dataclass
class ParseResult:
    """Outcome of parsing a batch of rows: valid records plus per-row errors."""

    records: list[StrengthSetRecord]
    errors: list[MalformedRowError]


def _require(row: Mapping[str, str], field: str) -> str:
    """Return a non-empty required string field, else raise MalformedRowError."""
    value = row.get(field)
    if value is None or str(value).strip() == "":
        raise MalformedRowError(f"missing required field: {field!r}")
    return str(value)


def _to_int(row: Mapping[str, str], field: str) -> int:
    raw = _require(row, field)
    try:
        return int(raw)
    except ValueError as exc:
        raise MalformedRowError(f"unparseable int for {field!r}: {raw!r}") from exc


def _to_float(row: Mapping[str, str], field: str) -> float:
    raw = _require(row, field)
    try:
        return float(raw)
    except ValueError as exc:
        raise MalformedRowError(f"unparseable float for {field!r}: {raw!r}") from exc


def _to_optional_float(row: Mapping[str, str], field: str) -> float | None:
    """Nullable float field: absent/blank -> None, non-numeric -> raise."""
    raw = row.get(field)
    if raw is None or str(raw).strip() == "":
        return None
    try:
        return float(raw)
    except ValueError as exc:
        raise MalformedRowError(f"unparseable float for {field!r}: {raw!r}") from exc


def _to_timestamp(row: Mapping[str, str]) -> str:
    """Validate the timestamp parses as ISO-8601 but return the original string.

    The raw envelope keeps the source timestamp verbatim inside ``payload``;
    validation here only guarantees the producer can derive ``event_time`` from
    it later. ``datetime.fromisoformat`` (3.11+) accepts both 'T' and space
    separators.
    """
    raw = _require(row, "timestamp")
    try:
        datetime.fromisoformat(raw)
    except ValueError as exc:
        raise MalformedRowError(f"unparseable timestamp: {raw!r}") from exc
    return raw


def parse_row(raw_row: Mapping[str, str]) -> StrengthSetRecord:
    """Parse a single Strong CSV row into a typed StrengthSetRecord.

    Raises MalformedRowError if a required field is missing/empty or a numeric
    or timestamp value cannot be parsed.
    """
    return StrengthSetRecord(
        athlete_id=_require(raw_row, "athlete_id"),
        workout_id=_require(raw_row, "workout_id"),
        exercise_id=_require(raw_row, "exercise_id"),
        set_number=_to_int(raw_row, "set_number"),
        reps=_to_int(raw_row, "reps"),
        weight_kg=_to_float(raw_row, "weight_kg"),
        rpe=_to_optional_float(raw_row, "rpe"),
        rir=_to_optional_float(raw_row, "rir"),
        timestamp=_to_timestamp(raw_row),
    )


def parse_csv(rows: Iterable[Mapping[str, str]]) -> ParseResult:
    """Parse a stream of rows, collecting valid records and per-row errors.

    Malformed rows are skipped (not raised) so a single bad row does not abort
    an entire batch upload; each failure is captured in ``ParseResult.errors``
    for downstream reporting/DLQ wiring.
    """
    records: list[StrengthSetRecord] = []
    errors: list[MalformedRowError] = []
    for row in rows:
        try:
            records.append(parse_row(row))
        except MalformedRowError as exc:
            errors.append(exc)
    return ParseResult(records=records, errors=errors)
