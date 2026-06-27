"""Wellness CSV parser for the wellness ingestion connector (PR-W1).

Converts raw wellness CSV rows (column-name -> string-cell mappings) into typed
``WellnessRecord`` values. Per the event-contracts spec "Raw Topic JSON Shape",
the raw layer carries source fields **verbatim** - the parser therefore only
validates required fields and coerces CSV strings to their typed values.

Mirrors ``ingestion/strength/parser.py`` symbol-for-symbol.

Wellness CSV columns:
  Required: athlete_id (str), date (ISO date str)
  Optional/nullable: hrv, sleep_hours, body_weight_kg (float | None)
                     resting_hr, steps, energy, soreness, mood,
                     stress, perceived_recovery (int | None)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping


@dataclass(frozen=True)
class WellnessRecord:
    """A typed, source-faithful representation of one wellness CSV row.

    ``date`` is kept as the original ISO date string (raw-layer fidelity);
    the producer computes ``event_time`` as UTC midnight epoch-ms from it.
    """

    athlete_id: str
    date: str
    hrv: float | None
    sleep_hours: float | None
    resting_hr: int | None
    steps: int | None
    body_weight_kg: float | None
    energy: int | None
    soreness: int | None
    mood: int | None
    stress: int | None
    perceived_recovery: int | None


class MalformedRowError(ValueError):
    """Raised when a wellness CSV row cannot be parsed into a WellnessRecord."""


@dataclass
class ParseResult:
    """Outcome of parsing a batch of rows: valid records plus per-row errors."""

    records: list[WellnessRecord]
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


def _to_optional_int(row: Mapping[str, str], field: str) -> int | None:
    """Nullable int field: absent/blank -> None, non-integer -> raise."""
    raw = row.get(field)
    if raw is None or str(raw).strip() == "":
        return None
    try:
        return int(raw)
    except ValueError as exc:
        raise MalformedRowError(f"unparseable int for {field!r}: {raw!r}") from exc


def parse_row(raw_row: Mapping[str, str]) -> WellnessRecord:
    """Parse a single wellness CSV row into a typed WellnessRecord.

    Raises MalformedRowError if a required field is missing/empty or a numeric
    value cannot be parsed.
    """
    return WellnessRecord(
        athlete_id=_require(raw_row, "athlete_id"),
        date=_require(raw_row, "date"),
        hrv=_to_optional_float(raw_row, "hrv"),
        sleep_hours=_to_optional_float(raw_row, "sleep_hours"),
        resting_hr=_to_optional_int(raw_row, "resting_hr"),
        steps=_to_optional_int(raw_row, "steps"),
        body_weight_kg=_to_optional_float(raw_row, "body_weight_kg"),
        energy=_to_optional_int(raw_row, "energy"),
        soreness=_to_optional_int(raw_row, "soreness"),
        mood=_to_optional_int(raw_row, "mood"),
        stress=_to_optional_int(raw_row, "stress"),
        perceived_recovery=_to_optional_int(raw_row, "perceived_recovery"),
    )


def parse_csv(rows: Iterable[Mapping[str, str]]) -> ParseResult:
    """Parse a stream of rows, collecting valid records and per-row errors.

    Malformed rows are skipped (not raised) so a single bad row does not abort
    an entire batch upload; each failure is captured in ``ParseResult.errors``
    for downstream reporting/DLQ wiring.
    """
    records: list[WellnessRecord] = []
    errors: list[MalformedRowError] = []
    for row in rows:
        try:
            records.append(parse_row(row))
        except MalformedRowError as exc:
            errors.append(exc)
    return ParseResult(records=records, errors=errors)
