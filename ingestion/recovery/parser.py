"""Recovery CSV parser for the recovery ingestion connector (PR-R1).

Converts raw recovery CSV rows (column-name -> string-cell mappings) into typed
``RecoveryRecord`` values. Per the event-contracts spec "Raw Topic JSON Shape",
the raw layer carries source fields **verbatim** — the parser therefore only
validates required fields and coerces CSV strings to their typed values.

Mirrors ``ingestion/wellness/parser.py`` symbol-for-symbol, adapted for the
Apple Health export column set.

Recovery CSV columns (Apple Health export):
  Required: athlete_id (str), date (ISO date str)
  Optional/nullable: sleep_hours (float), resting_hr (int), hrv (float),
                     steps (int), body_weight_kg (float)
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import Iterable, Mapping


@dataclass(frozen=True)
class RecoveryRecord:
    """A typed, source-faithful representation of one recovery CSV row.

    ``date`` is kept as the original ISO date string (raw-layer fidelity);
    the producer computes ``event_time`` as UTC midnight epoch-ms from it.

    All five Apple Health data fields are nullable: absent/blank cells become
    None (null-row policy — downstream wellness_metrics no-ops all-null events).
    """

    athlete_id: str
    date: str
    sleep_hours: float | None
    resting_hr: int | None
    hrv: float | None
    steps: int | None
    body_weight_kg: float | None


class MalformedRowError(ValueError):
    """Raised when a recovery CSV row cannot be parsed into a RecoveryRecord."""


@dataclass
class ParseResult:
    """Outcome of parsing a batch of rows: valid records plus per-row errors."""

    records: list[RecoveryRecord]
    errors: list[MalformedRowError]


def _require(row: Mapping[str, str], field: str) -> str:
    """Return a non-empty required string field, else raise MalformedRowError."""
    value = row.get(field)
    if value is None or str(value).strip() == "":
        raise MalformedRowError(f"missing required field: {field!r}")
    return str(value)


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


def _require_iso_date(row: Mapping[str, str], field: str) -> str:
    """Return a required field that must parse as a real ISO date, else raise."""
    value = _require(row, field)
    try:
        datetime.date.fromisoformat(value)
    except ValueError as exc:
        raise MalformedRowError(
            f"invalid ISO date for {field!r}: {value!r}"
        ) from exc
    return value


def parse_row(raw_row: Mapping[str, str]) -> RecoveryRecord:
    """Parse a single recovery CSV row into a typed RecoveryRecord.

    Raises MalformedRowError if a required field (athlete_id, date) is
    missing/empty, the date is not a real ISO date, or a numeric value
    cannot be parsed.
    """
    return RecoveryRecord(
        athlete_id=_require(raw_row, "athlete_id"),
        date=_require_iso_date(raw_row, "date"),
        sleep_hours=_to_optional_float(raw_row, "sleep_hours"),
        resting_hr=_to_optional_int(raw_row, "resting_hr"),
        hrv=_to_optional_float(raw_row, "hrv"),
        steps=_to_optional_int(raw_row, "steps"),
        body_weight_kg=_to_optional_float(raw_row, "body_weight_kg"),
    )


def parse_csv(rows: Iterable[Mapping[str, str]]) -> ParseResult:
    """Parse a stream of rows, collecting valid records and per-row errors.

    Malformed rows are skipped (not raised) so a single bad row does not abort
    an entire batch upload; each failure is captured in ``ParseResult.errors``
    for downstream reporting/DLQ wiring.
    """
    records: list[RecoveryRecord] = []
    errors: list[MalformedRowError] = []
    for row in rows:
        try:
            records.append(parse_row(row))
        except MalformedRowError as exc:
            errors.append(exc)
    return ParseResult(records=records, errors=errors)
