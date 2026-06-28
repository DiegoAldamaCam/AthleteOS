"""Nutrition CSV parser for the nutrition ingestion connector (PR-N1).

Converts raw nutrition CSV rows (column-name -> string-cell mappings) into typed
``NutritionRecord`` values. Per the event-contracts spec "Raw Topic JSON Shape",
the raw layer carries source fields **verbatim** — the parser therefore only
validates required fields and coerces CSV strings to their typed values.

Mirrors ``ingestion/recovery/parser.py`` symbol-for-symbol, adapted for the
nutrition CSV export column set.

Nutrition CSV columns:
  Required: athlete_id (str), date (ISO date str)
  Optional/nullable: calories (int), protein_g (float), carbs_g (float),
                     fat_g (float), adherence_score (float)

Note: ``calories`` is an integer (W1-5 boundary); non-integer float strings
raise ``MalformedRowError`` via ``_to_optional_int``. The column name
``adherence_score`` is source-faithful — the rename to ``nutrition_adherence``
happens exclusively in the canonicalize transform (ADR-N2).
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import Iterable, Mapping


@dataclass(frozen=True)
class NutritionRecord:
    """A typed, source-faithful representation of one nutrition CSV row.

    ``date`` is kept as the original ISO date string (raw-layer fidelity);
    the producer computes ``event_time`` as UTC midnight epoch-ms from it.

    All five nutrition data fields are nullable: absent/blank cells become
    None (null-row policy — downstream wellness_metrics no-ops all-null events).

    ``calories`` is typed as ``int`` (not float); non-integer CSV values raise
    ``MalformedRowError`` at parse time.

    ``adherence_score`` preserves the source column name verbatim (ADR-N2).
    The rename to ``nutrition_adherence`` is performed by the canonicalize
    transform, not here.
    """

    athlete_id: str
    date: str
    calories: int | None
    protein_g: float | None
    carbs_g: float | None
    fat_g: float | None
    adherence_score: float | None


class MalformedRowError(ValueError):
    """Raised when a nutrition CSV row cannot be parsed into a NutritionRecord."""


@dataclass
class ParseResult:
    """Outcome of parsing a batch of rows: valid records plus per-row errors."""

    records: list[NutritionRecord]
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
    """Nullable int field: absent/blank -> None, non-integer (including floats) -> raise.

    Rejects fractional strings like '2400.5' to preserve int type contract (sc-7).
    """
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


def parse_row(raw_row: Mapping[str, str]) -> NutritionRecord:
    """Parse a single nutrition CSV row into a typed NutritionRecord.

    Raises MalformedRowError if a required field (athlete_id, date) is
    missing/empty, the date is not a real ISO date, calories is a non-integer
    float string, or any other numeric value cannot be parsed.
    """
    return NutritionRecord(
        athlete_id=_require(raw_row, "athlete_id"),
        date=_require_iso_date(raw_row, "date"),
        calories=_to_optional_int(raw_row, "calories"),
        protein_g=_to_optional_float(raw_row, "protein_g"),
        carbs_g=_to_optional_float(raw_row, "carbs_g"),
        fat_g=_to_optional_float(raw_row, "fat_g"),
        adherence_score=_to_optional_float(raw_row, "adherence_score"),
    )


def parse_csv(rows: Iterable[Mapping[str, str]]) -> ParseResult:
    """Parse a stream of rows, collecting valid records and per-row errors.

    Malformed rows are skipped (not raised) so a single bad row does not abort
    an entire batch upload; each failure is captured in ``ParseResult.errors``
    for downstream reporting/DLQ wiring.
    """
    records: list[NutritionRecord] = []
    errors: list[MalformedRowError] = []
    for row in rows:
        try:
            records.append(parse_row(row))
        except MalformedRowError as exc:
            errors.append(exc)
    return ParseResult(records=records, errors=errors)
