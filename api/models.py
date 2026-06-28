"""Pydantic response models for the AthleteOS API.

These models mirror the `athlete_metrics` PostgreSQL table schema (Phase 6 PG sink).
Field order and names match the spec exactly (obs #65, Domain A).
"""

from __future__ import annotations

from datetime import date
from typing import Optional

from pydantic import BaseModel


class MetricRow(BaseModel):
    """One day of computed athlete training-load metrics."""

    athlete_id: str
    metric_date: date
    acute_load: Optional[float]
    chronic_load_28d: Optional[float]
    chronic_load_42d: Optional[float]
    acute_chronic_ratio: Optional[float]
    deload_flag: Optional[int]
    # metrics-v2: load-based scores + coaching flags (additive, nullable)
    fatigue_score: Optional[float] = None
    readiness_score: Optional[float] = None
    coaching_flags: Optional[list[str]] = None
    # wellness-source: recovery score (additive, nullable — W3-10/W3-11)
    recovery_score: Optional[float] = None
    # adherence-source: adherence score (additive, nullable — ADH-A1/A2)
    adherence_score: Optional[float] = None
