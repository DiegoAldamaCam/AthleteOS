"""Seed a large, realistic demo dataset directly into Postgres.

Generates N athletes across multiple sports and, for each, a run of daily
``athlete_metrics`` rows whose load curves follow a sport-specific profile so
the dashboard tells a believable story (an endurance athlete carries sustained
high chronic load; a powerlifter shows sharp acute peaks; a team-sport athlete
is more seasonal/variable).

This is a DEMO seeder — it writes canonical serving-store rows directly, bypassing
the Kafka/Flink pipeline on purpose (fast, deterministic, no late-data churn).
The real pipeline path is unchanged and still owns the two "real" athletes.

Usage (inside the fastapi container or any env with DATABASE_URL + psycopg2):
    python -m tools.seed_demo --athletes 1000 --days 45
    python -m tools.seed_demo --athletes 1000 --days 45 --reset

Idempotent per athlete_id+metric_date via ON CONFLICT upsert. --reset first
deletes demo athletes (athlete_id starting with 'demo-').
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
from dataclasses import dataclass
from datetime import date, timedelta

import psycopg2
from psycopg2.extras import execute_values

DEMO_PREFIX = "demo-"


@dataclass(frozen=True)
class SportProfile:
    """Shapes a sport's training-load curve.

    base_chronic:   typical 28-day chronic load baseline (AU)
    acute_amp:      amplitude of acute-load oscillation around chronic
    volatility:     day-to-day randomness (0..1)
    peak_bias:      how spiky the acute peaks are (endurance low, power high)
    """

    sport: str
    base_chronic: float
    acute_amp: float
    volatility: float
    peak_bias: float


# Curated sport profiles — tuned so each discipline looks distinct on the chart.
SPORT_PROFILES: tuple[SportProfile, ...] = (
    SportProfile("running", 1400, 350, 0.18, 0.35),
    SportProfile("cycling", 1800, 420, 0.20, 0.40),
    SportProfile("triathlon", 2000, 500, 0.22, 0.45),
    SportProfile("swimming", 1300, 300, 0.16, 0.35),
    SportProfile("rowing", 1600, 400, 0.20, 0.45),
    SportProfile("powerlifting", 800, 600, 0.30, 0.85),
    SportProfile("weightlifting", 850, 620, 0.30, 0.85),
    SportProfile("crossfit", 1100, 520, 0.28, 0.65),
    SportProfile("football", 1500, 480, 0.35, 0.55),
    SportProfile("basketball", 1400, 460, 0.34, 0.55),
    SportProfile("martial_arts", 1200, 500, 0.32, 0.70),
    SportProfile("climbing", 900, 380, 0.26, 0.60),
)

# Small curated name pools -> combined for variety without external deps (Faker).
_FIRST = [
    "Alex", "Sam", "Jordan", "Taylor", "Casey", "Morgan", "Riley", "Jamie",
    "Diego", "Lucia", "Mateo", "Sofia", "Noah", "Emma", "Liam", "Olivia",
    "Kenji", "Yuki", "Ana", "Marco", "Ingrid", "Lars", "Priya", "Arjun",
    "Chloe", "Ethan", "Nadia", "Omar", "Elena", "Tomas", "Zoe", "Hugo",
]
_LAST = [
    "Rivera", "Chen", "Kim", "Muller", "Rossi", "Silva", "Novak", "Haas",
    "Okafor", "Nguyen", "Petrov", "Andersson", "Kaur", "Costa", "Weber",
    "Moreno", "Larsen", "Dubois", "Tanaka", "Ferrari", "Santos", "Brandt",
]


def _rng_for(seed: int) -> random.Random:
    return random.Random(seed)


def build_athletes(n: int, seed: int = 42) -> list[tuple[str, str, str]]:
    """Return n (athlete_id, name, sport) tuples with sports round-robin+random."""
    rng = _rng_for(seed)
    rows: list[tuple[str, str, str]] = []
    for i in range(1, n + 1):
        athlete_id = f"{DEMO_PREFIX}{i:05d}"
        name = f"{rng.choice(_FIRST)} {rng.choice(_LAST)}"
        # Weighted-ish spread: cycle profiles so every sport is represented.
        profile = SPORT_PROFILES[i % len(SPORT_PROFILES)]
        rows.append((athlete_id, name, profile.sport))
    return rows


def _profile_by_sport(sport: str) -> SportProfile:
    for p in SPORT_PROFILES:
        if p.sport == sport:
            return p
    return SPORT_PROFILES[0]


def build_metrics(
    athlete_id: str,
    sport: str,
    days: int,
    end: date,
    seed: int,
) -> list[tuple]:
    """Generate `days` daily metric rows for one athlete with a sport-shaped curve."""
    p = _profile_by_sport(sport)
    rng = _rng_for(seed)
    rows: list[tuple] = []

    # Personal offset so athletes of the same sport are not identical.
    chronic = p.base_chronic * rng.uniform(0.8, 1.2)

    for d in range(days):
        metric_date = end - timedelta(days=(days - 1 - d))
        # Chronic drifts slowly; acute oscillates + spikes by peak_bias.
        chronic += rng.uniform(-1, 1) * p.base_chronic * 0.01
        chronic = max(200.0, chronic)

        wave = math.sin((d / 7.0) * math.pi) * p.acute_amp
        spike = (rng.random() ** (1 / (p.peak_bias + 0.1))) * p.acute_amp * p.peak_bias
        noise = rng.uniform(-1, 1) * p.acute_amp * p.volatility
        acute = max(0.0, chronic + wave + spike + noise)

        chronic_28 = chronic
        chronic_42 = chronic * rng.uniform(0.92, 1.02)
        acr = round(acute / chronic_28, 3) if chronic_28 > 0 else None

        # Derived scores (0..100). Higher ACR -> higher fatigue, lower readiness.
        fatigue = max(0.0, min(100.0, (acr or 1.0) * 55.0 + rng.uniform(-8, 8)))
        readiness = max(0.0, min(100.0, 100.0 - fatigue + rng.uniform(-6, 6)))
        recovery = max(0.0, min(100.0, readiness + rng.uniform(-10, 10)))
        adherence = max(0.0, min(100.0, rng.uniform(60, 100)))

        flags: list[str] = []
        if (acr or 0) >= 1.5:
            flags.append("high_fatigue")
        if (acr or 0) >= 1.3 and d % 7 == 6:
            flags.append("deload")
        if 33 <= readiness < 55:
            flags.append("monitor")
        deload_flag = 1 if "deload" in flags else 0

        rows.append(
            (
                athlete_id,
                metric_date,
                round(acute, 2),
                round(chronic_28, 2),
                round(chronic_42, 2),
                acr,
                deload_flag,
                round(fatigue, 1),
                round(readiness, 1),
                json.dumps(flags),
                round(recovery, 1),
                round(adherence, 1),
            )
        )
    return rows


_INSERT_ATHLETES = """
INSERT INTO athletes (athlete_id, name, sport)
VALUES %s
ON CONFLICT (athlete_id) DO UPDATE
    SET name = EXCLUDED.name, sport = EXCLUDED.sport
"""

_INSERT_METRICS = """
INSERT INTO athlete_metrics (
    athlete_id, metric_date, acute_load, chronic_load_28d, chronic_load_42d,
    acute_chronic_ratio, deload_flag, fatigue_score, readiness_score,
    coaching_flags, recovery_score, adherence_score
) VALUES %s
ON CONFLICT (athlete_id, metric_date) DO UPDATE SET
    acute_load = EXCLUDED.acute_load,
    chronic_load_28d = EXCLUDED.chronic_load_28d,
    chronic_load_42d = EXCLUDED.chronic_load_42d,
    acute_chronic_ratio = EXCLUDED.acute_chronic_ratio,
    deload_flag = EXCLUDED.deload_flag,
    fatigue_score = EXCLUDED.fatigue_score,
    readiness_score = EXCLUDED.readiness_score,
    coaching_flags = EXCLUDED.coaching_flags,
    recovery_score = EXCLUDED.recovery_score,
    adherence_score = EXCLUDED.adherence_score
"""


def seed(dsn: str, n_athletes: int, days: int, reset: bool) -> tuple[int, int]:
    """Seed the DB; returns (athletes_written, metric_rows_written)."""
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            if reset:
                cur.execute(
                    "DELETE FROM athlete_metrics WHERE athlete_id LIKE %s",
                    (DEMO_PREFIX + "%",),
                )
                cur.execute(
                    "DELETE FROM athletes WHERE athlete_id LIKE %s",
                    (DEMO_PREFIX + "%",),
                )

            athletes = build_athletes(n_athletes)
            execute_values(cur, _INSERT_ATHLETES, athletes, page_size=500)

            end = date.today()
            total_metrics = 0
            batch: list[tuple] = []
            for idx, (athlete_id, _name, sport) in enumerate(athletes):
                batch.extend(build_metrics(athlete_id, sport, days, end, seed=1000 + idx))
                if len(batch) >= 5000:
                    execute_values(cur, _INSERT_METRICS, batch, page_size=1000)
                    total_metrics += len(batch)
                    batch = []
            if batch:
                execute_values(cur, _INSERT_METRICS, batch, page_size=1000)
                total_metrics += len(batch)

        conn.commit()
        return len(athletes), total_metrics
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _dsn_from_env() -> str:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL not set")
    return dsn


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed demo athletes + metrics.")
    parser.add_argument("--athletes", type=int, default=1000)
    parser.add_argument("--days", type=int, default=45)
    parser.add_argument("--reset", action="store_true", help="Delete existing demo-* rows first")
    args = parser.parse_args()

    written_a, written_m = seed(_dsn_from_env(), args.athletes, args.days, args.reset)
    print(f"Seeded {written_a} athletes and {written_m} metric rows.")


if __name__ == "__main__":
    main()
