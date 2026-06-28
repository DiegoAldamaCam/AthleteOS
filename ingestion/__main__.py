"""
ingestion/__main__.py

Supervisor entry point for the AthleteOS ingestion stack (Gap #7, ADR-IW1..6).

Constructs one Publisher per connector, spawns one daemon thread per connector
running watch_directory(), and joins on shutdown.

Design decisions:
- ADR-IW1: single supervisor service, 6 daemon threads (I/O-bound polling loops).
- ADR-IW2: each thread body wraps watch_directory() in try/except — supervisor is
  the isolation boundary (strength/watcher.py has no internal guard).
- ADR-IW3: CONNECTORS registry + injectable watch_fn/connectors for testability.
- ADR-IW4: fail-fast env reads (KeyError on missing vars); depends_on bootstrap.
- ADR-IW5: python:3.11-slim base image (aligns pyproject requires-python >=3.11,<3.12).
- ADR-IW6: single INGEST_WATCH_DIR + per-connector subdirs via mkdir(parents, exist_ok).

Cross-profile note: schema-bootstrap uses profile 'bootstrap'; ingestion uses profile
'ingest'. Docker Compose does NOT auto-start cross-profile dependencies. Operator must
run `docker compose --profile bootstrap up -d` before `--profile ingest up`.
"""
from __future__ import annotations

import logging
import os
import threading
from pathlib import Path
from threading import Thread
from typing import Any, Callable, List, Optional, Tuple

from ingestion.cardio.producer import CardioPublisher
from ingestion.cardio.watcher import watch_directory as cardio_watch
from ingestion.nutrition.producer import NutritionPublisher
from ingestion.nutrition.watcher import watch_directory as nutrition_watch
from ingestion.planning.producer import PlanningPublisher
from ingestion.planning.watcher import watch_directory as planning_watch
from ingestion.recovery.producer import RecoveryPublisher
from ingestion.recovery.watcher import watch_directory as recovery_watch
from ingestion.strength.producer import StrengthPublisher
from ingestion.strength.watcher import watch_directory as strength_watch
from ingestion.wellness.producer import WellnessPublisher
from ingestion.wellness.watcher import watch_directory as wellness_watch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger(__name__)

# Registry: (name, watch_directory_fn, PublisherClass, subdir_name)
# Order is stable; each connector gets its own Publisher instance (no sharing).
CONNECTORS: List[Tuple[str, Callable, Any, str]] = [
    ("strength",  strength_watch,  StrengthPublisher,  "strength"),
    ("wellness",  wellness_watch,  WellnessPublisher,  "wellness"),
    ("planning",  planning_watch,  PlanningPublisher,  "planning"),
    ("cardio",    cardio_watch,    CardioPublisher,    "cardio"),
    ("recovery",  recovery_watch,  RecoveryPublisher,  "recovery"),
    ("nutrition", nutrition_watch, NutritionPublisher, "nutrition"),
]


def build_registry(
    bootstrap_servers: str,
    base_dir: Path,
    connectors: List[Tuple[str, Callable, Any, str]] = CONNECTORS,
) -> List[Tuple[str, Callable, Any, Path]]:
    """Instantiate publishers and create per-connector watch subdirectories.

    Returns a list of (name, watch_fn, publisher_instance, subdir_path) tuples.
    Each subdir is created with parents=True, exist_ok=True before any thread starts.
    """
    registry = []
    for name, watch_fn, publisher_class, subdir_name in connectors:
        subdir = base_dir / subdir_name
        subdir.mkdir(parents=True, exist_ok=True)
        publisher = publisher_class(bootstrap_servers=bootstrap_servers)
        registry.append((name, watch_fn, publisher, subdir))
    return registry


def run_watcher(
    name: str,
    watch_fn: Callable,
    publisher: Any,
    subdir: Path,
    stop_event: threading.Event,
) -> None:
    """Thread target: call watch_fn with isolation guard (ADR-IW2).

    An exception here exits this thread cleanly without affecting siblings.
    strength/watcher.py has no internal try/except — this is its isolation boundary.
    """
    try:
        watch_fn(subdir, publisher, stop_event=stop_event)
    except Exception:
        logger.exception("Watcher '%s' crashed; thread exits", name)


def main(
    connectors: Optional[List[Tuple[str, Callable, Any, str]]] = None,
    watcher_factory: Optional[Callable] = None,
    stop_event: Optional[threading.Event] = None,
) -> None:
    """Start all connector watchers and block until stop_event is set.

    Parameters
    ----------
    connectors:
        Registry of (name, watch_fn, PublisherClass, subdir_name) tuples.
        Defaults to CONNECTORS (all 6 real connectors). Injectable for tests.
    watcher_factory:
        Reserved for future extension. Currently unused; run_watcher is the
        default thread target.
    stop_event:
        Shared threading.Event. When set, all watch_directory loops exit and
        the supervisor joins every thread. Injectable so tests can pre-set it.
    """
    if connectors is None:
        connectors = CONNECTORS

    # Fail-fast: KeyError surfaces missing env immediately (sc-5 / ADR-IW4).
    bootstrap_servers: str = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    base_dir = Path(os.environ["INGEST_WATCH_DIR"])

    ev = stop_event if stop_event is not None else threading.Event()

    registry = build_registry(bootstrap_servers, base_dir, connectors)

    threads: List[Thread] = [
        Thread(
            target=run_watcher,
            args=(name, watch_fn, publisher, subdir, ev),
            name=f"watcher-{name}",
            daemon=True,
        )
        for name, watch_fn, publisher, subdir in registry
    ]

    for t in threads:
        t.start()

    logger.info("All %d watchers started; waiting for stop signal.", len(threads))

    try:
        ev.wait()
    finally:
        ev.set()
        for t in threads:
            t.join(timeout=10)

    logger.info("Supervisor shutdown complete.")


if __name__ == "__main__":
    main()
