"""File/directory watcher for the recovery ingestion connector (PR-R1).

``process_csv_file`` reads a recovery CSV file, parses every row via
``parse_csv`` (skip-and-collect), publishes each valid record to
``raw.recovery`` through the injected publisher, and flushes once at the end.

``process_directory`` scans a directory for ``*.csv`` files and processes each
once in sorted order (determinism). ``watch_directory`` wraps that in a polling
loop (threading.Event for responsive shutdown).

The publisher is injected (duck-typed: ``publish(record, **kwargs)`` + ``flush``)
so unit tests verify parsing+publishing with a recording fake and the
integration test wires a real ``RecoveryPublisher`` against testcontainers Kafka.

Mirrors ``ingestion/wellness/watcher.py`` symbol-for-symbol.
"""

from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from pathlib import Path
from threading import Event
from typing import Callable, Iterable, Protocol

from ingestion.recovery.parser import parse_csv

_DEFAULT_POLL_INTERVAL = 5.0


class _Publisher(Protocol):
    """Structural type matching RecoveryPublisher (publish + flush)."""

    def publish(self, record, **kwargs) -> str: ...
    def flush(self) -> None: ...


@dataclass
class ProcessingSummary:
    """Result of processing one CSV file: published + skipped counts."""

    published: int
    skipped: int
    path: str


def process_csv_file(
    path: Path,
    publisher: _Publisher,
    uuid_factory: Callable[[], object] | None = None,
    now: Callable[[], object] | None = None,
) -> ProcessingSummary:
    """Parse a recovery CSV file and publish every valid record; skip malformed.

    Opens the file as UTF-8 with ``newline=""`` (csv module requirement), reads
    via ``DictReader``, and publishes each parsed record. The publisher is
    flushed exactly once before returning so queued records are delivered.
    """
    with open(path, "r", encoding="utf-8", newline="") as fh:
        rows: Iterable[dict] = csv.DictReader(fh)
        result = parse_csv(rows)

    published = 0
    for record in result.records:
        publisher.publish(record, uuid_factory=uuid_factory, now=now)
        published += 1

    publisher.flush()
    return ProcessingSummary(published=published, skipped=len(result.errors), path=str(path))


def process_directory(
    dir_path: Path,
    publisher: _Publisher,
    uuid_factory: Callable[[], object] | None = None,
    now: Callable[[], object] | None = None,
) -> list[ProcessingSummary]:
    """Process every ``*.csv`` file in ``dir_path`` once (sorted for determinism).

    Non-CSV files are ignored. Returns one ``ProcessingSummary`` per processed
    file, in sorted filename order.
    """
    summaries: list[ProcessingSummary] = []
    for csv_path in sorted(Path(dir_path).glob("*.csv")):
        summaries.append(process_csv_file(csv_path, publisher, uuid_factory, now))
    return summaries


def watch_directory(
    dir_path: Path,
    publisher: _Publisher,
    poll_interval: float = _DEFAULT_POLL_INTERVAL,
    stop_event: Event | None = None,
    uuid_factory: Callable[[], object] | None = None,
    now: Callable[[], object] | None = None,
) -> None:
    """Poll ``dir_path`` for CSV files and process them until ``stop_event`` is set.

    Blocks the calling thread. Uses ``stop_event.wait(poll_interval)`` so a set
    event is observed promptly rather than after a full sleep.
    """
    event = stop_event or Event()
    dir_path = Path(dir_path)
    while not event.is_set():
        try:
            process_directory(dir_path, publisher, uuid_factory, now)
        except Exception:
            logging.exception("watch_directory: poll cycle failed; continuing")
        event.wait(poll_interval)
