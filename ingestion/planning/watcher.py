"""File/directory watcher for the planning ingestion connector (PR-PL1).

``process_file`` reads a single YAML or JSON planning file, parses it via
the appropriate format parser, publishes each valid record, and flushes.

``process_csv_file`` reads a planning CSV file, parses every row via
``parse_csv`` (skip-and-collect), publishes each valid record, and flushes.

``process_directory`` scans a directory for YAML, JSON, and CSV files and
processes each once in sorted order (determinism).

``watch_directory`` wraps ``process_directory`` in a polling loop with
``threading.Event`` for responsive shutdown.

The publisher is injected (duck-typed: ``publish(record, **kwargs)`` + ``flush``)
so unit tests verify parsing+publishing with a recording fake.

Mirrors ``ingestion/wellness/watcher.py`` symbol-for-symbol, extended with
multi-format dispatch (YAML/JSON/CSV) per the planning connector design.
"""

from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from pathlib import Path
from threading import Event
from typing import Callable, Protocol

from ingestion.planning.parser import parse_csv, parse_json, parse_yaml

_DEFAULT_POLL_INTERVAL = 5.0

# File extensions handled by each format parser
_YAML_EXTENSIONS = {".yaml", ".yml"}
_JSON_EXTENSIONS = {".json"}
_CSV_EXTENSIONS = {".csv"}


class _Publisher(Protocol):
    """Structural type matching PlanningPublisher (publish + flush)."""

    def publish(self, record, **kwargs) -> str: ...
    def flush(self) -> None: ...


@dataclass
class ProcessingSummary:
    """Result of processing one file: published + skipped counts."""

    published: int
    skipped: int
    path: str


def process_file(
    path: Path,
    publisher: _Publisher,
    uuid_factory: Callable[[], object] | None = None,
    now: Callable[[], object] | None = None,
) -> ProcessingSummary:
    """Parse a YAML or JSON planning file and publish every valid record.

    Files with unsupported extensions are silently skipped (0 published, 0 skipped).
    The publisher is flushed once after all records from the file are published.
    """
    path = Path(path)
    suffix = path.suffix.lower()

    if suffix in _CSV_EXTENSIONS:
        return process_csv_file(path, publisher, uuid_factory=uuid_factory, now=now)

    if suffix not in (_YAML_EXTENSIONS | _JSON_EXTENSIONS):
        # Unknown extension: skip gracefully without error
        return ProcessingSummary(published=0, skipped=0, path=str(path))

    content = path.read_text(encoding="utf-8")

    if suffix in _YAML_EXTENSIONS:
        result = parse_yaml(content)
    else:
        result = parse_json(content)

    published = 0
    for record in result.records:
        publisher.publish(record, uuid_factory=uuid_factory, now=now)
        published += 1

    publisher.flush()
    return ProcessingSummary(
        published=published,
        skipped=len(result.errors),
        path=str(path),
    )


def process_csv_file(
    path: Path,
    publisher: _Publisher,
    uuid_factory: Callable[[], object] | None = None,
    now: Callable[[], object] | None = None,
) -> ProcessingSummary:
    """Parse a planning CSV file and publish every valid record; skip malformed.

    Opens the file as UTF-8 with ``newline=""`` (csv module requirement), reads
    via ``DictReader``, and publishes each parsed record. The publisher is
    flushed exactly once before returning so queued records are delivered.
    """
    with open(path, "r", encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
        result = parse_csv(rows)

    published = 0
    for record in result.records:
        publisher.publish(record, uuid_factory=uuid_factory, now=now)
        published += 1

    publisher.flush()
    return ProcessingSummary(
        published=published,
        skipped=len(result.errors),
        path=str(path),
    )


def process_directory(
    dir_path: Path,
    publisher: _Publisher,
    uuid_factory: Callable[[], object] | None = None,
    now: Callable[[], object] | None = None,
) -> list[ProcessingSummary]:
    """Process every YAML, JSON, and CSV file in ``dir_path`` once (sorted).

    Non-planning files are ignored. Returns one ``ProcessingSummary`` per
    processed file, in sorted filename order.
    """
    supported_extensions = _YAML_EXTENSIONS | _JSON_EXTENSIONS | _CSV_EXTENSIONS
    summaries: list[ProcessingSummary] = []

    for file_path in sorted(Path(dir_path).iterdir()):
        if file_path.suffix.lower() not in supported_extensions:
            continue
        summaries.append(process_file(file_path, publisher, uuid_factory, now))

    return summaries


def watch_directory(
    dir_path: Path,
    publisher: _Publisher,
    poll_interval: float = _DEFAULT_POLL_INTERVAL,
    stop_event: Event | None = None,
    uuid_factory: Callable[[], object] | None = None,
    now: Callable[[], object] | None = None,
) -> None:
    """Poll ``dir_path`` for planning files and process them until ``stop_event`` is set.

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
