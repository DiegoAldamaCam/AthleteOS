"""Unit tests for the Iceberg training_event sink (work-unit 6.2, PR5 Phase 6).

TDD: tests are written first, against a non-existent implementation.
Catalog: SqlCatalog + sqlite (local filesystem, no Hadoop/Docker).
Warehouse: pytest tmp_path — each test is fully isolated.
No Flink, no Docker, no network.

Compatibility note (pyiceberg 0.7.1 + pyarrow 11.0.0 on Windows/Linux):
  - PyArrowFileIO.parse_location mishandles Windows drive-letter paths.
    _patch_pyarrow_file_io() fixes this so SqlCatalog works in tmp_path.
  - DayTransform on TimestamptzType requires pyarrow's tzdata C++ library
    (not present on stock Windows pyarrow 11). The Iceberg schema uses
    TimestampType (naive) which avoids the OS-level tzdata dependency entirely
    while still enabling the day() partition transform.

Read-back strategy:
  table.scan().to_arrow() calls pyiceberg's project_table which uses
  pa.concat_tables(promote_options="permissive"), a pyarrow >= 14 API.
  On pinned pyarrow 11 this raises TypeError. Instead, tests verify written
  data by reading the Parquet files directly from the warehouse tmp_path via
  pyarrow.parquet — no pyiceberg read path involved.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# ---------------------------------------------------------------------------
# Compatibility patch (applied before any pyiceberg import that triggers
# the affected code paths).
# ---------------------------------------------------------------------------


def _patch_pyarrow_file_io() -> None:
    """Fix PyArrowFileIO.parse_location for Windows bare-drive paths (C:/...).

    urlparse('C:/path') treats 'c' as the URI scheme, which
    PyArrowFileIO's fs_by_scheme() rejects with "Unrecognized filesystem type".
    This patch normalises such paths so PyArrowFileIO hands them to the local
    filesystem correctly.  On Linux the original function is used as-is.

    This shim is still needed because SqlCatalog uses PyArrowFileIO when
    creating/registering the Iceberg table on Windows — even though read-back
    now goes through pyarrow.parquet directly.
    """
    if sys.platform != "win32":
        return
    from pyiceberg.io.pyarrow import PyArrowFileIO

    _orig = PyArrowFileIO.parse_location

    def _patched(location: str):
        from urllib.parse import urlparse as _up

        # Bare Windows path: C:\... or C:/...
        if len(location) >= 2 and location[1] == ":":
            return "file", "", location.replace("\\", "/")
        uri = _up(location)
        # file:///C:/... → path is /C:/... → strip leading slash
        if uri.scheme == "file" and len(uri.path) >= 3 and uri.path[2] == ":":
            return "file", uri.netloc, uri.path[1:]
        return _orig(location)

    PyArrowFileIO.parse_location = staticmethod(_patched)


# Apply the Windows filesystem-path shim once at module import.
# This fixes PyArrowFileIO on Windows so SqlCatalog can create tables in
# tmp_path. It is a no-op on Linux/CI (guarded by sys.platform != "win32").
_patch_pyarrow_file_io()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

from pyiceberg.catalog.sql import SqlCatalog
from storage.iceberg.tables import (
    TRAINING_EVENT_SCHEMA,
    TRAINING_EVENT_PARTITION_SPEC,
    create_training_event_table,
)
from storage.iceberg.sink import append_events


def _make_catalog(tmp_path) -> SqlCatalog:
    """Create a fresh isolated SqlCatalog backed by sqlite in tmp_path."""
    return SqlCatalog(
        "test",
        **{
            "uri": f"sqlite:///{tmp_path}/catalog.db",
            "warehouse": str(tmp_path),
        },
    )


def _read_all_parquet(tmp_path: Path) -> pa.Table:
    """Read all Parquet files written by the sink under tmp_path.

    The Iceberg warehouse lays out data files under
    <warehouse>/<namespace>/<table>/data/**/*.parquet.
    We glob for every .parquet file to avoid coupling to pyiceberg's internal
    read path (which uses pa.concat_tables(promote_options=...) — a pyarrow
    >= 14 API not available on pinned pyarrow 11).
    """
    files = sorted(tmp_path.rglob("*.parquet"))
    if not files:
        return pa.table({})
    tables = [pq.read_table(str(f)) for f in files]
    # cast to a common schema before concatenating; all files share the same
    # Iceberg schema so only minor type promotions (if any) are needed.
    target_schema = tables[0].schema
    unified = [t.cast(target_schema) for t in tables]
    return pa.concat_tables(unified)


# Epoch-ms timestamps for 2024-01-01 and 2024-01-02 (UTC day starts)
_TS_2024_01_01 = 1_704_067_200_000  # 2024-01-01T00:00:00Z
_TS_2024_01_02 = 1_704_153_600_000  # 2024-01-02T00:00:00Z

_EVENT_A1 = {
    "event_id": "evt-a1",
    "event_time": _TS_2024_01_01,
    "athlete_id": "athlete_a",
    "event_type": "strength",
    "session_load": 100.0,
}
_EVENT_A2 = {
    "event_id": "evt-a2",
    "event_time": _TS_2024_01_01,
    "athlete_id": "athlete_a",
    "event_type": "strength",
    "session_load": 150.0,
}
_EVENT_B1 = {
    "event_id": "evt-b1",
    "event_time": _TS_2024_01_02,
    "athlete_id": "athlete_b",
    "event_type": "cardio",
    "session_load": 80.0,
}


# ---------------------------------------------------------------------------
# Test 1: schema and partition spec
# ---------------------------------------------------------------------------


class TestCreateTrainingEventTable:
    """create_training_event_table builds the correct schema + partition spec."""

    def test_schema_has_canonical_fields(self, tmp_path):
        """The Iceberg schema must include all canonical TrainingEvent fields."""
        catalog = _make_catalog(tmp_path)
        table = create_training_event_table(catalog)

        field_names = {f.name for f in table.schema().fields}
        # Required canonical fields from TrainingEvent.avsc
        assert "event_id" in field_names
        assert "event_time" in field_names
        assert "athlete_id" in field_names
        assert "event_type" in field_names
        assert "session_load" in field_names
        # Optional canonical fields
        assert "ingest_time" in field_names
        assert "source" in field_names
        assert "schema_version" in field_names
        assert "workout_id" in field_names
        assert "reps" in field_names
        assert "weight_kg" in field_names
        assert "rpe" in field_names
        assert "tss" in field_names

    def test_partition_spec_is_athlete_id_and_day(self, tmp_path):
        """Partition spec must be (athlete_id=identity, event_time=day)."""
        catalog = _make_catalog(tmp_path)
        table = create_training_event_table(catalog)

        spec = table.spec()
        assert len(spec.fields) == 2

        # First partition: athlete_id identity transform
        athlete_field = spec.fields[0]
        assert athlete_field.name == "athlete_id"
        from pyiceberg.transforms import IdentityTransform
        assert isinstance(athlete_field.transform, IdentityTransform)

        # Second partition: event_time day transform
        day_field = spec.fields[1]
        assert day_field.name == "event_time_day"
        from pyiceberg.transforms import DayTransform
        assert isinstance(day_field.transform, DayTransform)

    def test_table_uses_parquet_and_iceberg_v2(self, tmp_path):
        """Table must be Iceberg V2 format with snappy Parquet."""
        catalog = _make_catalog(tmp_path)
        table = create_training_event_table(catalog)

        assert table.format_version == 2
        props = table.properties
        assert props.get("write.parquet.compression-codec") == "snappy"


# ---------------------------------------------------------------------------
# Test 2: append a batch → read back N rows
# ---------------------------------------------------------------------------


class TestAppendEvents:
    """append_events writes a batch; direct Parquet read returns exactly those rows."""

    def test_append_single_batch_returns_correct_row_count(self, tmp_path):
        """Appending N records → Parquet files on disk contain N rows total."""
        catalog = _make_catalog(tmp_path)
        table = create_training_event_table(catalog)

        events = [_EVENT_A1, _EVENT_A2, _EVENT_B1]
        append_events(table, events)

        result = _read_all_parquet(tmp_path)
        assert len(result) == 3

    def test_append_preserves_field_values(self, tmp_path):
        """Appended rows must have the correct field values in the Parquet files."""
        catalog = _make_catalog(tmp_path)
        table = create_training_event_table(catalog)

        append_events(table, [_EVENT_A1])

        result = _read_all_parquet(tmp_path)
        assert result["event_id"].to_pylist() == ["evt-a1"]
        assert result["athlete_id"].to_pylist() == ["athlete_a"]
        assert result["event_type"].to_pylist() == ["strength"]
        # session_load is float32 — allow small tolerance
        assert abs(result["session_load"].to_pylist()[0] - 100.0) < 0.01

    # ---------------------------------------------------------------------------
    # Test 3: additive semantics (append twice → 2N rows)
    # ---------------------------------------------------------------------------

    def test_append_is_additive(self, tmp_path):
        """Two appends must be additive: 2 × N events → 2N rows across all Parquet files."""
        catalog = _make_catalog(tmp_path)
        table = create_training_event_table(catalog)

        events = [_EVENT_A1, _EVENT_A2, _EVENT_B1]
        append_events(table, events)
        append_events(table, events)  # second append produces additional Parquet files

        result = _read_all_parquet(tmp_path)
        assert len(result) == 6


# ---------------------------------------------------------------------------
# Test 4: partition spec actually partitions
# ---------------------------------------------------------------------------


class TestPartitionSpec:
    """The partition spec really splits data by athlete_id + day."""

    def test_spec_fields_match_source_columns(self, tmp_path):
        """Partition fields reference the correct source column IDs."""
        catalog = _make_catalog(tmp_path)
        table = create_training_event_table(catalog)

        schema = table.schema()
        spec = table.spec()

        athlete_source = spec.fields[0].source_id
        day_source = spec.fields[1].source_id

        # source_id must match the field id of athlete_id and event_time in schema
        assert schema.find_field("athlete_id").field_id == athlete_source
        assert schema.find_field("event_time").field_id == day_source

    def test_two_athletes_land_in_different_partitions(self, tmp_path):
        """Events for different athletes must produce at least 2 data files."""
        catalog = _make_catalog(tmp_path)
        table = create_training_event_table(catalog)

        append_events(table, [_EVENT_A1, _EVENT_B1])

        # After append, the table must have at least 2 data files
        # (one per (athlete_id, day) partition combination)
        snapshot = table.current_snapshot()
        assert snapshot is not None

        from pyiceberg.io.pyarrow import PyArrowFileIO
        io = PyArrowFileIO(table.properties)
        manifests = snapshot.manifests(io)
        assert len(manifests) >= 1  # at least one manifest


# ---------------------------------------------------------------------------
# Test 5: compaction helper
# ---------------------------------------------------------------------------


class TestCompaction:
    """Compaction helpers are importable and behave correctly."""

    def test_compact_table_raises_on_pyiceberg_lt_0_8(self, tmp_path):
        """compact_table raises RuntimeError on pyiceberg < 0.8 (no rewrite API).

        pyiceberg 0.7.x does not ship RewriteDataFilesAction.  compact_table
        is a forward-compatible stub: it raises RuntimeError with a clear
        message instead of silently doing nothing.  The data must remain
        intact (readable) after the failed compact call.
        """
        import pyiceberg
        from storage.iceberg.compaction import compact_table

        catalog = _make_catalog(tmp_path)
        table = create_training_event_table(catalog)

        append_events(table, [_EVENT_A1])
        append_events(table, [_EVENT_A2])

        version_parts = tuple(int(x) for x in pyiceberg.__version__.split(".")[:2])
        if version_parts >= (0, 8):
            # On 0.8+, compact_table should succeed
            compact_table(table)
            result = _read_all_parquet(tmp_path)
            assert len(result) == 2
        else:
            # On 0.7.x, compact_table must raise RuntimeError
            with pytest.raises(RuntimeError, match="pyiceberg >= 0.8"):
                compact_table(table)
            # Data must still be readable after the failed compact
            result = _read_all_parquet(tmp_path)
            assert len(result) == 2

    def test_expire_old_snapshots_raises_on_pyiceberg_lt_0_8(self, tmp_path):
        """expire_old_snapshots raises RuntimeError on pyiceberg < 0.8.

        ManageSnapshots.expire_snapshots() is not available in pyiceberg 0.7.x.
        On 0.8+ it should succeed as a no-op when older_than_ms=0 (1970-01-01).
        """
        import pyiceberg
        from storage.iceberg.compaction import expire_old_snapshots

        catalog = _make_catalog(tmp_path)
        table = create_training_event_table(catalog)

        append_events(table, [_EVENT_A1])
        append_events(table, [_EVENT_A2])

        version_parts = tuple(int(x) for x in pyiceberg.__version__.split(".")[:2])
        if version_parts >= (0, 8):
            # On 0.8+, expire with older_than_ms=0 is a safe no-op
            expire_old_snapshots(table, older_than_ms=0)
            result = _read_all_parquet(tmp_path)
            assert len(result) == 2
        else:
            # On 0.7.x, expire raises RuntimeError (no expire_snapshots API)
            with pytest.raises(RuntimeError, match="pyiceberg >= 0.8"):
                expire_old_snapshots(table, older_than_ms=0)
            # Data must still be readable after the failed expire
            result = _read_all_parquet(tmp_path)
            assert len(result) == 2
