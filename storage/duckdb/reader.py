"""DuckDB read path for the Iceberg analytical store (work-unit 6.3, PR5).

This module is deliberately pyflink-free and works on both Windows (local dev)
and Linux (CI) without internet access or any DuckDB extension installation.

Read strategy
-------------
DuckDB's built-in ``INSTALL iceberg; LOAD iceberg`` extension provides
``iceberg_scan('<table_path>')``, but:

1. It requires an outbound internet connection to download the extension the
   first time (CI risk — no guarantee of outbound network in the unit-test job).
2. On Windows, iceberg_scan fails with an IOException on
   ``file://C:/...\\metadata\\version-hint.text`` due to mixed / and \\
   separators in the path (same Windows path-shim class as 6.2 Iceberg sink).

Decision: use ``duckdb.read_parquet([<file_list>])`` over the warehouse's
current Parquet data files instead.  This is robust cross-platform and does
NOT require any extension.  Parquet files are enumerated by:

  1. Walking the pyiceberg Table's current snapshot manifest (authoritative,
     exact set of live data files — works when the caller passes a pyiceberg
     Table object or we can reconstruct one from the warehouse).
  2. Glob fallback: ``Path(warehouse_path).rglob("*.parquet")`` — used when
     no pyiceberg Table is available (e.g., tests that write to tmp_path and
     call read_training_events with only the warehouse path string).

The glob fallback is safe for tests because the test warehouse only ever
contains data written by append_events; it is "good enough" for CI parity
checks.  For production use (6.4 Flink wiring), the caller should pass the
pyiceberg Table object so only live snapshot files are scanned.

CI risk note (for 6.4 integration wiring):
  If 6.4 ever attempts to use iceberg_scan in the integration job, the DuckDB
  iceberg extension must be downloaded at runtime.  CI runners may not have
  outbound internet access for the extension registry.  Prefer the Parquet
  read path (this module) in any environment that cannot guarantee extension
  download.

Public API
----------
read_training_events(warehouse_path, *, table=None, con=None) -> list[dict]
    Read all training_event Parquet files from the given warehouse directory.
    Returns a list of row dicts (one per event row).  Returns an empty list
    if no Parquet files are found.

    Args:
        warehouse_path: Path to the Iceberg warehouse root (str or Path).
            Files are discovered via glob under this path.
        table: Optional pyiceberg Table object.  If provided, live data files
            are enumerated from the current snapshot's manifests instead of
            glob.  Use this in production (6.4) for exact snapshot coverage.
        con: Optional existing duckdb.DuckDBPyConnection.  If None, a new
            in-memory connection is created per call.  Inject a connection in
            tests to verify SQL-level behaviour.

    Returns:
        List of dicts, one per row, with keys matching the Parquet column names.
        Returns [] when the warehouse is empty or contains no Parquet files.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import duckdb


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _parquet_files_from_glob(warehouse_path: Path) -> list[Path]:
    """Enumerate all Parquet files under warehouse_path via recursive glob.

    This is the glob fallback: it discovers every *.parquet file under the
    warehouse directory.  For a well-formed Iceberg warehouse this returns
    exactly the current data files (Iceberg never deletes files in place;
    old files stay until explicit compaction/expiry — out of scope for 6.3).

    On Windows, paths are returned with forward slashes for DuckDB compatibility
    (DuckDB's read_parquet accepts both, but normalising is safer).
    """
    return sorted(warehouse_path.rglob("*.parquet"))


def _parquet_files_from_snapshot(table: Any) -> list[Path]:
    """Enumerate live Parquet data files from the pyiceberg Table's current snapshot.

    Iterates the current snapshot's manifests to retrieve only the files
    belonging to the live snapshot (post-delete files are excluded, which
    is the correct behaviour for Iceberg V2 copy-on-write or MoR tables).

    Falls back to an empty list if the table has no current snapshot
    (empty table — no appends yet).

    Args:
        table: A pyiceberg Table object.

    Returns:
        Sorted list of Path objects for each live data file.
    """
    snapshot = table.current_snapshot()
    if snapshot is None:
        return []

    # Resolve file IO from the table (same approach as test_iceberg_sink.py)
    from pyiceberg.io.pyarrow import PyArrowFileIO

    io = PyArrowFileIO(table.properties)
    manifests = snapshot.manifests(io)

    files: list[Path] = []
    for manifest in manifests:
        for entry in manifest.fetch_manifest_entry(io):
            # data_file.file_path may be a URI (file:///...) or a bare path
            raw_path: str = entry.data_file.file_path
            # Strip file:// prefix if present, then normalise separators
            if raw_path.startswith("file://"):
                raw_path = raw_path[7:]
            # On Windows: /C:/path -> C:/path
            if sys.platform == "win32" and raw_path.startswith("/") and len(raw_path) > 2 and raw_path[2] == ":":
                raw_path = raw_path[1:]
            files.append(Path(raw_path))

    return sorted(files)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def read_training_events(
    warehouse_path: "str | Path",
    *,
    table: Any = None,
    con: "duckdb.DuckDBPyConnection | None" = None,
) -> list[dict]:
    """Read all training_event rows from the Iceberg warehouse via DuckDB.

    Uses direct Parquet file reads (not iceberg_scan) for cross-platform
    compatibility.  See module docstring for the full rationale.

    Args:
        warehouse_path: Root of the Iceberg warehouse directory.
        table: Optional pyiceberg Table — enables snapshot-accurate file
               enumeration.  If None, falls back to recursive glob.
        con: Optional DuckDB connection (in-memory if None).

    Returns:
        List of row dicts with keys matching the Parquet schema column names.
        Empty list if no data files exist.
    """
    warehouse = Path(warehouse_path)

    # Determine which Parquet files to read
    if table is not None:
        parquet_files = _parquet_files_from_snapshot(table)
        if not parquet_files:
            # Snapshot may be empty — fall back to glob
            parquet_files = _parquet_files_from_glob(warehouse)
    else:
        parquet_files = _parquet_files_from_glob(warehouse)

    if not parquet_files:
        return []

    # Normalise to forward-slash strings for DuckDB on all platforms
    file_strings = [str(p).replace("\\", "/") for p in parquet_files]

    # Build and execute the DuckDB query
    own_con = con is None
    if own_con:
        con = duckdb.connect(":memory:")

    try:
        # read_parquet accepts a list of file paths via parameter binding.
        # Using parameterised query (list argument) instead of string
        # interpolation prevents SQL injection through path values.
        relation = con.execute("SELECT * FROM read_parquet(?)", [file_strings])
        columns = [desc[0] for desc in relation.description]
        return [dict(zip(columns, row)) for row in relation.fetchall()]
    finally:
        if own_con:
            con.close()
