"""DuckDB read-path package for the AthleteOS analytical store (work-unit 6.3, PR5).

This package is deliberately pyflink-free and has no Docker dependency.
The Flink wiring (6.4) imports from storage.iceberg and storage.postgres directly.

Public modules:
  reader  -- read_training_events(warehouse_path) -> list[dict]
  parity  -- check_parity(pg_rows, iceberg_rows, *, tolerance=1e-3) -> list[dict]
"""
