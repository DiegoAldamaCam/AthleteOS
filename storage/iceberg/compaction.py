"""Iceberg compaction helpers for the training_event table (work-unit 6.2, PR5).

Compaction is an operational concern: it is invoked by a scheduled job
(cron / Airflow / manual trigger), not per-record or per-checkpoint.

Public API
----------
compact_table(table, target_file_size_bytes=134_217_728) -> None
    Compact small Parquet files using pyiceberg's RewriteDataFilesAction
    when available (pyiceberg >= 0.8).  On pyiceberg < 0.8 the action is
    not available; this function raises RuntimeError with a clear upgrade
    message so operators know what to do.

    Args:
        table: A pyiceberg Table (from create_training_event_table).
        target_file_size_bytes: Target output file size in bytes.
            Defaults to 128 MiB.  Ignored on pyiceberg < 0.8.

expire_old_snapshots(table, older_than_ms) -> None
    Expire snapshots older than the given epoch-ms timestamp.  This is
    available in pyiceberg 0.7+ via manage_snapshots() and reduces the
    metadata growth caused by per-checkpoint appends.  Expiring snapshots
    does NOT compact data files (that requires RewriteDataFilesAction);
    it only removes old snapshot entries and their manifest files.

    Args:
        table: A pyiceberg Table.
        older_than_ms: Expire snapshots older than this epoch-ms timestamp.

Design notes (pyiceberg version matrix)
---------------------------------------
- pyiceberg 0.5.x: no write API at all.
- pyiceberg 0.6.x: append() for unpartitioned tables only.
- pyiceberg 0.7.x: append() for partitioned tables (this project).
  No rewrite/compaction API.  ManageSnapshots provides expire_snapshots.
- pyiceberg 0.8+: RewriteDataFilesAction added → full compaction support.

The project currently pins pyiceberg==0.7.1 (pinned in pyproject.toml at
"pyiceberg[pyarrow,sql-sqlite]>=0.7.1,<0.8" to stay compatible with the
apache-flink 1.19 → apache-beam 2.48.0 → pyarrow<12.0.0 constraint).
When apache-beam relaxes its pyarrow upper bound, upgrade pyarrow → 14+
then upgrade pyiceberg to 0.8+ and the compaction stub becomes real.
"""

from __future__ import annotations

from pyiceberg.table import Table

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_TARGET_FILE_SIZE = 128 * 1024 * 1024  # 128 MiB


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def compact_table(
    table: Table,
    target_file_size_bytes: int = _DEFAULT_TARGET_FILE_SIZE,
) -> None:
    """Compact small Parquet files using pyiceberg's RewriteDataFilesAction.

    Requires pyiceberg >= 0.8.  On the currently installed 0.7.x this
    raises RuntimeError with upgrade instructions.

    Args:
        table: The pyiceberg Table to compact.
        target_file_size_bytes: Target output Parquet file size.
            Defaults to 128 MiB.

    Raises:
        RuntimeError: If pyiceberg < 0.8 is installed (no rewrite API).
    """
    try:
        from pyiceberg.table.rewrite import RewriteDataFilesAction  # type: ignore[import]
    except ImportError as exc:
        import pyiceberg

        raise RuntimeError(
            f"RewriteDataFilesAction requires pyiceberg >= 0.8; "
            f"currently installed: {pyiceberg.__version__}.  "
            "Upgrade pyiceberg (requires relaxing the pyarrow<12 constraint "
            "from apache-beam; see storage/iceberg/compaction.py)."
        ) from exc

    action = RewriteDataFilesAction(
        table,
        options={"target-file-size-bytes": str(target_file_size_bytes)},
    )
    action.execute()


def expire_old_snapshots(table: Table, older_than_ms: int) -> None:
    """Expire snapshots older than *older_than_ms* (epoch-milliseconds).

    Available in pyiceberg >= 0.8 via the expire_snapshots() API.
    On pyiceberg 0.7.x this raises RuntimeError with an upgrade message.

    Args:
        table: The pyiceberg Table whose old snapshots will be expired.
        older_than_ms: Epoch-ms timestamp.  Snapshots created before this
            time are eligible for expiration.

    Raises:
        RuntimeError: If pyiceberg < 0.8 is installed (no expire_snapshots API).
    """
    import pyiceberg

    version_parts = tuple(int(x) for x in pyiceberg.__version__.split(".")[:2])
    if version_parts < (0, 8):
        raise RuntimeError(
            f"expire_snapshots() requires pyiceberg >= 0.8; "
            f"currently installed: {pyiceberg.__version__}.  "
            "Upgrade pyiceberg (requires relaxing the pyarrow<12 constraint "
            "from apache-beam; see storage/iceberg/compaction.py)."
        )

    from datetime import datetime, timezone

    older_than_dt = datetime.fromtimestamp(older_than_ms / 1000, tz=timezone.utc)

    with table.manage_snapshots() as ms:
        ms.expire_snapshots(older_than=older_than_dt).commit()
