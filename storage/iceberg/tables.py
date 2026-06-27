"""Iceberg table definitions for the analytical store (work-unit 6.2, PR5).

This module is deliberately pyflink-free: it creates and returns Iceberg
Table objects using pyiceberg (pure Python) against a local SqlCatalog
backed by sqlite.  The Flink wiring (calling append per checkpoint) is
added in work-unit 6.4.

Schema source of truth
----------------------
schemas/canonical/TrainingEvent.avsc defines the canonical fields.
The Iceberg schema mirrors every field, translating Avro types to Iceberg:
  - string → StringType()
  - long (epoch-ms) → TimestampType()   [naive; epoch-us inside Iceberg]
  - int   → IntegerType()
  - float → FloatType()
  - union[null, X] → required=False (nullable)

TimestampType (naive) vs TimestamptzType (UTC-aware)
----------------------------------------------------
The design specifies (athlete_id, days(event_time)) as the partition.
DayTransform requires a timestamp type (not Long).  TimestamptzType adds
a UTC timezone and requires the Arrow C++ IANA timezone database to be
present when computing pc.days_between(), which is NOT available on stock
pyarrow 11 on Windows (without external tzdata setup).  TimestampType
(naive, no tz) is semantically equivalent here because all event_time
values are UTC epoch-milliseconds that we convert to epoch-microseconds,
and DayTransform works correctly without the tzdata dependency.  CI (Linux)
is unaffected: both types work there.

Catalog convention
------------------
Use SqlCatalog with a sqlite URI pointing to a file inside the warehouse
directory.  The caller provides the catalog object so tests can pass an
in-tmp_path catalog while production code wires the configured path.

Partition spec (from approved PR5 decisions)
--------------------------------------------
  athlete_id → IdentityTransform (field_id=1000)
  event_time → DayTransform      (field_id=1001, name="event_time_day")

Table properties
----------------
  format-version: 2   (Iceberg V2)
  write.parquet.compression-codec: snappy

Namespace
---------
All tables live under the "default" namespace, created here if absent.
"""

from __future__ import annotations

from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import (
    FloatType,
    IntegerType,
    NestedField,
    StringType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_NAMESPACE = "default"
_TABLE_NAME = "training_event"
_FULL_NAME = f"{_NAMESPACE}.{_TABLE_NAME}"

_TABLE_PROPERTIES: dict = {
    "format-version": "2",
    "write.parquet.compression-codec": "snappy",
}

# ---------------------------------------------------------------------------
# Canonical schema
# ---------------------------------------------------------------------------
# Field IDs are stable: 1-20 matching the field order in TrainingEvent.avsc.
# All required fields in the Avro schema → required=True here.
# Union[null, X] in Avro → required=False (nullable) here.

TRAINING_EVENT_SCHEMA = Schema(
    # --- common envelope (required) ---
    NestedField(1, "event_id", StringType(), required=True),
    NestedField(2, "event_time", TimestampType(), required=True),   # epoch-us (naive)
    NestedField(3, "ingest_time", TimestampType(), required=False),
    NestedField(4, "source", StringType(), required=False),
    NestedField(5, "schema_version", IntegerType(), required=False),
    NestedField(6, "athlete_id", StringType(), required=True),
    # --- training event fields ---
    NestedField(7, "event_type", StringType(), required=True),
    NestedField(8, "session_load", FloatType(), required=True),
    # --- strength-specific (nullable) ---
    NestedField(9, "workout_id", StringType(), required=False),
    NestedField(10, "exercise_id", StringType(), required=False),
    NestedField(11, "set_number", IntegerType(), required=False),
    NestedField(12, "reps", IntegerType(), required=False),
    NestedField(13, "weight_kg", FloatType(), required=False),
    NestedField(14, "rpe", FloatType(), required=False),
    NestedField(15, "rir", FloatType(), required=False),
    # --- cardio-specific (nullable) ---
    NestedField(16, "activity_type", StringType(), required=False),
    NestedField(17, "distance_km", FloatType(), required=False),
    NestedField(18, "duration_sec", IntegerType(), required=False),
    NestedField(19, "avg_hr", IntegerType(), required=False),
    NestedField(20, "tss", FloatType(), required=False),
)

# ---------------------------------------------------------------------------
# Partition spec
# ---------------------------------------------------------------------------
# Partition spec: (athlete_id=identity, event_time=day).
# Approved in PR5 decisions (athleteos-foundation #48).

TRAINING_EVENT_PARTITION_SPEC = PartitionSpec(
    PartitionField(
        source_id=6,           # athlete_id field id
        field_id=1000,
        transform=IdentityTransform(),
        name="athlete_id",
    ),
    PartitionField(
        source_id=2,           # event_time field id
        field_id=1001,
        transform=DayTransform(),
        name="event_time_day",
    ),
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def create_training_event_table(catalog: Catalog) -> Table:
    """Create (or load) the training_event Iceberg table.

    If the table already exists, the existing table is returned.
    The "default" namespace is created if it does not exist.

    Args:
        catalog: A pyiceberg Catalog (typically SqlCatalog backed by sqlite).

    Returns:
        The pyiceberg Table object ready for reads and writes.
    """
    # Ensure namespace exists
    try:
        catalog.create_namespace(_NAMESPACE)
    except NamespaceAlreadyExistsError:
        pass

    # Load existing table or create it
    try:
        return catalog.load_table(_FULL_NAME)
    except NoSuchTableError:
        pass

    return catalog.create_table(
        _FULL_NAME,
        schema=TRAINING_EVENT_SCHEMA,
        partition_spec=TRAINING_EVENT_PARTITION_SPEC,
        properties=_TABLE_PROPERTIES,
    )
