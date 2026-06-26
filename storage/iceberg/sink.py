"""Iceberg append sink for canonical training_event records (work-unit 6.2, PR5).

This module is deliberately pyflink-free.  The Flink wiring (invoking
append_events per checkpoint) is added in work-unit 6.4.

Public API
----------
append_events(table, events) -> None
    Append a batch of canonical event dicts to the training_event Iceberg
    table.  Events are immutable facts → append-only; at-least-once is
    sufficient (approved PR5 decision, obs #48).

    Args:
        table: A pyiceberg Table (returned by create_training_event_table).
        events: Iterable of dicts with canonical training_event fields.
                Required keys: event_id, event_time (epoch-ms), athlete_id,
                event_type, session_load.  All other fields are optional;
                absent keys default to None.

Schema notes
------------
event_time in the canonical stream is epoch-milliseconds (int).
TimestampType in Iceberg maps to timestamp[us] (microseconds) in Arrow.
The sink multiplies epoch_ms × 1000 to get epoch_us before writing.

All optional fields not present in a record dict are filled with None
(Python → Arrow null → Parquet null).  This matches the Avro schema where
optional fields are union[null, type].
"""

from __future__ import annotations

from typing import Iterable, Mapping, Any

import pyarrow as pa
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.table import Table

# Type alias for a single event record dict
EventRecord = Mapping[str, Any]


def _build_arrow_batch(table: Table, events: list[EventRecord]) -> pa.Table:
    """Convert a list of event dicts to a PyArrow Table matching the Iceberg schema.

    epoch_ms values in event_time / ingest_time are multiplied by 1000 to
    produce the epoch_us values that Iceberg's TimestampType expects.

    Args:
        table: The target Iceberg Table (used to fetch the canonical schema).
        events: List of event dicts (canonical training_event records).

    Returns:
        A PyArrow Table whose schema matches the Iceberg table schema exactly.
    """
    arrow_schema = schema_to_pyarrow(table.schema())

    # Collect column values in schema order
    cols: dict[str, list] = {name: [] for name in arrow_schema.names}

    for evt in events:
        cols["event_id"].append(str(evt["event_id"]))
        # epoch_ms → epoch_us (TimestampType is naive microseconds)
        cols["event_time"].append(int(evt["event_time"]) * 1000)
        ingest_ms = evt.get("ingest_time")
        cols["ingest_time"].append(int(ingest_ms) * 1000 if ingest_ms is not None else None)
        cols["source"].append(evt.get("source"))
        schema_ver = evt.get("schema_version")
        cols["schema_version"].append(int(schema_ver) if schema_ver is not None else None)
        cols["athlete_id"].append(str(evt["athlete_id"]))
        cols["event_type"].append(str(evt["event_type"]))
        cols["session_load"].append(float(evt["session_load"]))
        cols["workout_id"].append(evt.get("workout_id"))
        cols["exercise_id"].append(evt.get("exercise_id"))
        set_num = evt.get("set_number")
        cols["set_number"].append(int(set_num) if set_num is not None else None)
        reps = evt.get("reps")
        cols["reps"].append(int(reps) if reps is not None else None)
        wkg = evt.get("weight_kg")
        cols["weight_kg"].append(float(wkg) if wkg is not None else None)
        rpe = evt.get("rpe")
        cols["rpe"].append(float(rpe) if rpe is not None else None)
        rir = evt.get("rir")
        cols["rir"].append(float(rir) if rir is not None else None)
        cols["activity_type"].append(evt.get("activity_type"))
        dkm = evt.get("distance_km")
        cols["distance_km"].append(float(dkm) if dkm is not None else None)
        dur = evt.get("duration_sec")
        cols["duration_sec"].append(int(dur) if dur is not None else None)
        hr = evt.get("avg_hr")
        cols["avg_hr"].append(int(hr) if hr is not None else None)
        tss = evt.get("tss")
        cols["tss"].append(float(tss) if tss is not None else None)

    # Build typed arrays matching the Iceberg-derived Arrow schema
    arrays: dict[str, pa.Array] = {}
    for field in arrow_schema:
        t = field.type
        if t == pa.timestamp("us"):
            arrays[field.name] = pa.array(cols[field.name], type=pa.timestamp("us"))
        elif t == pa.large_utf8():
            arrays[field.name] = pa.array(cols[field.name], type=pa.large_utf8())
        elif t == pa.int32():
            arrays[field.name] = pa.array(cols[field.name], type=pa.int32())
        elif t == pa.float32():
            arrays[field.name] = pa.array(cols[field.name], type=pa.float32())
        else:
            arrays[field.name] = pa.array(cols[field.name])

    return pa.table(arrays, schema=arrow_schema)


def append_events(table: Table, events: Iterable[EventRecord]) -> None:
    """Append a batch of canonical training_event records to the Iceberg table.

    Events are immutable facts; this function always appends, never upserts.
    At-least-once delivery semantics apply: duplicate event_ids may appear
    in the table if the same batch is replayed; downstream consumers must
    handle or ignore duplicates (upstream dedup via event_id TTL in Flink
    already limits the replay window to 7d).

    Args:
        table: The target pyiceberg Table (from create_training_event_table).
        events: Iterable of canonical event record dicts.  Empty iterables
                result in a no-op (no snapshot is committed).

    Raises:
        ValueError: If a required field (event_id, event_time, athlete_id,
                    event_type, session_load) is missing from any record.
    """
    records = list(events)
    if not records:
        return  # nothing to append; no snapshot committed

    arrow_batch = _build_arrow_batch(table, records)
    table.append(arrow_batch)
