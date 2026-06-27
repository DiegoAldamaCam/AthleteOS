"""Wellness-metrics Flink job: canonical.wellness_event → recovery_score UPSERT.

Import-isolation contract
=========================
All pyflink imports are LAZY — they live inside run(). The pure compute math
(compute_recovery_score) lives in jobs.wellness_metrics.compute, which is
fully unit-tested under CPython 3.14 without a Flink runtime.

The integration tests (tests/integration/test_wellness_metrics_job.py) exercise
the UPSERT logic directly against a real PostgreSQL container (no Flink required).

Topology (ADR-17: separate job, no stream-stream join)
======================================================
canonical.wellness_event (Avro, avro-confluent Table source)
  -> toDataStream -> DataStream[Row]
  -> assign_timestamps_and_watermarks (event_time epoch-ms, 24h OOO)
  -> key_by(athlete_id)
  -> RecoveryKeyedProcessFunction.process_element:
       hrv = row.hrv (None if absent)
       sleep_hours = row.sleep_hours
       perceived_recovery = row.perceived_recovery
       rs = compute_recovery_score(hrv, cfg.hrv_baseline_default, sleep_hours, perceived_recovery)
       if rs is None: skip (W3-5: all-null event → no DB write)
       metric_date = epoch_ms_to_date(event_time)
       record = {athlete_id, metric_date, recovery_score: rs}
       upsert_with_retry(record, conn, conn_factory) using build_recovery_upsert
  -> athlete_metrics (recovery_score ONLY; load cols untouched / NULL on first insert)

ADR-18: hrv_baseline is a fixed configurable constant (not rolling per-athlete state).
ADR-19: load columns are nullable so wellness-only rows can be INSERTed without them.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Optional


# ---------------------------------------------------------------------------
# Job configuration (import-safe — no pyflink at module level)
# ---------------------------------------------------------------------------


@dataclass
class WellnessMetricsJobConfig:
    """Configuration for the wellness-metrics Flink job.

    All fields have defaults so the job can be run with minimal configuration.
    ADR-18: hrv_baseline_default is the fixed MVP HRV baseline used for ALL
    athletes. A future iteration will replace this with per-athlete ListState.
    """

    kafka_bootstrap_servers: str = "localhost:9092"
    schema_registry_url: str = "http://localhost:8081"
    canonical_topic: str = "canonical.wellness_event"
    group_id: str = "wellness-metrics-job"
    postgres_dsn: str = ""
    hrv_baseline_default: float = 60.0  # ADR-18: fixed MVP baseline
    bounded: bool = False  # True for integration tests (bounded source)
    parallelism: int = 1


# ---------------------------------------------------------------------------
# Job entry point (all pyflink imports are LAZY inside run())
# ---------------------------------------------------------------------------


def run(cfg: Optional[WellnessMetricsJobConfig] = None) -> None:
    """Run the wellness-metrics Flink job.

    All pyflink imports are lazy so the module is importable under CPython 3.14
    (no apache-flink wheel for 3.14).
    """
    if cfg is None:
        cfg = WellnessMetricsJobConfig()

    # Lazy imports — only executed when the Flink runtime is present
    from pyflink.datastream import StreamExecutionEnvironment  # type: ignore
    from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext  # type: ignore
    from pyflink.datastream.state import ValueStateDescriptor  # type: ignore
    from pyflink.common.typeinfo import Types  # type: ignore
    from pyflink.table import StreamTableEnvironment  # type: ignore
    from pyflink.common import WatermarkStrategy, Duration  # type: ignore
    from pyflink.common.watermark_strategy import TimestampAssigner  # type: ignore

    from storage.postgres.sink import build_recovery_upsert, epoch_ms_to_date, upsert_with_retry
    from jobs.wellness_metrics.compute import compute_recovery_score

    # -----------------------------------------------------------------------
    # Environment setup
    # -----------------------------------------------------------------------

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(cfg.parallelism)
    if cfg.bounded:
        env.set_runtime_mode(RuntimeExecutionMode.BATCH)  # type: ignore

    t_env = StreamTableEnvironment.create(env)

    # -----------------------------------------------------------------------
    # Avro-confluent Table source for canonical.wellness_event
    # -----------------------------------------------------------------------

    _ddl_source = f"""
    CREATE TABLE wellness_event_source (
        athlete_id          STRING,
        event_id            STRING,
        event_time          BIGINT,
        event_type          STRING,
        hrv                 DOUBLE,
        sleep_hours         DOUBLE,
        resting_hr          INT,
        steps               INT,
        body_weight_kg      DOUBLE,
        energy              INT,
        soreness            INT,
        mood                INT,
        stress              INT,
        perceived_recovery  INT,
        schema_version      INT
    ) WITH (
        'connector'                     = 'kafka',
        'topic'                         = '{cfg.canonical_topic}',
        'properties.bootstrap.servers'  = '{cfg.kafka_bootstrap_servers}',
        'properties.group.id'           = '{cfg.group_id}',
        'scan.startup.mode'             = 'earliest-offset',
        'format'                        = 'avro-confluent',
        'avro-confluent.schema-registry.url' = '{cfg.schema_registry_url}'
    )
    """
    t_env.execute_sql(_ddl_source)

    wellness_table = t_env.from_path("wellness_event_source")
    ds = t_env.to_data_stream(wellness_table)

    # -----------------------------------------------------------------------
    # Timestamp assigner + watermark
    # -----------------------------------------------------------------------

    class _WellnessTimestampAssigner(TimestampAssigner):
        def extract_timestamp(self, value: Any, record_timestamp: int) -> int:  # type: ignore
            try:
                return int(value["event_time"])
            except (KeyError, TypeError, ValueError):
                return 0

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_hours(24))
        .with_timestamp_assigner(_WellnessTimestampAssigner())
    )
    ds = ds.assign_timestamps_and_watermarks(watermark_strategy)

    # -----------------------------------------------------------------------
    # RecoveryKeyedProcessFunction
    # -----------------------------------------------------------------------

    import psycopg2 as _psycopg2  # type: ignore

    class RecoveryKeyedProcessFunction(KeyedProcessFunction):
        """Per-athlete process function: computes and UPSERTs recovery_score."""

        def __init__(self, job_cfg: WellnessMetricsJobConfig) -> None:
            self._cfg = job_cfg
            self._conn: Any = None

        def open(self, runtime_context: RuntimeContext) -> None:
            self._conn = _psycopg2.connect(self._cfg.postgres_dsn)

        def close(self) -> None:
            if self._conn is not None:
                try:
                    self._conn.close()
                except Exception:  # noqa: BLE001
                    pass

        def process_element(self, value: Any, ctx: Any) -> None:
            try:
                hrv = value.get("hrv") if isinstance(value, dict) else getattr(value, "hrv", None)
                sleep_hours = (
                    value.get("sleep_hours") if isinstance(value, dict)
                    else getattr(value, "sleep_hours", None)
                )
                perceived_recovery = (
                    value.get("perceived_recovery") if isinstance(value, dict)
                    else getattr(value, "perceived_recovery", None)
                )
                event_time = (
                    value.get("event_time") if isinstance(value, dict)
                    else getattr(value, "event_time", 0)
                )

                rs = compute_recovery_score(
                    hrv,
                    self._cfg.hrv_baseline_default,
                    sleep_hours,
                    perceived_recovery,
                )
                if rs is None:
                    return  # W3-5: all-null event → no DB write

                athlete_id = (
                    value.get("athlete_id") if isinstance(value, dict)
                    else getattr(value, "athlete_id", None)
                )
                metric_date = epoch_ms_to_date(int(event_time))

                record = {
                    "athlete_id": athlete_id,
                    "metric_date": int(event_time),
                    "recovery_score": rs,
                }
                self._conn = upsert_with_retry(
                    record,
                    self._conn,
                    lambda: _psycopg2.connect(self._cfg.postgres_dsn),
                    max_retries=3,
                    base_backoff_s=0.5,
                )
            except Exception as exc:  # noqa: BLE001
                # Log and continue — a single bad event must not crash the job
                import logging
                logging.getLogger(__name__).warning(
                    "RecoveryKeyedProcessFunction: error processing event: %s", exc
                )

    ds.key_by(lambda row: (
        row.get("athlete_id") if isinstance(row, dict) else getattr(row, "athlete_id", "")
    )).process(RecoveryKeyedProcessFunction(cfg))

    env.execute("wellness-metrics-job")
