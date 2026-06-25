"""Canonicalize job package (PR3, Phase 4).

Layout (continuity with PR2's import-isolation discipline):

  jobs/canonicalize/transform.py  - PURE transform/validation/session_load/
                                   DLQ-envelope logic + fastavro Avro helpers.
                                   Imports WITHOUT pyflink; fully unit-tested.
  jobs/canonicalize/main.py       - PyFlink job wiring. pyflink is imported
                                   LAZILY inside the entrypoint so packaging,
                                   collection and the unit tests run on
                                   interpreters where apache-flink has no wheel
                                   (e.g. CPython 3.14).

Spec contracts honored here (event-contracts / design):
  - Common Event Envelope: event_time/ingest_time epoch-ms long, schema_version
    REQUIRED int (added here; raw omits it).
  - TrainingEvent Avro schema, event_type = STRENGTH_SET for strength source.
  - session_load (required) derived at canonicalization:
      reps * weight_kg * (rpe / 10.0)   when rpe present
      reps * weight_kg                  when rpe absent
  - DLQ error envelope (JSON): original_topic, original_key, original_value
    (base64), error_type, error_message, error_stack, timestamp.
"""