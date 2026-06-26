"""Metrics job package (PR4, Phase 5).

Import-isolation contract
=========================
``jobs.metrics.compute`` is pyflink-free and unit-tested on any interpreter.
``jobs.metrics.main`` keeps ALL pyflink imports lazy inside ``run()`` so the
module imports cleanly on interpreters without apache-flink (CPython 3.14),
satisfying the lazy-import contract (``python -c "import jobs.metrics.main"``
must work without pyflink).
"""
