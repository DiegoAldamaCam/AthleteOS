# AthleteOS Python-enabled Flink 1.19 image.
#
# Shared by flink-jobmanager, flink-taskmanager, and flink-job-submit.
# Python UDFs (KeyedProcessFunction, ProcessFunction) run inside the TaskManager
# via py4j/Beam fn_execution — TM MUST carry the full Python runtime, not just
# the connector JARs. One shared image avoids any divergence.
#
# Layout contract (W1 — CRITICAL):
#   jobs/canonicalize/transform.py line 407 reads:
#     _SCHEMA_DIR = Path(__file__).resolve().parent.parent.parent / "schemas" / "canonical"
#   This resolves: /opt/flink/usrlib/jobs/canonicalize/transform.py
#     → .parent       = /opt/flink/usrlib/jobs/canonicalize/
#     → .parent.parent = /opt/flink/usrlib/jobs/
#     → .parent.parent.parent = /opt/flink/usrlib/
#     → / "schemas" / "canonical" = /opt/flink/usrlib/schemas/canonical/
#   Therefore jobs/, storage/, and schemas/ MUST be siblings under /opt/flink/usrlib/.
#   The RUN ls assertion below enforces this at build time — a wrong COPY layout
#   fails the build immediately, not silently at runtime.

FROM flink:1.19

# Install Python 3.10 (Ubuntu Jammy 22.04 default) + pip + dev headers.
# Do NOT pip install -e . — pyproject.toml requires-python>=3.11,<3.12 would
# reject 3.10. Jobs use lazy pyflink imports and COPY-based module layout;
# the requires-python gate is bypassed entirely by not invoking pip install -e .
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-dev \
        python-is-python3 \
    && rm -rf /var/lib/apt/lists/*

# W2 build-time assertion: verify that `python` resolves (Flink's PythonDriver
# shells out to `python` on `flink run -pym`; without this the submit crashes
# with "Cannot run program python: error=2" at runtime — not at build time).
RUN python --version

# Install the apache-flink PyPI wheel (must match the Flink cluster version).
# psycopg2-binary: required by the metrics PG UPSERT sink (jobs/metrics/main.py:981).
# Binary wheel avoids needing libpq-dev + build toolchain; version pinned to match
# pyproject.toml (psycopg2-binary>=2.9,<3) — cp310 wheels available for both bounds.
RUN pip3 install --no-cache-dir apache-flink==1.19.3 "psycopg2-binary>=2.9,<3"

# Build-time assertion: fail the build if psycopg2 is missing or broken so the
# error surfaces here instead of crashing the metrics PG sink at the first window
# fire at runtime (mirrors the W2 `RUN python --version` gate above).
RUN python3 -c "import psycopg2"

# Copy connector JARs into $FLINK_HOME/lib so the cluster JVM classpath picks
# them up automatically on startup (not pyflink/lib — that path is for the
# embedded minicluster only used in unit/integration tests).
COPY docker/flink/lib/*.jar $FLINK_HOME/lib/

# Copy job source packages as siblings under /opt/flink/usrlib/ (W1 layout).
# -pyfs /opt/flink/usrlib in submit.sh adds this dir to PYTHONPATH, covering
# jobs.*, storage.*, and schemas.* package imports in one flag.
COPY jobs/ /opt/flink/usrlib/jobs/
COPY storage/ /opt/flink/usrlib/storage/
COPY schemas/ /opt/flink/usrlib/schemas/

# Copy the one-shot submit script and make it executable.
COPY docker/flink/submit.sh /opt/flink/usrlib/submit.sh
RUN chmod +x /opt/flink/usrlib/submit.sh

# DEFECT-#3 fix: entrypoint wrapper that chowns /flink-checkpoints at startup
# before handing off to the real Flink entrypoint via gosu.
#
# Background: Docker named volumes initialise as root:root 0755. A RUN chown
# in the Dockerfile is overwritten by Docker when the volume is first mounted.
# The wrapper runs as root (compose sets user: root for JM/TM), fixes the
# ownership, then execs "gosu flink /docker-entrypoint.sh" so all Flink
# processes run as uid 9999 (flink) — not root. The flink-job-submit service
# does NOT use this wrapper (no checkpoint writes needed there).
COPY docker/flink/docker-entrypoint-wrapper.sh /docker-entrypoint-wrapper.sh
RUN chmod +x /docker-entrypoint-wrapper.sh

# W1 build-time assertion: verify the .avsc path that transform.py:407 will
# read at runtime. If the COPY layout is wrong, this RUN step fails the build
# immediately with a clear error — not silently at job submission time.
RUN ls /opt/flink/usrlib/schemas/canonical/TrainingEvent.avsc
