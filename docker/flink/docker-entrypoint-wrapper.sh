#!/usr/bin/env bash
# docker-entrypoint-wrapper.sh — run as root to fix volume ownership, then
# drop to the flink user (uid 9999) before handing off to the Flink entrypoint.
#
# Problem (DEFECT-#3): Docker named volumes initialise with root:root 0755
# ownership. The Flink JM/TM processes run as uid 9999 (flink) and cannot
# create sub-directories under /flink-checkpoints at startup, which causes
# the metrics job to fail with:
#   "Failed to create directory for shared state: file:/flink-checkpoints/<id>/shared"
#
# Fix: run this wrapper as root (via compose `user: root`), chown the volume
# mount point to flink:flink, then exec the real Flink entrypoint via gosu
# so all Flink processes run as uid 9999 — not root. A Dockerfile RUN chown
# alone is insufficient because Docker overwrites the mount-point ownership
# when it initialises the named volume on first mount.
#
# gosu is included in flink:1.19 (/usr/local/bin/gosu); it replaces the
# current process (exec semantics) and properly propagates signals.

set -euo pipefail

# Ensure the checkpoint dir exists and is owned by the flink user.
mkdir -p /flink-checkpoints
chown -R flink:flink /flink-checkpoints

# Hand off to the original Flink Docker entrypoint, running as the flink user.
exec gosu flink /docker-entrypoint.sh "$@"
