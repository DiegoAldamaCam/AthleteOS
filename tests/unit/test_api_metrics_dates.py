"""Unit tests for the metrics endpoint's date helpers (no DB, no Docker).

Guards the spec contract that the default ``to`` boundary resolves in UTC, not
the host's local timezone. Regression guard for the bug where ``_today_utc()``
called ``date.today()`` (local time), which shifts the default window by a
calendar day on any non-UTC server.

Spec source: obs #65 (sdd/athleteos-phase7-web/spec), Domain A:
"to defaults to today (server time, UTC)".
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest import mock

from api.routers.metrics import _today_utc


def test_today_utc_returns_utc_date_not_local():
    """At an instant where UTC and a positive-offset local clock differ by a day,
    _today_utc() must return the UTC date.

    23:30 on 2025-06-30 UTC is already 2025-07-01 in, e.g., UTC+1. A local-time
    implementation would return 2025-07-01; the UTC contract requires 2025-06-30.
    """
    fixed_utc = datetime(2025, 6, 30, 23, 30, tzinfo=timezone.utc)

    class _FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            assert tz is timezone.utc, "_today_utc must request UTC explicitly"
            return fixed_utc

    with mock.patch("api.routers.metrics.datetime", _FixedDatetime):
        assert _today_utc() == date(2025, 6, 30)


def test_today_utc_matches_current_utc_date():
    """Sanity: with no mocking, the helper agrees with the real UTC date."""
    assert _today_utc() == datetime.now(timezone.utc).date()
