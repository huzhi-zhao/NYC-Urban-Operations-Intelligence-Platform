"""
Unit tests for ``_window_to_past_forecast`` — the pure helper that
translates a caller's ``[start, end)`` date window into the
``(past_days, forecast_days)`` query parameters the Open-Meteo API expects.

The helper is a single ``if/elif/else`` with three branches:

1. ``end <= today``           → window entirely in the past
2. ``start >= today``         → window entirely in the future
3. otherwise                  → window straddles today

Boundary cases (``end == today`` and ``start == today``) are also
covered so we can refactor the comparisons safely.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from ingestion.backfill.fetchers.open_meteo import (  # noqa: E402
    MAX_FORECAST_DAYS,
    MAX_PAST_DAYS,
    _window_to_past_forecast,
)

TODAY = date(2026, 6, 13)


# ── Branch 1: window entirely in the past ────────────────────────────────────


def test_window_all_past_one_day_before_today():
    # [2026-06-12, 2026-06-13) — yesterday only
    past, forecast = _window_to_past_forecast(date(2026, 6, 12), date(2026, 6, 13), TODAY)
    assert (past, forecast) == (1, 0)


def test_window_all_past_one_week_before_today():
    # [2026-06-06, 2026-06-13) — 7 days, all past
    past, forecast = _window_to_past_forecast(date(2026, 6, 6), date(2026, 6, 13), TODAY)
    assert (past, forecast) == (7, 0)


def test_window_all_past_including_today_boundary():
    """``end == today`` triggers the past branch."""
    past, forecast = _window_to_past_forecast(date(2026, 6, 1), date(2026, 6, 13), TODAY)
    assert (past, forecast) == (12, 0)


# ── Branch 2: window entirely in the future ──────────────────────────────────


def test_window_all_future_starts_today():
    """``start == today`` triggers the future branch (not the past one)."""
    past, forecast = _window_to_past_forecast(date(2026, 6, 13), date(2026, 6, 20), TODAY)
    assert (past, forecast) == (0, 7)


def test_window_all_future_next_week():
    past, forecast = _window_to_past_forecast(date(2026, 6, 14), date(2026, 6, 21), TODAY)
    assert (past, forecast) == (0, 7)


# ── Branch 3: window straddles today ────────────────────────────────────────


def test_window_straddles_today_symmetric():
    # 3 days back, 3 days forward
    past, forecast = _window_to_past_forecast(date(2026, 6, 10), date(2026, 6, 16), TODAY)
    assert (past, forecast) == (3, 3)


def test_window_straddles_today_with_today_as_left_edge():
    """``start < today < end`` is the classic straddle."""
    past, forecast = _window_to_past_forecast(date(2026, 6, 12), date(2026, 6, 16), TODAY)
    assert (past, forecast) == (1, 3)


def test_window_straddles_today_with_today_as_right_edge():
    past, forecast = _window_to_past_forecast(date(2026, 6, 10), date(2026, 6, 14), TODAY)
    assert (past, forecast) == (3, 1)


# ── Edge cases ───────────────────────────────────────────────────────────────


def test_zero_day_window_returns_zero_zero():
    """start == end is a degenerate window — past and forecast both 0."""
    past, forecast = _window_to_past_forecast(TODAY, TODAY, TODAY)
    assert (past, forecast) == (0, 0)


def test_zero_day_window_in_the_past():
    past, forecast = _window_to_past_forecast(date(2026, 6, 12), date(2026, 6, 12), TODAY)
    assert (past, forecast) == (0, 0)


def test_zero_day_window_in_the_future():
    past, forecast = _window_to_past_forecast(date(2026, 6, 14), date(2026, 6, 14), TODAY)
    assert (past, forecast) == (0, 0)


def test_one_year_past_window():
    """Open-Meteo's archive API supports 365 days — the helper doesn't
    enforce limits; that's the caller's job (see MAX_PAST_DAYS constant)."""
    past, forecast = _window_to_past_forecast(date(2025, 6, 13), date(2026, 6, 13), TODAY)
    assert (past, forecast) == (365, 0)


def test_end_before_start_yields_negative_past():
    """Defensive: an invalid window (end < start) returns a negative past
    count. The caller is expected to validate the window before calling;
    we don't raise here on purpose. This test documents the behavior so a
    future refactor doesn't silently change it."""
    past, forecast = _window_to_past_forecast(date(2026, 6, 15), date(2026, 6, 10), TODAY)
    assert past == -5
    assert forecast == 0


# ── Return type contract ─────────────────────────────────────────────────────


def test_returns_pair_of_python_ints():
    past, forecast = _window_to_past_forecast(date(2026, 6, 10), date(2026, 6, 16), TODAY)
    assert type(past) is int
    assert type(forecast) is int


# ── Sanity: the public MAX_*_DAYS limits are still as expected ──────────────


def test_open_meteo_limits_documented_constants():
    """Lock the public Open-Meteo limits so a silent bump in the library
    triggers a test failure and a deliberate code change."""
    assert MAX_PAST_DAYS == 92
    assert MAX_FORECAST_DAYS == 16


# ── Parametrized matrix: (start, end, today) → (past, forecast) ─────────────


@pytest.mark.parametrize(
    "start,end,today,expected",
    [
        # All past
        (date(2026, 6, 10), date(2026, 6, 13), date(2026, 6, 13), (3, 0)),
        (date(2026, 1, 1), date(2026, 6, 13), date(2026, 6, 13), (163, 0)),
        # All future
        (date(2026, 6, 13), date(2026, 6, 14), date(2026, 6, 13), (0, 1)),
        (date(2026, 6, 13), date(2026, 6, 20), date(2026, 6, 13), (0, 7)),
        # Straddle
        (date(2026, 6, 12), date(2026, 6, 14), date(2026, 6, 13), (1, 1)),
        (date(2026, 6, 1), date(2026, 6, 30), date(2026, 6, 13), (12, 17)),
    ],
    ids=[
        "past-3d",
        "past-half-year",
        "future-1d",
        "future-1w",
        "straddle-tight",
        "straddle-half-month",
    ],
)
def test_window_translation_matrix(start, end, today, expected):
    assert _window_to_past_forecast(start, end, today) == expected
