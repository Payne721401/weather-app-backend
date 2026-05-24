"""End-to-end smoke tests for every update task in ``functions/main.py``.

These tests execute the real task functions, which means:
- They call the real CWA / NCDR / MONEV APIs.
- They write to the real Firestore collections (production data).
- They write to the real R2 buckets.
- They send Telegram notifications.

Run with intent — don't include in PR CI. Use:
    pytest -m integration tests/integration/test_main_tasks_smoke.py

These are smoke tests in the strictest sense: the only assertion is "the
function ran to completion without raising". Data quality is validated
by the dedicated shape tests in ``test_cwa_api.py`` etc.
"""
from __future__ import annotations

import pytest

from functions.main import (
    update_air_quality,
    update_current_weather,
    update_radar_rainfall,
    update_sunrise_sunset,
    update_three_hour_forecast,
    update_typhoon_forecast,
    update_uv_index,
    update_weekly_forecast,
)

pytestmark = [pytest.mark.integration, pytest.mark.slow]


def test_update_current_weather_smoke():
    update_current_weather()


def test_update_three_hour_forecast_smoke():
    update_three_hour_forecast()


def test_update_weekly_forecast_smoke():
    update_weekly_forecast()


def test_update_uv_index_smoke():
    update_uv_index()


def test_update_air_quality_smoke():
    update_air_quality()


def test_update_radar_rainfall_smoke():
    update_radar_rainfall()


def test_update_sunrise_sunset_smoke():
    update_sunrise_sunset()


def test_update_typhoon_forecast_smoke():
    update_typhoon_forecast()
