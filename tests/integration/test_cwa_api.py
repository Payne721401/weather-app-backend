"""Integration tests for the Taiwan CWA (Central Weather Administration) API.

These tests hit the real CWA endpoints — they only verify that the API
responds and the response shape matches what the code expects. They do
NOT validate weather data semantics (temperature ranges, station counts
etc) because those values are noisy in real time.

Auto-skipped if CWA_API_KEY (and the other required env vars) are missing.
"""
from __future__ import annotations

import pytest

from functions.services.weather_api import WeatherAPIService

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def weather_service() -> WeatherAPIService:
    return WeatherAPIService()


class TestCWAConnection:
    def test_connection_returns_ok(self, weather_service):
        status = weather_service.test_connection(api_type="cwa")
        assert status.get("cwa") is True, f"CWA connection failed, full status: {status}"


class TestCWAObservation:
    def test_get_observation_data_returns_records(self, weather_service):
        result = weather_service.get_observation_data()
        assert isinstance(result, dict)
        assert result.get("success") == "true"
        assert "records" in result
        stations = result["records"].get("Station", [])
        assert isinstance(stations, list)
        assert len(stations) > 0, "Expected at least one observation station"


class TestCWAForecast:
    def test_get_three_hour_forecast_single_zone_returns_data(self, weather_service):
        # 只取 1 個 zone 以避免打太多請求 — 臺北市 (F-D0047-061)
        result = weather_service.get_three_hour_forecast(locations=["F-D0047-061"])
        assert isinstance(result, dict)
        assert result.get("success") == "true"
        records = result.get("records", {})
        assert "locations" in records or "Locations" in records


class TestCWASunrise:
    def test_get_sunrise_sunset_returns_data(self, weather_service):
        result = weather_service.get_sunrise_sunset_data()
        assert isinstance(result, dict)
        assert result.get("success") == "true"
