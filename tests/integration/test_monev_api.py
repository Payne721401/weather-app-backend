"""Integration tests for the MONEV (Ministry of Environment) air quality API.

Auto-skipped if MONEV_API_KEY (or other required keys) are missing.
"""
from __future__ import annotations

import pytest

from functions.services.weather_api import WeatherAPIService

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def weather_service() -> WeatherAPIService:
    return WeatherAPIService()


class TestMONEVConnection:
    def test_connection_returns_ok(self, weather_service):
        status = weather_service.test_connection(api_type="monev")
        assert status.get("monev") is True, f"MONEV connection failed, full status: {status}"


class TestMONEVAirQuality:
    def test_get_air_quality_returns_records(self, weather_service):
        result = weather_service.get_air_quality()
        assert isinstance(result, dict)
        # MONEV API 回傳格式包含 records 陣列
        records = result.get("records") or result.get("data", [])
        assert isinstance(records, list)
        assert len(records) > 0, "Expected at least one air quality record"
