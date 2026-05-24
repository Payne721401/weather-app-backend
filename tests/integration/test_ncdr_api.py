"""Integration tests for the NCDR (National Science and Technology Center for
Disaster Reduction) API.

Auto-skipped if NCDR_API_KEY (or other required keys) are missing.
"""
from __future__ import annotations

import pytest

from functions.services.weather_api import WeatherAPIService

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def weather_service() -> WeatherAPIService:
    return WeatherAPIService()


class TestNCDRConnection:
    def test_connection_returns_ok(self, weather_service):
        status = weather_service.test_connection(api_type="ncdr")
        assert status.get("ncdr") is True, f"NCDR connection failed, full status: {status}"
