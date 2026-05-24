"""Unit tests for ``functions/utils/data_processing.py`` pure functions.

These tests cover the weather description parser which is the most complex
and edge-case-prone pure logic in the codebase. Failures here usually mean
CWA changed their description format.
"""
from __future__ import annotations

import pytest

from functions.utils.data_processing import parse_weather_description

pytestmark = pytest.mark.unit


class TestParseWeatherDescription:
    def test_full_description_extracts_all_fields(self):
        desc = (
            "晴。降雨機率20%。溫度攝氏22至28度。舒適。"
            "偏東風 風速2-3級。相對濕度75至85%。"
        )
        result = parse_weather_description(desc)

        assert result["weather"] == "晴"
        assert result["rainProb"] == "20%"
        assert result["minTemp"] == 22.0
        assert result["maxTemp"] == 28.0
        assert result["windDirection"] == "偏東"
        assert result["windSpeed"] == 3
        assert result["humidity"] == "85%"

    def test_single_temperature_value(self):
        result = parse_weather_description("陰。溫度攝氏25度。")
        assert result.get("Temp") == 25.0
        assert "minTemp" not in result

    def test_wind_speed_single_value(self):
        result = parse_weather_description("多雲。偏南風 風速3級。")
        assert result["windDirection"] == "偏南"
        assert result["windSpeed"] == 3

    def test_empty_string_returns_empty_dict(self):
        assert parse_weather_description("") == {}

    def test_only_weather_no_other_fields(self):
        result = parse_weather_description("晴")
        assert result.get("weather") == "晴"

    def test_malformed_temperature_does_not_crash(self):
        # 沒有 "至" 也沒有純數字 — 解析應該失敗但不 raise
        result = parse_weather_description("陰。溫度攝氏ABC度。")
        # 拋出 ValueError 會被 catch、不會進 data
        assert "Temp" not in result and "minTemp" not in result

    def test_humidity_with_range(self):
        result = parse_weather_description("晴。相對濕度70至80%。")
        # 解析邏輯只取「至」的右半邊
        assert result["humidity"] == "80%"

    def test_humidity_without_range(self):
        result = parse_weather_description("晴。相對濕度80%。")
        assert result["humidity"] == "80%"
