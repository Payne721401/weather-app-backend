"""Unit tests for ``functions/config/settings.py``.

The Settings class is the single point where environment variables enter
the application. We make sure reloading via ``importlib`` picks up changes
and that key fields have sensible defaults.
"""
from __future__ import annotations

import importlib

import pytest

pytestmark = pytest.mark.unit


def _reload_settings():
    """Reload the Settings module so class-level os.environ.get() reruns."""
    import functions.config.settings as settings_module

    return importlib.reload(settings_module).Settings


class TestSettings:
    def test_cwa_api_key_picked_up_from_env(self, monkeypatch):
        monkeypatch.setenv("CWA_API_KEY", "test-key-abc")
        Settings = _reload_settings()
        assert Settings.CWA_API_KEY == "test-key-abc"

    def test_log_level_defaults_to_info(self, monkeypatch):
        monkeypatch.delenv("LOG_LEVEL", raising=False)
        Settings = _reload_settings()
        assert Settings.LOG_LEVEL == "INFO"

    def test_log_level_overridable(self, monkeypatch):
        monkeypatch.setenv("LOG_LEVEL", "DEBUG")
        Settings = _reload_settings()
        assert Settings.LOG_LEVEL == "DEBUG"

    def test_use_emulator_defaults_false(self, monkeypatch):
        monkeypatch.delenv("USE_EMULATOR", raising=False)
        Settings = _reload_settings()
        assert Settings.USE_EMULATOR is False

    def test_use_emulator_true_when_env_set(self, monkeypatch):
        monkeypatch.setenv("USE_EMULATOR", "true")
        Settings = _reload_settings()
        assert Settings.USE_EMULATOR is True

    def test_data_ids_contain_expected_keys(self):
        Settings = _reload_settings()
        for key in ("three_hour_forecast", "weekly_forecast", "observation"):
            assert key in Settings.DATA_IDS

    def test_get_all_returns_dict_of_settings(self):
        Settings = _reload_settings()
        all_settings = Settings.get_all()
        assert isinstance(all_settings, dict)
        assert "CWA_API_BASE_URL" in all_settings
