"""Unit tests for ``services/notification.py`` message formatting and send.

We mock ``requests.post`` so no real Telegram traffic occurs. The tests
cover the three behaviors we rely on in production: success message
contains the right fields, error message includes the traceback location,
and the service silently no-ops when Telegram credentials are missing.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from functions.services.notification import NotificationService

pytestmark = pytest.mark.unit


@pytest.fixture
def with_telegram_credentials(monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "test-chat-id")


@pytest.fixture
def without_telegram_credentials(monkeypatch):
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)


class TestSuccessMessage:
    def test_message_contains_task_name_and_stats(self, with_telegram_credentials):
        service = NotificationService()
        stats = {"success_count": 42, "failed_count": 1}
        start = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)

        msg = service._format_success_message("update_radar", stats, 3.21, start)

        assert "update_radar" in msg
        assert "42" in msg and "1" in msg
        assert "3.21" in msg
        assert "✅" in msg

    def test_message_uses_utc_plus_8_timestamp(self, with_telegram_credentials):
        service = NotificationService()
        start = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)
        msg = service._format_success_message("t", {}, 0.0, start)
        # 12:00 UTC -> 20:00 Taiwan time
        assert "20:00:00" in msg


class TestErrorMessage:
    def test_message_includes_error_type_and_message(self, with_telegram_credentials):
        service = NotificationService()
        start = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)
        try:
            raise ValueError("simulated failure")
        except ValueError as exc:
            msg = service._format_error_message("update_radar", exc, 1.5, start)

        assert "ValueError" in msg
        assert "simulated failure" in msg
        assert "🚨" in msg
        assert "update_radar" in msg

    def test_message_includes_source_location(self, with_telegram_credentials):
        service = NotificationService()
        start = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)
        try:
            raise RuntimeError("boom")
        except RuntimeError as exc:
            msg = service._format_error_message("t", exc, 0.0, start)
        # 訊息應該包含「行號」「函式」「檔案」這幾個欄位的標題
        assert "行號" in msg and "函式" in msg and "檔案" in msg


class TestNotifyBehavior:
    def test_notify_success_calls_telegram_api(self, with_telegram_credentials):
        service = NotificationService()
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None

        with patch(
            "functions.services.notification.requests.post", return_value=mock_response
        ) as mock_post:
            service.notify_success(
                "t", {"success_count": 1}, 0.5, datetime.now(timezone.utc)
            )
            assert mock_post.called

    def test_notify_skipped_when_credentials_missing(self, without_telegram_credentials):
        service = NotificationService()
        with patch("functions.services.notification.requests.post") as mock_post:
            service.notify_success(
                "t", {"success_count": 1}, 0.5, datetime.now(timezone.utc)
            )
            assert not mock_post.called

    def test_notify_swallows_request_exception(self, with_telegram_credentials):
        """Telegram down should never break the actual data pipeline."""
        import requests as real_requests

        service = NotificationService()
        with patch(
            "functions.services.notification.requests.post",
            side_effect=real_requests.exceptions.ConnectionError("network down"),
        ):
            # 不應拋例外
            service.notify_success(
                "t", {"success_count": 0}, 0.0, datetime.now(timezone.utc)
            )
