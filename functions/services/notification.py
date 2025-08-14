# weather_backend/functions/services/notification.py
import requests
import os
import logging
import traceback
from typing import Dict
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

class NotificationService:
    """專門用於發送 Telegram 通知的服務"""
    def __init__(self):
        """初始化通知服務，並從環境變數載入 Telegram 設定。"""
        self.telegram_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')

        if not self.telegram_token or not self.telegram_chat_id:
            logger.warning("Telegram Bot Token 或 Chat ID 未設定，通知功能將被停用。")

    def _send_telegram_message(self, message: str) -> bool:
        """
        發送格式化訊息到 Telegram Bot。

        Args:
            message: 要發送的訊息內容。

        Returns:
            bool: 如果成功發送則為 True，否則為 False。
        """
        if not self.telegram_token or not self.telegram_chat_id:
            return False

        url = f'https://api.telegram.org/bot{self.telegram_token}/sendMessage'
        payload = {
            'chat_id': self.telegram_chat_id,
            'text': message,
            'parse_mode': 'Markdown'
        }
        try:
            response = requests.post(url, json=payload, timeout=10)
            response.raise_for_status()
            logger.info("Telegram 訊息已成功發送。")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"發送 Telegram 訊息失敗: {e}")
            if e.response is not None:
                logger.error(f"Telegram API 回應: {e.response.text}")
            return False

    def _format_success_message(self, task_name: str, stats: Dict, duration: float, start_time_utc: datetime) -> str:
        """格式化成功的通知訊息。"""
        success_count = stats.get('success_count', 'N/A')
        failed_count = stats.get('failed_count', 'N/A')
        start_time_local = start_time_utc.astimezone(timezone(timedelta(hours=8)))
        
        message = (
            f"✅ *任務成功*\n\n"
            f"*任務名稱*: `{task_name}`\n"
            f"*開始時間 (UTC+8)*: `{start_time_local.strftime('%Y-%m-%d %H:%M:%S')}`\n"
            f"*執行時間*: `{duration:.2f} 秒`\n"
            f"*處理結果*: 成功 {success_count} 筆, 失敗 {failed_count} 筆"
        )
        return message

    def _format_error_message(self, task_name: str, error: Exception, duration: float, start_time_utc: datetime) -> str:
        """格式化失敗的通知訊息，包含詳細的追蹤資訊。"""
        tb_info = traceback.extract_tb(error.__traceback__)
        last_trace = tb_info[-1]
        
        file_path = last_trace.filename
        func_name = last_trace.name
        line_no = last_trace.lineno
        start_time_local = start_time_utc.astimezone(timezone(timedelta(hours=8)))
        
        message = (
            f"🚨 *任務執行失敗*\n\n"
            f"*任務名稱*: `{task_name}`\n"
            f"*開始時間 (UTC+8)*: `{start_time_local.strftime('%Y-%m-%d %H:%M:%S')}`\n"
            f"*執行時間*: `{duration:.2f} 秒`\n\n"
            f"*錯誤類型*: `{type(error).__name__}`\n"
            f"*錯誤訊息*: `{str(error)}`\n\n"
            f"*發生位置*:\n"
            f"📄 *檔案*: `{file_path}`\n"
            f"🔧 *函式*: `{func_name}`\n"
            f"➡️ *行號*: `{line_no}`"
        )
        return message

    def notify_success(self, task_name: str, stats: Dict, duration: float, start_time_utc: datetime):
        """發送成功的通知。"""
        message = self._format_success_message(task_name, stats, duration, start_time_utc)
        self._send_telegram_message(message)

    def notify_failure(self, task_name: str, error: Exception, duration: float, start_time_utc: datetime):
        """發送失敗的通知。"""
        message = self._format_error_message(task_name, error, duration, start_time_utc)
        self._send_telegram_message(message)