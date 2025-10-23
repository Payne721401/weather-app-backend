# tests/test_main_tasks.py

import unittest
import sys
from pathlib import Path
import os
import logging
from dotenv import load_dotenv

project_root = Path(__file__).resolve().parent.parent
functions_dir = project_root / 'functions'
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
if str(functions_dir) not in sys.path:
    sys.path.insert(0, str(functions_dir))

dotenv_path = project_root / '.env'
load_dotenv(dotenv_path=dotenv_path)

if not os.getenv('CWA_API_KEY'):
    raise ValueError("CWA_API_KEY is not set. Please check your .env file.")
if not os.getenv('MONEV_API_KEY'):
    logging.warning("MONEV_API_KEY is not set. Air quality test might fail locally.")
if not os.getenv('R2_ACCESS_KEY_ID'):
    logging.warning("R2 variables are not set. R2 tests might fail locally.")

from functions.main import (
    update_current_weather,
    update_three_hour_forecast,
    update_weekly_forecast,
    update_uv_index,
    update_air_quality,
    update_radar_rainfall, 
    update_sunrise_sunset,
    update_typhoon_forecast
)

class TestMainTasks(unittest.TestCase):
    """
    對 main.py 中的所有主要更新任務進行整合測試。
    這些測試會實際呼叫 API 並嘗試寫入 Firestore / R2。
    """

    def test_update_current_weather(self):
        """測試 update_current_weather 執行"""
        print("開始測試: update_current_weather...")
        try:
            update_current_weather()
            print("測試成功: update_current_weather 執行完畢。\n")
        except Exception as e:
            self.fail(f"update_current_weather() 執行時引發未預期的例外: {e}")

    def test_update_three_hour_forecast(self):
        """測試 update_three_hour_forecast 執行"""
        print("開始測試: update_three_hour_forecast...")
        try:
            update_three_hour_forecast()
            print("測試成功: update_three_hour_forecast 執行完畢。\n")
        except Exception as e:
            self.fail(f"update_three_hour_forecast() 執行時引發未預期的例外: {e}")

    def test_update_weekly_forecast(self):
        """測試 update_weekly_forecast 執行"""
        print("開始測試: update_weekly_forecast...")
        try:
            update_weekly_forecast()
            print("測試成功: update_weekly_forecast 執行完畢。\n")
        except Exception as e:
            self.fail(f"update_weekly_forecast() 執行時引發未預期的例外: {e}")

    def test_update_uv_index(self):
        """測試 update_uv_index 執行"""
        print("開始測試: update_uv_index...")
        try:
            update_uv_index()
            print("測試成功: update_uv_index 執行完畢。\n")
        except Exception as e:
            self.fail(f"update_uv_index() 執行時引發未預期的例外: {e}")

    def test_update_air_quality(self):
        """測試 update_air_quality 執行"""
        print("開始測試: update_air_quality...")
        try:
            update_air_quality()
            print("測試成功: update_air_quality 執行完畢。\n")
        except Exception as e:
            self.fail(f"update_air_quality() 執行時引發未預期的例外: {e}")

    def test_update_radar_rainfall(self):
        """測試 update_radar_rainfall 執行"""
        print("開始測試: update_radar_rainfall...")
        try:
            update_radar_rainfall()
            print("測試成功: update_radar_rainfall 執行完畢。\n")
        except Exception as e:
            self.fail(f"update_radar_rainfall() 執行時引發未預期的例外: {e}")

    def test_update_sunrise_sunset(self):
        """測試 update_sunrise_sunset 執行"""
        print("開始測試: update_sunrise_sunset...")
        try:
            update_sunrise_sunset()
            print("測試成功: update_sunrise_sunset 執行完畢。\n")
        except Exception as e:
            self.fail(f"update_sunrise_sunset() 執行時引發未預期的例外: {e}")

    def test_update_typhoon_forecast(self):
        """測試 update_typhoon_forecast 執行"""
        print("開始測試: update_typhoon_forecast...")
        try:
            update_typhoon_forecast()
            print("測試成功: update_typhoon_forecast 執行完畢。\n")
        except Exception as e:
            self.fail(f"update_typhoon_forecast() 執行時引發未預期的例外: {e}")

if __name__ == '__main__':
    unittest.main()