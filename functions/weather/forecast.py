# weather_backend/functions/weather/forecast.py
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import sys
from pathlib import Path

# 添加 functions 目錄到 Python 路徑
functions_dir = Path(__file__).parent.parent
if str(functions_dir) not in sys.path:
    sys.path.append(str(functions_dir))

from services.weather_api import WeatherAPIService
from utils.data_processing import extract_three_hour_forecast, extract_weekly_forecast
from database.models import ThreeHourForecast, WeeklyForecast

logger = logging.getLogger(__name__)

class ForecastService:
    """
    服務用於獲取與處理天氣預報資料
    """
    
    def __init__(self, api_service: WeatherAPIService = None, api_key: str = None):
        """
        初始化天氣預報服務
        
        參數:
            api_service: WeatherAPIService 實例，若為 None 則建立新實例
            api_key: 中央氣象署 API 金鑰，當 api_service 為 None 時使用
        """
        self.api_service = api_service or WeatherAPIService(api_key)
        
    def fetch_three_hour_forecast(self, locations: List[str] = None) -> List[Dict]:
        """
        從中央氣象署 API 獲取三小時天氣預報
        
        參數:
            locations: 地區 ID 列表，若為 None 則獲取所有地區資料
        
        返回:
            處理後的三小時預報資料列表
        """
        try:
            raw_data = self.api_service.get_three_hour_forecast(locations)
            return extract_three_hour_forecast(raw_data)
        except Exception as e:
            logger.error(f"獲取三小時天氣預報時發生錯誤: {e}")
            raise
    
    def fetch_weekly_forecast(self, locations: List[str] = None) -> List[Dict]:
        """
        從中央氣象署 API 獲取一週天氣預報
        
        參數:
            locations: 地區 ID 列表，若為 None 則獲取所有地區資料
        
        返回:
            處理後的一週預報資料列表
        """
        try:
            raw_data = self.api_service.get_weekly_forecast(locations)
            return extract_weekly_forecast(raw_data)
        except Exception as e:
            logger.error(f"獲取一週天氣預報時發生錯誤: {e}")
            raise
    
    def update_firebase_three_hour(self, data: List[Dict]) -> None:
        """
        將三小時天氣預報更新至 Firebase
        """

        try:
            stats = ThreeHourForecast.batch_save(data)
            if stats['failed_items']:
                logger.info(
                    f"失敗數量: {stats['failed_count']}\n"
                    f"更新失敗的地區:\n" +
                    "\n".join([f"{item.get('countyName', '')}{item.get('townName', '')}: {item['error']}"
                               for item in stats['failed_items']])
                )
            else:
                logger.info(f"成功更新: {stats['success_count']} 個鄉鎮資料\n")
            return stats
        except Exception as e:
            logger.error(f"批次更新 Firebase 三小時預報資料時發生錯誤: {e}")
            raise
        

    def update_firebase_weekly(self, data: List[Dict]) -> None:
        """
        將一週天氣預報更新至 Firebase
        """
        try:
            stats = WeeklyForecast.batch_save(data)
            if stats['failed_items']:
                logger.info(
                    f"失敗數量: {stats['failed_count']}\n"
                    f"更新失敗的地區:\n" +
                    "\n".join([f"{item.get('countyName', '')}{item.get('townName', '')}: {item['error']}"
                               for item in stats['failed_items']])
                )
            else:
                logger.info(f"成功更新: {stats['success_count']} 個地區資料\n")
            return stats
        except Exception as e:
            logger.error(f"批次更新 Firebase 一週預報資料時發生錯誤: {e}")
            raise
