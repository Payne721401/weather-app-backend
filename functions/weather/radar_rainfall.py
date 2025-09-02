# weather_backend/functions/weather/radar.py
import logging
# from typing import Dict, Any, List
# from datetime import datetime
import sys
from pathlib import Path

# 添加 functions 目錄到 Python 路徑
functions_dir = Path(__file__).parent.parent
if str(functions_dir) not in sys.path:
    sys.path.append(str(functions_dir))

from services.weather_api import WeatherAPIService
from utils.data_processing import extract_radar_rainfall
from database.models import RadarPredict

logger = logging.getLogger(__name__)

class RadarService:
    """
    服務用於獲取與處理雷達回波及定量降水預報資料
    """
    
    def __init__(self, api_service: WeatherAPIService = None, api_key: str = None):
        """
        初始化雷達資料服務
        
        參數:
            api_service: WeatherAPIService 實例，若為 None 則建立新實例
            api_key: 中央氣象署 API 金鑰，當 api_service 為 None 時使用
        """
        self.api_service = api_service or WeatherAPIService(api_key)
        
    def fetch_radar_rainfall(self) -> dict:
        """
        從氣象署 API 獲取雷達降雨預報並處理
        
        返回:
            dict: 聚合後的雷達降雨預報資料
        """
        try:
            raw_data = self.api_service.get_radar_rainfall_json()
            return extract_radar_rainfall(raw_data)
        except Exception as e:
            logger.error(f"獲取雷達降雨預報資料時發生錯誤: {e}")
            raise

    def update_r2_radar(self, radar_json: dict) -> dict:
        """
        將雷達降雨預測資料（dict）上傳至 Cloudflare R2 並回傳統計數據
        
        參數:
            radar_json: extract_radar_rainfall 處理後的 dict
        """
        try:
            RadarPredict.save_to_r2(radar_json, 'radar/forecast.json')
            logger.info(f"成功更新雷達降雨預測資料至 Cloudflare R2 ")
            return {'success_count': 1, 'failed_count': 0, 'failed_items': []}
        except Exception as e:
            logger.error(f"更新雷達降雨預測資料至 Cloudflare R2  時發生錯誤: {e}")
            raise