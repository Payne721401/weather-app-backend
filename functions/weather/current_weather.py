# weather_backend/functions/weather/current_weather.py
import logging
from typing import Dict, List
# from datetime import datetime
import sys
from pathlib import Path

# 添加 functions 目錄到 Python 路徑
functions_dir = Path(__file__).parent.parent
if str(functions_dir) not in sys.path:
    sys.path.append(str(functions_dir))

# from ..services.weather_api import WeatherAPIService
# from ..utils.data_processing import extract_observation_data
# from ..database.models import ObservationData

from services.weather_api import WeatherAPIService
from utils.data_processing import extract_observation_data
from database.models import ObservationData


logger = logging.getLogger(__name__)

class CurrentWeatherService:
    """
    服務用於獲取與處理當前天氣資料
    """
    
    def __init__(self, api_service: WeatherAPIService = None, api_key: str = None):
        """
        初始化當前天氣服務
        
        參數:
            api_service: WeatherAPIService 實例，若為 None 則建立新實例
            api_key: 中央氣象署 API 金鑰，當 api_service 為 None 時使用
        """
        self.api_service = api_service or WeatherAPIService(api_key)
        
    def fetch_current_weather(self, stations: List[str] = None) -> List[Dict]:
        """
        從中央氣象署 API 獲取當前天氣資料
        
        參數:
            stations: 氣象站 ID 列表，若為 None 則獲取所有氣象站資料
        
        返回:
            依氣象站分類的處理後觀測資料列表
        """
        try:
            raw_data = self.api_service.get_observation_data(stations)
            return extract_observation_data(raw_data)
        except Exception as e:
            logger.error(f"獲取當前天氣資料時發生錯誤: {e}")
            raise
    
    def update_firebase(self, data: List[Dict]) -> Dict:
        """
        將當前天氣資料批次更新至 Firebase 並回傳統計數據
        """
        try:
            stats = ObservationData.batch_save(data)
            logger.info(f"成功批次更新 {stats['success_count']} 筆觀測資料，失敗 {stats['failed_count']} 筆")
            if stats['failed_items']:
                logger.warning(f"失敗項目: {stats['failed_items']}")
            return stats
        except Exception as e:
            logger.error(f"批次更新 Firebase 觀測資料時發生錯誤: {e}")
            raise
