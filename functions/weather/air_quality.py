import logging
from typing import List, Dict
from services.weather_api import WeatherAPIService
from utils.data_processing import extract_air_quality_data
from database.models import AirQualityData

logger = logging.getLogger(__name__)

class AirQualityService:
    def __init__(self, api_service: WeatherAPIService):
        self.api_service = api_service
        
    def fetch_air_quality(self) -> List[Dict]:
        try:
            raw_data = self.api_service.get_air_quality()
            return extract_air_quality_data(raw_data)
        except Exception as e:
            logger.error(f"取得空氣品質資料時發生錯誤: {e}")
            raise
            
    def update_firebase(self, data: List[Dict]) -> Dict:
        """批次更新 Firestore 中的空氣品質資料並回傳統計數據"""
        try:
            stats = AirQualityData.batch_save(data)
            logger.info(f"成功批次更新 {stats['success_count']} 筆空氣品質資料，失敗 {stats['failed_count']} 筆")
            if stats['failed_items']:
                logger.warning(f"失敗項目: {stats['failed_items']}")
            return stats
        except Exception as e:
            logger.error(f"批次更新空氣品質資料時發生錯誤: {e}")
            raise