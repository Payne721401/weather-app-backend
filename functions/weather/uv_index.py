import logging
from typing import List, Dict
from services.weather_api import WeatherAPIService
from utils.data_processing import extract_uv_data
from database.models import UVIndexData

logger = logging.getLogger(__name__)

class UVIndexService:
    """紫外線指數服務"""
    
    def __init__(self, api_service: WeatherAPIService):
        self.api_service = api_service
        
    def fetch_uv_index(self) -> List[Dict]:
        """取得紫外線指數資料"""
        try:
            raw_data = self.api_service.get_uv_index()
            return extract_uv_data(raw_data)
        except Exception as e:
            logger.error(f"取得紫外線指數資料時發生錯誤: {e}")
            raise
            
    def update_firebase(self, data: List[Dict]) -> Dict:
        """批次更新 Firestore 中的紫外線指數資料並回傳統計數據"""
        try:
            stats = UVIndexData.batch_save(data)
            logger.info(f"成功批次更新 {stats['success_count']} 筆紫外線指數資料，失敗 {stats['failed_count']} 筆")
            if stats['failed_items']:
                logger.warning(f"失敗項目: {stats['failed_items']}")
            return stats
        except Exception as e:
            logger.error(f"批次更新紫外線指數資料時發生錯誤: {e}")
            raise