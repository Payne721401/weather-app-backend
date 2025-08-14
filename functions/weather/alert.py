import logging
from typing import List, Dict
from services.weather_api import WeatherAPIService
from database.models import AlertData

logger = logging.getLogger(__name__)

class AlertService:
    """示警資訊服務"""
    
    def __init__(self, api_service: WeatherAPIService):
        self.api_service = api_service
        self.logger = logging.getLogger(__name__)
        
    def fetch_alerts(self) -> List[Dict]:
        """獲取示警資訊"""
        try:
            return self.api_service.get_alerts()
        except Exception as e:
            self.logger.error(f"獲取示警資訊時發生錯誤: {e}")
            raise
            
    def update_firebase(self, data: List[Dict]) -> None:
        """更新 Firestore 中的示警資訊"""
        try:
            success_count = 0
            for alert_data in data:
                try:
                    alert = AlertData(
                        alert_id=alert_data['id'],
                        title=alert_data['title'],
                        updated=alert_data['updated'],
                        author=alert_data['author'],
                        summary=alert_data['summary'],
                        category=alert_data['category'],
                        timestamp=alert_data['timestamp']
                    )
                    alert.save_to_firestore()
                    success_count += 1
                except Exception as e:
                    self.logger.error(f"儲存示警資訊時發生錯誤 ({alert_data['id']}): {e}")
                    continue
                
            self.logger.info(f"成功更新 {success_count} 筆示警資訊")
        except Exception as e:
            self.logger.error(f"更新示警資訊時發生錯誤: {e}")
            raise
