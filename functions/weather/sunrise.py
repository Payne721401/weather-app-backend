import logging
from typing import List, Dict
from datetime import datetime
from services.weather_api import WeatherAPIService
from database.models import SunriseData

logger = logging.getLogger(__name__)

class SunriseService:
    """日出日落資料服務"""
    
    def __init__(self, api_service: WeatherAPIService):
        self.api_service = api_service
        
    def fetch_sunrise_data(self) -> List[Dict]:
        """取得日出日落與月出月落資料並合併"""
        try:
            # 分別獲取日出日落和月出月落的原始資料
            sun_raw_data = self.api_service.get_sunrise_sunset_data()
            moon_raw_data = self.api_service.get_moonrise_moonset_data()

            if not sun_raw_data or "records" not in sun_raw_data:
                raise ValueError("無效的日出日落 API 回應")
            if not moon_raw_data or "records" not in moon_raw_data:
                raise ValueError("無效的月出月落 API 回應")

            # 使用字典來合併資料，以 countyName_date 作為 key
            combined_data = {}
            current_timestamp = datetime.now().timestamp()

            # 處理日出日落資料
            for location in sun_raw_data["records"]["locations"]["location"]:
                county_name = location["CountyName"]
                for time_data in location["time"]:
                    key = f"{county_name}_{time_data['Date']}"
                    combined_data[key] = {
                        "countyName": county_name,
                        "date": time_data["Date"],
                        "sunriseTime": time_data["SunRiseTime"],
                        "sunsetTime": time_data["SunSetTime"],
                        "timestamp": current_timestamp
                    }
            
            # 處理月出月落資料並合併
            for location in moon_raw_data["records"]["locations"]["location"]:
                county_name = location["CountyName"]
                for time_data in location["time"]:
                    key = f"{county_name}_{time_data['Date']}"
                    if key in combined_data:
                        combined_data[key]["moonriseTime"] = time_data.get("MoonRiseTime", "N/A")
                        combined_data[key]["moonsetTime"] = time_data.get("MoonSetTime", "N/A")

            # 將合併後的字典轉換為列表
            return list(combined_data.values())

        except Exception as e:
            logger.error(f"取得天文資料時發生錯誤: {e}")
            raise

    def update_firebase(self, data: List[Dict]):
        """儲存日出日落資料"""
        try:
            stats = SunriseData.batch_save(data)
            logger.info(f"成功批次更新 {stats['success_count']} 筆日出日落資料，失敗 {stats['failed_count']} 筆")
            if stats['failed_items']:
                logger.info(f"失敗項目: {stats['failed_items']}")
            return stats
        except Exception as e:
            logger.error(f"批次更新日出日落資料時發生嚴重錯誤: {e}")
            raise
