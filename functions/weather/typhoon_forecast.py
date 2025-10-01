import logging
import sys
from pathlib import Path
from datetime import datetime, timezone

functions_dir = Path(__file__).resolve().parent.parent
if str(functions_dir) not in sys.path:
    sys.path.append(str(functions_dir))

from services.weather_api import WeatherAPIService
from database.models import TyphoonForecastImage

logger = logging.getLogger(__name__)

class TyphoonForecastService:
    """
    服務用於獲取與儲存颱風系集預報圖片。
    """
    
    def __init__(self, api_service: WeatherAPIService):
        self.api_service = api_service
        
    def fetch_forecast_image(self) -> bytes:
        try:
            return self.api_service.get_typhoon_forecast_image()
        except Exception as e:
            logger.error(f"獲取颱風預報圖片時發生錯誤: {e}")
            raise

    def update_r2_image(self, image_data: bytes) -> dict:
        try:
            # 儲存一份為 'latest.png'，方便前端固定引用
            latest_path = 'typhoon/latest.png'
            TyphoonForecastImage.save_to_r2(image_data, latest_path)

            logger.info(f"成功更新颱風預報圖片至 Cloudflare R2")
            return {'success_count': 1, 'failed_count': 0, 'failed_items': []}
        except Exception as e:
            logger.error(f"更新颱風預報圖片至 Cloudflare R2 時發生錯誤: {e}")
            raise
