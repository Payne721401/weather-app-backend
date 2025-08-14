import os
from typing import Dict, Any
from dotenv import load_dotenv

load_dotenv()

class Settings:
    """
    全域設定類別
    """
    
    # API 設定
    CWA_API_KEY = os.environ.get('CWA_API_KEY')
    CWA_API_BASE_URL = "https://opendata.cwa.gov.tw/api/v1/rest/datastore"
    RADAR_API_BASE_URL = "https://opendata.cwa.gov.tw/fileapi/v1/opendataapi/O-A0058-001"
    
    # Firebase 設定
    FIREBASE_PROJECT_ID = os.getenv('FIREBASE_PROJECT_ID', 'ai-weather-app-6c9ac')
    USE_EMULATOR = os.getenv('USE_EMULATOR', 'false').lower() == 'true'
    FIRESTORE_EMULATOR_HOST = os.getenv('FIRESTORE_EMULATOR_HOST', None)
    FUNCTIONS_EMULATOR_HOST = os.getenv('FUNCTIONS_EMULATOR_HOST', None)
    FIREBASE_STORAGE_EMULATOR_HOST = os.getenv('FIREBASE_STORAGE_EMULATOR_HOST', None)
    # 資料更新頻率 (秒)
    UPDATE_FREQUENCY = {
        'current_weather': 60 * 10,  # 10 分鐘
        'three_hour_forecast': 60 * 60 * 3,  # 3 小時
        'weekly_forecast': 60 * 60 * 12,  # 12 小時
        'radar': 60 * 10,  # 10 分鐘
        'qpf': 60 * 60 * 6,  # 6 小時
    }
    
    # 氣象資料 ID
    DATA_IDS = {
        'three_hour_forecast': 'F-D0047-091',  # 三小時鄉鎮天氣預報
        'weekly_forecast': 'F-D0047-087',  # 一週鄉鎮天氣預報
        'observation': 'O-A0001-001',  # 氣象站觀測資料
        'radar_echo': 'F-C0035-001',  # 雷達回波
        'qpf': 'F-D0047-061',  # 定量降水預報
    }
    
    # 快取設定
    CACHE_TIMEOUT = {
        'current_weather': 60 * 10,  # 10 分鐘
        'three_hour_forecast': 60 * 60 * 3,  # 3 小時
        'weekly_forecast': 60 * 60 * 12,  # 12 小時
        'radar': 60 * 10,  # 10 分鐘
        'qpf': 60 * 60 * 6,  # 6 小時
    }
    
    # 日誌設定
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    @classmethod
    def get_all(cls) -> Dict[str, Any]:
        """
        取得所有設定值
        
        返回:
            包含所有設定的字典
        """
        return {key: value for key, value in cls.__dict__.items() 
                if not key.startswith('__') and not callable(value)}