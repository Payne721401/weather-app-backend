# weather_backend/functions/main.py
import logging
import os
# from typing import Dict, Any
import time
import sys
import argparse
from datetime import datetime, timezone

from dotenv import load_dotenv
# from firebase_functions import scheduler_fn, https_fn, options

from services.weather_api import WeatherAPIService
from weather.current_weather import CurrentWeatherService
from weather.forecast import ForecastService
from weather.radar_rainfall import RadarService
from weather.uv_index import UVIndexService
from weather.sunrise import SunriseService
from weather.air_quality import AirQualityService
# from weather.alert import AlertService
from services.notification import NotificationService
from config.settings import Settings

# 導入 client 預載功能
from database.models import (
    start_firestore_preloading, 
    start_r2_preloading,
    wait_for_firestore_preloading, 
    wait_for_r2_preloading
)

#載入環境變數
def load_environment():
    """載入環境變數"""
    if os.getenv('USE_EMULATOR'):
        # 本地開發環境：從 .env 載入
        from pathlib import Path
        env_path = Path(__file__).parent.parent / '.env'
        load_dotenv(env_path)
    else:
        # Firebase 正式環境：從 functions.config() 載入
        # config = options.get()
        # os.environ['CWA_API_KEY'] = config.weather.cwa_api_key
        # os.environ['NCDR_API_KEY'] = config.weather.ncdr_api_key

        print("正式環境(GitHub Actions)：從環境變數載入")
        # 在 GitHub Actions 中，我們會在 workflow YAML 中設定環境變數
        # 所以這裡不需要做特別的事，os.getenv() 會自動抓取
        pass

# 確保環境變數被載入
load_environment()

# 設定日誌
logging.basicConfig(level=getattr(logging, Settings.LOG_LEVEL),
                   format=Settings.LOG_FORMAT)
logger = logging.getLogger(__name__)

# 排程函數

def update_current_weather() -> None:
    """每 10 分鐘更新當前天氣資料"""
    # 1. 立即開始預載 Firestore client（在背景執行）
    start_firestore_preloading()
    
    # 2. 初始化 API 服務
    weather_api = WeatherAPIService(api_key=Settings.CWA_API_KEY)
    current_weather_service = CurrentWeatherService(api_service=weather_api)
    notification_service = NotificationService()
    
    task_name = "update_current_weather"
    start_time = time.time()
    start_time_utc = datetime.now(timezone.utc)
    try:
        # 3. API 連線檢查（與 client 預載平行執行）
        api_status = weather_api.test_connection(api_type='cwa')
        if not api_status['cwa']:
            error_message = "CWA API 連線失敗，無法更新當前天氣資料"
            logger.error(error_message)
            duration = time.time() - start_time
            notification_service.notify_failure(task_name, ConnectionError(error_message), duration, start_time_utc)
            return
            
        logger.info(f"[{task_name}] 開始更新")
        
        # 4. 獲取資料（與 client 預載平行執行）
        data = current_weather_service.fetch_current_weather()
        
        # 5. 確保 Firestore client 已經準備好
        wait_for_firestore_preloading(timeout=5)
        
        # 6. 資料處理和寫入
        stats = current_weather_service.update_firebase(data)
        duration = time.time() - start_time
        logger.info(f"[{task_name}] 成功更新")
        notification_service.notify_success(task_name, stats, duration, start_time_utc)
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"[{task_name}] 發生錯誤: {e}")
        notification_service.notify_failure(task_name, e, duration, start_time_utc)
        raise

def update_three_hour_forecast() -> None:
    """每三小時更新三小時天氣預報"""
    # 1. 立即開始預載 Firestore client（在背景執行）
    start_firestore_preloading()
    
    # 2. 初始化 API 服務（不需要 DB clients）
    weather_api = WeatherAPIService(api_key=Settings.CWA_API_KEY)
    forecast_service = ForecastService(api_service=weather_api)
    notification_service = NotificationService()
    
    task_name = "update_three_hour_forecast"
    start_time = time.time()
    start_time_utc = datetime.now(timezone.utc)
    try:
        # 3. API 連線檢查（與 client 預載平行執行）
        api_status = weather_api.test_connection(api_type='cwa')
        if not api_status['cwa']:
            error_message = "CWA API 連線失敗，無法更新三小時天氣預報"
            logger.error(error_message)
            duration = time.time() - start_time
            notification_service.notify_failure(task_name, ConnectionError(error_message), duration, start_time_utc)
            return
        
        logger.info(f"[{task_name}] 開始更新")
        
        # 4. 獲取資料（與 client 預載平行執行）
        data = forecast_service.fetch_three_hour_forecast()
        
        # 5. 確保 Firestore client 已經準備好
        wait_for_firestore_preloading(timeout=5)
        
        # 6. 資料處理和寫入（clients 已預載完成）
        stats = forecast_service.update_firebase_three_hour(data)
        
        # 驗證是否達到預期數量 (368個鄉鎮)
        if stats['success_count'] != 368:
            logger.warning(
                f"[{task_name}] 更新數量不符預期！\n"
                f"預期: 368, 實際: {stats['success_count']}"
            )

        if stats['failed_items']:
            logger.warning(f"[{task_name}] 失敗項目: {stats['failed_items']}")
        
        duration = time.time() - start_time
        logger.info(f"[{task_name}] 成功更新")
        notification_service.notify_success(task_name, stats, duration, start_time_utc)
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"[{task_name}] 發生錯誤: {e}")
        notification_service.notify_failure(task_name, e, duration, start_time_utc)
        raise

def update_weekly_forecast() -> None:
    """每 12 小時更新一週天氣預報"""
    # 1. 立即開始預載 Firestore client（在背景執行）
    start_firestore_preloading()
    
    # 2. 初始化 API 服務（不需要 DB clients）
    weather_api = WeatherAPIService(api_key=Settings.CWA_API_KEY)
    forecast_service = ForecastService(api_service=weather_api)
    notification_service = NotificationService()
    
    task_name = "update_weekly_forecast"
    start_time = time.time()
    start_time_utc = datetime.now(timezone.utc)
    try:
        # 3. API 連線檢查（與 client 預載平行執行）
        api_status = weather_api.test_connection(api_type='cwa')
        if not api_status['cwa']:
            error_message = "CWA API 連線失敗，無法更新一週天氣預報"
            logger.error(error_message)
            duration = time.time() - start_time
            notification_service.notify_failure(task_name, ConnectionError(error_message), duration, start_time_utc)
            return
        
        logger.info(f"[{task_name}] 開始更新")
        
        # 4. 獲取資料（與 client 預載平行執行）
        data = forecast_service.fetch_weekly_forecast()
        
        # 5. 確保 Firestore client 已經準備好
        wait_for_firestore_preloading(timeout=5)
        
        # 6. 資料處理和寫入（clients 已預載完成）
        stats = forecast_service.update_firebase_weekly(data)

        # 驗證是否達到預期數量 (368個鄉鎮)
        if stats['success_count'] != 368:
            logger.warning(
                f"[{task_name}] 更新數量不符預期！\n"
                f"預期: 368, 實際: {stats['success_count']}"
            )

        if stats['failed_items']:
            logger.warning(f"[{task_name}] 失敗項目: {stats['failed_items']}")

        duration = time.time() - start_time
        logger.info(f"[{task_name}] 成功更新")
        notification_service.notify_success(task_name, stats, duration, start_time_utc)
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"[{task_name}] 發生錯誤: {e}")
        notification_service.notify_failure(task_name, e, duration, start_time_utc)
        raise

def update_uv_index() -> None:
    """每小時更新紫外線指數資料"""
    # 1. 立即開始預載 Firestore client（在背景執行）
    start_firestore_preloading()
    
    # 2. 初始化 API 服務
    weather_api = WeatherAPIService(api_key=Settings.CWA_API_KEY)
    uv_index_service = UVIndexService(api_service=weather_api)
    notification_service = NotificationService()
    
    task_name = "update_uv_index"
    start_time = time.time()
    start_time_utc = datetime.now(timezone.utc)
    try:
        # 3. API 連線檢查（與 client 預載平行執行）
        api_status = weather_api.test_connection(api_type='cwa')
        if not api_status['cwa']:
            error_message = "CWA API 連線失敗，無法更新紫外線指數資料"
            logger.error(error_message)
            duration = time.time() - start_time
            notification_service.notify_failure(task_name, ConnectionError(error_message), duration, start_time_utc)
            return
        
        logger.info(f"[{task_name}] 開始更新")
        
        # 4. 獲取資料（與 client 預載平行執行）
        data = uv_index_service.fetch_uv_index()
        
        # 5. 確保 Firestore client 已經準備好
        wait_for_firestore_preloading(timeout=5)
        
        # 6. 資料處理和寫入
        stats = uv_index_service.update_firebase(data)
        duration = time.time() - start_time
        logger.info(f"[{task_name}] 成功更新")
        notification_service.notify_success(task_name, stats, duration, start_time_utc)
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"[{task_name}] 發生錯誤: {e}")
        notification_service.notify_failure(task_name, e, duration, start_time_utc)
        raise

def update_air_quality() -> None:
    """每小時更新空氣品質資料"""
    # 1. 立即開始預載 Firestore client（在背景執行）
    start_firestore_preloading()
    
    # 2. 初始化 API 服務
    weather_api = WeatherAPIService(api_key=Settings.CWA_API_KEY)
    air_quality_service = AirQualityService(api_service=weather_api)
    notification_service = NotificationService()
    
    task_name = "update_air_quality"
    start_time = time.time()
    start_time_utc = datetime.now(timezone.utc)
    try:
        # 3. API 連線檢查（與 client 預載平行執行）
        api_status = weather_api.test_connection(api_type='monev')
        if not api_status['monev']:
            error_message = "MONEV API 連線失敗，無法更新空氣品質資料"
            logger.error(error_message)
            duration = time.time() - start_time
            notification_service.notify_failure(task_name, ConnectionError(error_message), duration, start_time_utc)
            return
        
        logger.info(f"[{task_name}] 開始更新")
        
        # 4. 獲取資料（與 client 預載平行執行）
        data = air_quality_service.fetch_air_quality()
        
        # 5. 確保 Firestore client 已經準備好
        wait_for_firestore_preloading(timeout=5)
        
        # 6. 資料處理和寫入
        stats = air_quality_service.update_firebase(data)
        duration = time.time() - start_time
        logger.info(f"[{task_name}] 成功更新")
        notification_service.notify_success(task_name, stats, duration, start_time_utc)
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"[{task_name}] 發生錯誤: {e}")
        notification_service.notify_failure(task_name, e, duration, start_time_utc)
        raise

def update_radar_rainfall() -> None:
    """每 10 分鐘更新雷達降雨預測資料"""
    # 🔍 時間測量開始
    task_start_time = time.time()
    
    # 1. 立即開始預載 R2 client（在背景執行）
    preload_start = time.time()
    start_r2_preloading()
    preload_launch_time = (time.time() - preload_start) * 1000
    logger.info(f"⏱️ R2 預載啟動耗時: {preload_launch_time:.1f}ms")
    
    # 2. 快速初始化服務（測量時間）
    api_init_start = time.time()
    weather_api = WeatherAPIService(api_key=Settings.CWA_API_KEY)
    api_init_time = (time.time() - api_init_start) * 1000
    logger.info(f"⏱️ WeatherAPIService 初始化耗時: {api_init_time:.1f}ms")
    
    radar_init_start = time.time()
    radar_service = RadarService(api_service=weather_api)
    radar_init_time = (time.time() - radar_init_start) * 1000
    logger.info(f"⏱️ RadarService 初始化耗時: {radar_init_time:.1f}ms")
    
    notification_init_start = time.time()
    notification_service = NotificationService()
    notification_init_time = (time.time() - notification_init_start) * 1000
    logger.info(f"⏱️ NotificationService 初始化耗時: {notification_init_time:.1f}ms")
    
    total_init_time = (time.time() - task_start_time) * 1000
    logger.info(f"⏱️ 總初始化耗時: {total_init_time:.1f}ms")
    
    task_name = "update_radar_rainfall"
    start_time = time.time()
    start_time_utc = datetime.now(timezone.utc)
    try:
        # 測量 API 連線檢查時間
        # api_test_start = time.time()
        # api_status = weather_api.test_connection(api_type='cwa')
        # api_test_time = (time.time() - api_test_start) * 1000
        # logger.info(f"⏱️ API 連線檢查耗時: {api_test_time:.1f}ms")
        
        # if not api_status['cwa']:
        #     error_message = "CWA API 連線失敗，無法更新雷達降雨預測資料"
        #     logger.error(error_message)
        #     duration = time.time() - start_time
        #     notification_service.notify_failure(task_name, ConnectionError(error_message), duration, start_time_utc)
        #     return

        logger.info(f"[{task_name}] 開始更新")
        
        # 3. 直接獲取資料（與 R2 預載平行，移除 API 檢測）
        fetch_start = time.time()
        data = radar_service.fetch_radar_rainfall()  # 如果 API 有問題，這裡會拋出例外
        fetch_time = (time.time() - fetch_start) * 1000
        logger.info(f"⏱️ 雷達資料獲取耗時: {fetch_time:.1f}ms")
        
        # 4. 確保 R2 client 已經準備好
        wait_start = time.time()
        wait_for_r2_preloading(timeout=5)  # 縮短 timeout，應該已經完成
        wait_time = (time.time() - wait_start) * 1000
        logger.info(f"⏱️ R2 預載等待耗時: {wait_time:.1f}ms")
        
        # 5. 資料處理和上傳
        upload_start = time.time()
        stats = radar_service.update_r2_radar(data)
        upload_time = (time.time() - upload_start) * 1000
        logger.info(f"⏱️ 資料處理+上傳耗時: {upload_time:.1f}ms")
        
        # 總時間統計
        total_task_time = (time.time() - task_start_time) * 1000
        actual_task_time = (time.time() - start_time) * 1000
        logger.info(f"⏱️ 總執行時間: {total_task_time:.1f}ms (含初始化: {total_init_time:.1f}ms)")
        logger.info(f"⏱️ 實際任務時間: {actual_task_time:.1f}ms")
        
        duration = time.time() - start_time
        logger.info(f"[{task_name}] 成功更新")
        notification_service.notify_success(task_name, stats, duration, start_time_utc)
        
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"[{task_name}] 發生錯誤: {e}")
        notification_service.notify_failure(task_name, e, duration, start_time_utc)
        raise

def update_sunrise_sunset() -> None:
    """每天凌晨更新日出日落資料"""
    # 1. 立即開始預載 Firestore client（在背景執行）
    start_firestore_preloading()
    
    # 2. 初始化 API 服務
    weather_api = WeatherAPIService(api_key=Settings.CWA_API_KEY)
    sunrise_service = SunriseService(api_service=weather_api)
    notification_service = NotificationService()
    
    task_name = "update_sunrise_sunset"
    start_time = time.time()
    start_time_utc = datetime.now(timezone.utc)
    try:
        # 3. API 連線檢查（與 client 預載平行執行）
        api_status = weather_api.test_connection(api_type='cwa')
        if not api_status['cwa']:
            error_message = "CWA API 連線失敗，無法更新日出日落資料"
            logger.error(error_message)
            duration = time.time() - start_time
            notification_service.notify_failure(task_name, ConnectionError(error_message), duration, start_time_utc)
            return
        
        logger.info(f"[{task_name}] 開始更新")
        
        # 4. 獲取資料（與 client 預載平行執行）
        data = sunrise_service.fetch_sunrise_data()
        
        # 5. 確保 Firestore client 已經準備好
        wait_for_firestore_preloading(timeout=5)
        
        # 6. 資料處理和寫入
        stats = sunrise_service.update_firebase(data)
        duration = time.time() - start_time
        logger.info(f"[{task_name}] 成功更新")
        notification_service.notify_success(task_name, stats, duration, start_time_utc)
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"[{task_name}] 發生錯誤: {e}")
        notification_service.notify_failure(task_name, e, duration, start_time_utc)
        raise

'''
# @scheduler_fn.on_schedule(schedule="every 10 minutes")
def update_alerts(event: scheduler_fn.ScheduledEvent) -> None:
    """每 10 分鐘更新示警資訊"""
    try:
        logger.info("開始更新示警資訊")
        data = alert_service.fetch_alerts()
        alert_service.update_firebase(data)
        logger.info("成功更新示警資訊")
    except Exception as e:
        logger.error(f"更新示警資訊時發生錯誤: {e}")
        raise
        
'''

'''
@https_fn.on_request()
def health_check(request: Request) -> https_fn.Response:
    """
    API 健康檢查端點
    
    參數:
        request: HTTP 請求物件
        
    返回:
        HTTP 回應，包含服務狀態和版本資訊
    """
    try:
        # 檢查各項服務是否正常運作
        api_status = weather_api.test_connection()
        
        response = {
            'status': 'ok' if api_status else 'degraded',
            'timestamp': time.time(),
            'version': '1.0.0',
            'services': {
                'api': 'up' if api_status else 'down',
                'database': 'up'  # 可增加實際檢查資料庫連線的邏輯
            }
        }
        
        status_code = 200 if api_status else 503
        return jsonify(response), status_code
    except Exception as e:
        logger.error(f"健康檢查時發生錯誤: {e}")
        return jsonify({
            'status': 'error',
            'timestamp': time.time(),
            'error': str(e)
        }), 500

'''

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="執行指定的天氣資料更新任務。")
    
    # 定義一個叫做 'task' 的參數
    parser.add_argument('task', choices=[
        'update_current_weather',
        'update_three_hour_forecast',
        'update_weekly_forecast',
        'update_uv_index',
        'update_air_quality',
        'update_radar',
        'update_sunrise_sunset',
        # 'update_alerts'
    ], help="要執行的任務名稱")
    
    args = parser.parse_args()
    
    # 建立任務名稱與函式的對應關係
    tasks = {
        'update_current_weather': update_current_weather,
        'update_three_hour_forecast': update_three_hour_forecast,
        'update_weekly_forecast': update_weekly_forecast,
        'update_uv_index': update_uv_index,
        'update_air_quality': update_air_quality,
        'update_radar': update_radar_rainfall,
        'update_sunrise_sunset': update_sunrise_sunset,
    }
    
    # 根據傳入的參數，執行對應的函式
    task_to_run = tasks.get(args.task)
    if task_to_run:
        logger.info(f"從命令列執行任務: {args.task}")
        task_to_run()
        logger.info(f"任務 {args.task} 執行完畢。")
    else:
        logger.error(f"錯誤：找不到名為 {args.task} 的任務。")
        sys.exit(1)