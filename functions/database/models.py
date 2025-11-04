# weather_backend/database/models.py
import logging
import os
import ujson as json
import concurrent.futures
import gzip
import io

from typing import Dict, List, Optional
# from datetime import datetime
from dotenv import load_dotenv
from firebase_admin import firestore
import boto3
import firebase_admin
from firebase_admin import credentials
import geohash

from config.settings import Settings

logger = logging.getLogger(__name__)

# 載入環境變數
load_dotenv()

# 檢查是否使用模擬器
USE_EMULATOR = Settings.USE_EMULATOR
# FIRESTORE_EMULATOR_HOST = Settings.FIRESTORE_EMULATOR_HOST or "localhost:8080"
# FUNCTIONS_EMULATOR_HOST = Settings.FUNCTIONS_EMULATOR_HOST or "localhost:5001"
FIREBASE_PROJECT_ID = Settings.FIREBASE_PROJECT_ID

# 初始化 Firebase Admin SDK
try:
    #檢測是否已初始化
    firebase_admin.get_app()
    logger.info("Firebase Admin SDK already initialized")
except ValueError:
    if USE_EMULATOR:

        # 模擬器模式設定
        firebase_admin.initialize_app(options={
            'projectId': Settings.FIREBASE_PROJECT_ID,
        })
        # logger.info(f"Firebase 模擬器已初始化 (host: {FIRESTORE_EMULATOR_HOST})")
    else:
        try:
            # 正式環境設定
            service_account_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
            if service_account_path and os.path.exists(service_account_path):
                cred = credentials.Certificate(service_account_path)
                logger.info(f"使用服務帳號憑證: {service_account_path}")

            else:
                # 如果找不到服務帳號檔案，使用預設憑證
                cred = credentials.ApplicationDefault()
                logger.info("使用預設憑證")

            firebase_admin.initialize_app(cred, {
                'projectId': Settings.FIREBASE_PROJECT_ID
            })
            logger.info("Firebase 正式環境已初始化")
        except Exception as e:
            logger.error(f"Firebase 初始化失敗: {e}")
            raise

# 快取的客戶端實例
_db_client = None
_r2_client = None

# Client 預載器類別
class ClientPreloader:
    """Client 預載器，在背景平行初始化"""
    
    def __init__(self):
        self.firestore_future: Optional[concurrent.futures.Future] = None
        self.r2_future: Optional[concurrent.futures.Future] = None
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=2, thread_name_prefix="client-preloader")
        self._firestore_preloading_started = False
        self._r2_preloading_started = False
    
    def start_firestore_preloading(self):
        """只預載 Firestore client"""
        if self._firestore_preloading_started:
            return  # 避免重複啟動
        
        self._firestore_preloading_started = True
        logger.info("開始預載 Firestore client...")
        
        self.firestore_future = self.executor.submit(self._preload_firestore)
    
    def start_r2_preloading(self):
        """只預載 R2 client"""
        if self._r2_preloading_started:
            return  # 避免重複啟動
        
        self._r2_preloading_started = True
        logger.info("開始預載 R2 client...")
        
        self.r2_future = self.executor.submit(self._preload_r2)
    
    def start_preloading(self):
        """預載所有 clients（保持向後兼容）"""
        self.start_firestore_preloading()
        self.start_r2_preloading()
    
    def _preload_firestore(self):
        """預載 Firestore client"""
        try:
            client = get_firestore_client()
            logger.info("Firestore client 預載完成")
            return client
        except Exception as e:
            logger.warning(f"Firestore client 預載失敗: {e}")
            return None
    
    def _preload_r2(self):
        """預載 R2 client"""
        try:
            client = get_r2_client()
            logger.info("R2 client 預載完成")
            return client
        except Exception as e:
            logger.warning(f"R2 client 預載失敗: {e}")
            return None
    
    def wait_for_firestore(self, timeout=10):
        """等待 Firestore client 預載完成"""
        try:
            if self.firestore_future:
                result = self.firestore_future.result(timeout=timeout)
                if result is not None:
                    logger.info("Firestore client 預載完成")
                else:
                    logger.warning("Firestore client 預載失敗，將在實際使用時重新初始化")
            else:
                logger.warning("Firestore client 預載未啟動")
        except concurrent.futures.TimeoutError:
            logger.warning("Firestore client 預載超時，繼續執行")
        except Exception as e:
            logger.warning(f"Firestore client 預載出錯: {e}")
    
    def wait_for_r2(self, timeout=10):
        """等待 R2 client 預載完成"""
        try:
            if self.r2_future:
                result = self.r2_future.result(timeout=timeout)
                if result is not None:
                    logger.info("R2 client 預載完成")
                else:
                    logger.warning("R2 client 預載失敗，將在實際使用時重新初始化")
            else:
                logger.warning("R2 client 預載未啟動")
        except concurrent.futures.TimeoutError:
            logger.warning("R2 client 預載超時，繼續執行")
        except Exception as e:
            logger.warning(f"R2 client 預載出錯: {e}")
    
    def wait_for_clients(self, timeout=10):
        """等待所有 clients 預載完成（保持向後兼容）"""
        self.wait_for_firestore(timeout)
        self.wait_for_r2(timeout)
    
    def shutdown(self):
        """關閉執行緒池"""
        self.executor.shutdown(wait=False)

# 全域預載器實例
_client_preloader = ClientPreloader()

def get_firestore_client():
    """獲取快取的 Firestore 客戶端"""
    global _db_client
    if _db_client is None:
        try:
            _db_client = firestore.client()
            logger.info("Firestore client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to create Firestore client: {e}")
            raise
    return _db_client

def get_r2_client():
    """獲取快取的 R2 (S3) 客戶端"""
    global _r2_client
    if _r2_client is None:
        try:
            access_key = os.getenv('R2_ACCESS_KEY_ID')
            secret_key = os.getenv('R2_SECRET_ACCESS_KEY')
            endpoint_url = os.getenv('R2_ENDPOINT_URL')
            
            if not all([access_key, secret_key, endpoint_url]):
                raise ValueError("R2 credentials not properly configured")
            
            _r2_client = boto3.client(
                's3',
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                endpoint_url=endpoint_url,
                region_name='auto'
            )
            logger.info("R2 client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to create R2 client: {e}")
            raise
    return _r2_client

# 取得 Firestore 客戶端（向後相容）
db = get_firestore_client()

class FirestoreModel:
    """
    Firestore 資料模型基類
    """
    collection_name = None
    
    def save_to_firestore(self):
        """
        儲存模型資料至 Firestore
        """
        if not self.collection_name:
            raise ValueError("未設定集合名稱")
        
        # 轉換模型為字典
        data_dict = self.to_dict()
        
        # 儲存至 Firestore
        db = get_firestore_client()
        collection_ref = db.collection(self.collection_name)
        
        # 使用 id 作為文件 ID，若不存在則自動生成
        doc_id = data_dict.get('id')
        if doc_id:
            doc_ref = collection_ref.document(doc_id)
            doc_ref.set(data_dict)
        else:
            collection_ref.add(data_dict)
        
        return self
    
    def to_dict(self) -> Dict:
        """
        將模型轉換為字典
        
        返回:
            表示模型的字典
        """
        raise NotImplementedError("子類必須實現 to_dict 方法")


class ObservationData(FirestoreModel):
    """氣象站觀測資料模型"""
    collection_name = 'observations'
    
    def __init__(self, station_id: str, station_name: str, latitude: float, 
                 longitude: float, observations: Dict, timestamp: float):
        """
        初始化氣象站觀測資料模型
        
        參數:
            station_id: 氣象站 ID
            station_name: 氣象站名稱
            latitude: 緯度(WSG84)
            longitude: 經度(WSG84)
            observations: 觀測資料
            timestamp: 時間戳記
        """
        self.station_id = station_id
        self.station_name = station_name
        self.latitude = float(latitude)
        self.longitude = float(longitude)
        self.observations = observations
        self.timestamp = timestamp
        
        # 建立文件 ID: 氣象站ID_時間戳記
        self.id = f"{station_name}_{station_id}"
    def to_dict(self) -> Dict:
        """
        將模型轉換為字典
        
        返回:
            表示模型的字典
        """
        # 生成 geohash，精度為 7 位
        location_hash = geohash.encode(self.latitude, self.longitude, precision=7)
        
        return {
            'id': self.id,
            'stationId': self.station_id,
            'stationName': self.station_name,
            'latitude': self.latitude,
            'longitude': self.longitude,
            'geohash': location_hash,
            'observations': self.observations,
            # 'timestamp': self.timestamp,
            'createdAt': firestore.SERVER_TIMESTAMP
        }

    @staticmethod
    def batch_save(observations: List[Dict], batch_size: int = 500) -> Dict:
    
        """
        批次儲存氣象站觀測資料
        
        參數:
            observations: 觀測資料列表
            batch_size: 每批次處理的文件數量 (Firestore 限制為 500)
        回傳:
            stats: 執行結果統計資訊
        """
        from google.api_core.exceptions import ResourceExhausted, DeadlineExceeded
        circuit_open = False

        stats = {
            'total_attempts': len(observations),
            'success_count': 0,
            'failed_count': 0,
            'failed_items': []
        }

        try:
            db = get_firestore_client()
            batch = db.batch()
            count = 0
            
            # 預編譯集合引用（避免重複查找）
            collection_ref = db.collection('observations')

            for data in observations:
                if circuit_open:
                    # 標記失敗
                    stats['failed_count'] += 1
                    stats['failed_items'].append({
                        'stationId': data.get('stationId'),
                        'stationName': data.get('stationName'),
                        'error': 'Firestore quota exceeded. Write operation aborted.'
                    })
                    continue
                try:
                    model = ObservationData(
                        station_id=data['stationId'],
                        station_name=data['stationName'],
                        latitude=float(data['latitude']),
                        longitude=float(data['longitude']),
                        observations=data['observations'],
                        # timestamp=data.get('timestamp', datetime.now().timestamp())
                        timestamp=0  # 臨時值，因為已註解掉
                    )

                    doc_ref = collection_ref.document(model.id)
                    batch.set(doc_ref, model.to_dict(), merge=True)
                    count += 1
                    stats['success_count'] += 1

                    if count >= batch_size:
                        batch.commit()
                        batch = db.batch()
                        count = 0
                        logger.info(f"已批次處理 {batch_size} 筆觀測資料")

                except (ResourceExhausted, DeadlineExceeded) as e:
                    circuit_open = True
                    stats['failed_count'] += 1
                    stats['failed_items'].append({
                        'stationId': data.get('stationId'),
                        'stationName': data.get('stationName'),
                        'error': f'{type(e).__name__}: {e}'
                    })
                    logger.error(f"Firestore quota exceeded, aborting batch_save: {e}")
                    raise

                except Exception as e:
                    stats['failed_count'] += 1
                    stats['failed_items'].append({
                        'stationId': data.get('stationId'),
                        'stationName': data.get('stationName'),
                        'error': str(e)
                    })

                    logger.error(f"處理觀測資料時發生錯誤: {data.get('stationName')}_{data.get('stationId')}, 錯誤: {e}")
                    continue

            if count > 0 and not circuit_open:
                batch.commit()
                logger.info(f"已處理剩餘 {count} 筆觀測資料")

            return stats
        
        except Exception as e:
            logger.error(f"批次儲存觀測資料時發生錯誤: {e}")
            raise


class ThreeHourForecast(FirestoreModel):
    """三小時天氣預報資料模型"""
    collection_name = 'weather_forecasts'
    
    def __init__(self, county_name: str, town_name: str, 
                 latitude: float, longitude: float,
                 forecasts: List[Dict], timestamp: float):
        """
        初始化三小時天氣預報資料模型
        
        參數:
            county_name: 縣市名稱
            town_name: 鄉鎮名稱
            latitude: 緯度
            longitude: 經度
            forecasts: 預報資料列表（每個 dict 需包含 apparent_temperature 欄位）
            timestamp: 時間戳記
        """
        self.county_name = county_name
        self.town_name = town_name
        self.latitude = latitude
        self.longitude = longitude
        self.forecasts = forecasts
        self.timestamp = timestamp
        
        # 建立文件 ID: county_town 格式
        self.id = f"{county_name}_{town_name}"
    
    def to_dict(self) -> Dict:
        """將模型轉換為字典"""
        return {
            'id': self.id,
            'countyName': self.county_name,
            'townName': self.town_name,
            'location': {
                'latitude': self.latitude,
                'longitude': self.longitude
            },
            'hourly_forecast': self.forecasts,  # 每個 dict 需有 apparent_temperature
            # 'timestamp': self.timestamp,
            'updatedAt': firestore.SERVER_TIMESTAMP
        }

    def save_to_firestore(self):
        try:
            doc_ref = db.collection(self.collection_name).document(self.id)
            
            # 更新文件
            doc_ref.set(self.to_dict() , merge=True)
            logger.info(f"成功更新三小時預報：{self.id}")
            return self
            
        except Exception as e:
            logger.error(f"儲存三小時預報資料時發生錯誤: {self.id}, 錯誤: {e}")
            raise

    @staticmethod
    def batch_save(forecasts: List[Dict], batch_size: int = 500) -> Dict:
        """
        批次儲存三小時天氣預報資料
        
        參數:
            forecasts: 預報資料列表
            batch_size: 每批次處理的文件數量 
        """
        from google.api_core.exceptions import ResourceExhausted, DeadlineExceeded
        circuit_open = False

        stats = {
            'total_attempts': len(forecasts),
            'success_count': 0,
            'failed_count': 0,
            'failed_items': []
        }

        try:
            db = get_firestore_client()
            batch = db.batch()
            count = 0
            
            # 預編譯集合引用（避免重複查找）
            collection_ref = db.collection('weather_forecasts')
            
            for data in forecasts:
                if circuit_open:
                    stats['failed_count'] += 1
                    stats['failed_items'].append({
                        'countyName': data.get('countyName'),
                        'townName': data.get('townName'),
                        'error': 'Firestore quota exceeded. Write operation aborted.'
                    })
                    continue                
                try:
                    # 用 model 統一欄位邏輯
                    model = ThreeHourForecast(
                        county_name=data['countyName'],
                        town_name=data['townName'],
                        latitude=float(data['latitude']),
                        longitude=float(data['longitude']),
                        forecasts=data['forecasts'],
                        # timestamp=data.get('timestamp', datetime.now().timestamp())
                        timestamp=0  # 臨時值，因為已註解掉
                    )
                    doc_ref = collection_ref.document(model.id)
                    batch.set(doc_ref, model.to_dict(), merge=True)
                    count += 1
                    stats['success_count'] += 1
                    
                    # 當達到批次大小時，提交並重置
                    if count >= batch_size:
                        batch.commit()
                        batch = db.batch()
                        count = 0
                        logger.info(f"已批次處理 {batch_size} 筆三小時預報資料")

                except (ResourceExhausted, DeadlineExceeded) as e:
                    circuit_open = True
                    stats['failed_count'] += 1
                    stats['failed_items'].append({
                        'countyName': data.get('countyName'),
                        'townName': data.get('townName'),
                        'error': f'{type(e).__name__}: {e}'
                    })
                    logger.error(f"Firestore quota exceeded, aborting batch_save: {e}")
                    raise

                except Exception as e:
                    stats['failed_count'] += 1
                    stats['failed_items'].append({
                        'countyName': data.get('countyName'),
                        'townName': data.get('townName'),
                        'error': str(e)
                    })
                    logger.error(f"處理三小時預報資料時發生錯誤: {data.get('countyName')}{data.get('townName')}, 錯誤: {e}")
                    continue
            
            # 處理剩餘的資料
            if count > 0 and not circuit_open:
                batch.commit()
                logger.info(f"已處理剩餘 {count} 筆三小時預報資料")
            
            return stats
                
        except Exception as e:
            logger.error(f"批次儲存三小時預報資料時發生錯誤: {e}")
            raise
class WeeklyForecast(FirestoreModel):
    """一週天氣預報資料模型"""
    collection_name = 'weather_forecasts'
    
    def __init__(self, county_name: str, town_name: str,
                 latitude: float, longitude: float, 
                 forecasts: List[Dict], timestamp: float):
        """
        初始化一週天氣預報資料模型
        """
        self.county_name = county_name
        self.town_name = town_name
        self.latitude = latitude
        self.longitude = longitude
        # 確保每個預報 dict 都有 max_apparent_temperature, min_apparent_temperature 欄位
        # for f in forecasts:
        #     if 'max_apparent_temperature' not in f:
        #         f['max_apparent_temperature'] = None
        #     if 'min_apparent_temperature' not in f:
        #         f['min_apparent_temperature'] = None
        self.forecasts = forecasts
        self.timestamp = timestamp
        
        # 建立文件 ID: county_town 格式
        self.id = f"{county_name}_{town_name}"
    
    def to_dict(self) -> Dict:
        return {
            'id': self.id,
            'countyName': self.county_name,
            'townName': self.town_name,
            'location': {
                'latitude': self.latitude,
                'longitude': self.longitude
            },
            'weekly_forecast': self.forecasts,  # 每個 dict 需有 max/min_apparent_temperature
            # 'timestamp': self.timestamp,
            'updatedAt': firestore.SERVER_TIMESTAMP
        }

    def save_to_firestore(self):
        """儲存到 Firestore"""
        try:
            db = get_firestore_client()
            doc_ref = db.collection(self.collection_name).document(self.id)
            doc_ref.set(self.to_dict(), merge=True)
            return self
        except Exception as e:
            logger.error(f"儲存週預報資料時發生錯誤: {e}")
            raise

    @staticmethod
    def batch_save(forecasts: List[Dict], batch_size: int = 500) -> Dict:
        """
        批次儲存一週天氣預報資料
        
        參數:
            forecasts: 預報資料列表
            batch_size: 每批次處理的文件數量 (Firestore 限制為 500)
        """
        from google.api_core.exceptions import ResourceExhausted, DeadlineExceeded
        circuit_open = False

        stats = {
            'total_attempts': len(forecasts),
            'success_count': 0,
            'failed_count': 0,
            'failed_items': []
        }

        try:
            db = get_firestore_client()
            batch = db.batch()
            count = 0
            
            # 預編譯集合引用（避免重複查找）
            collection_ref = db.collection('weather_forecasts')

            for data in forecasts:
                if circuit_open:
                    stats['failed_count'] += 1
                    stats['failed_items'].append({
                        'countyName': data.get('countyName'),
                        'townName': data.get('townName'),
                        'error': 'Firestore quota exceeded. Write operation aborted.'
                    })
                    continue
                try:
                    # 用 model 統一欄位邏輯
                    model = WeeklyForecast(
                        county_name=data['countyName'],
                        town_name=data['townName'],
                        latitude=float(data['latitude']),
                        longitude=float(data['longitude']),
                        forecasts=data['forecasts'],
                        # timestamp=data.get('timestamp', int(datetime.now().timestamp()))
                        timestamp=0  # 臨時值，因為已註解掉
                    )
                    doc_ref = collection_ref.document(model.id)
                    batch.set(doc_ref, model.to_dict(), merge=True)
                    count += 1
                    stats['success_count'] += 1
                    
                    # 當達到批次大小時，提交並重置
                    if count >= batch_size:
                        batch.commit()
                        batch = db.batch()
                        count = 0
                        logger.info(f"已批次處理 {batch_size} 筆週預報資料")

                except (ResourceExhausted, DeadlineExceeded) as e:
                    circuit_open = True
                    stats['failed_count'] += 1
                    stats['failed_items'].append({
                        'countyName': data.get('countyName'),
                        'townName': data.get('townName'),
                        'error': f'{type(e).__name__}: {e}'
                    })
                    logger.error(f"Firestore quota exceeded, aborting batch_save: {e}")
                    raise

                except Exception as e:
                    stats['failed_count'] += 1
                    stats['failed_items'].append({
                        'countyName': data.get('countyName'),
                        'townName': data.get('townName'),
                        'error': str(e)
                    })
                    logger.error(f"處理週預報資料時發生錯誤: {data.get('countyName')}{data.get('townName')}, 錯誤: {e}")
                    continue
            
            # 處理剩餘的資料
            if count > 0 and not circuit_open:
                batch.commit()
                logger.info(f"已處理剩餘 {count} 筆週預報資料")
            
            return stats
                
        except Exception as e:
            logger.error(f"批次儲存週預報資料時發生錯誤: {e}")
            raise

class RadarPredict:
    """
    將 extract_radar_rainfall 處理過的資料 (dict) 上傳到 Cloudflare R2。
    
    參數:
        radar_json: extract_radar_rainfall 處理後的 dict
        storage_path (str): 在儲存桶內的目標路徑（例如 'radar/forecast.json'
        use_compression (bool): 是否使用 gzip 壓縮，預設為 True
    """
    @staticmethod
    def save_to_r2(radar_json: dict, storage_path: str, use_compression: bool = True) -> None:
        try:
            # 使用快取的 R2 客戶端
            s3 = get_r2_client()
            bucket_name = os.getenv('R2_BUCKET_NAME')
            
            if not bucket_name:
                raise ValueError("R2_BUCKET_NAME not configured")

            # 序列化 JSON
            json_data = json.dumps(radar_json, ensure_ascii=False)
            
            if use_compression:
                # 使用 gzip 壓縮
                buffer = io.BytesIO()
                with gzip.GzipFile(fileobj=buffer, mode='wb') as gz_file:
                    gz_file.write(json_data.encode('utf-8'))
                
                compressed_data = buffer.getvalue()
                content_type = 'application/json'
                content_encoding = 'gzip'
                
                # 記錄壓縮效果
                original_size = len(json_data.encode('utf-8'))
                compressed_size = len(compressed_data)
                compression_ratio = (1 - compressed_size / original_size) * 100
                
                logger.info(f"雷達資料壓縮效果: {original_size:,} → {compressed_size:,} bytes ({compression_ratio:.1f}% 減少)")
                
                upload_data = compressed_data
                extra_args = {'ContentEncoding': content_encoding}
            else:
                # 不壓縮
                upload_data = json_data.encode('utf-8')
                content_type = 'application/json'
                extra_args = {}

            # 上傳至 R2
            s3.put_object(
                Bucket=bucket_name,
                Key=storage_path,
                Body=upload_data,
                ContentType=content_type,
                CacheControl='public, max-age=300',  # 快取5分鐘
                **extra_args
            )
            
            size_info = f"({len(upload_data):,} bytes)" if use_compression else f"({len(upload_data):,} bytes, 未壓縮)"
            logger.info(f"已成功上傳雷達降雨預報至 Cloudflare R2: {storage_path} {size_info}")
            
        except Exception as e:
            logger.error(f"上傳雷達降雨預報至 Cloudflare R2 失敗: {e}")
            raise

class TyphoonForecastImage:
    """
    將颱風預報圖片 (bytes) 上傳到 Cloudflare R2。
    """
    @staticmethod
    def save_to_r2(image_data: bytes, storage_path: str) -> None:
        """
        將圖片二進位資料上傳至 R2。

        參數:
            image_data (bytes): 圖片的二進位內容。
            storage_path (str): 在儲存桶內的目標路徑。
        """
        try:
            s3 = get_r2_client()
            bucket_name = os.getenv('R2_BUCKET_NAME')
            
            if not bucket_name:
                raise ValueError("R2_BUCKET_NAME not configured")

            s3.put_object(
                Bucket=bucket_name,
                Key=storage_path,
                Body=image_data,
                ContentType='image/png',
                CacheControl='public, max-age=3600',  # 設定快取 1 小時
            )
            
            size_info = f"({len(image_data):,} bytes)"
            logger.info(f"已成功上傳颱風預報圖片至 Cloudflare R2: {storage_path} {size_info}")
            
        except Exception as e:
            logger.error(f"上傳颱風預報圖片至 Cloudflare R2 失敗: {e}")
            raise

class UVIndexData(FirestoreModel):
    """紫外線指數資料模型"""
    collection_name = 'uv_index'
    
    def __init__(self, station_id: str, station_name: str,
                 latitude: float, longitude: float,
                 uv_index: int, timestamp: float):
        self.station_id = station_id
        self.station_name = station_name
        self.latitude = float(latitude)
        self.longitude = float(longitude)
        self.uv_index = uv_index
        self.timestamp = timestamp
        
        # 建立文件 ID: 氣象站ID_時間戳記
        self.id = f"{station_name}_{station_id}"
        
    def to_dict(self) -> Dict:
        # 生成 geohash，精度為 7 位
        location_hash = geohash.encode(self.latitude, self.longitude, precision=7)
        
        return {
            'id': self.id,
            'stationId': self.station_id,
            'stationName': self.station_name,
            'location': {
                'latitude': self.latitude,
                'longitude': self.longitude
            },
            'geohash': location_hash,
            'uvIndex': self.uv_index,
            # 'timestamp': self.timestamp,
            'updatedAt': firestore.SERVER_TIMESTAMP
        }

    @staticmethod
    def batch_save(uv_data: List[Dict], batch_size: int = 500) -> Dict:
        """
        批次儲存紫外線指數資料
        
        參數:
            uv_data: 紫外線指數資料列表
            batch_size: 每批次處理的文件數量 (Firestore 限制為 500)
        回傳:
            stats: 執行結果統計資訊
        """
        from google.api_core.exceptions import ResourceExhausted, DeadlineExceeded
        circuit_open = False
        stats = {
            'total_attempts': len(uv_data),
            'success_count': 0,
            'failed_count': 0,
            'failed_items': []
        }

        try:
            db = get_firestore_client()
            batch = db.batch()
            count = 0
            
            # 預編譯集合引用（避免重複查找）
            collection_ref = db.collection('uv_index')
            
            for data in uv_data:
                if circuit_open:
                    stats['failed_count'] += 1
                    stats['failed_items'].append({
                        'stationId': data.get('stationId'),
                        'stationName': data.get('stationName'),
                        'error': 'Firestore quota exceeded. Write operation aborted.'
                    })
                    continue

                try:

                    model = UVIndexData(
                        station_id=data['stationId'],
                        station_name=data['stationName'],
                        latitude=float(data['latitude']),
                        longitude=float(data['longitude']),
                        uv_index=int(data['uvIndex']),
                        # timestamp=data.get('timestamp', datetime.now().timestamp())
                        timestamp=0  # 臨時值，因為已註解掉
                    )

                    doc_ref = collection_ref.document(model.id)
                    batch.set(doc_ref, model.to_dict(), merge=True)
                    count += 1
                    stats['success_count'] += 1

                    if count >= batch_size:

                        batch.commit()
                        batch = db.batch()
                        count = 0
                        logger.info(f"已批次處理 {batch_size} 筆紫外線指數資料")

                except (ResourceExhausted, DeadlineExceeded) as e:
                    circuit_open = True
                    stats['failed_count'] += 1
                    stats['failed_items'].append({
                        'stationId': data.get('stationId'),
                        'stationName': data.get('stationName'),
                        'error': f'{type(e).__name__}: {e}'
                    })
                    logger.error(f"Firestore quota exceeded, aborting batch_save: {e}")
                    raise

                except Exception as e:

                    stats['failed_count'] += 1
                    stats['failed_items'].append({
                        'stationId': data.get('stationId'),
                        'stationName': data.get('stationName'),
                        'error': str(e)
                    })

                    logger.error(f"處理紫外線指數資料時發生錯誤: {data.get('stationName')}_{data.get('stationId')}, 錯誤: {e}")
                    continue
                
            if count > 0 and not circuit_open:
                batch.commit()
                logger.info(f"已處理剩餘 {count} 筆紫外線指數資料")

            return stats
        
        except Exception as e:
            logger.error(f"批次儲存紫外線指數資料時發生錯誤: {e}")
            raise

class AirQualityData(FirestoreModel):
    """空氣品質資料模型"""
    collection_name = 'air_quality'
    
    def __init__(self, station_id: str, station_name: str,
                 county: str, latitude: float, longitude: float,
                 measurements: Dict, publish_time: str,
                 timestamp: float):
        self.station_id = station_id
        self.station_name = station_name
        self.county = county
        self.latitude = float(latitude)
        self.longitude = float(longitude)
        self.measurements = measurements
        self.publish_time = publish_time
        self.timestamp = timestamp
        
        # 建立文件 ID: 測站ID_時間戳記
        self.id = f"{station_name}_{station_id}"

    def to_dict(self) -> Dict:
        # 生成 geohash，精度為 7 位
        location_hash = geohash.encode(self.latitude, self.longitude, precision=7)
        
        return {
            'id': self.id,
            'stationId': self.station_id,
            'stationName': self.station_name,
            'county': self.county,
            'location': {
                'latitude': self.latitude,
                'longitude': self.longitude
            },
            'geohash': location_hash,
            'measurements': self.measurements,
            'publishTime': self.publish_time,
            # 'timestamp': self.timestamp,
            'updatedAt': firestore.SERVER_TIMESTAMP
        }

    @staticmethod
    def batch_save(aq_data: List[Dict], batch_size: int = 500) -> Dict:
        """
        批次儲存空氣品質資料
        
        參數:
            aq_data: 空氣品質資料列表
            batch_size: 每批次處理的文件數量 (Firestore 限制為 500)
        回傳:
            stats: 執行結果統計資訊
        """
        from google.api_core.exceptions import ResourceExhausted, DeadlineExceeded
        circuit_open = False
        stats = {
            'total_attempts': len(aq_data),
            'success_count': 0,
            'failed_count': 0,
            'failed_items': []
        }

        try:
            db = get_firestore_client()
            batch = db.batch()
            count = 0
            
            # 預編譯集合引用（避免重複查找）
            collection_ref = db.collection('air_quality')

            for data in aq_data:
                if circuit_open:
                    stats['failed_count'] += 1
                    stats['failed_items'].append({
                        'stationId': data.get('stationId'),
                        'stationName': data.get('stationName'),
                        'error': 'Firestore quota exceeded. Write operation aborted.'
                    })
                    continue

                try:
                    model = AirQualityData(
                        station_id=data['stationId'],
                        station_name=data['stationName'],
                        county=data['county'],
                        latitude=float(data['location']['latitude']),
                        longitude=float(data['location']['longitude']),
                        measurements=data['measurements'],
                        publish_time=data['publishTime'],
                        # timestamp=data.get('timestamp', datetime.now().timestamp())
                        timestamp=0  # 臨時值，因為已註解掉
                    )

                    doc_ref = collection_ref.document(model.id)
                    batch.set(doc_ref, model.to_dict(), merge=True)
                    count += 1
                    stats['success_count'] += 1

                    if count >= batch_size:
                        batch.commit()
                        batch = db.batch()
                        count = 0
                        logger.info(f"已批次處理 {batch_size} 筆空氣品質資料")

                except (ResourceExhausted, DeadlineExceeded) as e:
                    circuit_open = True
                    stats['failed_count'] += 1
                    stats['failed_items'].append({
                        'stationId': data.get('stationId'),
                        'stationName': data.get('stationName'),
                        'error': f'{type(e).__name__}: {e}'
                    })
                    logger.error(f"Firestore quota exceeded, aborting batch_save: {e}")
                    raise

                except Exception as e:

                    stats['failed_count'] += 1
                    stats['failed_items'].append({
                        'stationId': data.get('stationId'),
                        'stationName': data.get('stationName'),
                        'error': str(e)
                    })

                    logger.error(f"處理空氣品質資料時發生錯誤: {data.get('stationName')}_{data.get('stationId')}, 錯誤: {e}")
                    continue

            if count > 0 and not circuit_open:
                batch.commit()
                logger.info(f"已處理剩餘 {count} 筆空氣品質資料")
            return stats
        
        except Exception as e:
            logger.error(f"批次儲存空氣品質資料時發生錯誤: {e}")
            raise
        
class SunriseData(FirestoreModel):
    """日出日落與月出月落資料模型"""
    collection_name = 'sunrise_sunset'
    
    def __init__(self, county_name: str, date: str,
                 sunrise_time: str, sunset_time: str,
                 moonrise_time: str, moonset_time: str,
                 timestamp: float):
        self.county_name = county_name
        self.date = date
        self.sunrise_time = sunrise_time
        self.sunset_time = sunset_time
        self.moonrise_time = moonrise_time
        self.moonset_time = moonset_time
        self.timestamp = timestamp
        
        # 建立文件 ID: county_name 格式，每日覆蓋
        self.id = f"{county_name}"
    
    def to_dict(self) -> Dict:
        return {
            'id': self.id,
            'countyName': self.county_name,
            'date': self.date,
            'sunriseTime': self.sunrise_time,
            'sunsetTime': self.sunset_time,
            'moonriseTime': self.moonrise_time,
            'moonsetTime': self.moonset_time,
            # 'timestamp': self.timestamp,
            'updatedAt': firestore.SERVER_TIMESTAMP
        }

    @staticmethod
    def batch_save(sunrise_list: List[Dict], batch_size: int = 500) -> Dict:
        """
        批次儲存日出日落與月出月落資料
        
        參數:
            sunrise_list: 包含日月資料的列表
            batch_size: 每批次處理的文件數量 (Firestore 限制為 500)
        回傳:
            stats: 執行結果統計資訊
        """
        from google.api_core.exceptions import ResourceExhausted, DeadlineExceeded
        circuit_open = False
        stats = {
            'total_attempts': len(sunrise_list),
            'success_count': 0,
            'failed_count': 0,
            'failed_items': []
        }

        try:
            db = get_firestore_client()
            batch = db.batch()
            count = 0
            
            # 預編譯集合引用（避免重複查找）
            collection_ref = db.collection('sunrise_sunset')
            
            for data in sunrise_list:
                if circuit_open:
                    stats['failed_count'] += 1
                    stats['failed_items'].append({
                        'countyName': data.get('countyName'),
                        'date': data.get('date'),
                        'error': 'Firestore quota exceeded. Write operation aborted.'
                    })
                    continue
                try:
                    model = SunriseData(
                        county_name=data['countyName'],
                        date=data['date'],
                        sunrise_time=data['sunriseTime'],
                        sunset_time=data['sunsetTime'],
                        moonrise_time=data.get('moonriseTime', 'N/A'), # 使用 .get() 增加彈性
                        moonset_time=data.get('moonsetTime', 'N/A'),
                        # timestamp=data.get('timestamp', datetime.now().timestamp())
                        timestamp=0  # 臨時值，因為已註解掉
                    )

                    doc_ref = collection_ref.document(model.id)
                    batch.set(doc_ref, model.to_dict(), merge=True)
                    count += 1
                    stats['success_count'] += 1

                    if count >= batch_size:
                        batch.commit()
                        batch = db.batch()
                        count = 0
                        logger.info(f"已批次處理 {batch_size} 筆天文資料")

                except (ResourceExhausted, DeadlineExceeded) as e:
                    circuit_open = True
                    stats['failed_count'] += 1
                    stats['failed_items'].append({
                        'countyName': data.get('countyName'),
                        'date': data.get('date'),
                        'error': f'{type(e).__name__}: {e}'
                    })
                    logger.error(f"Firestore quota exceeded, aborting batch_save: {e}")
                    raise
                
                except Exception as e:
                    item_id = f"{data.get('countyName', 'N/A')}-{data.get('date', 'N/A')}"
                    stats['failed_count'] += 1
                    stats['failed_items'].append({
                        'id': item_id,
                        'error': str(e)
                    })
                    logger.error(f"處理日出日落資料時發生錯誤: {item_id}, 錯誤: {e}")
                    continue

            if count > 0 and not circuit_open:
                batch.commit()
                logger.info(f"已處理剩餘 {count} 筆日出日落資料")

            return stats
        
        except Exception as e:
            logger.error(f"批次儲存日出日落資料時發生錯誤: {e}")
            raise

class AlertData(FirestoreModel):
    """示警資訊資料模型"""
    collection_name = 'alerts'
    
    def __init__(self, alert_id: str, title: str, updated: str, 
                 author: str, summary: str, category: str,
                 timestamp: float):
        self.alert_id = alert_id
        self.title = title
        self.updated = updated
        self.author = author
        self.summary = summary
        self.category = category
        self.timestamp = timestamp
        
        # 使用原始 alert_id 作為文件 ID
        self.id = alert_id
    
    def to_dict(self) -> Dict:
        return {
            'id': self.id,
            'title': self.title,
            'updated': self.updated,
            'author': self.author,
            'summary': self.summary,
            'category': self.category,
            # 'timestamp': self.timestamp,
            'updatedAt': firestore.SERVER_TIMESTAMP
        }

# Client 預載器便利函數
def get_client_preloader():
    """獲取全域 client 預載器"""
    return _client_preloader

def start_firestore_preloading():
    """啟動 Firestore client 預載（便利函數）"""
    _client_preloader.start_firestore_preloading()

def start_r2_preloading():
    """啟動 R2 client 預載（便利函數）"""
    _client_preloader.start_r2_preloading()

def start_client_preloading():
    """啟動所有 client 預載（便利函數，保持向後兼容）"""
    _client_preloader.start_preloading()

def wait_for_firestore_preloading(timeout=10):
    """等待 Firestore client 預載完成（便利函數）"""
    _client_preloader.wait_for_firestore(timeout=timeout)

def wait_for_r2_preloading(timeout=10):
    """等待 R2 client 預載完成（便利函數）"""
    _client_preloader.wait_for_r2(timeout=timeout)

def wait_for_client_preloading(timeout=10):
    """等待所有 client 預載完成（便利函數，保持向後兼容）"""
    _client_preloader.wait_for_clients(timeout=timeout)