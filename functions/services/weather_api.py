# weather_backend/functions/services/weather_api.py
import os
import requests
# import aiohttp
import asyncio
import datetime
from typing import Dict, Any, List, Optional
import logging
import time

class WeatherAPIService:
    """
    Service to handle communication with Taiwan Central Weather Bureau (CWB) API
    """
    
    def __init__(self, api_key: str = None):
        """初始化 WeatherAPIService"""
        self.cwa_api_key = api_key or os.environ.get('CWA_API_KEY')
        self.ncdr_api_key = os.environ.get('NCDR_API_KEY')
        self.monev_api_key = os.environ.get('MONEV_API_KEY')
        
        if not self.cwa_api_key:
            raise ValueError("CWA API key is required")
        if not self.ncdr_api_key:
            raise ValueError("NCDR API key is required")
        if not self.monev_api_key:
            raise ValueError("MONEV API key is required")
        
        self.cwa_base_url = "https://opendata.cwa.gov.tw/api/v1/rest/datastore"
        self.ncdr_base_url = "https://dataapi2.ncdr.nat.gov.tw/NCDR"
        self.monev_base_url = "https://data.moenv.gov.tw/api/v2/aqx_p_432"
        self.alert_base_url = "https://alerts.ncdr.nat.gov.tw/JSONAtomFeeds.ashx"
        self.radar_base_url = "https://opendata.cwa.gov.tw/fileapi/v1/opendataapi/F-B0046-001?Authorization=CWA-ADEE995D-155F-45D1-AD36-E41D081A0084&downloadType=WEB&format=JSON"
        self.logger = logging.getLogger(__name__)

    def _make_request(self, endpoint: str, params: Dict[str, Any] = None, api_type: str = 'cwa') -> Dict:
        """發送 HTTP 請求到指定的 API"""
        if api_type == 'cwa':
            default_url = f"{self.cwa_base_url}/{endpoint}"
            default_params = {
                "Authorization": self.cwa_api_key,
                "format": "JSON"
            }
            headers = None
            
        elif api_type == 'ncdr':
            default_url = f"{self.ncdr_base_url}/{endpoint}"
            headers = {'Token': self.ncdr_api_key}
            default_params = {                
                "format": "csv",
                "stream": True,
            }

        elif api_type == 'monev':
            default_url = f"{self.monev_base_url}"
            default_params = {
                "dataset":"aqx_p_432",
                "api_key": self.monev_api_key,
                "format": "JSON",
                "limit": "1000"
            }
            headers = None

        else:
            raise ValueError(f"不支援的 API 類型: {api_type}")

        if params:
            default_params.update(params)

        try:
            self.logger.info(f"正在請求 {api_type.upper()} API: {endpoint}")
            response = requests.get(
                default_url, 
                params=default_params, 
                headers=headers,
                timeout=30
            )
            response.raise_for_status()

            if api_type == 'cwa':
                return response.json()
            elif api_type == 'ncdr':
                return response.text  # 回傳 CSV 文字內容
            elif api_type == 'monev':
                return response.json()
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"從 {api_type.upper()} API 獲取資料時發生錯誤: {e}")
            raise
    
    def get_three_hour_forecast(self, locations: List[str] = None) -> Dict:
        """
        使用批次方式取得三小時天氣預報
        """
        ALL_LOCATION_IDS = [
            "F-D0047-001", "F-D0047-005", "F-D0047-009", 
            "F-D0047-013", "F-D0047-017", "F-D0047-021", 
            "F-D0047-025", "F-D0047-029", "F-D0047-033", 
            "F-D0047-037", "F-D0047-041", "F-D0047-045", 
            "F-D0047-049", "F-D0047-053", "F-D0047-057", 
            "F-D0047-061", "F-D0047-065", "F-D0047-069",
            "F-D0047-073", "F-D0047-077", "F-D0047-081", 
            "F-D0047-085", 
        ]
        
        BATCH_SIZE = 5
        merged_result = {"success": True, "records": {"locations": []}}
        
        try:
            target_locations = locations if locations else ALL_LOCATION_IDS
            total_batches = (len(target_locations) + BATCH_SIZE - 1) // BATCH_SIZE
            
            for i in range(0, len(target_locations), BATCH_SIZE):
                batch_locations = target_locations[i:i + BATCH_SIZE]
                current_batch = (i // BATCH_SIZE) + 1
                
                self.logger.info(
                    f"處理第 {current_batch}/{total_batches} 批次, "
                    f"包含 {len(batch_locations)} 個地區: {batch_locations}"
                )

                endpoint = "F-D0047-093" 
                
                params = {
                    "locationId": batch_locations,
                    "ElementName": "天氣預報綜合描述,體感溫度",
                    "format": "JSON"
                }
                
                try:
                    batch_result = self._make_request(endpoint, params)
                    
                    # 檢查資料結構
                    locations_data = batch_result.get("records", {}).get("Locations", [])
                    if not locations_data:
                        self.logger.error(f"批次 {current_batch} 無有效資料")
                        continue

                    merged_result["records"]["locations"].extend(locations_data)
                    self.logger.info(f"成功處理第 {current_batch} 批次的資料")
                    
                    if current_batch < total_batches:
                        time.sleep(1.5)  # 在最後一批次不需要等待
                        
                except Exception as e:
                    self.logger.error(f"批次 {current_batch} 處理失敗: {e}")
                    continue
                    
            return merged_result
            
        except Exception as e:
            self.logger.error(f"取得天氣預報時發生錯誤: {e}")
            raise
    
    def get_weekly_forecast(self, locations: List[str] = None) -> Dict:
        """
        使用批次方式取得一週天氣預報
        
        參數:
            locations: 地區 ID 列表，若為 None 則取得所有地區資料
        
        返回:
            包含一週天氣預報的字典
        """
        ALL_LOCATION_IDS = [
            "F-D0047-003", "F-D0047-007", "F-D0047-011", 
            "F-D0047-015", "F-D0047-019", "F-D0047-023", 
            "F-D0047-027", "F-D0047-031", "F-D0047-035", 
            "F-D0047-039", "F-D0047-043", "F-D0047-047", 
            "F-D0047-051", "F-D0047-055", "F-D0047-059", 
            "F-D0047-063", "F-D0047-067", "F-D0047-071",
            "F-D0047-075", "F-D0047-079", "F-D0047-083", 
            "F-D0047-087"
        ]
        
        BATCH_SIZE = 5
        merged_result = {"success": True, "records": {"locations": []}}
        # location_count = {}  # 用於統計各鄉鎮出現次數
        # total_locations = 0  # 用於統計總鄉鎮數
        
        try:
            target_locations = locations if locations else ALL_LOCATION_IDS
            total_batches = (len(target_locations) + BATCH_SIZE - 1) // BATCH_SIZE
            
            for i in range(0, len(target_locations), BATCH_SIZE):
                batch_locations = target_locations[i:i + BATCH_SIZE]
                current_batch = (i // BATCH_SIZE) + 1
                
                self.logger.info(
                    f"處理第 {current_batch}/{total_batches} 批次一週預報, "
                    f"包含 {len(batch_locations)} 個地區: {batch_locations}"
                )

                endpoint = "F-D0047-093" 
                
                params = {
                    "locationId": batch_locations,
                    "ElementName": "天氣預報綜合描述,最高體感溫度,最低體感溫度",
                    "format": "JSON"                
                }
                
                try:
                    # 使用 F-D0047-093 端點取得一週預報
                    batch_result = self._make_request(endpoint, params)
                    
                    # 檢查資料結構
                    locations_data = batch_result.get("records", {}).get("Locations", [])
                    if not locations_data:
                        self.logger.error(f"批次 {current_batch} 無有效資料")
                        continue

                    # # 統計鄉鎮數量
                    # for location in locations_data:
                    #     for sub_location in location.get("Location", []):
                    #         location_name = sub_location.get("LocationName")
                    #         if location_name:
                    #             location_count[location_name] = location_count.get(location_name, 0) + 1
                    #             total_locations += 1

                    merged_result["records"]["locations"].extend(locations_data)
                    self.logger.info(f"成功處理第 {current_batch} 批次的資料")

                    if current_batch < total_batches:
                        time.sleep(1.5)  # 最後一批次不需要等待
                        
                except Exception as e:
                    self.logger.error(f"一週預報批次 {current_batch} 處理失敗: {e}")
                    continue

            # self.logger.info(f"總鄉鎮數量: {total_locations}")
            # self.logger.info(f"不重複鄉鎮數量: {len(location_count)}")
            # self.logger.info("各鄉鎮出現次數:")
            # for name, count in sorted(location_count.items()):
            #     self.logger.info(f"{name}: {count}")
                    
            return merged_result
        
        except Exception as e:
            self.logger.error(f"取得一週天氣預報時發生錯誤: {e}")
            raise
    
    def get_observation_data(self, stations: List[str] = None) -> Dict:
        """
        Get weather observation data from weather stations
        
        Args:
            stations: List of station IDs to get data for,
                      if None will get data for all stations
        
        Returns:
            Dictionary containing observation data
        """
        # O-A0001-001 is the data ID for weather station observations
        endpoint = "O-A0001-001"
        params = {

            "GeoInfo": "Coordinates",
            
        }
        
        if stations:
            params["stationId"] = ",".join(stations)
            
        return self._make_request(endpoint, params)
    
    def get_uv_index(self, stations: List[str] = None) -> Dict:
        """
        取得紫外線觀測資料
        
        參數:
            stations: 觀測站 ID 列表，若為 None 則取得所有觀測站資料
        
        返回:
            Dict: 包含紫外線觀測資料的字典
        """
        try:
            # 設定基本參數
            params = {
                "GeoInfo": "Coordinates",
                "WeatherElement": "UVIndex"
            }
            
            # 如果有指定觀測站，加入參數
            if stations:
                params["stationId"] = ",".join(stations)
                
            # 使用 O-A0003-001 端點取得資料
            result = self._make_request("O-A0003-001", params)
            
            self.logger.info("成功獲取紫外線資料")
            return result
            
        except Exception as e:
            self.logger.error(f"取得紫外線資料時發生錯誤: {e}")
            raise

    def get_air_quality(self) -> Dict:
        """
        從環境部取得空氣品質資料
        
        返回:
            Dict: 包含空氣品質資料的字典
        """
        try:
            params = {
                "fields": "sitename,county,aqi,pollutant,status,so2,co,o3,o3_8hr,pm10,pm2.5,no2,nox,no,wind_speed,wind_direc,publishtime,co_8hr,pm2.5_avg,pm10_avg,so2_avg,longitude,latitude,siteid"
            }
            
            result = self._make_request("", params, api_type='monev')
            self.logger.info("成功獲取空氣品質資料")
            return result
            
        except Exception as e:
            self.logger.error(f"取得空氣品質資料時發生錯誤: {e}")
            raise
    def get_radar_rainfall_json(self) -> dict:
        """
        從氣象署取得雷達降雨預報 JSON 資料

        返回:
            dict: 雷達降雨預報的 JSON 資料
        """
        try:
            url = "https://opendata.cwa.gov.tw/fileapi/v1/opendataapi/F-B0046-001"
            params = {
                "Authorization": self.cwa_api_key,
                "downloadType": "WEB",
                "format": "JSON"
            }
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            self.logger.info("成功獲取雷達降雨預報 JSON")
            return response.json()
        except Exception as e:
            self.logger.error(f"取得雷達降雨預報 JSON 時發生錯誤: {e}")
            raise
        
    def get_radar_echo(self) -> Dict:
        """
        從 NCDR 獲取雷達迴波資料
        
        返回:
            包含雷達迴波資料的字典
        """
        try:
            # 使用 MaxDBZPic 端點
            endpoint = "MaxDBZ"
            
            # 發送請求到 NCDR API
            result = self._make_request(endpoint, api_type='ncdr')
            
            self.logger.info("成功獲取雷達迴波資料")
            return result
            
        except Exception as e:
            self.logger.error(f"獲取雷達迴波資料時發生錯誤: {e}")
            raise
    
    def get_sunrise_sunset_data(self) -> Dict:
        """
        從氣象署獲取各縣市的日出日落資料
        
        返回:
            Dict: 包含日出日落時間的字典
        """
        try:
            # 取得當天日期
            today = datetime.datetime.now().strftime("%Y-%m-%d")
            
            # 設定端點和參數
            endpoint = "A-B0062-001"
            params = {
                "Date": today,
                "WeatherElement": "SunRiseTime,SunSetTime"
            }
            
            # 發送請求
            result = self._make_request(endpoint, params)
            
            if result.get("success") == "true":
                self.logger.info("成功獲取日出日落資料")
                return result
            else:
                raise Exception("API 回傳失敗狀態")
            
        except Exception as e:
            self.logger.error(f"獲取日出日落資料時發生錯誤: {e}")
            raise
    
    def get_moonrise_moonset_data(self) -> Dict:
        """
        從氣象署獲取各縣市的月出月落資料
        
        返回:
            Dict: 包含月出月落時間的字典
        """
        try:
            # 取得當天日期
            today = datetime.datetime.now().strftime("%Y-%m-%d")
            
            # 設定端點和參數
            endpoint = "A-B0063-001"
            params = {
                "Date": today,
                "WeatherElement": "MoonRiseTime,MoonSetTime"
            }
            
            # 發送請求
            result = self._make_request(endpoint, params)
            
            if result.get("success") == "true":
                self.logger.info("成功獲取月出月落資料")
                return result
            else:
                raise Exception("API 回傳失敗狀態")
            
        except Exception as e:
            self.logger.error(f"獲取月出月落資料時發生錯誤: {e}")
            raise

    def test_connection(self, api_type: Optional[str] = None) -> Dict[str, bool]:
        """
        測試與氣象局 API 的連線狀態
        
        返回:
            Dict[str, bool]: 各 API 端點的連線狀態
        """
        status = {
            'cwa': False,
            'ncdr': False,
            'monev': False
        }
        
        if api_type:
            status.update(self._test_single_api(api_type))
        
        self.logger.info(f"API 連線測試結果: {status}")
        return status

    def _test_single_api(self, api_type: str) -> Dict[str, bool]:

        try:
            if api_type == 'cwa':
                response = requests.get(
                    f"{self.cwa_base_url}/F-C0032-001",
                    params={
                        "Authorization": self.cwa_api_key,
                        "limit": "1",
                        "format": "JSON"
                        }
                    )
                # 檢查狀態碼，如果不是 200，記錄下來
                if response.status_code != 200:
                    self.logger.warning(f"CWA API 連線測試失敗，狀態碼: {response.status_code}")
                    return {api_type: False}
                return {api_type: True}

            elif api_type == 'ncdr':
                response = requests.get(
                    f"{self.ncdr_base_url}/MaxDBZPic",
                    headers={'Token': self.ncdr_api_key},
                    params={"format": "csv"}
                    )
                if response.status_code != 200:
                    self.logger.warning(f"NCDR API 連線測試失敗，狀態碼: {response.status_code}")
                    return {api_type: False}
                return {api_type: True}

            elif api_type == 'monev':
                response = requests.get(
                    f"{self.monev_base_url}",
                    params={
                        "api_key": self.monev_api_key,
                        "format": "JSON",
                        "limit": "1"
                    }
                )
                if response.status_code != 200:
                    self.logger.warning(f"MONEV API 連線測試失敗，狀態碼: {response.status_code}")
                    return {api_type: False}
                return {api_type: True}

            else:
                # 對於不支援的類型，也回傳 False
                self.logger.error(f"不支援的 API 類型進行連線測試: {api_type}")
                return {api_type: False}
            
        except Exception as e:
            self.logger.error(f"{api_type.upper()} API 連線測試失敗: {e}")
            return {api_type: False}

    def get_alerts(self) -> Dict:
        """
        從 NCDR 獲取示警資訊
        
        返回:
            Dict: 包含示警資訊的字典
        """
        try:
            # 設定 NCDR 示警資訊 API 端點
            url = self.alert_base_url
            
            # 發送請求到 NCDR API
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            # 解析回應
            data = response.json()
            
            # 過濾只要中央氣象署和水利署的資料
            filtered_entries = []
            target_authors = ["中央氣象署", "水利署"]
            
            for entry in data.get('entry', []):
                if entry.get('author', {}).get('name') in target_authors:
                    filtered_entries.append({
                        'id': entry.get('id'),
                        'title': entry.get('title'),
                        'updated': entry.get('updated'),
                        'author': entry.get('author', {}).get('name'),
                        'summary': entry.get('summary', {}).get('#text'),
                        'category': entry.get('category', {}).get('@term'),
                        'timestamp': datetime.datetime.now().timestamp()
                    })
            
            self.logger.info("成功獲取示警資訊")
            return filtered_entries
            
        except Exception as e:
            self.logger.error(f"獲取示警資訊時發生錯誤: {e}")
            raise