# weather_backend/functions/utils/data_processing.py
import json
import csv
from io import StringIO
from typing import Dict, Any, List, Optional
import datetime
import logging

logger = logging.getLogger(__name__)

def extract_three_hour_forecast(raw_data: Dict) -> List[Dict]:
    """
    從氣象署API回應中擷取並轉換三小時預報資料
    
    參數:
        raw_data: 氣象署的原始API回應
    """
    processed_data = []
    
    try:

        # logger.info(f"原始資料類型: {type(raw_data)}")
        # logger.info(f"原始資料結構: {json.dumps(raw_data, indent=2, ensure_ascii=False)[:5000]}...")  # 只顯示前500字元
        # # 確保資料結構正確
        # if not isinstance(raw_data, dict):
        #     logger.info(f"原始資料不是字典類型: {type(raw_data)}")
        #     return []

        locations = raw_data.get('records', {}).get('locations', [])
        # locations = raw_data.get('records', {}).get('Locations', [])

        if not locations:
            logger.info("找不到 locations 資料")
            return []
        
        for location in locations:
            county_name = location.get('LocationsName')
            for town in location.get('Location', []):
                town_data = {
                    'countyName': county_name,
                    'townName': town.get('LocationName'),
                    'geocode': town.get('Geocode'),
                    'latitude': float(town.get('Latitude', 0)),
                    'longitude': float(town.get('Longitude', 0)),
                    'forecasts': []
                }

                # 先建立每個時段的預報 dict（以天氣預報綜合描述為主）
                time_map = {}
                for element in town.get('WeatherElement', []):
                    if element.get('ElementName') == '天氣預報綜合描述':
                        for time_period in element.get('Time', []):
                            start_time = time_period.get('StartTime')
                            end_time = time_period.get('EndTime')
                            
                            # 轉換時間格式
                            start_timestamp = datetime.datetime.fromisoformat(
                                start_time.replace('+08:00', '')
                            ).timestamp()
                            
                            forecast = {
                                'startTime': start_time,
                                'endTime': end_time,
                                'timestamp': start_timestamp
                            }
                            
                            # 解析天氣描述
                            description = time_period.get('ElementValue', [{}])[0].get('WeatherDescription', '')
                            weather_info = parse_weather_description(description)
                            forecast.update(weather_info)
                            # 以 startTime 當 key
                            time_map[start_time] = forecast

                # 體感溫度（apparent_temperature）
                for element in town.get('WeatherElement', []):
                    if element.get('ElementName') == '體感溫度':
                        # print(f"[DEBUG] 體感溫度 element: {element}")
                        for idx, time_period in enumerate(element.get('Time', [])):
                            data_time = time_period.get('DataTime')
                            value = None
                            # print(f"[DEBUG] 體感溫度 time_period: {time_period}")
                            # 取第一個 ElementValue 的 value
                            if time_period.get('ElementValue'):
                                value = time_period['ElementValue'][0].get('ApparentTemperature')
                            # print(f"[DEBUG] 體感溫度 start_time: {data_time}, value: {value}, in time_map: {start_time in time_map}")
                            # 寫入對應時段
                            if data_time in time_map:
                                time_map[data_time]['apparent_temperature'] = float(value) if value is not None else None

                # 將所有時段預報加入 town_data
                town_data['forecasts'] = list(time_map.values())
                processed_data.append(town_data)
        
        return processed_data
        
    except Exception as e:
        logger.error(f"處理三小時預報資料時發生錯誤: {e}")
        raise

def parse_weather_description(description: str) -> Dict:
    """
    解析天氣預報綜合描述文字
    
    參數:
        description: 天氣預報綜合描述文字
    返回:
        解析後的天氣資訊字典
    """
    data = {}
    try:
        # 分割描述文字
        parts = description.split('。')

        # 提取天氣現象
        if parts and parts[0]:
            data['weather'] = parts[0].strip()
                   
        
        for part in parts:
            if not part:
                continue
                
            if '降雨機率' in part:
                # 提取降雨機率，保留原始格式 (包含 %)
                prob = part.split('降雨機率')[1]
                data['rainProb'] = prob.strip()
                data['comfort'] = parts[3].strip()
            elif '溫度攝氏' in part:
                # 提取溫度範圍
                temp = part.split('溫度攝氏')[1].split('度')[0]
                if '至' in temp:
                    min_temp, max_temp = temp.split('至')
                    data['minTemp'] = float(min_temp)
                    data['maxTemp'] = float(max_temp)
                else:
                    # 單一溫度值
                    data['Temp'] = float(temp)
            elif '風' in part:
                # 處理風向和風速資料
                try:
                    # 提取風向 (例如: "偏北")
                    direction_end = part.find('風')
                    if direction_end > 0:
                        data['windDirection'] = part[:direction_end].strip()
                    
                    # 提取風速級數
                    if '風速' in part:
                        speed_text = part.split('風速')[1]
                        if '-' in speed_text:  # 處理範圍格式 (例如: 4-5級)
                            speed_range = speed_text.split('級')[0].strip()
                            max_speed = speed_range.split('-')[1]
                            data['windSpeed'] = int(max_speed)
                        else:  # 處理單一數值 (例如: 3級)
                            speed = ''.join(filter(str.isdigit, speed_text.split('級')[0]))
                            if speed:
                                data['windSpeed'] = int(speed)

                except ValueError as e:
                    logger.error(f"解析風速資料時發生錯誤: {e}")
            elif '相對濕度' in part:
                # 提取相對濕度
                humidity = part.split('相對濕度')[1]
                if '至' in humidity:
                    data['humidity'] = humidity.split('至')[1].strip()
                else:
                    data['humidity'] = humidity.strip()

        # 如果沒有找到降雨機率，體感在第三個部分
        if 'rainProb' not in data and len(parts) > 2:
            data['comfort'] = parts[2].strip()
                
        return data
    except Exception as e:
        logger.error(f"解析天氣描述時發生錯誤: {e}")
        return {}

def extract_weekly_forecast(raw_data: Dict) -> List[Dict]:
    """
    從氣象署API回應中擷取並轉換每週預報資料
    """
    processed_data = []
    
    try:
        locations = raw_data.get('records', {}).get('locations', [])
        
        for location in locations:
            county_name = location.get('LocationsName')
            
            for town in location.get('Location', []):
                town_data = {
                    'countyName': county_name,
                    'townName': town.get('LocationName'),
                    'geocode': town.get('Geocode'),
                    'latitude': float(town.get('Latitude', 0)),
                    'longitude': float(town.get('Longitude', 0)),
                    'forecasts': []
                }

                # 先建立每個時段的預報 dict（以天氣預報綜合描述為主）
                time_map = {}

                for element in town.get('WeatherElement', []):
                    if element.get('ElementName') == '天氣預報綜合描述':
                        for time_period in element.get('Time', []):
                            start_time = time_period.get('StartTime')
                            end_time = time_period.get('EndTime')
                            start_timestamp = datetime.datetime.fromisoformat(
                                start_time.replace('+08:00', '')
                            ).timestamp()

                            forecast = {
                                'startTime': start_time,
                                'endTime': end_time,
                                'timestamp': start_timestamp
                            }
                            
                            # 解析天氣描述
                            description = time_period.get('ElementValue', [{}])[0].get('WeatherDescription', '')
                            weather_data = parse_weather_description(description)
                            forecast.update(weather_data)
                            time_map[start_time] = forecast

                # 最高體感溫度
                for element in town.get('WeatherElement', []):
                    if element.get('ElementName') == '最高體感溫度':
                        for time_period in element.get('Time', []):
                            start_time = time_period.get('StartTime')
                            value = None
                            if time_period.get('ElementValue'):
                                value = time_period['ElementValue'][0].get('MaxApparentTemperature')
                            if start_time in time_map:
                                time_map[start_time]['max_apparent_temperature'] = float(value) if value is not None else None

                # 最低體感溫度
                for element in town.get('WeatherElement', []):
                    if element.get('ElementName') == '最低體感溫度':
                        for time_period in element.get('Time', []):
                            start_time = time_period.get('StartTime')
                            value = None
                            if time_period.get('ElementValue'):
                                value = time_period['ElementValue'][0].get('MinApparentTemperature')
                            if start_time in time_map:
                                time_map[start_time]['min_apparent_temperature'] = float(value) if value is not None else None

                town_data['forecasts'] = list(time_map.values())
                processed_data.append(town_data)
        
        return processed_data
        
    except Exception as e:
        logger.error(f"處理每週預報資料時發生錯誤: {e}")
        raise

def extract_observation_data(raw_data: Dict) -> List[Dict]:
    """
    從原始API回應提取觀測資料
    
    參數:
        raw_data: 原始API回應資料
        
    返回:
        處理後的觀測資料列表
    """
    processed_data = []
    
    try:
        stations = raw_data.get('records', {}).get('Station', [])
        
        for station in stations:
            # 取得 WGS84 座標
            coordinates = next(
                (coord for coord in station['GeoInfo']['Coordinates'] 
                 if coord['CoordinateName'] == 'WGS84'), 
                None
            )
            
            if not coordinates:
                continue
                
            weather_element = station['WeatherElement']
            daily_extreme = weather_element.get('DailyExtreme', {})
            
            station_data = {
                'stationId': station['StationId'],
                'stationName': station['StationName'],
                'latitude': coordinates['StationLatitude'],
                'longitude': coordinates['StationLongitude'],
                'timestamp': datetime.datetime.fromisoformat(station['ObsTime']['DateTime']).timestamp(),
                'observations': {
                    'weather': weather_element.get('Weather'),
                    'temperature': weather_element.get('AirTemperature'),
                    'humidity': weather_element.get('RelativeHumidity'),
                    'pressure': weather_element.get('AirPressure'),
                    'precipitation': weather_element.get('Now', {}).get('Precipitation'),
                    'windDirection': weather_element.get('WindDirection'),
                    'windSpeed': weather_element.get('WindSpeed'),
                    'dailyHigh': {
                        'temperature': daily_extreme.get('DailyHigh', {})
                            .get('TemperatureInfo', {})
                            .get('AirTemperature'),
                        'time': daily_extreme.get('DailyHigh', {})
                            .get('TemperatureInfo', {})
                            .get('Occurred_at', {})
                            .get('DateTime')
                    },
                    'dailyLow': {
                        'temperature': daily_extreme.get('DailyLow', {})
                            .get('TemperatureInfo', {})
                            .get('AirTemperature'),
                        'time': daily_extreme.get('DailyLow', {})
                            .get('TemperatureInfo', {})
                            .get('Occurred_at', {})
                            .get('DateTime')
                    }
                }
            }
            
            processed_data.append(station_data)
            
        return processed_data
        
    except Exception as e:
        logger.error(f"處理觀測資料時發生錯誤: {e}")
        raise

def extract_radar_rainfall(cwa_data: dict) -> dict:
    """
    從氣象署API回應中擷取並轉換雷達回波資料。
    (此版本已修正 KeyError: 'parameter' 的問題)
    
    參數:
        cwa_data (dict): Json格式的雷達外延降雨預報。
        logger (logging.Logger): 用於記錄資訊和錯誤的日誌記錄器實例。
        
    返回:
        dict: 包含 metadata 和聚合後雨量網格的優化後資料。
    """

    logger.info("開始處理雷達降雨預測資料")
    
    try:
        # 1. 提取核心資料
        dataset = cwa_data['cwaopendata']['dataset']
        rainfall_content_str = dataset['contents']['content']
        # 將以逗號分隔的字串轉換為浮點數列表
        original_data = [float(val) for val in rainfall_content_str.split(',')]

        # 2. 動態讀取網格參數 (*** 已修正此處邏輯 ***)
        # 直接獲取 parameterSet 字典，因為它就是參數的集合
        params = dataset['datasetInfo']['parameterSet']

        start_lon = float(params.get('StartPointLongitude', 118.0))
        start_lat = float(params.get('StartPointLatitude', 20.0))
        res_lon = float(params.get('GridResolution', 0.0125))
        res_lat = res_lon  # 假設網格為正方形
        grid_dim_x = int(params.get('GridDimensionX', 441))
        grid_dim_y = int(params.get('GridDimensionY', 561))

        # 3. 設定聚合參數並計算新維度
        FACTOR = 4  # 聚合比例 4x4 -> 1x1
        new_dim_x = grid_dim_x // FACTOR
        new_dim_y = grid_dim_y // FACTOR
        
        aggregated_rainfall = [0.0] * (new_dim_x * new_dim_y)

        # 4. 執行聚合計算
        for y in range(new_dim_y):
            for x in range(new_dim_x):
                grid_values = []
                # 遍歷原始的 4x4 網格區域
                for y_offset in range(FACTOR):
                    for x_offset in range(FACTOR):
                        original_x = x * FACTOR + x_offset
                        original_y = y * FACTOR + y_offset
                        
                        # 確保索引不會超出原始網格範圍
                        if original_x < grid_dim_x and original_y < grid_dim_y:
                            original_idx = original_y * grid_dim_x + original_x
                            # -99.0 或 -999.0 都是無效值
                            if original_data[original_idx] > -99.0:
                                grid_values.append(original_data[original_idx])
                
                # 計算平均值，如果區域內沒有有效值，則為 0.0
                avg_rainfall = sum(grid_values) / len(grid_values) if grid_values else 0.0
                
                # 將計算結果存入新的一維陣列
                new_idx = y * new_dim_x + x
                aggregated_rainfall[new_idx] = round(avg_rainfall, 2)

        # 5. 組合最終輸出的資料格式
        output_data = {
            "metadata": {
                "start_lon": start_lon,
                "start_lat": start_lat,
                "res_lon": res_lon * FACTOR,  # 更新為聚合後的解析度
                "res_lat": res_lat * FACTOR,
                "dim_x": new_dim_x,
                "dim_y": new_dim_y,
                # 使用 UTC 時間並符合 ISO 8601 標準
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
            },
            "rainfall_grid": aggregated_rainfall
        }
        
        logger.info(f"資料聚合完成。新網格維度: {new_dim_x}x{new_dim_y}")
        return output_data

    except KeyError as e:
        logger.error(f"處理資料時找不到預期的鍵 (Key): {e}，請檢查 API 回傳的資料結構是否已變更。")
        raise
    except Exception as e:
        logger.error(f"聚合資料時發生未預期的錯誤: {e}")
        raise

def extract_uv_data(raw_data: Dict) -> List[Dict]:
    """
    從氣象署API回應中擷取紫外線指數資料
    
    參數:
        raw_data: 氣象署的原始API回應
        
    返回:
        List[Dict]: 處理後的紫外線資料列表
    """
    processed_data = []
    
    try:
        stations = raw_data.get('records', {}).get('Station', [])
        
        for station in stations:
            # 取得 WGS84 座標
            coordinates = next(
                (coord for coord in station['GeoInfo']['Coordinates'] 
                 if coord['CoordinateName'] == 'WGS84'), 
                None
            )
            
            if not coordinates:
                continue
                
            uv_index = int(station['WeatherElement'].get('UVIndex', '-99'))
            
            # 只處理有效的紫外線指數
            if uv_index == -99:
                continue
                
            station_data = {
                'stationId': station['StationId'],
                'stationName': station['StationName'],
                'latitude': float(coordinates['StationLatitude']),
                'longitude': float(coordinates['StationLongitude']),
                'uvIndex': uv_index,
                'timestamp': datetime.datetime.fromisoformat(station['ObsTime']['DateTime']).timestamp()
            }
            
            processed_data.append(station_data)
            
    except Exception as e:
        logger.error(f"處理紫外線資料時發生錯誤: {e}")
        raise
        
    return processed_data

def extract_air_quality_data(raw_data: Dict) -> List[Dict]:
    """
    處理環境部空氣品質資料
    
    參數:
        raw_data: 原始 API 回應
        
    返回:
        List[Dict]: 處理後的空氣品質資料列表
    """
    processed_data = []

    def _safe_cast(value: Any, cast_type: type, default: Any = -99):
        """安全地將值轉換為指定類型，處理空值或無效值。"""
        if value is None or value == '' or value == '--':
            return default
        try:
            return cast_type(value)
        except (ValueError, TypeError):
            return default

    try:
        if not raw_data:
            logger.error("收到空的 API 回應")
            return processed_data
        
        stations = raw_data.get('records', [])
        
        for station in stations:
            # 使用 _safe_cast 安全地轉換所有數值型資料
            aqi = _safe_cast(station.get('aqi'), int)
            so2 = _safe_cast(station.get('so2'), float)
            co = _safe_cast(station.get('co'), float)
            o3 = _safe_cast(station.get('o3'), float)
            o3_8hr = _safe_cast(station.get('o3_8hr'), float)
            pm10 = _safe_cast(station.get('pm10'), float)
            pm2_5 = _safe_cast(station.get('pm2.5'), float)
            no2 = _safe_cast(station.get('no2'), float)
            latitude = _safe_cast(station.get('latitude'), float, 0)
            longitude = _safe_cast(station.get('longitude'), float, 0)

            station_data = {
                'stationId': station.get('siteid'),
                'stationName': station.get('sitename'),
                'county': station.get('county'),
                'location': {
                    'latitude': latitude,
                    'longitude': longitude
                },
                'measurements': {
                    'aqi': aqi,
                    'status': station.get('status'),
                    'so2': so2,
                    'co': co,
                    'o3': o3,
                    'o3_8hr': o3_8hr,
                    'pm10': pm10,
                    'pm2_5': pm2_5,
                    'no2': no2,
                },
                'publishTime': station.get('publishtime'),
                'timestamp': datetime.datetime.strptime(
                    station.get('publishtime'), 
                    '%Y/%m/%d %H:%M:%S'
                ).timestamp()
            }
            
            processed_data.append(station_data)
            
    except Exception as e:
        logger.error(f"處理空氣品質資料時發生錯誤: {e}")
        raise
        
    return processed_data