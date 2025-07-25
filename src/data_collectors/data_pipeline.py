import pandas as pd
import requests
from datetime import datetime, timedelta
from nasa_firms import fetch_modis_global_fires
from config.settings import NASA_FIRMS_API_KEY
import sqlite3 as sq
import logging
import time
import socket
from typing import List, Tuple, Dict, Any
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='app.log'
)
logger = logging.getLogger(__name__)


class WildfireDataPipeline:
    def __init__(self, db_path: str = './data/database/wildfire_data.db'):
        self.db_path = db_path
        self.baseUrlMeteo = 'https://api.open-meteo.com/v1/forecast'
        self.api_delay = 1.0  # Increased delay
        self.max_retries = 3
        self.timeout = 30
        self.session = self._create_robust_session()
        self.init_database()

    def _create_robust_session(self):
        """Create a requests session with retry strategy"""
        session = requests.Session()
        
        # Define retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        # Mount adapter with retry strategy
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session

    def test_api_connectivity(self):
        """Test if the Open-Meteo API is accessible"""
        test_params = {
            "latitude": 40.7128,
            "longitude": -74.0060,
            "hourly": ["temperature_2m"],
            "forecast_days": 1,
            "timezone": "UTC"
        }
        
        try:
            logger.info("Testing API connectivity...")
            response = self.session.get(
                self.baseUrlMeteo, 
                params=test_params, 
                timeout=10
            )
            
            if response.status_code == 200:
                logger.info("✅ API connectivity test passed")
                return True
            else:
                logger.error(f"❌ API test failed with status: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"❌ API connectivity test failed: {e}")
            return False

    def init_database(self):
        conn = sq.connect(self.db_path)
        cursor = conn.cursor()

        # Table for active fires
        cursor.execute('''CREATE TABLE IF NOT EXISTS active_fires(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_id TEXT,
                latitude REAL,
                longitude REAL,
                brightness REAL,
                scan REAL,
                track REAL,
                acq_date TEXT,
                acq_time TEXT,
                satellite TEXT,
                instrument TEXT,
                confidence INTEGER,
                version TEXT,
                bright_t31 REAL,
                frp REAL,
                daynight TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(latitude, longitude, acq_date, acq_time, satellite)  
                       )''')
        
        # Table for weather data
        cursor.execute('''CREATE TABLE IF NOT EXISTS weather_data(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                latitude REAL,
                longitude REAL,
                temperature REAL,
                humidity REAL,
                wind_speed REAL,
                wind_direction REAL,
                soil_temperature REAL,
                soil_moisture REAL,
                precipitation REAL,
                weather_datetime TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(latitude, longitude, weather_datetime)
                       )''')
        
        # Risk fire calculation table
        cursor.execute('''CREATE TABLE IF NOT EXISTS fire_risk(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                latitude REAL,
                longitude REAL,
                risk_level INTEGER,
                risk_score REAL,
                temperature_factor REAL,
                humidity_factor REAL,
                wind_factor REAL,
                soil_factor REAL,
                calculation_date TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(latitude, longitude, calculation_date)
                       )''')

        conn.commit()
        conn.close()
        logger.info("Database initialized successfully")

    def extract_coordinates_from_fires(self, fire_df: pd.DataFrame) -> List[Tuple[float, float]]:
        """Extract unique latitude and longitude pairs from fire dataframe"""
        coordinates = fire_df[['latitude', 'longitude']].drop_duplicates()
        coord_list = list(zip(coordinates['latitude'], coordinates['longitude']))
        logger.info(f"Extracted {len(coord_list)} unique coordinates")
        return coord_list
        
    def fetch_weather_data(self, lat: float, lon: float) -> Dict[str, Any]:
        """Fetch weather data with robust error handling"""
        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": [
                "temperature_2m",
                "relative_humidity_2m",
                "wind_speed_10m",
                "wind_direction_10m",
                "soil_temperature_0cm",
                "soil_moisture_0_to_1cm",
                "precipitation"
            ],
            "forecast_days": 3,
            "past_days": 1,
            "timezone": "UTC"
        }
        
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Fetching weather data for ({lat:.4f}, {lon:.4f}), attempt {attempt + 1}")
                
                response = self.session.get(
                    self.baseUrlMeteo, 
                    params=params, 
                    timeout=self.timeout
                )
                
                response.raise_for_status()
                data = response.json()
                
                logger.info(f"✅ Successfully fetched weather data for ({lat:.4f}, {lon:.4f})")
                return data
                
            except requests.exceptions.Timeout:
                logger.warning(f"⏱️ Timeout (>{self.timeout}s) for ({lat:.4f}, {lon:.4f}), attempt {attempt + 1}")
                
            except requests.exceptions.ConnectionError as e:
                logger.warning(f"🔌 Connection error for ({lat:.4f}, {lon:.4f}): {str(e)[:100]}...")
                
            except requests.exceptions.HTTPError as e:
                logger.warning(f"🌐 HTTP error for ({lat:.4f}, {lon:.4f}): {e}")
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"❌ Request error for ({lat:.4f}, {lon:.4f}): {e}")
                
            except Exception as e:
                logger.error(f"💥 Unexpected error for ({lat:.4f}, {lon:.4f}): {e}")
            
            # Wait before retry (exponential backoff)
            if attempt < self.max_retries - 1:
                wait_time = (2 ** attempt) * self.api_delay
                logger.info(f"⏳ Waiting {wait_time:.1f}s before retry...")
                time.sleep(wait_time)
        
        logger.error(f"❌ Failed to fetch weather data for ({lat:.4f}, {lon:.4f}) after {self.max_retries} attempts")
        return {}

    def process_weather_data(self, weather_data: Dict[str, Any], lat: float, lon: float) -> List[Dict[str, Any]]:
        """Process weather data from API response"""
        if not weather_data or 'hourly' not in weather_data:
            logger.warning(f"No weather data to process for ({lat:.4f}, {lon:.4f})")
            return []
        
        hourly = weather_data['hourly']
        processed_data = []

        try:
            for i in range(len(hourly['time'])):
                record = {
                    'latitude': lat,
                    'longitude': lon,
                    'temperature': hourly['temperature_2m'][i],
                    'humidity': hourly['relative_humidity_2m'][i],
                    'wind_speed': hourly['wind_speed_10m'][i],
                    'wind_direction': hourly['wind_direction_10m'][i],
                    'soil_temperature': hourly['soil_temperature_0cm'][i],
                    'soil_moisture': hourly['soil_moisture_0_to_1cm'][i],
                    'precipitation': hourly['precipitation'][i],
                    'weather_datetime': hourly['time'][i]
                }
                processed_data.append(record)

            logger.info(f"Processed {len(processed_data)} weather records for ({lat:.4f}, {lon:.4f})")
            return processed_data

        except (KeyError, IndexError) as e:
            logger.error(f"Error processing weather data: {e}")
            return []

    def calculate_fire_risk(self, weather_record: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate fire risk based on weather conditions"""
        temp = weather_record.get('temperature', 0) or 0
        humidity = weather_record.get('humidity', 100) or 100
        wind_speed = weather_record.get('wind_speed', 0) or 0
        soil_moisture = weather_record.get('soil_moisture', 1) or 1
        precipitation = weather_record.get('precipitation', 0) or 0
    
        # Simple risk factors (0-1 scale)
        temp_factor = min(max((temp - 10) / 30, 0), 1)
        humidity_factor = max((100 - humidity) / 100, 0)
        wind_factor = min(wind_speed / 50, 1)
        soil_factor = max((0.5 - soil_moisture) / 0.5, 0)
        precip_factor = max((1 - precipitation / 10), 0)
    
        # Combined risk score (0-1)
        risk_score = (temp_factor * 0.25 + 
                     humidity_factor * 0.3 + 
                     wind_factor * 0.2 + 
                     soil_factor * 0.15 + 
                     precip_factor * 0.1)
    
        # Convert to 1-5 risk level
        if risk_score < 0.2:
            risk_level = 1  # Low
        elif risk_score < 0.4:
            risk_level = 2  # Moderate
        elif risk_score < 0.6:
            risk_level = 3  # High
        elif risk_score < 0.8:
            risk_level = 4  # Very High
        else:
            risk_level = 5  # Extreme
    
        return {
            'latitude': weather_record['latitude'],
            'longitude': weather_record['longitude'],
            'risk_level': risk_level,
            'risk_score': risk_score,
            'temperature_factor': temp_factor,
            'humidity_factor': humidity_factor,
            'wind_factor': wind_factor,
            'soil_factor': soil_factor,
            'calculation_date': weather_record['weather_datetime']
        }
        
    def save_active_fires(self, fire_df: pd.DataFrame):
        """Save fire data to database"""
        if fire_df.empty:
            logger.warning("No fire data to save")
            return
        
        conn = sq.connect(self.db_path)
        try:
            fire_df.to_sql('active_fires', conn, if_exists='append', index=False)
            logger.info(f"Saved {len(fire_df)} fire records")
        except Exception as e:
            logger.error(f"Error saving fire data: {e}")
        finally:
            conn.close()

    def save_weather_data(self, weather_data: List[Dict[str, Any]]):
        """Save weather data to database"""
        if not weather_data:
            logger.warning("No weather data to save")
            return
        
        conn = sq.connect(self.db_path)
        try:
            df = pd.DataFrame(weather_data)
            df.to_sql('weather_data', conn, if_exists='append', index=False)
            logger.info(f"Saved {len(weather_data)} weather records")
        except Exception as e:
            logger.error(f"Error saving weather data: {e}")
        finally:
            conn.close()

    def save_risk_data(self, risk_records: List[Dict[str, Any]]):
        """Save risk calculation data to database"""
        if not risk_records:
            logger.warning("No risk data to save")
            return
            
        conn = sq.connect(self.db_path)
        try:
            df = pd.DataFrame(risk_records)
            df.to_sql('fire_risk', conn, if_exists='append', index=False)
            logger.info(f"Saved {len(risk_records)} risk calculations")
        except Exception as e:
            logger.error(f"Error saving risk data: {e}")
        finally:
            conn.close()

    def run_pipeline(self, country_code: str = "USA", max_locations: int = None):
        """Main pipeline execution with comprehensive error handling"""
        logger.info("🚀 Starting wildfire data pipeline")
        
        # Test API connectivity first
        if not self.test_api_connectivity():
            logger.error("❌ Cannot connect to Open-Meteo API. Please check your internet connection.")
            print("❌ API connectivity test failed. Check your internet connection or try again later.")
            return
        
        try:
            # Step 1: Fetch fire data
            logger.info("🔥 Fetching fire data from NASA FIRMS...")
            fire_df = fetch_modis_global_fires(
                f"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{NASA_FIRMS_API_KEY}/MODIS_NRT/USA/1"
            ) 
            
            if fire_df.empty:
                logger.warning("No fire data retrieved, stopping pipeline")
                return
            
            # Step 2: Save fire data
            self.save_active_fires(fire_df)
            
            # Step 3: Extract coordinates for weather data
            coordinates = self.extract_coordinates_from_fires(fire_df)
            
            # Limit coordinates for testing if specified
            if max_locations:
                coordinates = coordinates[:max_locations]
                logger.info(f"Limited to first {max_locations} locations for testing")
            
            # Step 4: Fetch and process weather data for each location
            all_weather_records = []
            all_risk_records = []
            successful_requests = 0
            failed_requests = 0
            
            for i, (lat, lon) in enumerate(coordinates):
                logger.info(f"🌍 Processing location {i+1}/{len(coordinates)}: ({lat:.4f}, {lon:.4f})")
                
                try:
                    # Fetch weather data
                    weather_data = self.fetch_weather_data(lat, lon)
                    
                    if weather_data:
                        successful_requests += 1
                        # Process weather data
                        weather_records = self.process_weather_data(weather_data, lat, lon)
                        all_weather_records.extend(weather_records)
                        
                        # Calculate fire risk for each weather record
                        for weather_record in weather_records:
                            try:
                                risk_record = self.calculate_fire_risk(weather_record)
                                all_risk_records.append(risk_record)
                            except Exception as e:
                                logger.error(f"Error calculating risk for record: {e}")
                    else:
                        failed_requests += 1
                    
                    # Add delay between API calls
                    time.sleep(self.api_delay)
                    
                except KeyboardInterrupt:
                    logger.info("⏹️ Pipeline interrupted by user")
                    break
                except Exception as e:
                    failed_requests += 1
                    logger.error(f"Error processing location ({lat:.4f}, {lon:.4f}): {e}")
                    continue
            
            # Step 5: Save weather and risk data
            if all_weather_records:
                self.save_weather_data(all_weather_records)
            if all_risk_records:
                self.save_risk_data(all_risk_records)
            
            # Final summary
            logger.info("✅ Pipeline completed successfully")
            logger.info(f"📊 Summary: {len(fire_df)} fires, {successful_requests} successful weather requests, {failed_requests} failed requests")
            logger.info(f"💾 Saved: {len(all_weather_records)} weather records, {len(all_risk_records)} risk calculations")
            
            print(f"\n✅ Pipeline completed!")
            print(f"📊 Processed {len(fire_df)} fires")
            print(f"🌤️ Weather API: {successful_requests} successful, {failed_requests} failed")
            print(f"💾 Saved: {len(all_weather_records)} weather records, {len(all_risk_records)} risk calculations")
            
        except KeyboardInterrupt:
            logger.info("⏹️ Pipeline interrupted by user")
            print("\n⏹️ Pipeline stopped by user")
        except Exception as e:
            logger.error(f"💥 Pipeline failed with error: {e}")
            print(f"\n❌ Pipeline failed: {e}")
            raise


if __name__ == '__main__':
    pipeline = WildfireDataPipeline()
    
    # Run with limited locations for testing
    pipeline.run_pipeline(max_locations=3)  # Start with just 3 locations