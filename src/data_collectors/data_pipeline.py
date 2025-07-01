import pandas as pd
import requests
from datetime import datetime, timedelta
from nasa_firms import fetch_modis_global_fires
from config.settings import NASA_FIRMS_API_KEY
import sqlite3 as sq
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='app.log'
)
logger = logging.getLogger(__name__)


#ETL format
class WildfireDataPipeline:
    def __init__(self, db_path: str = 'wildfire_data.db'):
        self.db_path = db_path
        self.baseUrlMeteo = 'https://api.open-meteo.com/v1/forecast'
        self.init_database

    def init_database(self):
        conn = sq.connect(self.db_path)
        cursor = conn.cursor()

        #Table for active fires
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
        
        #Table for weather data
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
        #Risk fire calculation table
        cursor.execute('''CREATE TABLE IF NOT EXISTS fire_risk(

                CREATE TABLE IF NOT EXISTS fire_risk (
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

    def extract_coordinates_from_fires(self, fire_df):
            """ extracts latitude and longitude of different fires 
            from inputed dataframe from NASA FIRMS returning -> [(),()] list of tuples """

            coordinates = fire_df[['latitude', 'longitude']].drop_duplicates()

            coord_list = list(zip(coordinates['latitude'], coordinates['longitude']))
            return coord_list
        
    def fetch_weather_data(self,lat:float , lon:float):
            """Gets weather data from API calls to open-meteo according to inputed 
            latittude and longitude float values and returns a dictionnary """

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
            
            try:
                response = requests.get(self.baseUrlMeteo, params=params)
                response.raise_for_status
                data = response.json()
                logger.info(f"Fetched weather data for ({lat}, {lon})")
                return data
            except requests.RequestException as e:
                logger.error(f"Error fetching weather data for ({lat}, {lon}): {e}")
                return {}
    def process_weather_data(self,weather_data, lon: float, lat:float):

            if not weather_data or 'hourly' not in weather_data:
                logger.warning(f"No weather data to process for ({lat}, {lon})")
                return []
            
            hourly = weather_data['hourly'] # key is stored in hourly (all temps, time, etc. are in hourly variable)
            processed_data = []

            try:
                for i in range(len(hourly['time'])):
                    records = {

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
                    processed_data.append(records)

                    logger.info(f"Processed {len(processed_data)} weather records for ({lat}, {lon})")
                    return processed_data


            except (KeyError, IndexError) as e:
                logger.error(f"Error processing weather data: {e}")
                return []

    def calculate_fire_risk(self, weather_record):
            """
        Calculate fire risk based on weather conditions
        Simple algorithm - you can enhance this later
        """
            temp = weather_record.get('temperature', 0)
            humidity = weather_record.get('humidity', 100)
            wind_speed = weather_record.get('wind_speed', 0)
            soil_moisture = weather_record.get('soil_moisture', 1)
            precipitation = weather_record.get('precipitation', 0)
        
            # Simple risk factors (0-1 scale)
            temp_factor = min(max((temp - 10) / 30, 0), 1)  # Risk increases above 10°C
            humidity_factor = max((100 - humidity) / 100, 0)  # Risk increases as humidity decreases
            wind_factor = min(wind_speed / 50, 1)  # Risk increases with wind speed
            soil_factor = max((0.5 - soil_moisture) / 0.5, 0)  # Risk increases as soil dries
            precip_factor = max((1 - precipitation / 10), 0)  # Risk decreases with recent rain
        
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
        
    def save_active_fires(self,fire_df):
            """takes in the raw data of fires from NASA FIRMS and saves it to a sql database """

            if fire_df.empty:
                logger.warning("No fire data to save")
                return
            
            conn = sq.connect(self.db_path)

            try:
                fire_df.to_sql('active_fires', conn, if_exists='append', index=False )
                logger.info(f"Saved {len(fire_df)} fire records")

            except Exception as e:
                logger.error(f"Error saving fire data: {e}")
            
            finally:
                conn.close

    def save_weather_data(self,weather_data):
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

    def save_risk_data(self,risk_records):
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
    def run_pipeline(self, country_code: str = "USA"):
        """
        Main pipeline execution
        """
        logger.info("Starting wildfire data pipeline")
        
        # Step 1: Fetch fire data
        logger.info("Fetching fire data from NASA FIRMS...")
        fire_df = fetch_modis_global_fires(f"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{NASA_FIRMS_API_KEY}/MODIS_NRT/USA/1") 
        
        if fire_df.empty:
            logger.warning("No fire data retrieved, stopping pipeline")
            return
        
        # Step 2: Save fire data
        self.save_active_fires(fire_df)
        
        # Step 3: Extract coordinates for weather data
        coordinates = self.extract_coordinates_from_fires(fire_df)
        
        # Step 4: Fetch and process weather data for each location
        all_weather_records = []
        all_risk_records = []
        
        for i, (lat, lon) in enumerate(coordinates):
            logger.info(f"Processing location {i+1}/{len(coordinates)}: ({lat}, {lon})")
            
            # Fetch weather data
            weather_data = self.fetch_weather_data(lat, lon)
            
            if weather_data:
                # Process weather data
                weather_records = self.process_weather_data(weather_data, lat, lon)
                all_weather_records.extend(weather_records)
                
                # Calculate fire risk for each weather record
                for weather_record in weather_records:
                    risk_record = self.calculate_fire_risk(weather_record)
                    all_risk_records.append(risk_record)
            
            # Add small delay to be nice to the API
            import time
            time.sleep(0.1)
        
        # Step 5: Save weather and risk data
        self.save_weather_data(all_weather_records)
        self.save_risk_data(all_risk_records)
        
        logger.info("Pipeline completed successfully")
        logger.info(f"Processed {len(fire_df)} fires, {len(all_weather_records)} weather records, {len(all_risk_records)} risk calculations")


if __name__ == '__main__':
    pipeline = WildfireDataPipeline()
    
    # Run the complete pipeline
    pipeline.run_pipeline()
    base_url_nasa = f"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{NASA_FIRMS_API_KEY}/MODIS_NRT/USA/1"
