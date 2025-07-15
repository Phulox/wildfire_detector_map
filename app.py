from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import sqlite3 as sq
import logging
from datetime import datetime, timedelta
import json

app = Flask(__name__, 
            template_folder='web/templates', 
            static_folder='web/static')
CORS(app)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_PATH = './data/database/wildfire_data.db'

def get_db_connect():
    """Get database connection"""
    conn = sq.connect(DB_PATH)
    conn.row_factory = sq.Row #getting each row of the database as a dict like object
    return conn

@app.route('/')
def index():
    return render_template('index.html') #homepage

@app.route('/api/active_fires') 
def get_active_fires():
    """Get all active fires from the last 24 hours"""
    try:
        conn = get_db_connect()

        # Get fires from the last 24 hours
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

        query = '''
            SELECT 
                latitude, longitude, brightness, confidence, 
                acq_date, acq_time, satellite, frp, daynight
            FROM active_fires 
            WHERE acq_date >= ? 
            ORDER BY acq_date DESC, acq_time DESC
        '''
        cursor = conn.execute(query,(yesterday,))
        fires = cursor.fetchall()
        conn.close()

        # Now put in understandable list of dictionnaries format 
        ac_fires = []

        for fire in fires:
            ac_fires.append({'latitude': fire['latitude'],
                'longitude': fire['longitude'],
                'brightness': fire['brightness'],
                'confidence': fire['confidence'],
                'acq_date': fire['acq_date'],
                'acq_time': fire['acq_time'],
                'satellite': fire['satellite'],
                'frp': fire['frp'],
                'daynight': fire['daynight']})
        return jsonify({
            'success': True,
            'data': ac_fires,
            'count': len(ac_fires)})

    except Exception as e:
        logger.error(f"Error fetching active fires: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500 #return json file with details of the error and the error code 500
    
@app.route('/api/fire_risk')

def get_fire_risk():
    try: 
        conn = get_db_connect()

        query = '''
            SELECT 
                latitude, longitude, risk_level, risk_score,
                temperature_factor, humidity_factor, wind_factor,
                soil_factor, calculation_date
            FROM fire_risk
            WHERE calculation_date >= datetime('now', '-24 hours')
            ORDER BY calculation_date DESC
        '''
        cursor = conn.execute(query)
        risks = cursor.fetchall()
        conn.close()

        # Convert to list of dictionaries
        risks_list = []
        for risk in risks:
            risks_list.append({
                'latitude': risk['latitude'],
                'longitude': risk['longitude'],
                'risk_level': risk['risk_level'],
                'risk_score': risk['risk_score'],
                'temperature_factor': risk['temperature_factor'],
                'humidity_factor': risk['humidity_factor'],
                'wind_factor': risk['wind_factor'],
                'soil_factor': risk['soil_factor'],
                'calculation_date': risk['calculation_date']
            })
        return jsonify({
            'success': True,
            'data': risks_list,
            'count': len(risks_list)
        })

        
    except Exception as e:
        logger.error(f"Error fetching active fires: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500 #return json file with details of the error and the error code 500

@app.route('/api/weather/<float:lat>/<float:lon>')
def get_weather_for_location(lat, lon):
    """Get weather data for specific location"""

    try:
        conn = get_db_connect()
        query = '''
            SELECT 
                temperature, humidity, wind_speed, wind_direction,
                soil_temperature, soil_moisture, precipitation,
                weather_datetime
            FROM weather_data 
            WHERE latitude BETWEEN ? AND ? 
            AND longitude BETWEEN ? AND ?
            AND weather_datetime >= datetime('now', '-24 hours')
            ORDER BY weather_datetime DESC
            LIMIT 24
        '''
        #User will get approximated coordinate location of the displayed weather data
        lat_min, lat_max = lat - 0.1, lat + 0.1 
        lon_min, lon_max = lon - 0.1, lon + 0.1

        cursor = conn.execute(query,(lat_min,lat_max,lon_min,lon_max))
        weather_data = cursor.fetchall()
        conn.close()

        weather_list = []
        for weather in weather_data:
            weather_list.append({
                'temperature': weather['temperature'],
                'humidity': weather['humidity'],
                'wind_speed': weather['wind_speed'],
                'wind_direction': weather['wind_direction'],
                'soil_temperature': weather['soil_temperature'],
                'soil_moisture': weather['soil_moisture'],
                'precipitation': weather['precipitation'],
                'weather_datetime': weather['weather_datetime']
            })
        
        return jsonify({
            'success': True,
            'data': weather_list,
            'count': len(weather_list)
        })


    except Exception as e:
        logger.error(f"Error fetching weather data: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    
@app.route('/api/risk-summary')
def get_risk_summary():
    """Get summary of risk levels"""
    try:
        conn = get_db_connect()
        
        query = '''
            SELECT 
                risk_level,
                COUNT(*) as count,
                AVG(risk_score) as avg_score
            FROM fire_risk 
            WHERE calculation_date >= datetime('now', '-24 hours')
            GROUP BY risk_level
            ORDER BY risk_level
        '''
        
        cursor = conn.execute(query)
        summary = cursor.fetchall()
        conn.close()
        
        # Convert to list of dictionaries
        summary_list = []
        risk_labels = {1: 'Low', 2: 'Moderate', 3: 'High', 4: 'Very High', 5: 'Extreme'}
        
        for item in summary:
            summary_list.append({
                'risk_level': item['risk_level'],
                'risk_label': risk_labels.get(item['risk_level'], 'Unknown'),
                'count': item['count'],
                'avg_score': round(item['avg_score'], 3)
            })
        
        return jsonify({
            'success': True,
            'data': summary_list
        })
        
    except Exception as e:
        logger.error(f"Error fetching risk summary: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

if __name__ == '__main__':
    app.run(debug=True)