import json
import logging
import time
from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer

default_args = {
    'owner': 'samyak',
    'start_date': datetime(2024, 5, 18, 10, 0)
}

def fetch_weather_data():
    try:
        api_url = "https://api.open-meteo.com/v1/forecast?latitude=48.1374,50.1155,52.5244,53.5507&" \
                  "longitude=11.5755,8.6842,13.4105,9.993&hourly=temperature_2m,relative_humidity_2m," \
                  "dew_point_2m,apparent_temperature,precipitation_probability,precipitation,rain," \
                  "showers,snowfall,snow_depth,weather_code,cloud_cover,cloud_cover_low,cloud_cover_mid," \
                  "cloud_cover_high&past_days=92&forecast_days=16"
        response = requests.get(api_url)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            logging.error(f"Failed to fetch weather data. Status code: {response.status_code}")
            return None
    except Exception as e:
        logging.error(f"Failed to fetch weather data: {e}")
        return None

def process_weather_data(data):
    processed_data = []
    forecasts = data.get('forecasts', [])
    for forecast in forecasts:
        location = forecast.get('location', {})
        latitude = location.get('latitude')
        longitude = location.get('longitude')
        hourly_forecasts = forecast.get('hours', [])
        for hourly_forecast in hourly_forecasts:
            timestamp = hourly_forecast.get('timestamp')
            temperature = hourly_forecast.get('temperature_2m')
            humidity = hourly_forecast.get('relative_humidity_2m')
            processed_entry = {
                'latitude': latitude,
                'longitude': longitude,
                'timestamp': timestamp,
                'temperature': temperature,
                'humidity': humidity
                # Add more fields as needed
            }
            processed_data.append(processed_entry)
    return processed_data

def stream_weather_data():
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()
    weather_data = fetch_weather_data()
    if weather_data:
        while True:
            if time.time() > curr_time + 60:  # 1 minute
                break
            try:
                processed_data = process_weather_data(weather_data)
                for data_entry in processed_data:
                    producer.send('weather_forecast', json.dumps(data_entry).encode('utf-8'))
            except Exception as e:
                logging.error(f'An error occurred while streaming weather data: {e}')
                continue

with DAG('weather_forecast_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_weather_data_to_kafka',
        python_callable=stream_weather_data
    )
