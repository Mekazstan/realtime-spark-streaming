"""_summary_
    This is where production code will be written.
"""

import os
from confluent_kafka import SerializingProducer
import simplejson as json

# Setting the Lattitude & Longitude for both locations
CITY_A_COORDINATES = {"lattitude": 51.5074, "longitude": -0.1278}
CITY_B_COORDINATES = {"lattitude": 52.4862, "longitude": -1.8904}

# Calculate movement per increments
LATITUDE_INCREMENT = (CITY_B_COORDINATES['lattitude'] 
                      - CITY_A_COORDINATES["lattitude"]) / 100
LONGITUDE_INCREMENT = (CITY_B_COORDINATES['longitude'] 
                      - CITY_A_COORDINATES["longitude"]) / 100

# Setting envrionment variables for config or using default variable
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')