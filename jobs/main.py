"""_summary_
    This is where production code will be written.
"""

import datetime
import random
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

# Setting start details [time & location]
start_time = datetime.now()
start_location = CITY_A_COORDINATES.copy()


# Simulating a moving vehicle
def simulate_vehicle_movement():
    global start_location
    
    # Move towards destination
    start_location['lattitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT
    
    # Adding randomness to simulate actual road travel
    start_location['lattitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += random.uniform(-0.0005, 0.0005)
    
    return start_location


# Generating vehicle data
def generate_vehicle_data(device_id):
    simulate_vehicle_movement()



# Simulating a journey
def simulate_journey(producer, device_id):
    driving = True
    
    while driving:
        vehicle_data = generate_vehicle_data(device_id)
    
    

if __name__ == "__main__":
    # Setting up producer config
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka error: {err}')
    }
    
    producer = SerializingProducer(producer_config)
    
    try:
        simulate_journey(producer, "My-VehicleNo-123")
    except KeyboardInterrupt:
        print("Simulation ended by the user")
    except Exception as e:
        print(f"An unexpected error occured: {e}")