"""_summary_
    This is where production code will be written.
"""

import datetime
import random
import os
import uuid
from confluent_kafka import SerializingProducer
import simplejson as json

# Setting the latitude & Longitude for both locations
CITY_A_COORDINATES = {"latitude": 51.5074, "longitude": -0.1278}
CITY_B_COORDINATES = {"latitude": 52.4862, "longitude": -1.8904}

# Calculate movement per increments
LATITUDE_INCREMENT = (CITY_B_COORDINATES['latitude'] 
                      - CITY_A_COORDINATES["latitude"]) / 100
LONGITUDE_INCREMENT = (CITY_B_COORDINATES['longitude'] 
                      - CITY_A_COORDINATES["longitude"]) / 100

# Setting envrionment variables for config or using default variable
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')

random.seed(42)
# Setting start details [time & location]
start_time = datetime.now()
start_location = CITY_A_COORDINATES.copy()

# ---------------- FOR GENERATING VEHICLE DATA -----------------------

def get_next_time():
    """_summary_
        This function is used to generate new timestamp for each movement of the vehicle.
        It increments the current start_time by a random number from 30 - 60 seconds & 
        add to it. 

    Returns:
        It returns the new start_time
    """
    global start_time
    
    start_time += datetime.timedelta(seconds=random.randint(30, 60))
    
    return start_time

# Simulating a moving vehicle
def simulate_vehicle_movement():
    """_summary_
        This function simulates a moving vehicle by increasing the start_loaction's
        longitude & latitude respectively. Although in a real world setting this data
        is meant to be gotten from e.g. IOT devices. This is hardcoded in to enable simulation
        and randomness is also included with a range to avoid anomaly in data generation.

    Returns:
        It returns the new start_location details
    """
    global start_location
    
    # Move towards destination
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT
    
    # Adding randomness to simulate actual road travel
    start_location['latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += random.uniform(-0.0005, 0.0005)
    
    return start_location


# Generating vehicle data
def generate_vehicle_data(device_id):
    """_summary_
        This function gets the newly generated location details & timestamp.
        It uses this new details to generate the car data. 

    Returns:
        It returns the vehicle's data.
    """
    location = simulate_vehicle_movement()
    
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed': random.uniform(10, 40),
        'direction': 'North-East',
        'make': 'BMW',
        'model': 'C500',
        'year': 2024,
        'fuelType': 'Hybrid'
    }


# ---------------- FOR GENERATING GPS DATA -----------------------

def generate_gps_data(device_id, timestamp, vehicle_type='private'):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': timestamp,
        'speed': random.uniform(0, 40),
        'direction': 'North-East',
        'vehicleType': vehicle_type
    }
    
# ---------------- FOR GENERATING TRAFFIC CAMERA DATA -----------------------

def generate_traffic_camera_data(device_id, timestamp, location, camera_id):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'cameraId': camera_id,
        'timestamp': timestamp,
        'location': location,
        'snapshot': 'Base64EncodedString'
    }


# ---------------- FOR GENERATING WEATHER DATA -----------------------
def generate_weather_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'location': location,
        'timestamp': timestamp,
        'temperature' : random.uniform(-5, 26),
        'weatherCondition' : random.choice(['Sunny', 'Cloudy', 'Rain', 'Snow'])
    }
    

# Simulating a journey
def simulate_journey(producer, device_id):
    driving = True
    
    while driving:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])
        traffic_camera_data = generate_traffic_camera_data(device_id, vehicle_data['timestamp'], vehicle_data['location'], 'Camer123')
        weather_data =  generate_weather_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
        
    
    

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