"""_summary_
    This is where production code will be written.
"""

import datetime
import random
import os
import time
import uuid
from confluent_kafka import SerializingProducer
import simplejson as json

# Setting the latitude & Longitude for both locations
# These data should be gotten from Users input   <------------ TODO
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

random.seed(50)
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
    # These data should be gotten from Users input   <------------ TODO
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
    # These data should be gotten from Users input   <------------ TODO
    """_summary_
    Returns:
        It returns the vehicle's generated GPS data.
    """
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
    # These data should be gotten from an API   <------------ TODO
    """_summary_
    Returns:
        It returns the traffic camera data.
    """
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
    """_summary_
    Returns:
        It returns the weather data.
    """
    # All these data needs to be gotten from a weather API  <------------ TODO
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'location': location,
        'timestamp': timestamp,
        'temperature' : random.uniform(-5, 26),
        'weatherCondition' : random.choice(['Sunny', 'Cloudy', 'Rain', 'Snow']),
        'precipitation': random.uniform(0, 25),
        'windSpeed': random.uniform(0, 100),
        'humidity': random.randint(0, 100), # in %
        'airQualityIndex': random.uniform(0, 500)
    }
    

# ---------------- FOR GENERATING EMERGENCY DATA -----------------------
def generate_emergency_incident_data(device_id, timestamp, location):
    """_summary_
    Returns:
        It returns the emergency data.
    Limitations:
        Not able to detect the accident type 
    """
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'incidentType': uuid.uuid4(),
        # 'type' : random.choice(['Accident', 'Fire', 'Medical', 'Police', 'None']),
        'type': 'None',
        'location': location,
        'timestamp': timestamp,
        'status':random.choice(['Active', 'Resolved']),
        'description': "Description of incident"
    }


# ---------------- FOR PRODUCING DATA TO KAFKA -----------------------
def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable.")

def deliver_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}].")
    
# Function to produce data to Kafka
def produce_data_to_kafka(producer, topic, data):
    # NB: the data is serialized in order to be produced into Kafka
    producer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery=deliver_report
    )
    
    producer.flush()

# Simulating a journey
def simulate_journey(producer, device_id):
    driving = True
    
    while driving:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])
        traffic_camera_data = generate_traffic_camera_data(device_id, vehicle_data['timestamp'], vehicle_data['location'], 'Camer123')
        weather_data =  generate_weather_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
        emergency_incident_data = generate_emergency_incident_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
    
        if (vehicle_data['location'][0] >= CITY_B_COORDINATES['latitude'] &
            vehicle_data['location'][1] <= CITY_B_COORDINATES['longitude']):
            print("Vehicle has reached it's destination. Ending Simulation")
            break 
        
        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_incident_data)
        
        time.sleep(3)
    

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