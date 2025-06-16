import os
import json
from kafka import KafkaProducer
from datetime import datetime,timedelta
from json import dumps
import random
import uuid
import time

start_time = datetime.now()


SD_COORDINATES = {
    "latitude": 32.7157,
    "longitude": -117.1611
}

LA_COORDINATES = {
    "latitude": 34.0522,
    "longitude": -118.2437
}

LATITUDE_INCREMENT = (LA_COORDINATES['latitude'] - SD_COORDINATES['latitude']) / 250
LONGITUDE_INCREMENT = (LA_COORDINATES['longitude'] - SD_COORDINATES['longitude']) / 250

start_location = SD_COORDINATES.copy()

random.seed(42)

vehicle_topic = "vehicle_data"
gps_topic = "gps_data"
traffic_topic = "traffic_data"
weather_topic = "weather_data"
emergency_topic = "emergency_data"


def get_next_time():
    global start_time
    start_time+=timedelta(seconds=random.randint(30,60))

    return start_time


def simulate_vehicle_movement():
    global start_location

    start_location['latitude']+=LATITUDE_INCREMENT + random.uniform(-0.0005, 0.0005)
    start_location['longitude']+=LONGITUDE_INCREMENT+ random.uniform(-0.0005, 0.0005)

    return start_location    


def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': get_next_time().isoformat(),
        'location' : (location['latitude'],location['longitude']),
        'speed' : random.randint(10,50),
        'direction': 'North-West',
        'make': 'Tesla',
        'model': 'Model 3',
        'year': 2025,
        'fueltype': 'Electric'      
    }


def generate_gps_data(device_id, timestamp, vehicle_type = 'private'):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': timestamp,
        'speed': int(random.uniform(10,50)),
        'direction': 'North-West',
        'vehicle_type': vehicle_type

    }


def generate_traffic_camera_data(device_id, timestamp, camera_id, location):
    return {
        'id' : uuid.uuid4(),
        'deviceId': device_id,
        'location': location,
        'cameraId': camera_id,
        'timestamp': timestamp,
        'snapshot': 'Base64EncodedString'
    }


def generate_weather_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'location': location,
        'timestamp': timestamp,
        'temperature': random.uniform(60,90),
        'weather_condition': random.choice(["Sunny","Cloudy","Rainy"]),
        'windSpeed': random.uniform(0,100),
        'humidity': random.randint(0,100),
        'precipitation': random.uniform(0,25),
        'AQI': random.uniform(200,500)
    }

def generate_emergency_incident_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'incidentId': uuid.uuid4(),
        'type': random.choice(['Accident','Fire','Medical','Police','None']),
        'location': location,
        'timestamp': timestamp,
        'status': random.choice(['Active','Resolved'])
    }



producer = KafkaProducer(bootstrap_servers=['0.0.0.0:9092'])


def on_delivery_success(record_metadata):
    print(f"Message delivered to {record_metadata.topic} at offset {record_metadata.offset}")

def on_delivery_error(excp):
    print(f"Delivery failed: {excp}")


def default_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f"Type {type(obj)} not serializable")


def produce_data_to_kafka(producer,data,topic):
    producer.send(
        topic = topic,
        key=str(data['id']).encode('utf-8'),
        value=json.dumps(data, default=default_serializer).encode('utf-8')
    ).add_callback(on_delivery_success).add_errback(on_delivery_error)

    producer.flush()



def simulate_journey(producer,device_id):
    while True:

        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])
        traffic_camera_data = generate_traffic_camera_data(device_id,vehicle_data['timestamp'], "CanonRebelT13", vehicle_data['location'])
        weather_data = generate_weather_data(device_id,vehicle_data['timestamp'], vehicle_data['location'])
        emergency_incident_data = generate_emergency_incident_data(device_id, vehicle_data['timestamp'],vehicle_data['location'])

        if (vehicle_data['location'][0] >= LA_COORDINATES['latitude'] and
            vehicle_data['location'][1] <= LA_COORDINATES['longitude']):
            print("Vehicle has reached Los Angeles. Simulation ending...")
            break

        produce_data_to_kafka(producer, vehicle_data, vehicle_topic)
        produce_data_to_kafka(producer, gps_data, gps_topic)
        produce_data_to_kafka(producer, traffic_camera_data, traffic_topic)
        produce_data_to_kafka(producer, weather_data, weather_topic)
        produce_data_to_kafka(producer, emergency_incident_data, emergency_topic)

        # time.sleep(5)

if __name__ == "__main__":

    try:
        simulate_journey(producer, "AkashVemula123")
    except KeyboardInterrupt:
        print("Simulation is stopped by user")

    except Exception as e:
        print("Unexpected Error occurred",e)