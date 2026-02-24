from kafka import KafkaProducer
import os
from time import sleep
import msgpack

import json
from json import *
def json_key_serializer(key):
    return json.dumps(key).encode('utf-8')

# Custom value serializer to serialize Python dictionary to JSON string
def json_value_serializer(value):
    return json.dumps(value).encode('utf-8')

# Initialize Kafka producer with custom serializers
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    key_serializer=json_key_serializer,
    value_serializer=json_value_serializer
)
path = "D:\\bigdata\\nifi-1.23.2\\livedata\\"
dir_list = os.listdir(path)

for file in dir_list:
    with open(path + str(file), errors="ignore") as f:
        # Read the entire file content
        file_data = f.read()
        # Send the entire file data to Kafka
        try:
            producer.send('mar23', file_data)
            print([file_data])
        except Exception as e:
            print(f"Error sending data to Kafka: {e}")
        sleep(2)


