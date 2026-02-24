import time
import os
from kafka import KafkaProducer

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'  # Replace with your Kafka broker address
KAFKA_TOPIC = 'indpak'  # Replace with your Kafka topic name

# File path
LOG_FILE_PATH = r'C:\tmp\livelogs\access_log_20240625-072805.log'

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)

def follow(file):
    """Generator function that yields new lines from a file."""
    file.seek(0, os.SEEK_END)  # Move to the end of the file
    while True:
        line = file.readline()
        print(line)
        if not line:
            time.sleep(3)  # Sleep briefly
            continue
        yield line

with open(LOG_FILE_PATH, 'r') as logfile:
    loglines = follow(logfile)
    for line in loglines:
        # Produce message to Kafka
        producer.send(KAFKA_TOPIC, value=line.encode('utf-8'))
        producer.flush()  # Ensure the message is sent
