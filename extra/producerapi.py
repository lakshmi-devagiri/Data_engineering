from kafka import KafkaProducer
import time

# Path to the log file
log_path = r"C:\tmp\access_log_20250326-073125.log"

# Create a Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# Kafka topic name
topic_name = 'mar25'

try:
    # Open the log file
    with open(log_path, 'r') as file:
        # Read the file line by line
        for line in file:
            print(line)
            # Send each line to the Kafka topic
            producer.send(topic_name, line.encode('utf-8'))
            # Ensure data is sent by flushing the producer
            producer.flush()
            # Optional: sleep for a second for demo purposes (you can remove this in production)
            time.sleep(5)
finally:
    # Close the producer
    producer.close()

print("Log data sent to Kafka topic successfully.")