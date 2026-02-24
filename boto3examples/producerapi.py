from kafka import KafkaProducer
import os
import json
import time

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

path = "D:\\bigdata\\nifi-1.23.2\\livedata"
topic_name = 'feb12'  # Replace with your desired topic name

while True:
    for file in os.listdir(path):
        file_path = os.path.join(path, file)
        try:
            with open(file_path, encoding='utf-8') as f:  # Ensure correct encoding
                # Check if entire file contains single JSON object:
                all_data = f.read().strip()  # Read and strip whitespace
                try:
                    data = json.loads(all_data)  # Attempt to parse entire file
                except json.JSONDecodeError:
                    # If single object parsing fails, proceed line-by-line:
                    for line in f:
                        data = json.loads(line.strip())  # Strip whitespace and parse
                        try:
                            producer.send(topic_name, data)
                        except Exception as e:
                            print(f"Error sending data: {e}")
                else:
                    # If single object parsed successfully, send it:
                    producer.send(topic_name, data)
                    print(data)
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")

        os.remove(file_path)  # Remove processed file

    time.sleep(1)  # Check for new files every second

producer.flush()  # Ensure all messages are sent before exiting
producer.close()  # Close the producer
