from confluent_kafka import Producer
from time import sleep

# Kafka configuration for Confluent Cloud
conf = {
    'bootstrap.servers': 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',  # broker
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': '3W5TJPYDIFPKZAED',         # Replace with your Confluent Cloud API Key
    'sasl.password': 'cfltNnXgsuep6Nx7uqJy/XDfn+mcir9aH53q7XyFlIoIPmymON/NDlxckZI3KFqg',      # Replace with your Confluent Cloud API Secret
    'client.id': 'python-producer'
}

producer = Producer(conf)

log_file_name = r'C:\logs\access_log_20251001-073602.log'
topic = "oct1topic"

def delivery_report(err, msg):
    """ Callback called once for each message to confirm delivery result """
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

with open(log_file_name, 'r') as log_file:
    for line in log_file:
        print("Producing:", line.strip())
        producer.produce(
            topic=topic,
            value=line.encode('utf-8'),
            callback=delivery_report
        )
        # Flush ensures the message is sent
        producer.poll(0)
        sleep(3)

# Wait for all messages in the queue to be delivered
producer.flush()
