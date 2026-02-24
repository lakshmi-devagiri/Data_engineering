import os
import time
from kafka import KafkaProducer
# --- Adjust these for your setup ---
path = r"C:\tmp\access_log_20250907-102820.log"
topic = "sep8topic"
from kafka import KafkaProducer
from time import sleep
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
#i want to read data from this above folder one by one file i want to send to kafka broker
with open(path,errors="ignore",mode='r') as f:
    for line in f:
        print(line)
        producer.send(topic, line.encode('utf-8') )
        sleep(5)