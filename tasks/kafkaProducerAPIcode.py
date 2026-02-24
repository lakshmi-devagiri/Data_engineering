from kafka import KafkaProducer
from kafka.errors import KafkaError
import os
from time import sleep
import json
from json import dumps
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

path="C:\\tmp\\livelogs\\access_log_20240603-081713.log"
#i want to read data from this above folder one by one file i want to send to kafka broker
with open(path,errors="ignore",mode='r') as f:
    for line in f:
        print(line)
        producer.send('june3', line.encode('utf-8') )
        sleep(5)