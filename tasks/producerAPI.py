from kafka import KafkaProducer
import os
import json
from time import sleep

path="C:\\bigdata\\nifilogs\\"
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
file_list = os.listdir(path)
for fil in file_list:
    with open ( path+str(fil) ,errors="ignore", mode='r' ) as file:
        line = file.read()
        print(line)
        producer.send( "dec14" , line.encode("utf-8"))
        sleep(3)