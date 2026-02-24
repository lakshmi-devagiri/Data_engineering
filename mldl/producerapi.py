from kafka import KafkaProducer
from time import sleep

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

path="C:\\tmp\\access_log_20250210-201141.log"
#i want to read data from this above folder one by one file i want to send to kafka broker
with open(path,errors="ignore",mode='r') as f:
    for line in f:
        print(line)
        producer.send('feb10', line.encode('utf-8') )
        sleep(10)
        #kafka 1 million messages sene per seonc ..
        #feb10,"venu,32,hyd"
        #feb10,"satya,55,del"
        #feb10,"nani,66,hyd"