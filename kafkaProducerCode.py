from kafka import KafkaProducer
from time import sleep
producer = KafkaProducer(bootstrap_servers='pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092')

log_file_name = r'C:\logs\access_log_20251001-073602.log'
topic="oct1topic"
with open(log_file_name, 'r') as log_file:
        for line in log_file:
            print(line)
            producer.send(topic,line.encode('ascii'))
            sleep(3)