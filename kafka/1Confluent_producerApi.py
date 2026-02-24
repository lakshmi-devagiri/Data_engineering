import time
from confluent_kafka import Producer
from time import sleep
# --- Adjust these for your setup ---
LOG_FILE = r"C:\tmp\access_log_20250907-102820.log"
topic = "kafkatopic"
BOOTSTRAP = "pkc-921jm.us-east-2.aws.confluent.cloud:9092"
# Replace with your Confluent Cloud API key & secret
API_KEY = "EFJGEWCZUBUABSOQ"
API_SECRET = r"cfltCeV+PKKl430ChVpDcScmHq93RnWf76BCczK0WLcGrY5m6yVu+Vy/iNmcM1lg"
# ----------------------------------

conf = {
    "bootstrap.servers": BOOTSTRAP,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": API_KEY,
    "sasl.password": API_SECRET,
    "client.id": "python-producer",
}

producer = Producer(conf)

with open(LOG_FILE, errors="ignore", mode='r') as f:
    for line in f:
        print(line)
        producer.produce(topic, value=line.encode('utf-8'))
        producer.poll(0)   # process delivery events in background
        sleep(5)

producer.flush()