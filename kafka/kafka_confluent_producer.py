from confluent_kafka import Producer
from time import sleep

# ---- fill these with your Confluent Cloud details ----
BOOTSTRAP = "pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092"
API_KEY = "LCFUP2NCBAPEQ25X"
API_SECRET = "cfltXOHwkM/6ttU35ATLoenUL5GE+128LvetuV1+v0wqbE0NbsGaVdQHzS7LKrvQ"
TOPIC = "venutopicsep10"
# ------------------------------------------------------

producer = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": API_KEY,
    "sasl.password": API_SECRET,
})

path = r"C:\tmp\access_log_20250910-081302.log"

# read this file line-by-line and send to Kafka
with open(path, mode="r", encoding="utf-8", errors="ignore") as f:
    for line in f:
        value = line.strip()
        if not value:
            continue
        print(value)
        producer.produce(TOPIC, value=value.encode("utf-8"))
        producer.poll(0)   # process delivery events
        sleep(3)

producer.flush()          # wait for all messages to send
