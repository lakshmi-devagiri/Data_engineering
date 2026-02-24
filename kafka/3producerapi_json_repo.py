from pyspark.sql import *
from pyspark.sql.functions import *
import time
import json
BOOTSTRAP_SERVERS = "pkc-921jm.us-east-2.aws.confluent.cloud:9092"
CCLOUD_API_KEY = "EFJGEWCZUBUABSOQ"
CCLOUD_API_SECRET = r"cfltCeV+PKKl430ChVpDcScmHq93RnWf76BCczK0WLcGrY5m6yVu+Vy/iNmcM1lg"
SCHEMA_REGISTRY_URL = "https://psrc-13gydo7.ap-south-1.aws.confluent.cloud"
SCHEMA_REGISTRY_API_KEY = "WCUAFY6I76B3UINZ"
SCHEMA_REGISTRY_API_SECRET = "cfltRmb3Axpc9qHVJLdsnZbM11V14oUTJRtI1lf6BnjEyLkeP4F9EgvNAxaoJXgg"


topic = "kafkatopic"
csv_file=r"D:\bigdata\drivers\bank_full.csv"
import os
import csv
import argparse
from uuid import uuid4

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer

# --- JSON Schema (allows numbers or nulls for safety) ---
SCHEMA_STR = """
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "BankMarketingRecord",
  "type": "object",
  "additionalProperties": false,
  "properties": {
    "age": { "type": ["number", "null"] },
    "job": { "type": ["string", "null"] },
    "marital": { "type": ["string", "null"] },
    "education": { "type": ["string", "null"] },
    "default": { "type": ["string", "null"] },
    "balance": { "type": ["number", "null"] },
    "housing": { "type": ["string", "null"] },
    "loan": { "type": ["string", "null"] },
    "contact": { "type": ["string", "null"] },
    "day": { "type": ["number", "null"] },
    "month": { "type": ["string", "null"] },
    "duration": { "type": ["number", "null"] },
    "campaign": { "type": ["number", "null"] },
    "pdays": { "type": ["number", "null"] },
    "previous": { "type": ["number", "null"] },
    "poutcome": { "type": ["string", "null"] },
    "y": { "type": ["string", "null"] }
  }
}
"""


# --- Column map with desired Python casters ---
COLUMNS = {
    "age": int,
    "job": str,
    "marital": str,
    "education": str,
    "default": str,
    "balance": int,              # use int; switch to float if you expect decimals
    "housing": str,
    "loan": str,
    "contact": str,
    "day": int,
    "month": str,
    "duration": int,
    "campaign": int,
    "pdays": int,                # often -1 when unknown
    "previous": int,
    "poutcome": str,
    "y": str
}


# --- CLI ---
'''parser = argparse.ArgumentParser(description="Produce weather CSV rows to Kafka (single-file script).")
parser.add_argument("--file", default="Weather.csv", help="Path to CSV file (with header)")
parser.add_argument("--topic", default="Weather", help="Kafka topic")
args = parser.parse_args()'''

# --- Secrets & endpoints (use environment variables) ---
# export BOOTSTRAP_SERVERS=...
# export CCLOUD_API_KEY=...
# export CCLOUD_API_SECRET=...
# export SCHEMA_REGISTRY_URL=...
# export SCHEMA_REGISTRY_API_KEY=...
# export SCHEMA_REGISTRY_API_SECRET=...
producer_conf = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": CCLOUD_API_KEY,
    "sasl.password": CCLOUD_API_SECRET,
    "enable.idempotence": True,
    "linger.ms": 50,
    "batch.num.messages": 10000,
}

schema_registry_conf = {
    "url": SCHEMA_REGISTRY_URL,
    "basic.auth.user.info": f'{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}',
}

# --- Build clients / serializers ---
producer = Producer(producer_conf)
sr_client = SchemaRegistryClient(schema_registry_conf)
key_serializer = StringSerializer("utf8")
value_serializer = JSONSerializer(SCHEMA_STR, sr_client, lambda d, _: d)

print(f"Producing to topic '{topic}' from '{csv_file}' ...")

# --- Read CSV, cast types, produce rows ---
with open(csv_file, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f, delimiter=';')
    for row in reader:
        record = {}
        for col, caster in COLUMNS.items():
            val = row.get(col)
            if caster is str:
                record[col] = (val if (val is not None and val != "") else None)
            else:
                if val is None or str(val).strip() == "":
                    record[col] = None
                else:
                    try:
                        record[col] = caster(str(val).strip())
                    except Exception:
                        record[col] = None  # fall back to null if bad data

        key = key_serializer(str(uuid4()), SerializationContext(topic, MessageField.KEY))
        #value = value_serializer(record, SerializationContext(topic, MessageField.VALUE))
        #if u convvert to json below line use this in consumer json_df = df.select(col("value").cast("string").alias("json"))
        value = json.dumps(record).encode("utf-8")

        print("Sending:", json.dumps(record, ensure_ascii=False))

        producer.produce(
            topic=topic,
            key=key,
            value=value,
            on_delivery=lambda err, msg: print("Delivered" if err is None else f"Error: {err}")
        )
        producer.poll(0)  # serve the callback soon
        time.sleep(5)  # <-- send once every 5 seconds

    print("Flushing...")
    producer.flush(30)
    print("Done.")
