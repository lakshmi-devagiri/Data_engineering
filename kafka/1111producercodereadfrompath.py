# pip install kafka-python
from kafka import KafkaProducer
import os, csv, time

# ---- config ----
FOLDER = r"D:\logs\data"           # read all CSVs from this folder (non-recursive)
TOPIC  = "oct15"
BROKERS = ['localhost:9092']       # change if different

# ---- kafka producer ----
producer = KafkaProducer(
    bootstrap_servers=BROKERS,
    value_serializer=lambda v: v.encode('utf-8')
)

# ---- loop over files, then rows, with 5s delay ----
files = [f for f in os.listdir(FOLDER) if f.lower().endswith(".csv")]
files.sort()  # optional: stable order

for fname in files:
    path = os.path.join(FOLDER, fname)
    print(f"Reading file: {path}")
    try:
        with open(path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)  # expects header: name, age, city
            for row in reader:
                message = f"{row.get('name','')},{row.get('age','')},{row.get('city','')}"
                producer.send(TOPIC, message)
                print("Sent:", message)
                time.sleep(0.1)  # wait 5 seconds per row
    except Exception as e:
        print(f"[WARN] skipping {path}: {e}")

producer.flush()
print("All messages sent to Kafka topic:", TOPIC)
