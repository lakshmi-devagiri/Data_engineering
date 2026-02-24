from confluent_kafka import Producer
from pathlib import Path
from time import *
# Confluent Cloud (use your real values)
host = "pkc-921jm.us-east-2.aws.confluent.cloud:9092"
topic = "test"
api_key = "XHOL7RHJAF75UL5G"
api_secret = "cfltz+jL2ONC2a3XEMat+3dGP+Fe1JlK7lF7j0BTGtY1cRdEt02IJC2/IBscTnaQ"
conf = {
    "bootstrap.servers": host,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": api_key,
    "sasl.password": api_secret,
    # throughput
    "linger.ms": 100,
    "batch.num.messages": 20000,
    "compression.type": "zstd",
    "enable.idempotence": True,
    "ssl.endpoint.identification.algorithm": "https"

}
p = Producer(conf)

path = Path(r"D:\tmp\livedata")
topic = "venutopic"

for fp in sorted(path.iterdir()):
    if not fp.is_file(): continue
    with open(fp, "rb", buffering=1<<20) as f:     # read bytes (fast)
        for line in f:
            line = line.rstrip(b"\r\n")
            if not line: continue
            print(line.decode("utf-8", errors="replace"))   # <-- show as string
            while True:
                try:
                    p.produce(topic, value=line)   # send bytes as-is
                    sleep(0.1)
                    break

                except BufferError:
                    p.poll(5)
p.flush()