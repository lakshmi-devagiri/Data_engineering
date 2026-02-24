from kafka import KafkaProducer
import os, time, sys, json, shutil

TOPIC = "indsa"
DIR_PATH = r"C:\bigdata\output"        # where your JSON files are
BOOTSTRAP = "localhost:9092"
MOVE_SENT = True                        # set False if you don't want files moved after send
SENT_DIR = os.path.join(DIR_PATH, "sent")

def main():
    # ensure dirs
    if not os.path.isdir(DIR_PATH):
        print(f"Directory not found: {DIR_PATH}")
        sys.exit(1)
    if MOVE_SENT:
        os.makedirs(SENT_DIR, exist_ok=True)

    # kafka producer
    producer = KafkaProducer(
        bootstrap_servers=[BOOTSTRAP],
        value_serializer=lambda v: v.encode("utf-8"),
        retries=5,
        retry_backoff_ms=500,
        request_timeout_ms=15000,
        linger_ms=10,
    )

    files = sorted(os.listdir(DIR_PATH))
    if not files:
        print(f"No files in {DIR_PATH}")
        return

    for name in files:
        fp = os.path.join(DIR_PATH, name)
        if not os.path.isfile(fp):
            continue

        with open(fp, "r", encoding="utf-8", errors="ignore") as f:
            payload = f.read().strip()
        if not payload:
            continue

        # normalize to one-line JSON (like your tutor’s output); if not JSON, send as-is
        try:
            payload = json.dumps(json.loads(payload), separators=(",", ":"))
        except Exception:
            pass

        # print the actual JSON being sent (matches tutor’s console)
        print(payload)

        # send + flush to surface errors immediately
        producer.send(TOPIC, value=payload)
        producer.flush(timeout=15)

        # move file after successful send to avoid resending on next run
        if MOVE_SENT:
            try:
                shutil.move(fp, os.path.join(SENT_DIR, name))
            except Exception:
                # if move fails (e.g., file locked), just skip moving
                pass

        time.sleep(2)  # slow down so you can watch messages arrive

    try:
        producer.flush(timeout=15)
    finally:
        producer.close()
    print("Done.")

if __name__ == "__main__":
    main()
