
# write_new_files.py — creates NEW files with unique names (Spark-friendly)
import os, csv, time, random
from datetime import datetime

OUT_DIR = r"C:\bigdata\in"
os.makedirs(OUT_DIR, exist_ok=True)

names = [
    "venu", "sita", "gita", "nani", "murali", "renu", "Bhanu", "munna",
    "modi", "rahul", "lalu", "uma", "pramod", "ali", "sarat", "venkat", "venu"
]

cities = [
    "bza", "tpty", "hyd", "del", "blr", "mas", "goa", "sri", "jammu", "kol",
    "goh", "bho", "tri", "simla", "ooty", "gnt", "Pune", "Mum", "ahm", "patna"
]

i = 0
while True:
    i += 1
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    tmp = os.path.join(OUT_DIR, f"tmp_{ts}_{i}.csv")     # write temp…
    final = os.path.join(OUT_DIR, f"batch_{ts}_{i}.csv") # …then rename (atomic)

    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["name","age","city"])  # header
        for _ in range(5):                  # a few rows per file
            w.writerow([random.choice(names), random.randint(20,99), random.choice(cities)])

    os.replace(tmp, final)  # atomic publish so Spark never sees partial files
    print("Wrote", final)
    time.sleep(5)  # new file every 5s
