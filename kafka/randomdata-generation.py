import os, csv, random, time
from datetime import datetime

OUT_DIR = r"D:\logs\data"
NUM_FILES = 4000            # how many files to create
ROWS_PER_FILE = 20       # rows per file
SLEEP_BETWEEN_FILES_SEC = 3  # pause between files (lets your watcher pick them up)

names  = ["venu","sita","gita","nani","murali","renu","bhanu","munna","modi","rahul",
          "lalu","uma","pramod","ali","sarat","venkat","nidhi","arun","vikas","ravi","raja","roja"]
cities = ["bza","tpty","hyd","del","blr","mas","goa","sri","jammu","kol",
          "goh","bho","tri","shimla","ooty","gnt","pune","mum","ahm","patna"]

os.makedirs(OUT_DIR, exist_ok=True)

for i in range(1, NUM_FILES + 1):
    file_path = os.path.join(OUT_DIR, f"asl{i}.csv")
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["name", "age", "city", "event_ts"])  # header
        for _ in range(ROWS_PER_FILE):
            w.writerow([
                random.choice(names),
                random.randint(18, 75),
                random.choice(cities),
                datetime.utcnow().isoformat() + "Z"
            ])
    print(f"Created {file_path} with {ROWS_PER_FILE} rows")
    time.sleep(SLEEP_BETWEEN_FILES_SEC)