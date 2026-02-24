import csv
import os
import random
import string
import time
from datetime import datetime
from pathlib import Path

# ----------------------------
# Settings
# ----------------------------
OUTPUT_DIR = r"C:\bigdata\livedata"   # <- your target folder
TOTAL_FILES = 10_000                  # number of files to generate
INTERVAL_SECONDS = 5                  # delay between files

# If True, write a header (name,age,city) in every CSV file
WRITE_HEADER_PER_FILE = True

# ----------------------------
# Sample data pools
# ----------------------------
FIRST_NAMES = [
    "Lakshmi", "Arjun", "Priya", "Vivek", "Ravi", "Anita", "Neha", "Kiran",
    "Maya", "Sanjay", "Asha", "Rahul", "Nisha", "Anil", "Kavya", "Rohan",
    "Meera", "Dev", "Isha", "Varun"
]

CITIES = [
    "Wichita", "Houston", "Dallas", "Chicago", "Minneapolis", "San Jose",
    "New York", "Seattle", "Austin", "Phoenix", "Atlanta", "Boston",
    "San Francisco", "Kansas City", "Denver"
]

# ----------------------------
# Helpers
# ----------------------------
def random_name() -> str:
    # Simple first name + random last initial for variety
    return f"{random.choice(FIRST_NAMES)} {random.choice(string.ascii_uppercase)}."

def random_age() -> int:
    # Age between 18 and 75
    return random.randint(18, 75)

def random_city() -> str:
    return random.choice(CITIES)

def unique_filename(counter: int) -> str:
    # Timestamp + counter to avoid collisions
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    return f"livedata_{ts}_{counter:05d}.csv"

def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)

# ----------------------------
# Main
# ----------------------------
def main():
    out_dir = Path(OUTPUT_DIR)
    ensure_dir(out_dir)

    print(f"Writing {TOTAL_FILES} CSV files to: {out_dir}")
    print(f"One file every {INTERVAL_SECONDS} seconds. Press Ctrl+C to stop.")

    try:
        for i in range(1, TOTAL_FILES + 1):
            # Generate a single record
            row = {
                "name": random_name(),
                "age": random_age(),
                "city": random_city(),
            }

            # Build a unique file path
            filename = unique_filename(i)
            file_path = out_dir / filename

            # Write the CSV (one record per file)
            with open(file_path, mode="w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=["name", "age", "city"])
                if WRITE_HEADER_PER_FILE:
                    writer.writeheader()
                writer.writerow(row)

            # Optional console log
            print(f"[{i}/{TOTAL_FILES}] Wrote: {file_path}")

            # Sleep between files, except after the last one
            if i < TOTAL_FILES:
                time.sleep(INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\nInterrupted by user. Exiting gracefully...")
    except PermissionError as e:
        print(f"\nPermissionError: {e}\n"
              "Tip: Make sure no other program is locking the folder or files.")
    except Exception as e:
        print(f"\nUnexpected error: {e}")

if __name__ == "__main__":
    main()
