from __future__ import annotations

import argparse
import json
import random
import string
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict

FIRST_NAMES = [
    "John", "Jane", "Alice", "Bob", "Charlie", "Diana", "Venu", "Asha",
    "Ravi", "Priya", "Nikhil", "Meena", "Suresh", "Latha", "Kiran"
]
LAST_NAMES = [
    "Kumar", "Singh", "Reddy", "Sharma", "Patel", "Smith", "Johnson",
    "Gonzalez", "Lee", "Garcia", "Brown", "Katragadda", "Overes"
]

PHONE_PATTERNS = [
    "+91-##########",
    "+1 (###) ### ####",
    "0##-########",
    "(###)-###-####",
    "###-###-####"
]

def rand_name() -> str:
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"

def rand_email(name: str) -> str:
    base = "".join(c for c in name.lower() if c.isalnum() or c == " ").replace(" ", ".")
    return f"{base}{random.randint(1,9999)}@example.com"

def rand_phone() -> str:
    pat = random.choice(PHONE_PATTERNS)
    out = []
    for ch in pat:
        out.append(str(random.randint(0, 9)) if ch == "#" else ch)
    return "".join(out)

def rand_aadhar(with_spaces: bool = True) -> str:
    digits = "".join(str(random.randint(0, 9)) for _ in range(12))
    return f"{digits[0:4]} {digits[4:8]} {digits[8:12]}" if with_spaces else digits

def rand_pan() -> str:
    letters = "".join(random.choice(string.ascii_uppercase) for _ in range(5))
    digits = "".join(str(random.randint(0, 9)) for _ in range(4))
    last = random.choice(string.ascii_uppercase)
    return f"{letters}{digits}{last}"

def generate_record() -> Dict:
    name = rand_name()
    now = datetime.utcnow().isoformat(" ", "seconds") + "Z"
    return {
        "id": uuid.uuid4().hex,
        "generated_at_utc": now,
        "name": name,
        "email": rand_email(name),
        "phone": rand_phone(),
        "aadhar": rand_aadhar(with_spaces=True),
        "pan": rand_pan(),
        "score": round(random.uniform(0, 100), 2),
        "tags": random.sample(["sales", "dev", "ml", "etl", "qa", "ops", "hr"], k=2),
    }

def write_json_record(rec: Dict, out_dir: Path) -> Path:
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S%fZ")
    path = out_dir / f"rec_{ts}_{rec['id'][:8]}.json"
    with path.open("w", encoding="utf-8") as fh:
        json.dump(rec, fh, ensure_ascii=False)
    return path

def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)

def cli() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Random data generator (writes one file per record)")
    p.add_argument("--output", "-o", type=str, default=r"C:\bigdata\output",
                   help="Output directory for generated files")
    p.add_argument("--interval", "-i", type=float, default=5.0,
                   help="Interval between records in seconds (default: 5)")
    p.add_argument("--count", "-c", type=int, default=0,
                   help="If >0, generate exactly COUNT records and exit. Otherwise run forever until Ctrl+C")
    p.add_argument("--no-spaces-aadhar", action="store_true",
                   help="Generate Aadhar without spaces if set")
    return p.parse_args()

def main() -> None:
    args = cli()
    out_dir = Path(args.output)
    ensure_dir(out_dir)

    interval = max(0.1, float(args.interval))
    count = int(args.count)
    rec_generated = 0

    print(f"Starting random data generator. Output: {out_dir}\n"
          f"Interval: {interval}s  Count: {count or 'infinite'}")

    try:
        while True:
            rec = generate_record()
            if args.no_spaces_aadhar:
                rec["aadhar"] = rec["aadhar"].replace(" ", "")

            path = write_json_record(rec, out_dir)
            rec_generated += 1
            print(f"[{rec_generated}] Generated file: {path} | id={rec['id']} | "
                  f"aadhar={rec['aadhar']} | email={rec['email']}")

            if count and rec_generated >= count:
                print("Requested count reached — exiting.")
                break

            time.sleep(interval)
    except KeyboardInterrupt:
        print("Interrupted by user — exiting.")

if __name__ == "__main__":
    main()
