"""Generate a small, deterministic sample of raw events for dbt CI.

Mirrors the columns the Kafka producer sends (plus the sink connector's
create_time) and deliberately includes edge cases: duplicate deliveries,
missing brands, mixed-case event types and users who abandon carts.
"""
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)
OUT = Path(__file__).resolve().parents[1] / "seeds" / "sample_raw_events.csv"

BRANDS = ["samsung", "apple", "xiaomi", "huawei", "lucente", "", None]
CATEGORIES = [
    ("2053013555631882655", "electronics.smartphone"),
    ("2053013553559896355", "electronics.audio.headphone"),
    ("2053013563810775923", "appliances.kitchen.washer"),
    ("2053013558920217191", "computers.notebook"),
]
start = datetime(2019, 11, 1)
rows = []

for s in range(60):
    user_id = str(500000000 + s % 35)
    session = f"session-{s:04d}"
    t = start + timedelta(days=s % 7, hours=random.randint(0, 23), minutes=random.randint(0, 59))
    cat_id, cat_code = random.choice(CATEGORIES)
    product_id = str(1000000 + random.randint(1, 40))
    brand = random.choice(BRANDS)
    price = round(random.uniform(5, 900), 2)
    steps = ["view"] * random.randint(1, 3)
    roll = random.random()
    if roll < 0.45:
        steps.append("cart")
    if roll < 0.25:
        steps.append("purchase")
    for i, step in enumerate(steps):
        et = t + timedelta(minutes=2 * i)
        rows.append([et, step, product_id, cat_id, cat_code, brand, price, user_id, session,
                     et + timedelta(minutes=5)])

# Edge cases
dup = list(rows[3]); dup[-1] = dup[-1] + timedelta(minutes=1)
rows.append(dup)                                   # duplicate delivery of an event
rows.append([start + timedelta(days=2), "VIEW", "1000099", CATEGORIES[0][0], "", "  Apple ", 199.0,
             "500000099", "session-9999", start + timedelta(days=2, minutes=5)])  # messy casing and spacing
rows.append([start + timedelta(days=3), "cart", "1000098", CATEGORIES[1][0], None, None, 49.5,
             "500000098", "session-9998", start + timedelta(days=3, minutes=5)])  # abandoned cart, no brand

rows.sort(key=lambda r: r[0])
OUT.parent.mkdir(parents=True, exist_ok=True)
with OUT.open("w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["event_time", "event_type", "product_id", "category_id", "category_code", "brand",
                "price", "user_id", "user_session", "create_time"])
    for r in rows:
        w.writerow([r[0].isoformat(sep=" "), r[1], r[2], r[3], r[4] or "", "" if r[5] is None else r[5],
                    r[6], r[7], r[8], r[9].isoformat(sep=" ")])
print(f"wrote {len(rows)} rows to {OUT}")
