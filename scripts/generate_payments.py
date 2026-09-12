import csv
import json
import random
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

rng = random.Random(42)
output_dir = Path(__file__).resolve().parents[1] / "data"
output_dir.mkdir(exist_ok=True)

# Fixed dates and random seed make every run reproducible.
start = datetime(2026, 9, 1, tzinfo=timezone.utc)
rows = []

for hour_index in range(7 * 24):
    hour = start + timedelta(hours=hour_index)
    batch_id = f"batch_{hour:%Y%m%d_%H}"

    # Every hour has both payment methods.
    methods = ["card"] * 42 + ["wallet"] * 18
    rng.shuffle(methods)

    for method in methods:
        timestamp = hour + timedelta(seconds=rng.randrange(3600))
        payment_number = len(rows) + 1
        rows.append({
            "payment_id": f"pay_{payment_number:06d}",
            "order_id": f"order_{payment_number:06d}",
            "event_time": timestamp.isoformat(),
            "payment_method": method,
            "status": "succeeded" if rng.random() < 0.96 else "failed",
            "amount_cents": rng.randint(500, 15000),
            "currency": "SGD",
            "batch_id": batch_id,
        })

rows.sort(key=lambda row: (row["event_time"], row["payment_id"]))

assert len(rows) == 10080
assert len({row["payment_id"] for row in rows}) == len(rows)
assert len({row["batch_id"] for row in rows}) == 168

csv_path = output_dir / "raw_payments.csv"
with csv_path.open("w", newline="", encoding="utf-8") as handle:
    writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)

successful = [row for row in rows if row["status"] == "succeeded"]
summary = {
    "synthetic": True,
    "seed": 42,
    "currency": "SGD",
    "start_utc": start.isoformat(),
    "end_utc_exclusive": (start + timedelta(days=7)).isoformat(),
    "payment_count": len(rows),
    "batch_count": 168,
    "status_counts": dict(Counter(row["status"] for row in rows)),
    "successful_revenue_cents": {
        method: sum(
            row["amount_cents"]
            for row in successful
            if row["payment_method"] == method
        )
        for method in ("card", "wallet")
    },
}

summary_path = output_dir / "baseline_summary.json"
summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

print(f"Created {csv_path}")
print(f"Payments: {len(rows):,} across 168 hourly batches")
for method, cents in summary["successful_revenue_cents"].items():
    print(f"Successful {method} revenue: SGD {cents / 100:,.2f}")
print(f"Saved baseline: {summary_path}")
