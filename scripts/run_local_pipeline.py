import csv
import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

project_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_dir))

from app.transform import transform_payments

data_dir = project_dir / "data"

with (data_dir / "raw_payments.csv").open(
    newline="", encoding="utf-8"
) as handle:
    raw = list(csv.DictReader(handle))

processed = transform_payments(raw)

totals = defaultdict(lambda: {"payment_count": 0, "revenue_cents": 0})
for row in processed:
    hour = datetime.fromisoformat(row["event_time"]).replace(
        minute=0, second=0, microsecond=0
    ).isoformat()
    key = (hour, row["payment_method"], row["currency"])
    totals[key]["payment_count"] += 1
    totals[key]["revenue_cents"] += row["amount_cents"]

# Include zero-revenue groups so missing payments remain visible.
for row in raw:
    hour = datetime.fromisoformat(row["event_time"]).replace(
        minute=0, second=0, microsecond=0
    ).isoformat()
    totals[(hour, row["payment_method"], row["currency"])]

hourly = [
    {
        "hour": hour,
        "payment_method": method,
        "currency": currency,
        **values,
    }
    for (hour, method, currency), values in sorted(totals.items())
]

def write_csv(path, rows, fields):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

write_csv(
    data_dir / "processed_payments.csv",
    processed,
    list(raw[0]),
)
write_csv(
    data_dir / "hourly_revenue.csv",
    hourly,
    ["hour", "payment_method", "currency", "payment_count", "revenue_cents"],
)

baseline = json.loads(
    (data_dir / "baseline_summary.json").read_text(encoding="utf-8")
)

assert len(raw) == baseline["payment_count"]
assert len(processed) == baseline["status_counts"]["succeeded"]
assert len(hourly) == 336, "Expected 168 hours with two payment methods"

for method, expected in baseline["successful_revenue_cents"].items():
    actual = sum(
        row["revenue_cents"]
        for row in hourly
        if row["payment_method"] == method
    )
    assert actual == expected, f"{method} revenue differs from baseline"
    print(f"{method}: SGD {actual / 100:,.2f} — matches baseline")

print(f"Input payments: {len(raw):,}")
print(f"Successful payments: {len(processed):,}")
print(f"Excluded failed payments: {len(raw) - len(processed):,}")
print(f"Hourly revenue rows: {len(hourly):,}")
print("Healthy pipeline checks: PASSED")
