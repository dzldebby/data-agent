import csv
import json
import random
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

rng = random.Random(42)

project_dir = Path(__file__).resolve().parents[1]
output_dir = project_dir / "data"
output_dir.mkdir(exist_ok=True)

start = datetime(
    2026,
    9,
    1,
    tzinfo=timezone.utc,
)

total_hours = 7 * 24
v2_start_hour = total_hours - 6

rows = []

for hour_index in range(total_hours):
    hour = start + timedelta(hours=hour_index)
    batch_id = f"batch_{hour:%Y%m%d_%H}"

    methods = ["card"] * 42 + ["wallet"] * 18
    rng.shuffle(methods)

    for method in methods:
        timestamp = hour + timedelta(
            seconds=rng.randrange(3600)
        )
        payment_number = len(rows) + 1

        provider_payload_version = (
            "v2"
            if method == "card"
            and hour_index >= v2_start_hour
            else "v1"
        )

        rows.append({
            "payment_id": f"pay_{payment_number:06d}",
            "order_id": f"order_{payment_number:06d}",
            "event_time": timestamp.isoformat(),
            "payment_method": method,
            "provider_payload_version": (
                provider_payload_version
            ),
            "status": (
                "succeeded"
                if rng.random() < 0.96
                else "failed"
            ),
            "amount_cents": rng.randint(500, 15000),
            "currency": "SGD",
            "batch_id": batch_id,
        })

rows.sort(
    key=lambda row: (
        row["event_time"],
        row["payment_id"],
    )
)

assert len(rows) == 10080
assert len({
    row["payment_id"]
    for row in rows
}) == len(rows)
assert len({
    row["batch_id"]
    for row in rows
}) == 168

payload_version_counts = Counter(
    (
        row["payment_method"],
        row["provider_payload_version"],
    )
    for row in rows
)

assert payload_version_counts[
    ("card", "v2")
] == 252
assert payload_version_counts.get(
    ("wallet", "v2"),
    0,
) == 0

csv_path = output_dir / "raw_payments.csv"

with csv_path.open(
    "w",
    newline="",
    encoding="utf-8",
) as handle:
    writer = csv.DictWriter(
        handle,
        fieldnames=list(rows[0]),
    )
    writer.writeheader()
    writer.writerows(rows)

successful = [
    row
    for row in rows
    if row["status"] == "succeeded"
]

summary = {
    "synthetic": True,
    "seed": 42,
    "currency": "SGD",
    "start_utc": start.isoformat(),
    "end_utc_exclusive": (
        start + timedelta(days=7)
    ).isoformat(),
    "payment_count": len(rows),
    "batch_count": 168,
    "status_counts": dict(
        Counter(
            row["status"]
            for row in rows
        )
    ),
    "payload_version_counts": {
        f"{method}:{version}": count
        for (
            method,
            version,
        ), count in sorted(
            payload_version_counts.items()
        )
    },
    "successful_revenue_cents": {
        method: sum(
            row["amount_cents"]
            for row in successful
            if row["payment_method"] == method
        )
        for method in (
            "card",
            "wallet",
        )
    },
}

summary_path = (
    output_dir / "baseline_summary.json"
)
summary_path.write_text(
    json.dumps(summary, indent=2) + "\n",
    encoding="utf-8",
)

print(f"Created {csv_path}")
print(
    f"Payments: {len(rows):,} "
    f"across 168 hourly batches"
)

for (
    method,
    version,
), count in sorted(
    payload_version_counts.items()
):
    print(
        f"{method} {version}: "
        f"{count:,} payments"
    )

for method, cents in summary[
    "successful_revenue_cents"
].items():
    print(
        f"Successful {method} revenue: "
        f"SGD {cents / 100:,.2f}"
    )

print(f"Saved baseline: {summary_path}")
