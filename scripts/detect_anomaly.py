import json
import os
from datetime import datetime, timezone
from pathlib import Path

from databricks import sql
from dotenv import load_dotenv

project_dir = Path(__file__).resolve().parents[1]
output_path = (
    project_dir
    / "data"
    / "anomaly_status.json"
)

load_dotenv(project_dir / ".env")

host = os.environ["DATABRICKS_HOST"]
token = os.environ["DATABRICKS_TOKEN"]
warehouse_id = os.environ[
    "DATABRICKS_WAREHOUSE_ID"
]

server_hostname = (
    host
    .removeprefix("https://")
    .rstrip("/")
)
http_path = (
    f"/sql/1.0/warehouses/{warehouse_id}"
)

threshold_ratio = 0.50

query = """
WITH bounds AS (
    SELECT MAX(hour) AS latest_hour
    FROM workspace.default.current_hourly_revenue
)
SELECT
    payment_method,
    SUM(
        CASE
            WHEN hour BETWEEN
                latest_hour - INTERVAL 5 HOURS
                AND latest_hour
            THEN reported_revenue_cents
            ELSE 0
        END
    ) AS current_reported_cents,
    SUM(
        CASE
            WHEN hour BETWEEN
                latest_hour - INTERVAL 5 HOURS
                AND latest_hour
            THEN expected_revenue_cents
            ELSE 0
        END
    ) AS current_expected_cents,
    SUM(
        CASE
            WHEN hour BETWEEN
                latest_hour - INTERVAL 5 HOURS
                AND latest_hour
            THEN difference_cents
            ELSE 0
        END
    ) AS current_difference_cents,
    SUM(
        CASE
            WHEN hour BETWEEN
                latest_hour - INTERVAL 5 HOURS
                AND latest_hour
            THEN mismatch_count
            ELSE 0
        END
    ) AS current_mismatch_count,
    SUM(
        CASE
            WHEN hour BETWEEN
                latest_hour - INTERVAL 29 HOURS
                AND latest_hour - INTERVAL 24 HOURS
            THEN reported_revenue_cents
            ELSE 0
        END
    ) AS previous_reported_cents,
    latest_hour
FROM workspace.default.current_hourly_revenue
CROSS JOIN bounds
GROUP BY
    payment_method,
    latest_hour
ORDER BY payment_method
"""

with sql.connect(
    server_hostname=server_hostname,
    http_path=http_path,
    access_token=token,
) as connection:
    with connection.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

results = []
anomalies = []

for (
    payment_method,
    current_reported,
    current_expected,
    current_difference,
    current_mismatches,
    previous_reported,
    latest_hour,
) in rows:
    current_reported = int(
        current_reported or 0
    )
    current_expected = int(
        current_expected or 0
    )
    current_difference = int(
        current_difference or 0
    )
    current_mismatches = int(
        current_mismatches or 0
    )
    previous_reported = int(
        previous_reported or 0
    )

    if previous_reported > 0:
        trend_ratio = (
            current_reported
            / previous_reported
        )
    else:
        trend_ratio = None

    if current_expected > 0:
        reconciliation_ratio = (
            current_reported
            / current_expected
        )
    else:
        reconciliation_ratio = None

    trend_anomaly = (
        trend_ratio is not None
        and trend_ratio < threshold_ratio
    )
    reconciliation_anomaly = (
        current_mismatches > 0
        or current_difference != 0
    )
    is_anomaly = (
        trend_anomaly
        or reconciliation_anomaly
    )

    result = {
        "payment_method": payment_method,
        "current_reported_cents": (
            current_reported
        ),
        "current_expected_cents": (
            current_expected
        ),
        "current_difference_cents": (
            current_difference
        ),
        "current_mismatch_count": (
            current_mismatches
        ),
        "previous_reported_cents": (
            previous_reported
        ),
        "trend_ratio": (
            round(trend_ratio, 4)
            if trend_ratio is not None
            else None
        ),
        "reconciliation_ratio": (
            round(
                reconciliation_ratio,
                4,
            )
            if reconciliation_ratio
            is not None
            else None
        ),
        "trend_anomaly": trend_anomaly,
        "reconciliation_anomaly": (
            reconciliation_anomaly
        ),
        "is_anomaly": is_anomaly,
    }

    results.append(result)

    if is_anomaly:
        anomalies.append(result)

    trend_display = (
        f"{trend_ratio:.1%}"
        if trend_ratio is not None
        else "unavailable"
    )
    reconciliation_display = (
        f"{reconciliation_ratio:.1%}"
        if reconciliation_ratio
        is not None
        else "unavailable"
    )

    print(
        f"{payment_method}: "
        f"reported SGD "
        f"{current_reported / 100:,.2f}, "
        f"expected SGD "
        f"{current_expected / 100:,.2f}, "
        f"difference SGD "
        f"{current_difference / 100:,.2f}, "
        f"previous SGD "
        f"{previous_reported / 100:,.2f}, "
        f"trend {trend_display}, "
        f"reconciled "
        f"{reconciliation_display}, "
        f"mismatches "
        f"{current_mismatches}"
    )

status = {
    "checked_at": datetime.now(
        timezone.utc
    ).isoformat(),
    "latest_data_hour": (
        rows[0][6].isoformat()
        if rows
        else None
    ),
    "comparison_hours": 6,
    "threshold_ratio": threshold_ratio,
    "status": (
        "anomaly"
        if anomalies
        else "healthy"
    ),
    "results": results,
    "anomalies": anomalies,
}

output_path.parent.mkdir(
    parents=True,
    exist_ok=True,
)
output_path.write_text(
    json.dumps(
        status,
        indent=2,
    ) + "\n",
    encoding="utf-8",
)

if anomalies:
    print("Status: ANOMALY")

    for anomaly in anomalies:
        print(
            "Detected discrepancy for "
            f"{anomaly['payment_method']}"
        )
else:
    print("Status: HEALTHY")

print(f"Saved: {output_path}")
