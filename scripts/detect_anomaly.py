import json
import os
from datetime import datetime, timezone
from pathlib import Path

from databricks import sql
from dotenv import load_dotenv

project_dir = Path(__file__).resolve().parents[1]
output_path = project_dir / "data" / "anomaly_status.json"

load_dotenv(project_dir / ".env")

host = os.environ["DATABRICKS_HOST"]
token = os.environ["DATABRICKS_TOKEN"]
warehouse_id = os.environ["DATABRICKS_WAREHOUSE_ID"]

server_hostname = host.removeprefix("https://").rstrip("/")
http_path = f"/sql/1.0/warehouses/{warehouse_id}"

threshold_ratio = 0.50

query = """
WITH bounds AS (
    SELECT MAX(hour) AS latest_hour
    FROM workspace.default.hourly_revenue
)
SELECT
    payment_method,
    SUM(
        CASE
            WHEN hour BETWEEN
                latest_hour - INTERVAL 5 HOURS
                AND latest_hour
            THEN revenue_cents
            ELSE 0
        END
    ) AS current_revenue_cents,
    SUM(
        CASE
            WHEN hour BETWEEN
                latest_hour - INTERVAL 29 HOURS
                AND latest_hour - INTERVAL 24 HOURS
            THEN revenue_cents
            ELSE 0
        END
    ) AS previous_revenue_cents,
    latest_hour
FROM workspace.default.hourly_revenue
CROSS JOIN bounds
GROUP BY payment_method, latest_hour
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
    current_revenue,
    previous_revenue,
    latest_hour,
) in rows:
    current_revenue = int(current_revenue or 0)
    previous_revenue = int(previous_revenue or 0)

    if previous_revenue > 0:
        ratio = current_revenue / previous_revenue
    else:
        ratio = None

    is_anomaly = (
        ratio is not None
        and ratio < threshold_ratio
    )

    result = {
        "payment_method": payment_method,
        "current_revenue_cents": current_revenue,
        "previous_revenue_cents": previous_revenue,
        "ratio": round(ratio, 4)
        if ratio is not None
        else None,
        "is_anomaly": is_anomaly,
    }
    results.append(result)

    if is_anomaly:
        anomalies.append(result)

    ratio_display = (
        f"{ratio:.1%}"
        if ratio is not None
        else "unavailable"
    )

    print(
        f"{payment_method}: "
        f"current SGD {current_revenue / 100:,.2f}, "
        f"previous SGD {previous_revenue / 100:,.2f}, "
        f"ratio {ratio_display}"
    )

status = {
    "checked_at": datetime.now(timezone.utc).isoformat(),
    "latest_data_hour": (
        rows[0][3].isoformat()
        if rows
        else None
    ),
    "comparison_hours": 6,
    "threshold_ratio": threshold_ratio,
    "status": "anomaly" if anomalies else "healthy",
    "results": results,
    "anomalies": anomalies,
}

output_path.parent.mkdir(
    parents=True,
    exist_ok=True,
)
output_path.write_text(
    json.dumps(status, indent=2) + "\n",
    encoding="utf-8",
)

if anomalies:
    print("Status: ANOMALY")
    for anomaly in anomalies:
        print(
            f"Detected {anomaly['payment_method']} "
            f"revenue drop"
        )
else:
    print("Status: HEALTHY")

print(f"Saved: {output_path}")
