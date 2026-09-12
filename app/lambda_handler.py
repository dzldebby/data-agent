import csv
import io
import json
import logging
import os
from collections import defaultdict
from datetime import datetime

import boto3

from app.transform import transform_payments

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def csv_text(rows, fieldnames):
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def process_csv(source_text):
    raw = list(csv.DictReader(io.StringIO(source_text)))
    if not raw:
        raise ValueError("Source CSV contains no payment records")

    processed = transform_payments(raw)
    totals = defaultdict(lambda: {
        "payment_count": 0,
        "revenue_cents": 0,
    })

    for row in processed:
        hour = datetime.fromisoformat(row["event_time"]).replace(
            minute=0,
            second=0,
            microsecond=0,
        ).isoformat()
        key = (hour, row["payment_method"], row["currency"])
        totals[key]["payment_count"] += 1
        totals[key]["revenue_cents"] += int(row["amount_cents"])

    for row in raw:
        hour = datetime.fromisoformat(row["event_time"]).replace(
            minute=0,
            second=0,
            microsecond=0,
        ).isoformat()
        key = (hour, row["payment_method"], row["currency"])
        totals[key]

    hourly = [
        {
            "hour": hour,
            "payment_method": method,
            "currency": currency,
            **values,
        }
        for (hour, method, currency), values in sorted(totals.items())
    ]

    return {
        "processed_csv": csv_text(processed, list(raw[0])),
        "hourly_csv": csv_text(
            hourly,
            [
                "hour",
                "payment_method",
                "currency",
                "payment_count",
                "revenue_cents",
            ],
        ),
        "input_count": len(raw),
        "processed_count": len(processed),
        "hourly_count": len(hourly),
    }


def lambda_handler(event, context):
    record = event["Records"][0]["s3"]
    bucket = record["bucket"]["name"]
    source_key = record["object"]["key"]

    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket, Key=source_key)
    source_text = response["Body"].read().decode("utf-8")
    result = process_csv(source_text)

    processed_key = "processed/processed_payments.csv"
    hourly_key = "analytics/hourly_revenue.csv"
    run_key = f"runs/{context.aws_request_id}.json"

    s3.put_object(
        Bucket=bucket,
        Key=processed_key,
        Body=result["processed_csv"].encode("utf-8"),
        ContentType="text/csv",
    )
    s3.put_object(
        Bucket=bucket,
        Key=hourly_key,
        Body=result["hourly_csv"].encode("utf-8"),
        ContentType="text/csv",
    )

    manifest = {
        "request_id": context.aws_request_id,
        "source_bucket": bucket,
        "source_key": source_key,
        "processed_key": processed_key,
        "hourly_key": hourly_key,
        "input_count": result["input_count"],
        "processed_count": result["processed_count"],
        "hourly_count": result["hourly_count"],
        "commit_sha": os.environ.get("DEPLOY_COMMIT_SHA", "unknown"),
    }

    s3.put_object(
        Bucket=bucket,
        Key=run_key,
        Body=(json.dumps(manifest, indent=2) + "\n").encode("utf-8"),
        ContentType="application/json",
    )

    logger.info(json.dumps({
        "event": "pipeline_run_completed",
        **manifest,
    }))

    return manifest

