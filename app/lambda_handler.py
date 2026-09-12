import csv
import io
import json
import logging
import os
from collections import defaultdict
from datetime import datetime
from urllib.parse import unquote_plus

import boto3

from app.transform import transform_payments

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def csv_text(rows, fieldnames):
    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=fieldnames,
    )
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def build_evidence(raw, processed):
    groups = {}

    def group_for(row):
        key = (
            row["payment_method"],
            row["provider_payload_version"],
        )

        if key not in groups:
            groups[key] = {
                "payment_method": key[0],
                "provider_payload_version": key[1],
                "input_count": 0,
                "successful_input_count": 0,
                "successful_input_revenue_cents": 0,
                "processed_count": 0,
                "processed_revenue_cents": 0,
            }

        return groups[key]

    for row in raw:
        group = group_for(row)
        group["input_count"] += 1

        if row["status"] == "succeeded":
            group["successful_input_count"] += 1
            group[
                "successful_input_revenue_cents"
            ] += int(row["amount_cents"])

    for row in processed:
        group = group_for(row)
        group["processed_count"] += 1
        group[
            "processed_revenue_cents"
        ] += int(row["amount_cents"])

    return [
        groups[key]
        for key in sorted(groups)
    ]


def process_csv(source_text):
    raw = list(
        csv.DictReader(
            io.StringIO(source_text)
        )
    )

    if not raw:
        raise ValueError(
            "Source CSV contains no payment records"
        )

    required_columns = {
        "payment_id",
        "order_id",
        "event_time",
        "payment_method",
        "provider_payload_version",
        "status",
        "amount_cents",
        "currency",
        "batch_id",
    }

    missing_columns = (
        required_columns - raw[0].keys()
    )

    if missing_columns:
        raise ValueError(
            f"Source CSV is missing columns: "
            f"{sorted(missing_columns)}"
        )

    processed = transform_payments(raw)

    totals = defaultdict(
        lambda: {
            "payment_count": 0,
            "revenue_cents": 0,
        }
    )

    for row in processed:
        hour = datetime.fromisoformat(
            row["event_time"]
        ).replace(
            minute=0,
            second=0,
            microsecond=0,
        ).isoformat()

        key = (
            hour,
            row["payment_method"],
            row["currency"],
        )

        totals[key]["payment_count"] += 1
        totals[key]["revenue_cents"] += int(
            row["amount_cents"]
        )

    # Preserve zero-revenue groups on the chart.
    for row in raw:
        hour = datetime.fromisoformat(
            row["event_time"]
        ).replace(
            minute=0,
            second=0,
            microsecond=0,
        ).isoformat()

        key = (
            hour,
            row["payment_method"],
            row["currency"],
        )

        totals[key]

    hourly = [
        {
            "hour": hour,
            "payment_method": method,
            "currency": currency,
            **values,
        }
        for (
            hour,
            method,
            currency,
        ), values in sorted(totals.items())
    ]

    return {
        "processed_csv": csv_text(
            processed,
            list(raw[0]),
        ),
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
        "evidence_by_method_and_version": (
            build_evidence(raw, processed)
        ),
    }


def lambda_handler(event, context):
    record = event["Records"][0]["s3"]

    bucket = record["bucket"]["name"]
    source_object = record["object"]
    source_key = unquote_plus(
        source_object["key"]
    )

    s3 = boto3.client("s3")

    response = s3.get_object(
        Bucket=bucket,
        Key=source_key,
    )
    source_text = (
        response["Body"]
        .read()
        .decode("utf-8")
    )

    result = process_csv(source_text)

    request_id = context.aws_request_id
    run_prefix = f"runs/{request_id}"

    processed_key = (
        f"{run_prefix}/processed_payments.csv"
    )
    hourly_key = (
        f"{run_prefix}/hourly_revenue.csv"
    )
    manifest_key = (
        f"{run_prefix}/manifest.json"
    )

    s3.put_object(
        Bucket=bucket,
        Key=processed_key,
        Body=result[
            "processed_csv"
        ].encode("utf-8"),
        ContentType="text/csv",
    )

    s3.put_object(
        Bucket=bucket,
        Key=hourly_key,
        Body=result[
            "hourly_csv"
        ].encode("utf-8"),
        ContentType="text/csv",
    )

    manifest = {
        "request_id": request_id,
        "source_bucket": bucket,
        "source_key": source_key,
        "source_etag": (
            source_object.get("eTag")
            or response.get("ETag", "").strip('"')
        ),
        "source_size_bytes": (
            source_object.get("size")
            or response.get("ContentLength")
        ),
        "processed_key": processed_key,
        "hourly_key": hourly_key,
        "manifest_key": manifest_key,
        "input_count": result["input_count"],
        "processed_count": (
            result["processed_count"]
        ),
        "hourly_count": result["hourly_count"],
        "commit_sha": os.environ.get(
            "DEPLOY_COMMIT_SHA",
            "unknown",
        ),
        "evidence_by_method_and_version": (
            result[
                "evidence_by_method_and_version"
            ]
        ),
    }

    s3.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=(
            json.dumps(manifest, indent=2)
            + "\n"
        ).encode("utf-8"),
        ContentType="application/json",
    )

    logger.info(
        json.dumps({
            "event": "pipeline_run_completed",
            **manifest,
        })
    )

    return manifest
