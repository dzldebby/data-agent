import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from dotenv import load_dotenv

project_dir = Path(__file__).resolve().parents[1]
data_dir = project_dir / "data" / "aws"
state_path = project_dir / "data" / ".last_manifest_key"

load_dotenv(project_dir / ".env")

bucket = os.environ["AWS_S3_BUCKET"]
profile = os.environ.get("AWS_PROFILE", "data-agent")
region = os.environ.get(
    "AWS_DEFAULT_REGION",
    "ap-southeast-1",
)

session = boto3.Session(
    profile_name=profile,
    region_name=region,
)
s3 = session.client("s3")


def log(message):
    timestamp = datetime.now(timezone.utc).isoformat(
        timespec="seconds"
    )
    print(f"{timestamp} {message}", flush=True)


def newest_manifest_key():
    paginator = s3.get_paginator("list_objects_v2")
    manifests = []

    for page in paginator.paginate(
        Bucket=bucket,
        Prefix="runs/",
    ):
        for item in page.get("Contents", []):
            if item["Key"].endswith(".json"):
                manifests.append(item)

    if not manifests:
        return None

    newest = max(
        manifests,
        key=lambda item: item["LastModified"],
    )
    return newest["Key"]


def previously_processed_key():
    if not state_path.exists():
        return None

    return state_path.read_text(
        encoding="utf-8"
    ).strip() or None


def download_object(key, destination):
    destination.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    s3.download_file(
        bucket,
        key,
        str(destination),
    )

    log(f"Downloaded s3://{bucket}/{key}")


def synchronize_once():
    manifest_key = newest_manifest_key()

    if manifest_key is None:
        log("No pipeline manifests found")
        return False

    if manifest_key == previously_processed_key():
        log(f"No new run; latest is {manifest_key}")
        return False

    response = s3.get_object(
        Bucket=bucket,
        Key=manifest_key,
    )
    manifest = json.loads(
        response["Body"].read().decode("utf-8")
    )

    required_fields = {
        "request_id",
        "source_key",
        "processed_key",
        "hourly_key",
        "commit_sha",
    }
    missing_fields = required_fields - manifest.keys()

    if missing_fields:
        raise ValueError(
            f"Manifest is missing fields: "
            f"{sorted(missing_fields)}"
        )

    log(
        f"Processing Lambda request "
        f"{manifest['request_id']}"
    )
    log(f"Deployed commit: {manifest['commit_sha']}")

    download_object(
        manifest["source_key"],
        data_dir / "raw_payments.csv",
    )
    download_object(
        manifest["processed_key"],
        data_dir / "processed_payments.csv",
    )
    download_object(
        manifest["hourly_key"],
        data_dir / "hourly_revenue.csv",
    )

    subprocess.run(
        [
            sys.executable,
            str(
                project_dir
                / "scripts"
                / "load_databricks.py"
            ),
        ],
        cwd=project_dir,
        check=True,
    )

    state_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    state_path.write_text(
        manifest_key + "\n",
        encoding="utf-8",
    )

    log(f"Completed synchronization: {manifest_key}")
    return True


def watch(interval):
    log(
        f"Watching s3://{bucket}/runs/ "
        f"every {interval} seconds"
    )

    while True:
        try:
            synchronize_once()
        except Exception as error:
            log(
                f"Synchronization failed: "
                f"{type(error).__name__}: {error}"
            )

        time.sleep(interval)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Continue watching for new pipeline runs",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=30,
        help="Polling interval in seconds",
    )
    arguments = parser.parse_args()

    if arguments.interval < 10:
        parser.error("--interval must be at least 10 seconds")

    if arguments.watch:
        watch(arguments.interval)
    else:
        synchronize_once()


if __name__ == "__main__":
    main()
