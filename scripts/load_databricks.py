import json
import os
from pathlib import Path

from databricks import sql
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

project_dir = Path(__file__).resolve().parents[1]
data_dir = project_dir / "data" / "aws"
manifest_path = data_dir / "manifest.json"

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


def quote_identifier(value):
    escaped = value.replace("`", "``")
    return f"`{escaped}`"


def table_name(catalog, schema, table):
    return ".".join(
        quote_identifier(value)
        for value in (
            catalog,
            schema,
            table,
        )
    )


def sql_value(value):
    if value is None:
        return "NULL"

    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"

    if isinstance(value, (int, float)):
        return str(value)

    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


if not manifest_path.exists():
    raise FileNotFoundError(
        f"Missing {manifest_path}. "
        "Run sync_pipeline.py after a new "
        "Lambda execution."
    )

manifest = json.loads(
    manifest_path.read_text(
        encoding="utf-8"
    )
)

required_manifest_fields = {
    "request_id",
    "source_key",
    "processed_key",
    "hourly_key",
    "commit_sha",
    "input_count",
    "processed_count",
    "hourly_count",
}

missing_manifest_fields = (
    required_manifest_fields - manifest.keys()
)

if missing_manifest_fields:
    raise ValueError(
        "Manifest is missing fields: "
        f"{sorted(missing_manifest_fields)}"
    )

run_id = manifest["request_id"]
commit_sha = manifest["commit_sha"]
source_key = manifest["source_key"]
processed_key = manifest["processed_key"]
hourly_key = manifest["hourly_key"]
manifest_key = manifest.get(
    "manifest_key",
    "",
)
source_etag = manifest.get("source_etag")
source_size_bytes = manifest.get(
    "source_size_bytes"
)

for required_file in (
    "raw_payments.csv",
    "processed_payments.csv",
    "hourly_revenue.csv",
):
    local_path = data_dir / required_file

    if not local_path.exists():
        raise FileNotFoundError(local_path)

with sql.connect(
    server_hostname=server_hostname,
    http_path=http_path,
    access_token=token,
) as connection:
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT "
            "current_catalog(), "
            "current_schema()"
        )
        catalog, schema = cursor.fetchone()

        volume = "data_agent_files"
        volume_table = table_name(
            catalog,
            schema,
            volume,
        )

        cursor.execute(
            f"CREATE VOLUME IF NOT EXISTS "
            f"{volume_table}"
        )

        print(f"Catalog: {catalog}")
        print(f"Schema: {schema}")
        print(f"Run ID: {run_id}")
        print(f"Commit: {commit_sha}")

volume_path = (
    f"/Volumes/{catalog}/{schema}/{volume}"
)

workspace = WorkspaceClient(
    host=host,
    token=token,
)

uploads = {
    "raw_payments.csv": (
        data_dir / "raw_payments.csv"
    ),
    "processed_payments.csv": (
        data_dir / "processed_payments.csv"
    ),
    "hourly_revenue.csv": (
        data_dir / "hourly_revenue.csv"
    ),
}

for remote_name, local_path in uploads.items():
    remote_path = (
        f"{volume_path}/{remote_name}"
    )

    workspace.files.upload_from(
        remote_path,
        str(local_path),
        overwrite=True,
    )

    print(f"Uploaded: {remote_path}")

pipeline_runs = table_name(
    catalog,
    schema,
    "pipeline_runs",
)
bronze_raw = table_name(
    catalog,
    schema,
    "bronze_raw_payments",
)
silver_reported = table_name(
    catalog,
    schema,
    "silver_reported_payments",
)
silver_hourly = table_name(
    catalog,
    schema,
    "silver_lambda_hourly_revenue",
)
gold_reconciliation = table_name(
    catalog,
    schema,
    "gold_payment_reconciliation",
)
gold_hourly = table_name(
    catalog,
    schema,
    "gold_hourly_revenue",
)
current_reconciliation = table_name(
    catalog,
    schema,
    "current_payment_reconciliation",
)
current_hourly = table_name(
    catalog,
    schema,
    "current_hourly_revenue",
)

payment_schema = (
    "payment_id STRING, "
    "order_id STRING, "
    "event_time STRING, "
    "payment_method STRING, "
    "provider_payload_version STRING, "
    "status STRING, "
    "amount_cents STRING, "
    "currency STRING, "
    "batch_id STRING"
)

hourly_schema = (
    "hour STRING, "
    "payment_method STRING, "
    "currency STRING, "
    "payment_count STRING, "
    "revenue_cents STRING"
)

run_literal = sql_value(run_id)
commit_literal = sql_value(commit_sha)
source_literal = sql_value(source_key)

with sql.connect(
    server_hostname=server_hostname,
    http_path=http_path,
    access_token=token,
) as connection:
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {pipeline_runs} (
                run_id STRING,
                manifest_key STRING,
                source_key STRING,
                processed_key STRING,
                hourly_key STRING,
                commit_sha STRING,
                source_etag STRING,
                source_size_bytes BIGINT,
                input_count BIGINT,
                processed_count BIGINT,
                hourly_count BIGINT,
                ingested_at TIMESTAMP
            )
            USING DELTA
            """
        )

        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {bronze_raw} (
                run_id STRING,
                source_key STRING,
                commit_sha STRING,
                payment_id STRING,
                order_id STRING,
                event_time TIMESTAMP,
                payment_method STRING,
                provider_payload_version STRING,
                status STRING,
                raw_amount_cents BIGINT,
                currency STRING,
                batch_id STRING,
                ingested_at TIMESTAMP
            )
            USING DELTA
            """
        )

        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {silver_reported} (
                run_id STRING,
                source_key STRING,
                commit_sha STRING,
                payment_id STRING,
                order_id STRING,
                event_time TIMESTAMP,
                payment_method STRING,
                provider_payload_version STRING,
                status STRING,
                reported_amount_cents BIGINT,
                currency STRING,
                batch_id STRING,
                ingested_at TIMESTAMP
            )
            USING DELTA
            """
        )

        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {silver_hourly} (
                run_id STRING,
                commit_sha STRING,
                hour TIMESTAMP,
                payment_method STRING,
                currency STRING,
                payment_count BIGINT,
                lambda_revenue_cents BIGINT,
                ingested_at TIMESTAMP
            )
            USING DELTA
            """
        )

        for target in (
            pipeline_runs,
            bronze_raw,
            silver_reported,
            silver_hourly,
        ):
            cursor.execute(
                f"DELETE FROM {target} "
                f"WHERE run_id = {run_literal}"
            )

        cursor.execute(
            f"""
            INSERT INTO {bronze_raw}
            SELECT
                {run_literal},
                {source_literal},
                {commit_literal},
                payment_id,
                order_id,
                CAST(event_time AS TIMESTAMP),
                payment_method,
                provider_payload_version,
                status,
                CAST(amount_cents AS BIGINT),
                currency,
                batch_id,
                current_timestamp()
            FROM read_files(
                '{volume_path}/raw_payments.csv',
                format => 'csv',
                header => true,
                schema => '{payment_schema}',
                schemaEvolutionMode => 'none'
            )
            """
        )

        cursor.execute(
            f"""
            INSERT INTO {silver_reported}
            SELECT
                {run_literal},
                {source_literal},
                {commit_literal},
                payment_id,
                order_id,
                CAST(event_time AS TIMESTAMP),
                payment_method,
                provider_payload_version,
                status,
                CAST(amount_cents AS BIGINT),
                currency,
                batch_id,
                current_timestamp()
            FROM read_files(
                '{volume_path}/processed_payments.csv',
                format => 'csv',
                header => true,
                schema => '{payment_schema}',
                schemaEvolutionMode => 'none'
            )
            """
        )

        cursor.execute(
            f"""
            INSERT INTO {silver_hourly}
            SELECT
                {run_literal},
                {commit_literal},
                CAST(hour AS TIMESTAMP),
                payment_method,
                currency,
                CAST(payment_count AS BIGINT),
                CAST(revenue_cents AS BIGINT),
                current_timestamp()
            FROM read_files(
                '{volume_path}/hourly_revenue.csv',
                format => 'csv',
                header => true,
                schema => '{hourly_schema}',
                schemaEvolutionMode => 'none'
            )
            """
        )

        cursor.execute(
            f"""
            CREATE OR REPLACE TABLE {gold_reconciliation}
            USING DELTA
            AS
            SELECT
                raw.run_id,
                raw.commit_sha,
                raw.source_key,
                raw.payment_id,
                raw.order_id,
                raw.event_time,
                raw.payment_method,
                raw.provider_payload_version,
                raw.currency,
                raw.raw_amount_cents
                    AS expected_amount_cents,
                reported.reported_amount_cents,
                raw.raw_amount_cents
                    - COALESCE(
                        reported.reported_amount_cents,
                        0
                    )
                    AS difference_cents,
                CASE
                    WHEN reported.payment_id IS NULL
                        THEN 'missing'
                    WHEN raw.raw_amount_cents
                        = reported.reported_amount_cents
                        THEN 'matched'
                    ELSE 'mismatch'
                END AS reconciliation_result
            FROM {bronze_raw} AS raw
            LEFT JOIN {silver_reported} AS reported
                ON raw.run_id = reported.run_id
                AND raw.payment_id
                    = reported.payment_id
            WHERE raw.status = 'succeeded'
            """
        )

        cursor.execute(
            f"""
            CREATE OR REPLACE TABLE {gold_hourly}
            USING DELTA
            AS
            SELECT
                run_id,
                commit_sha,
                DATE_TRUNC(
                    'HOUR',
                    event_time
                ) AS hour,
                payment_method,
                provider_payload_version,
                currency,
                COUNT(*)
                    AS successful_payment_count,
                SUM(expected_amount_cents)
                    AS expected_revenue_cents,
                SUM(
                    COALESCE(
                        reported_amount_cents,
                        0
                    )
                ) AS reported_revenue_cents,
                SUM(difference_cents)
                    AS difference_cents,
                SUM(
                    CASE
                        WHEN reconciliation_result
                            <> 'matched'
                            THEN 1
                        ELSE 0
                    END
                ) AS mismatch_count
            FROM {gold_reconciliation}
            GROUP BY
                run_id,
                commit_sha,
                DATE_TRUNC(
                    'HOUR',
                    event_time
                ),
                payment_method,
                provider_payload_version,
                currency
            """
        )

        cursor.execute(
            f"""
            INSERT INTO {pipeline_runs}
            VALUES (
                {run_literal},
                {sql_value(manifest_key)},
                {source_literal},
                {sql_value(processed_key)},
                {sql_value(hourly_key)},
                {commit_literal},
                {sql_value(source_etag)},
                {sql_value(source_size_bytes)},
                {sql_value(manifest["input_count"])},
                {sql_value(manifest["processed_count"])},
                {sql_value(manifest["hourly_count"])},
                current_timestamp()
            )
            """
        )

        cursor.execute(
            f"""
            CREATE OR REPLACE VIEW {current_reconciliation}
            AS
            WITH latest_run AS (
                SELECT run_id
                FROM {pipeline_runs}
                QUALIFY ROW_NUMBER() OVER (
                    ORDER BY ingested_at DESC
                ) = 1
            )
            SELECT reconciliation.*
            FROM {gold_reconciliation}
                AS reconciliation
            INNER JOIN latest_run
                ON reconciliation.run_id
                    = latest_run.run_id
            """
        )

        cursor.execute(
            f"""
            CREATE OR REPLACE VIEW {current_hourly}
            AS
            WITH latest_run AS (
                SELECT run_id
                FROM {pipeline_runs}
                QUALIFY ROW_NUMBER() OVER (
                    ORDER BY ingested_at DESC
                ) = 1
            )
            SELECT hourly.*
            FROM {gold_hourly} AS hourly
            INNER JOIN latest_run
                ON hourly.run_id
                    = latest_run.run_id
            """
        )

        checks = {
            "raw": (
                bronze_raw,
                int(manifest["input_count"]),
            ),
            "processed": (
                silver_reported,
                int(manifest["processed_count"]),
            ),
            "lambda hourly": (
                silver_hourly,
                int(manifest["hourly_count"]),
            ),
        }

        for label, (
            target,
            expected_count,
        ) in checks.items():
            cursor.execute(
                f"""
                SELECT COUNT(*)
                FROM {target}
                WHERE run_id = {run_literal}
                """
            )
            actual_count = cursor.fetchone()[0]

            if actual_count != expected_count:
                raise RuntimeError(
                    f"{label} count mismatch: "
                    f"expected {expected_count}, "
                    f"got {actual_count}"
                )

            print(
                f"{label}: {actual_count} rows"
            )

        cursor.execute(
            f"""
            SELECT
                payment_method,
                provider_payload_version,
                COUNT(*) AS payment_count,
                SUM(expected_amount_cents)
                    AS expected_cents,
                SUM(
                    COALESCE(
                        reported_amount_cents,
                        0
                    )
                ) AS reported_cents,
                SUM(difference_cents)
                    AS difference_cents,
                SUM(
                    CASE
                        WHEN reconciliation_result
                            <> 'matched'
                            THEN 1
                        ELSE 0
                    END
                ) AS mismatches
            FROM {gold_reconciliation}
            WHERE run_id = {run_literal}
            GROUP BY
                payment_method,
                provider_payload_version
            ORDER BY
                payment_method,
                provider_payload_version
            """
        )

        for (
            method,
            version,
            payment_count,
            expected_cents,
            reported_cents,
            difference_cents,
            mismatches,
        ) in cursor.fetchall():
            print(
                f"{method} {version}: "
                f"{payment_count} payments, "
                f"expected SGD "
                f"{expected_cents / 100:,.2f}, "
                f"reported SGD "
                f"{reported_cents / 100:,.2f}, "
                f"difference SGD "
                f"{difference_cents / 100:,.2f}, "
                f"mismatches {mismatches}"
            )

print("Databricks lineage load: PASSED")
