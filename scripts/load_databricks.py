import os
from pathlib import Path

from databricks import sql
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

project_dir = Path(__file__).resolve().parents[1]
data_dir = project_dir / "data" / "aws"

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
        print(f"Volume: {volume_table}")

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
    if not local_path.exists():
        raise FileNotFoundError(local_path)

    remote_path = (
        f"{volume_path}/{remote_name}"
    )

    workspace.files.upload_from(
        remote_path,
        str(local_path),
        overwrite=True,
    )

    print(f"Uploaded: {remote_path}")

raw_table = table_name(
    catalog,
    schema,
    "raw_payments",
)
processed_table = table_name(
    catalog,
    schema,
    "processed_payments",
)
hourly_table = table_name(
    catalog,
    schema,
    "hourly_revenue",
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

payment_select = """
SELECT
    payment_id,
    order_id,
    CAST(event_time AS TIMESTAMP) AS event_time,
    payment_method,
    provider_payload_version,
    status,
    CAST(amount_cents AS BIGINT) AS amount_cents,
    currency,
    batch_id
"""

with sql.connect(
    server_hostname=server_hostname,
    http_path=http_path,
    access_token=token,
) as connection:
    with connection.cursor() as cursor:
        for stage_name in (
            "_raw_payments_csv_stage",
            "_processed_payments_csv_stage",
            "_hourly_revenue_csv_stage",
        ):
            stage_table = table_name(
                catalog,
                schema,
                stage_name,
            )
            cursor.execute(
                f"DROP TABLE IF EXISTS "
                f"{stage_table}"
            )

        cursor.execute(
            f"""
            CREATE OR REPLACE TABLE {raw_table}
            USING DELTA
            AS
            {payment_select}
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
            CREATE OR REPLACE TABLE {processed_table}
            USING DELTA
            AS
            {payment_select}
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
            CREATE OR REPLACE TABLE {hourly_table}
            USING DELTA
            AS
            SELECT
                CAST(hour AS TIMESTAMP)
                    AS hour,
                payment_method,
                currency,
                CAST(payment_count AS BIGINT)
                    AS payment_count,
                CAST(revenue_cents AS BIGINT)
                    AS revenue_cents
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
            f"SELECT COUNT(*) FROM {raw_table}"
        )
        raw_count = cursor.fetchone()[0]

        cursor.execute(
            f"SELECT COUNT(*) "
            f"FROM {processed_table}"
        )
        processed_count = cursor.fetchone()[0]

        cursor.execute(
            f"SELECT COUNT(*) "
            f"FROM {hourly_table}"
        )
        hourly_count = cursor.fetchone()[0]

        assert raw_count == 10080
        assert processed_count == 9679
        assert hourly_count == 336

        print(f"Raw table rows: {raw_count}")
        print(
            f"Processed table rows: "
            f"{processed_count}"
        )
        print(
            f"Hourly table rows: "
            f"{hourly_count}"
        )

        cursor.execute(
            f"""
            SELECT
                payment_method,
                COUNT(*) AS payment_count,
                SUM(amount_cents)
                    AS revenue_cents
            FROM {processed_table}
            GROUP BY payment_method
            ORDER BY payment_method
            """
        )

        for (
            method,
            count,
            revenue_cents,
        ) in cursor.fetchall():
            print(
                f"{method}: {count} payments, "
                f"SGD "
                f"{revenue_cents / 100:,.2f}"
            )

        cursor.execute(
            f"""
            SELECT
                payment_method,
                provider_payload_version,
                COUNT(*) AS payment_count
            FROM {raw_table}
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
            count,
        ) in cursor.fetchall():
            print(
                f"{method} {version}: "
                f"{count} raw payments"
            )

print("Databricks load: PASSED")
