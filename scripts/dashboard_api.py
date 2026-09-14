import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from databricks import sql
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from investigation_steps import read_steps


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_DIR = PROJECT_ROOT / "dashboard"
DATA_DIR = PROJECT_ROOT / "data"
ALERT_PATH = DATA_DIR / "anomaly_status.json"
INVESTIGATIONS_DIR = DATA_DIR / "investigations"

load_dotenv(PROJECT_ROOT / ".env", override=True)

app = FastAPI(
    title="Payment Revenue Incident Console",
    version="1.0.0",
)


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def serialize(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()

    if hasattr(value, "asDict"):
        return value.asDict()

    return value


def databricks_connection() -> Any:
    required = [
        "DATABRICKS_HOST",
        "DATABRICKS_TOKEN",
        "DATABRICKS_WAREHOUSE_ID",
    ]
    missing = [
        name
        for name in required
        if not os.getenv(name)
    ]

    if missing:
        raise RuntimeError(
            "Missing environment variables: "
            + ", ".join(missing)
        )

    hostname = (
        os.environ["DATABRICKS_HOST"]
        .removeprefix("https://")
        .rstrip("/")
    )

    return sql.connect(
        server_hostname=hostname,
        http_path=(
            "/sql/1.0/warehouses/"
            + os.environ["DATABRICKS_WAREHOUSE_ID"]
        ),
        access_token=os.environ["DATABRICKS_TOKEN"],
    )


def query_rows(statement: str) -> list[dict[str, Any]]:
    with databricks_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(statement)
            columns = [
                column[0]
                for column in cursor.description
            ]

            return [
                {
                    column: serialize(value)
                    for column, value in zip(columns, row)
                }
                for row in cursor.fetchall()
            ]


def find_report(directory: Path) -> Path | None:
    preferred_names = [
        "report.md",
        "final_report.md",
        "incident_report.md",
    ]

    for name in preferred_names:
        candidate = directory / name
        if candidate.exists():
            return candidate

    markdown_files = sorted(directory.glob("*.md"))

    if markdown_files:
        return markdown_files[0]

    return None


def tool_activity(events_path: Path) -> dict[str, str]:
    statuses = {
        "investigate_databricks": "waiting",
        "investigate_aws": "waiting",
        "investigate_github": "waiting",
        "verify_financial_impact": "waiting",
    }

    if not events_path.exists():
        return statuses

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            name = value.get("name")
            status = value.get("status")

            if name in statuses:
                statuses[name] = str(
                    status or "in_progress"
                )

            for child in value.values():
                visit(child)

        elif isinstance(value, list):
            for child in value:
                visit(child)

    for line in events_path.read_text(
        encoding="utf-8",
        errors="replace",
    ).splitlines():
        if not line.strip():
            continue

        try:
            visit(json.loads(line))
        except json.JSONDecodeError:
            continue

    return statuses


def investigation_snapshot(
    run_id: str | None,
) -> dict[str, Any]:
    if not run_id:
        return {
            "exists": False,
            "run_id": None,
            "state": {},
            "tools": {},
            "report": None,
        }

    directory = INVESTIGATIONS_DIR / run_id

    if not directory.exists():
        return {
            "exists": False,
            "run_id": run_id,
            "state": {},
            "tools": {},
            "report": None,
        }

    state = read_json(directory / "state.json")
    events_path = directory / "events.jsonl"
    report_path = find_report(directory)

    report = None
    if report_path:
        report = report_path.read_text(
            encoding="utf-8",
            errors="replace",
        )

    tools = tool_activity(events_path)

    steps = read_steps(run_id)
    for step in steps:
        if step["tool"] in tools:
            tools[step["tool"]] = step["status"]

    return {
        "exists": True,
        "run_id": run_id,
        "state": state,
        "tools": tools,
        "steps": steps,
        "report": report,
        "directory": str(directory),
    }


REVENUE_QUERY = """
SELECT
    hour,
    payment_method,
    SUM(expected_revenue_cents) AS expected_revenue_cents,
    SUM(reported_revenue_cents) AS reported_revenue_cents,
    SUM(difference_cents) AS difference_cents,
    SUM(mismatch_count) AS mismatch_count
FROM workspace.default.current_hourly_revenue
GROUP BY
    hour,
    payment_method
ORDER BY
    hour,
    payment_method
"""


@app.get("/")
def index() -> FileResponse:
    index_path = DASHBOARD_DIR / "index.html"

    if not index_path.exists():
        raise HTTPException(
            status_code=404,
            detail=(
                "dashboard/index.html has not been created yet"
            ),
        )

    return FileResponse(index_path)


@app.get("/api/health")
def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "checked_at": datetime.now(
            timezone.utc
        ).isoformat(),
    }


@app.get("/api/dashboard")
def dashboard_data() -> dict[str, Any]:
    alert = read_json(ALERT_PATH)
    run_id = alert.get("run_id")

    try:
        revenue = query_rows(REVENUE_QUERY)
        databricks_error = None
    except Exception as error:
        revenue = []
        databricks_error = (
            f"{type(error).__name__}: {error}"
        )

    return {
        "generated_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "alert": alert,
        "revenue": revenue,
        "investigation": investigation_snapshot(run_id),
        "databricks_error": databricks_error,
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host="127.0.0.1",
        port=8080,
        reload=False,
    )
