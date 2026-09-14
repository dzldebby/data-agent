import json
import os
import re
import subprocess
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

import boto3
from databricks import sql
from dotenv import load_dotenv
from mcp.server import MCPServer
from investigation_steps import audit_call, financial_breakdown


PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

mcp = MCPServer(
    "data-agent-evidence",
    instructions=(
        "Read-only payment incident evidence. Every tool requires the "
        "alert's run_id. Never substitute a newer run. Provider records "
        "are the reconciliation baseline; actual customer charges have "
        "not been independently verified."
    ),
)


def require_environment(*names: str) -> None:
    missing = [name for name in names if not os.getenv(name)]
    if missing:
        raise RuntimeError(
            "Missing environment variables: " + ", ".join(missing)
        )


def validate_run_id(run_id: str) -> str:
    value = str(UUID(run_id))
    if value != run_id:
        raise ValueError("run_id must be a canonical lowercase UUID.")
    return value


def serialize(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


def query_rows(query: str) -> list[dict[str, Any]]:
    require_environment(
        "DATABRICKS_HOST",
        "DATABRICKS_TOKEN",
        "DATABRICKS_WAREHOUSE_ID",
    )

    with sql.connect(
        server_hostname=(
            os.environ["DATABRICKS_HOST"]
            .removeprefix("https://")
            .rstrip("/")
        ),
        http_path=(
            "/sql/1.0/warehouses/"
            + os.environ["DATABRICKS_WAREHOUSE_ID"]
        ),
        access_token=os.environ["DATABRICKS_TOKEN"],
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            columns = [column[0] for column in cursor.description]
            return [
                dict(zip(columns, map(serialize, row)))
                for row in cursor.fetchall()
            ]


def aws_session() -> boto3.Session:
    return boto3.Session(
        profile_name=os.getenv("AWS_PROFILE") or None,
        region_name=os.getenv(
            "AWS_DEFAULT_REGION", "ap-southeast-1"
        ),
    )


def read_manifest(run_id: str) -> dict[str, Any]:
    run_id = validate_run_id(run_id)
    require_environment("AWS_S3_BUCKET")

    bucket = os.environ["AWS_S3_BUCKET"]
    key = f"runs/{run_id}/manifest.json"
    response = aws_session().client("s3").get_object(
        Bucket=bucket,
        Key=key,
    )

    with response["Body"] as body:
        manifest = json.loads(body.read().decode("utf-8"))

    recorded_id = manifest.get("run_id") or manifest.get("request_id")
    if recorded_id != run_id:
        raise RuntimeError("Manifest run identifier does not match.")

    commit = manifest.get("commit_sha")
    if not isinstance(commit, str) or not re.fullmatch(
        r"[0-9a-f]{40}", commit
    ):
        raise RuntimeError("Manifest lacks a valid full commit SHA.")

    return {
        "run_id": run_id,
        "commit_sha": commit,
        "bucket": bucket,
        "manifest_key": key,
        "manifest_last_modified": response["LastModified"].isoformat(),
        "manifest": manifest,
    }


def reconciliation_rows(run_id: str) -> list[dict[str, Any]]:
    run_id = validate_run_id(run_id)

    # Interpolation is limited to a strictly validated canonical UUID.
    rows = query_rows(
        f"""
SELECT
    run_id,
    commit_sha,
    source_key,
    payment_id,
    event_time,
    payment_method,
    provider_payload_version,
    currency,
    expected_amount_cents,
    reported_amount_cents,
    difference_cents,
    reconciliation_result
FROM workspace.default.current_payment_reconciliation
WHERE run_id = '{run_id}'
ORDER BY payment_method, provider_payload_version, payment_id
"""
    )

    if not rows:
        raise RuntimeError(
            f"Run {run_id} is unavailable in the current reconciliation "
            "view. Stop this investigation; do not substitute another run."
        )

    commits = {row["commit_sha"] for row in rows}
    if len(commits) != 1 or not next(iter(commits)):
        raise RuntimeError("Reconciliation has missing or mixed commits.")

    return rows


def money(cents: int) -> str:
    return f"{Decimal(cents) / Decimal(100):.2f}"


def summarize(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cohorts = {}

    for row in rows:
        key = (
            row["payment_method"],
            row["provider_payload_version"],
            row["currency"],
        )
        cohort = cohorts.setdefault(
            key,
            {
                "payment_method": key[0],
                "provider_payload_version": key[1],
                "currency": key[2],
                "payment_count": 0,
                "mismatch_count": 0,
                "expected_amount_cents": 0,
                "reported_amount_cents": 0,
                "difference_cents": 0,
            },
        )

        expected = row["expected_amount_cents"]
        reported = row["reported_amount_cents"]

        if expected is None or reported is None:
            raise RuntimeError(
                "A payment has a missing amount. "
                "Cannot treat an absent value as zero."
            )

        expected = int(expected)
        reported = int(reported)
        difference = expected - reported

        if row["difference_cents"] is None or (
            difference != int(row["difference_cents"])
        ):
            raise RuntimeError(
                "Stored difference disagrees with Python arithmetic."
            )

        cohort["payment_count"] += 1
        cohort["mismatch_count"] += int(expected != reported)
        cohort["expected_amount_cents"] += expected
        cohort["reported_amount_cents"] += reported
        cohort["difference_cents"] += difference

    for cohort in cohorts.values():
        expected = cohort["expected_amount_cents"]
        reported = cohort["reported_amount_cents"]
        cohort["expected_amount"] = money(expected)
        cohort["reported_amount"] = money(reported)
        cohort["difference_amount"] = money(cohort["difference_cents"])
        cohort["reported_to_expected_percent"] = (
            round(reported / expected * 100, 2)
            if expected
            else None
        )

    return list(cohorts.values())


@mcp.tool()
@audit_call
def investigate_databricks(run_id: str, level: str = "method", payment_method: str = "", provider_version: str = "") -> dict[str, Any]:
    """Start at method; choose version or samples based on evidence. Filter using returned cohort names."""
    rows = reconciliation_rows(run_id)

    pipeline_runs = query_rows(
        f"""
SELECT *
FROM workspace.default.pipeline_runs
WHERE run_id = '{validate_run_id(run_id)}'
"""
    )

    if len(pipeline_runs) != 1:
        raise RuntimeError("Expected exactly one pipeline lineage record.")

    if pipeline_runs[0]["commit_sha"] != rows[0]["commit_sha"]:
        raise RuntimeError("Pipeline and reconciliation commits disagree.")

    return {
        "source": "Databricks",
        "scope": "Full requested run",
        "run_id": run_id,
        "commit_sha": rows[0]["commit_sha"],
        "pipeline_run": pipeline_runs[0],
        **financial_breakdown(rows, level, payment_method, provider_version),
        "limitation": (
            "Provider records are the baseline. "
            "Actual customer charges are not independently verified."
        ),
    }


@mcp.tool()
@audit_call
def investigate_aws(run_id: str) -> dict[str, Any]:
    """Read the exact run's immutable S3 manifest and output metadata."""
    evidence = read_manifest(run_id)
    manifest = evidence["manifest"]
    s3 = aws_session().client("s3")
    artifacts = []

    for field in ("processed_key", "hourly_key"):
        key = manifest.get(field)

        if not isinstance(key, str) or not key.startswith(
            f"runs/{run_id}/"
        ):
            raise RuntimeError(
                f"{field} does not reference an artifact for this run."
            )

        response = s3.head_object(
            Bucket=evidence["bucket"],
            Key=key,
        )
        artifacts.append(
            {
                "key": key,
                "size_bytes": response["ContentLength"],
                "last_modified": response["LastModified"].isoformat(),
                "etag": response["ETag"],
            }
        )

    return {
        "source": "AWS S3",
        **evidence,
        "output_artifacts": artifacts,
        "step_summary": "Run manifest and output objects exist; deployed commit " + evidence["commit_sha"][:12],
        "interpretation": (
            "The run manifest and output objects exist. The pipeline "
            "writes the manifest after its outputs. This supports "
            "artifact publication, not financial correctness."
        ),
        "limitations": (
            "No CloudWatch execution status or retry history is asserted. "
            "Current Lambda configuration is not historical run evidence."
        ),
    }


def run_git(*arguments: str) -> str:
    return subprocess.run(
        ["git", *arguments],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=20,
    ).stdout.strip()


@mcp.tool()
@audit_call
def investigate_github(run_id: str, file_path: str = "") -> dict[str, Any]:
    """List changed source files first. Then inspect one returned file based on the affected cohort."""
    evidence = read_manifest(run_id)
    commit = evidence["commit_sha"]

    resolved = run_git(
        "rev-parse", "--verify", f"{commit}^{{commit}}"
    )
    if resolved != commit:
        raise RuntimeError("Git commit does not match the manifest.")

    baseline = run_git(
        "rev-parse",
        "--verify",
        "healthy-reconciliation-baseline^{commit}",
    )

    changed_paths = run_git("diff", "--name-only", baseline, commit, "--", "app", "scripts").splitlines()
    if file_path and (file_path not in changed_paths or not re.fullmatch(r"(?:app|scripts)/[A-Za-z0-9_./-]+\.py", file_path) or ".." in file_path.split("/")):
        raise ValueError("Choose a changed Python source path returned by the overview")
    diff = run_git(
        "diff",
        "--no-ext-diff",
        "--no-textconv",
        "--unified=20",
        baseline,
        commit,
        "--",
        file_path,
    ) if file_path else ""

    return {
        "source": "Local checkout of the GitHub repository",
        "run_id": run_id,
        "commit_sha": commit,
        "healthy_commit_sha": baseline,
        "changed_source_files": changed_paths,
        "step_summary": ("Reviewed deployed changes in " + file_path) if file_path else f"Identified {len(changed_paths)} changed source files; awaiting focused review.",
        "commit_metadata": run_git(
            "show", "--no-patch", "--format=fuller", commit
        ),
        "changed_files": run_git(
            "diff", "--name-status", baseline, commit
        ),
        "diff_from_healthy_baseline": diff[:20000],
        "diff_truncated": len(diff) > 20000,
    }


@mcp.tool()
@audit_call
def verify_financial_impact(run_id: str) -> dict[str, Any]:
    """Cross-check amounts using Python arithmetic for the exact run."""
    rows = reconciliation_rows(run_id)
    cohorts = summarize(rows)
    affected = [
        cohort for cohort in cohorts
        if cohort["mismatch_count"] > 0
    ]

    totals = {}
    for cohort in cohorts:
        currency = cohort["currency"]
        totals[currency] = (
            totals.get(currency, 0) + cohort["difference_cents"]
        )

    return {
        "source": "Python arithmetic over Databricks reconciliation rows",
        "scope": "Full requested run",
        "run_id": run_id,
        "commit_sha": rows[0]["commit_sha"],
        "status": "IMPACT_CONFIRMED" if affected else "NO_IMPACT",
        "step_summary": f"Python cross-check complete: {sum(c['mismatch_count'] for c in affected)} mismatched payments.",
        "totals_by_currency": [
            {
                "currency": currency,
                "difference_cents": cents,
                "difference_amount": money(cents),
            }
            for currency, cents in sorted(totals.items())
        ],
        "affected_cohorts": affected,
        "all_cohorts": cohorts,
        "limitation": (
            "Arithmetic cross-check using the same reconciliation rows; "
            "not an independent audit of original source files."
        ),
    }


if __name__ == "__main__":
    require_environment("MCP_PATH_TOKEN")
    mcp.run(
        transport="streamable-http",
        host="127.0.0.1",
        port=8000,
        streamable_http_path=(
            f"/{os.environ['MCP_PATH_TOKEN']}/mcp"
        ),
        stateless_http=True,
        json_response=True,
    )
