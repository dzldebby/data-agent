"""Bounded evidence calls and a factual dashboard trail (no model reasoning)."""
import functools
import json
import sqlite3
from contextlib import contextmanager, closing
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

ROOT = Path(__file__).resolve().parents[1]
STORE = ROOT / "data" / "evidence_calls.sqlite3"
CALL_LIMIT = 12


def canonical_run(value):
    if not isinstance(value, str) or str(UUID(value)) != value:
        raise ValueError("Expected a canonical run UUID")
    return value


@contextmanager
def connect():
    STORE.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(STORE, timeout=10)
    db.execute("""CREATE TABLE IF NOT EXISTS calls (
        id INTEGER PRIMARY KEY, run_id TEXT NOT NULL, tool TEXT NOT NULL,
        arguments TEXT NOT NULL, status TEXT NOT NULL, started_at TEXT NOT NULL,
        summary TEXT NOT NULL DEFAULT '')""")
    try:
        with db:
            yield db
    finally:
        db.close()


def audit_call(function):
    @functools.wraps(function)
    def wrapped(run_id, *args, **kwargs):
        import inspect
        bound = inspect.signature(function).bind(run_id, *args, **kwargs)
        bound.apply_defaults()
        canonical_run(run_id)
        arguments = dict(bound.arguments)
        arguments.pop("run_id")
        with connect() as db:
            db.execute("BEGIN IMMEDIATE")
            count = db.execute(
                "SELECT COUNT(*) FROM calls WHERE run_id=?", (run_id,)
            ).fetchone()[0]
            # Reserve the last call for the main agent's financial check.
            limit = CALL_LIMIT if function.__name__ == "verify_financial_impact" else CALL_LIMIT - 1
            if count >= limit:
                raise RuntimeError("Evidence call budget exhausted; report incomplete findings.")
            call_id = db.execute(
                "INSERT INTO calls(run_id,tool,arguments,status,started_at) VALUES(?,?,?,?,?)",
                (run_id, function.__name__, json.dumps(arguments), "in_progress",
                 datetime.now(timezone.utc).isoformat()),
            ).lastrowid
        try:
            result = function(run_id, *args, **kwargs)
            summary = result.get("step_summary", "Evidence returned; review investigator findings.")
        except Exception:
            with connect() as db:
                db.execute("UPDATE calls SET status='failed',summary=? WHERE id=?",
                           ("Evidence check failed", call_id))
            raise
        with connect() as db:
            db.execute("UPDATE calls SET status='completed',summary=? WHERE id=?",
                       (summary, call_id))
        return result
    return wrapped


def read_steps(run_id):
    canonical_run(run_id)
    if not STORE.exists():
        return []
    # Read-only: the dashboard never creates or changes audit records.
    with closing(sqlite3.connect(STORE.as_uri() + "?mode=ro", uri=True)) as db:
        db.row_factory = sqlite3.Row
        rows = db.execute(
            "SELECT id,tool,arguments,status,started_at,summary FROM calls WHERE run_id=? ORDER BY id",
            (run_id,),
        ).fetchall()
    return [{**dict(row), "arguments": json.loads(row["arguments"])} for row in rows]


def financial_breakdown(rows, level, payment_method="", provider_version=""):
    if level not in ("method", "version", "samples"):
        raise ValueError("level must be method, version or samples")
    if level in ("version", "samples") and not payment_method:
        raise ValueError("Choose a payment_method from the previous evidence")
    if level == "samples" and not provider_version:
        raise ValueError("Choose a provider_version from the previous evidence")
    selected = [r for r in rows if
                (not payment_method or r["payment_method"] == payment_method) and
                (not provider_version or r["provider_payload_version"] == provider_version)]
    if not selected:
        raise ValueError("No records match the requested cohort")
    if level == "samples":
        mismatches = [r for r in selected if r["expected_amount_cents"] != r["reported_amount_cents"]]
        return {"samples": mismatches[:5], "mismatch_count": len(mismatches),
                "step_summary": f"Inspected {payment_method}/{provider_version}: {len(mismatches)} mismatches; returned up to 5 samples."}
    groups = {}
    for row in selected:
        key = (row["payment_method"], row["currency"],
               row["provider_payload_version"] if level == "version" else "")
        group = groups.setdefault(key, {"payment_method": key[0], "currency": key[1],
            **({"provider_payload_version": key[2]} if level == "version" else {}),
            "payment_count": 0, "mismatch_count": 0,
            "expected_amount_cents": 0, "reported_amount_cents": 0})
        expected, reported = int(row["expected_amount_cents"]), int(row["reported_amount_cents"])
        group["payment_count"] += 1
        group["mismatch_count"] += int(expected != reported)
        group["expected_amount_cents"] += expected
        group["reported_amount_cents"] += reported
    for group in groups.values():
        group["difference_cents"] = group["expected_amount_cents"] - group["reported_amount_cents"]
    return {"groups": list(groups.values()),
            "step_summary": "Compared " + "; ".join(
                f"{g['payment_method']}/{g.get('provider_payload_version', 'all versions')} ({g['currency']}): {g['mismatch_count']} mismatches"
                for g in groups.values())}
