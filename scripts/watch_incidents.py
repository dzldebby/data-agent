import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
ALERT_PATH = PROJECT_ROOT / "data" / "anomaly_status.json"

sys.path.insert(0, str(SCRIPTS_DIR))

import sync_pipeline


def log(message: str) -> None:
    timestamp = datetime.now(
        timezone.utc
    ).isoformat(timespec="seconds")
    print(f"{timestamp} {message}", flush=True)


def run_script(script_name: str, *arguments: str) -> None:
    subprocess.run(
        [
            sys.executable,
            str(SCRIPTS_DIR / script_name),
            *arguments,
        ],
        cwd=PROJECT_ROOT,
        check=True,
    )


def detect_and_investigate() -> None:
    log("Running anomaly detection")
    run_script("detect_anomaly.py")

    alert = json.loads(
        ALERT_PATH.read_text(encoding="utf-8")
    )

    run_id = alert.get("run_id")
    status = alert.get("status")

    log(f"Detector status for {run_id}: {status}")

    if status == "healthy":
        log("No agent investigation required")
        return

    if status != "anomaly":
        raise RuntimeError(
            f"Unexpected anomaly status: {status!r}"
        )

    log(f"Starting agent investigation for {run_id}")
    run_script("run_agents.py", "--run")
    log(f"Agent workflow finished for {run_id}")


def process_new_run() -> bool:
    synchronized = sync_pipeline.synchronize_once()

    if not synchronized:
        return False

    detect_and_investigate()
    return True


def watch(interval: int) -> None:
    log(
        "Watching AWS pipeline manifests "
        f"every {interval} seconds"
    )

    while True:
        try:
            process_new_run()
        except KeyboardInterrupt:
            raise
        except Exception as error:
            log(
                "Workflow failed: "
                f"{type(error).__name__}: {error}"
            )

        time.sleep(interval)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Synchronize new AWS runs, detect anomalies, "
            "and start one agent investigation per run."
        )
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Continue polling for new AWS pipeline runs.",
    )
    parser.add_argument(
        "--process-current",
        action="store_true",
        help=(
            "Run detection and duplicate-safe investigation "
            "for the current Databricks run without syncing."
        ),
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=30,
        help="Polling interval in seconds.",
    )
    arguments = parser.parse_args()

    if arguments.interval < 10:
        parser.error("--interval must be at least 10 seconds.")

    if arguments.watch and arguments.process_current:
        parser.error(
            "--watch and --process-current cannot be combined."
        )

    if arguments.process_current:
        detect_and_investigate()
        return

    if arguments.watch:
        watch(arguments.interval)
        return

    if not process_new_run():
        log("No new pipeline run to process")


if __name__ == "__main__":
    main()
