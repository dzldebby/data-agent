# Setup and operation

## Prerequisites

Use Python 3.12, Git and an isolated virtual environment. Full operation also requires AWS CLI credentials, an S3 bucket, a configured Python Lambda function, Databricks SQL warehouse access, and OpenAI access to the managed Agents API and selected model.

Cloud resources were configured separately during development. This repository currently does not include infrastructure-as-code or a complete automated provisioning script.

## Local environment

Run from the repository root on Ubuntu:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
cp .env.example .env
git fetch --tags
python scripts/generate_payments.py
```

Fill in `.env` locally. Never commit its values. The dependency file lists direct dependencies; it is not a lockfile exported from the original demo environment. Confirm that the installed OpenAI package exposes `client.beta.agents.sessions` and the selected model is available to your account before a paid run.

The default branch contains the deliberate incident transformation. `run_local_pipeline.py` checks healthy baseline equality and therefore fails on that branch by design. For an isolated healthy comparison:

```bash
git worktree add --detach ../data-agent-healthy healthy-reconciliation-baseline
cd ../data-agent-healthy
python scripts/generate_payments.py
python scripts/run_local_pipeline.py
```

## Cloud pipeline requirements

- Lambda handler: `app.lambda_handler.lambda_handler`, Python 3.12. Package the `app/` directory at the ZIP root so imports resolve.
- Set Lambda's `DEPLOY_COMMIT_SHA` to the full SHA of the packaged code, rather than the current checkout if those differ.
- The execution role needs access to the source and output objects in the demo bucket and appropriate logging permissions.
- Invocation must supply the S3 event shape consumed by `lambda_handler`. Uploading a file triggers processing only if an S3 notification has separately been configured.
- Successful processing writes per-run outputs and a manifest under `runs/`. Publish new inputs under unique keys to preserve evidence.
- Databricks access must allow the loader to create its volume, tables and views in `workspace.default`. Review `load_databricks.py` before using an existing workspace; it writes and replaces demo analytical tables.

After a new Lambda run exists:

```bash
python scripts/sync_pipeline.py
python scripts/detect_anomaly.py
```

Check the printed run ID, commit SHA and reconciliation totals before starting agents.

## Evidence server and public connection

Generate a path token, copy it into `MCP_PATH_TOKEN` in `.env`, and keep it private:

```bash
python -c 'import secrets; print(secrets.token_urlsafe(32))'
python scripts/evidence_mcp.py
```

Leave that process running. In a separate terminal, start a tunnel if using Cloudflare:

```bash
cloudflared tunnel --url http://127.0.0.1:8000 --http-host-header 127.0.0.1:8000
```

Set `MCP_PUBLIC_URL` in `.env` to the new HTTPS hostname followed by `/<your-path-token>/mcp`. Wait for the tunnel to connect and its hostname to resolve. Each new quick tunnel can have a different hostname. Keep the full tokenized URL out of screenshots and commits; this demo uses a secret path rather than a complete production authentication system.

## Preflight, investigation and dashboard

In another activated terminal at the project root:

```bash
python scripts/run_agents.py --check
python scripts/watch_incidents.py --process-current
```

The check lists MCP tools and validates required `run_id` parameters. It does not run an investigation or establish model access. Processing the current run starts a paid agent session only when the alert is anomalous and no attempt directory exists.

For continuous operation, use the watcher instead of repeatedly processing the current run:

```bash
python scripts/watch_incidents.py --watch --interval 30
```

In a separate terminal:

```bash
python scripts/dashboard_api.py
```

The dashboard listens on port 8080. When viewing from Windows, run this in **Windows PowerShell**, replacing the hostname with your Linux host:

```powershell
ssh -N -L 8081:127.0.0.1:8080 debby@YOUR_LINUX_HOST
```

Keep the SSH process open and visit `http://127.0.0.1:8081/` on Windows. The dashboard binds only to Linux localhost (`127.0.0.1`), so remote access uses the SSH tunnel. Restart an already running dashboard after updating the code for the new binding to take effect.

## Reports and recovery

Each attempt is retained under `data/investigations/<run_id>/`, including the alert, state, redacted events and, when saved successfully, `report.md`.

`SKIPPED` means an attempt already exists, not that a report necessarily succeeded. Inspect `state.json`. If the agent finished but local report saving failed, inspect the stored session items using the retained session ID. Do not delete the attempt directory simply to repeat paid work.

Stop the watcher, dashboard, MCP server and tunnel with Ctrl+C in their respective terminals when finished. Cloud storage and any independently configured cloud processes remain in the account; stopping local processes does not delete those resources.
