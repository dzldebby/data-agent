# Adaptive investigation

This feature keeps the existing managed session and four tool names. The tools now expose smaller checks so an investigator can choose its next request from actual evidence.

1. Three investigators start. Databricks compares payment methods, AWS reads the run artifacts, and Git lists changed source files.
2. The Databricks investigator selects an affected method for a provider-version breakdown, then optionally inspects up to five mismatched samples. Healthy groups need no sample inspection.
3. The main agent receives the affected cohort and sends a focused follow-up to the existing Git investigator. That investigator chooses one of the returned changed Python files for review.
4. The main agent waits for the completed investigations and runs the Python financial cross-check before reporting.

There is no programmed rule that chooses card, v2 or `app/transform.py`. The model selects the next check. Agent instructions request the follow-up sequence; they are not a deterministic enforcement mechanism. A live session must verify that the model follows it.

## Visible evidence

The dashboard's Evidence checks list comes from tool-side records, including requests executed by subagents. It shows tool arguments, execution status and brief summaries derived from returned data. It does not display hidden reasoning or invent a successful tool execution because a report exists.

`data/evidence_calls.sqlite3` stores those records locally. Each run has a persistent ceiling of 12 admitted evidence calls, including failed calls. Non-verification calls stop after 11 so the final slot is available for verification. This limit survives an MCP restart and uses SQLite transactions for concurrent requests on the same host. It does not cap model tokens, session duration or total API charges, and is not a distributed rate limiter.

## Run on the existing demo host

After integrating the feature with your latest Linux source, restart both the MCP server and dashboard. The MCP server and dashboard must use the same checkout so they share the evidence ledger. Keep the existing `.env` private and unchanged.

```bash
python -m unittest discover -s tests -v
python scripts/run_agents.py --check
```

Use a fresh pipeline run through the existing incident injection workflow, then run the watcher. Existing attempt directories still prevent automatic reinvestigation; do not erase them to replay a paid run.

```bash
python scripts/watch_incidents.py --process-current
```

## Live acceptance check

- The first Databricks request has `level=method`.
- Subsequent filters match cohorts returned by previous checks.
- Git begins with an overview; focused review uses a returned changed source path.
- Inspect the saved session and subagent items to confirm the main agent sent the observed cohort to the existing Git investigator. The evidence ledger proves tool execution, not who instructed whom.
- Exactly three unique subagents are created, with the Git follow-up using an existing one.
- Python verification and report amounts agree with the underlying run.
- A healthy run causes no agent session; missing evidence produces an incomplete report.

Offline fixtures cover alternative affected cohorts, healthy data, invalid filters and concurrent budget enforcement. They do not establish live model behavior or SDK compatibility; run the MCP preflight and one bounded live incident on the configured Linux host.
