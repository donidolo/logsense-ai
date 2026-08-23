# CoCo CLI Custom Agent Skills

LogSense AI includes four custom skills for the Cortex Code (CoCo) CLI, enabling headless log analysis directly from the terminal.

## Skills Overview

| Skill | Purpose | Procedure Called |
|-------|---------|-----------------|
| `log-ingest` | Ingest log files from local filesystem | `INGEST_LOG` |
| `log-diagnose` | AI-powered diagnosis on a component/query | `DIAGNOSE_LOGS` |
| `log-frequency` | Error frequency analysis with spike detection | `CHECK_ERROR_FREQUENCY` |
| `log-postmortem` | Search past resolutions for similar issues | `SEARCH_POSTMORTEMS` |

## Skill Definitions

### log-ingest

Ingests a log file into LogSense AI from the CoCo CLI.

**Usage:**
```
/log-ingest kafka /path/to/broker.log
```

**What it does:**
1. Reads the specified file from the local filesystem
2. Calls `KAFKA_LOGS.RAW.INGEST_LOG(file_content, app_name)`
3. Returns the number of rows parsed and inserted

**Parameters:**
- `app_name` (required): Name of the application (e.g., kafka, nginx, postgresql)
- `file_path` (required): Path to the log file to ingest

---

### log-diagnose

Runs AI-powered root cause analysis on log entries matching a search query.

**Usage:**
```
/log-diagnose "connection timeout" --app kafka
```

**What it does:**
1. Calls `KAFKA_LOGS.RAW.DIAGNOSE_LOGS(search_query, app_name)`
2. Searches matching logs, builds context with Service Registry data
3. Sends to AI_COMPLETE for diagnosis
4. Returns: matched log count, root cause analysis, business impact

**Parameters:**
- `search_query` (required): Keywords or error pattern to search for
- `--app` (optional): Filter to a specific application name

---

### log-frequency

Checks error frequency over time and detects spikes for a component.

**Usage:**
```
/log-frequency kafka-server-start.sh --hours 48
```

**What it does:**
1. Calls `KAFKA_LOGS.RAW.CHECK_ERROR_FREQUENCY(component, hours_back)`
2. Groups errors by hour, calculates average, flags spikes (>2x average)
3. Returns: total errors, spike hours, average per hour, hourly breakdown

**Parameters:**
- `component` (required): Component or app name to analyze
- `--hours` (optional, default 24): How far back to look

---

### log-postmortem

Searches resolved incidents for similar error patterns to suggest fixes.

**Usage:**
```
/log-postmortem "OutOfMemoryError"
```

**What it does:**
1. Calls `KAFKA_LOGS.RAW.SEARCH_POSTMORTEMS(error_pattern)`
2. Searches `AUDIT_RESOLUTIONS` for matching root causes and fixes
3. If no direct match, uses AI to find the most relevant past resolution
4. Returns: matching resolutions with ticket IDs, root causes, and fixes applied

**Parameters:**
- `error_pattern` (required): Error pattern or keyword to search past resolutions

## Installation

These skills are defined as CoCo CLI custom skills. To use them:

1. Ensure the stored procedures exist (run `setup/04_create_stored_procedures.sql`)
2. Use the skills directly in CoCo CLI conversations — the agent will call the underlying Snowflake procedures

## Integration with Streamlit Dashboard

The same stored procedures power both the CLI skills and the Streamlit dashboard:
- `PARSE_ANY_LOG` / `INGEST_LOG` → Sidebar ingestion panel
- `DIAGNOSE_LOGS` → AI Diagnosis tab
- `CHECK_ERROR_FREQUENCY` → Error spike detection in timeline
- `SEARCH_POSTMORTEMS` → "Search Past Fixes" button in action panel
