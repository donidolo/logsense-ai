# Architecture

## Overview

LogSense AI is a fully Snowflake-native log analytics platform. All ingestion, parsing, search, AI inference, and incident management run inside Snowflake with zero external dependencies.

## Components

### 1. Ingestion Layer (Dual-Path)

**Manual Path (Streamlit Upload):**
- User uploads `.log`/`.txt`/`.csv` files via the dashboard sidebar
- `PARSE_ANY_LOG` stored procedure splits content into lines, applies multi-format regex extraction
- Parsed rows written directly to `PARSED_LOGS` with `INGESTED_AT = CURRENT_TIMESTAMP()`

**Automated Path (Snowpipe Streaming):**
- External applications (Kafka, Docker containers, PowerShell scripts) write log files to `@AUTO_INGEST_STAGE`
- `LOG_AUTO_PIPE` (Snowpipe) continuously loads raw lines into `RAW_LOG_LANDING`
- `AUTO_PARSE_TASK` runs every 5 minutes:
  - Picks up rows where `PROCESSED = FALSE`
  - Applies regex parsing (timestamp, log level, component extraction)
  - Inserts structured rows into `PARSED_LOGS`
  - Marks source rows as `PROCESSED = TRUE`

### 2. Data Layer

| Table | Purpose |
|-------|---------|
| `PARSED_LOGS` | Central structured log store (all parsed entries) |
| `RAW_LOG_LANDING` | Staging table for Snowpipe raw ingestion |
| `SERVICE_REGISTRY` | Business context: tier, team, function, customers, deploy info |
| `INCIDENT_TICKETS` | Ticket lifecycle management (Open → In Progress → Resolved) |
| `AUDIT_RESOLUTIONS` | Past fix records for postmortem search |

### 3. Search Layer (Cortex Search Service)

- Service: `LOG_SEARCH_SERVICE`
- Embedding model: `snowflake-arctic-embed-m-v1.5`
- Search column: `MESSAGE`
- Attribute filters: `APP_NAME`, `LOG_LEVEL`, `COMPONENT`
- Refresh: Incremental, 1-minute target lag
- Used by: keyword search, AI diagnosis context retrieval, chat assistant

### 4. AI Layer (Cortex AI Complete)

- **Diagnosis:** Structured output — ROOT CAUSE, SEVERITY, IMPACT, FIX STEPS, PREVENTION
- **Chat Assistant:** Conversational analysis with persistent history
- **Severity Classification:** Auto-classifies diagnosis severity (Critical/High/Medium/Low)
- **Model fallback chain:** llama3.3-70b → llama3.1-70b
- **Context building:** Search results + Service Registry business data + user query

### 5. Business Context Layer (Service Registry)

The `SERVICE_REGISTRY` table enriches log analysis with:
- Service tier (Tier-1/2/3) for priority classification
- Team owner for routing
- Business function for impact assessment
- Customer impact scope
- Last deploy version/date for correlation
- Jira project key and PagerDuty service ID for integration

### 6. Action Layer

- **Incident Tickets:** Create tickets from diagnosis, manage status lifecycle
- **Audit Resolutions:** Log resolution notes, link to tickets, enable postmortem search
- **Export Reports:** Download diagnosis reports for external sharing
- **Past Fix Search:** AI-powered search across resolved incidents for similar patterns

### 7. Presentation Layer (Streamlit in Snowflake)

- **Sidebar:** File upload, app name input, ingestion progress
- **Dashboard tab:** Metric cards, severity timeline chart, log volume by app
- **Search & Diagnose tab:** Semantic search, AI diagnosis with business context panel
- **Incident Tracker tab:** Ticket list, status management, resolution logging
- **AI Chat tab:** Conversational interface for iterative log analysis

### 8. CoCo CLI Agent Skills

Four custom skills for headless automation:
- `log-ingest`: Ingest log files from local filesystem
- `log-diagnose`: Run AI diagnosis on a component or search query
- `log-frequency`: Check error frequency and detect spikes
- `log-postmortem`: Search past resolutions for similar issues

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                      INGESTION                                   │
│                                                                 │
│   [Streamlit Upload]          [Kafka / Docker / Script]         │
│         │                              │                        │
│         v                              v                        │
│   PARSE_ANY_LOG SP            AUTO_INGEST_STAGE                 │
│         │                              │                        │
│         │                     LOG_AUTO_PIPE (Snowpipe)           │
│         │                              │                        │
│         │                     RAW_LOG_LANDING                    │
│         │                              │                        │
│         │                     AUTO_PARSE_TASK (5 min)            │
│         │                              │                        │
│         └──────────┬───────────────────┘                        │
│                    v                                             │
│             PARSED_LOGS                                          │
└────────────────────┼────────────────────────────────────────────┘
                     │
     ┌───────────────┼───────────────────────┐
     │               │                       │
     v               v                       v
 Cortex Search   SERVICE_REGISTRY      SQL Aggregations
 Service         (business context)    (metrics/timeline)
     │               │
     └───────┬───────┘
             v
      AI_COMPLETE (llama3.3-70b)
             │
     ┌───────┼───────────────┐
     v       v               v
 Diagnosis  Chat         Severity
 Report     Response     Classification
     │
     v
 ┌────────────────────────────────────┐
 │  INCIDENT_TICKETS                   │
 │  AUDIT_RESOLUTIONS                  │
 │  Export / Download                   │
 └────────────────────────────────────┘
```
