# LogSense AI

AI-powered log diagnosis and analytics platform built entirely on Snowflake. Zero external dependencies — all ingestion, search, AI inference, and incident management run natively inside Snowflake.

## Features

- **Dual Ingestion Paths** — Manual upload via Streamlit dashboard AND automated real-time streaming from live applications (e.g. Kafka) via Snowpipe
- **Universal Log Parsing** — Auto-parse any log format (Kafka, Nginx, PostgreSQL, Hadoop, OpenStack, syslog) into structured data using multi-pattern regex extraction
- **Semantic Search** — Natural language search across all ingested logs powered by Cortex Search with Arctic embeddings (`snowflake-arctic-embed-m-v1.5`)
- **AI Diagnosis** — Root cause analysis using Cortex `AI_COMPLETE` with model fallback chain (llama3.3-70b → llama3.1-70b)
- **Business Context Integration** — Service Registry joins logs with structured business data: service tier, team owner, business function, customer impact, deploy version, and PagerDuty/Jira links
- **Interactive Dashboard** — Metric cards (Total Errors, Apps Monitored, Critical Issues, Latest Ingestion) + interactive log volume timeline with severity breakdown
- **Time Range Filtering** — Zoom into error spikes with date/time range filters on all views
- **Decision-to-Action Loop** — Create incident tickets, log resolutions, export diagnosis reports, and search past fixes from resolved incidents
- **Incident Tracker** — Full ticket lifecycle management with status transitions (Open → In Progress → Resolved)
- **AI Chat Assistant** — Conversational log analysis with persistent chat history and contextual follow-ups
- **CoCo CLI Custom Agent Skills** — Four custom skills (`log-ingest`, `log-diagnose`, `log-frequency`, `log-postmortem`) for headless automation from the Cortex Code CLI

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         INGESTION LAYER                                 │
│                                                                         │
│  ┌──────────────────┐         ┌──────────────────────────────────────┐  │
│  │ Streamlit Upload │         │  Application / Kafka / Docker        │  │
│  │  (Manual)        │         │          (Automated)                 │  │
│  └────────┬─────────┘         └──────────────────┬───────────────────┘  │
│           │                                      │                      │
│           v                                      v                      │
│  ┌────────────────────┐         ┌───────────────────────────────────┐   │
│  │  PARSE_ANY_LOG SP  │         │  Snowpipe (LOG_AUTO_PIPE)         │   │
│  │  (regex parsing)   │         │  AUTO_INGEST_STAGE → RAW_LOG_     │   │
│  └────────┬───────────┘         │  LANDING                          │   │
│           │                     └──────────────────┬────────────────┘   │
│           │                                        │                    │
│           │                                        v                    │
│           │                         ┌──────────────────────────────┐    │
│           │                         │  AUTO_PARSE_TASK (5 min)     │    │
│           │                         │  (regex parse + mark done)   │    │
│           │                         └──────────────┬───────────────┘    │
│           │                                        │                    │
│           └────────────────┬───────────────────────┘                    │
│                            v                                            │
│                   ┌────────────────┐                                    │
│                   │  PARSED_LOGS   │ (central structured log store)     │
│                   └───────┬────────┘                                    │
└───────────────────────────┼─────────────────────────────────────────────┘
                            │
        ┌───────────────────┼───────────────────────────┐
        │                   │                           │
        v                   v                           v
┌───────────────┐  ┌─────────────────┐  ┌──────────────────────────┐
│ Cortex Search │  │ SERVICE_REGISTRY│  │ Direct SQL Queries       │
│ Service       │  │ (business ctx)  │  │ (metrics, timeline)      │
│ (semantic)    │  └────────┬────────┘  └──────────────────────────┘
└───────┬───────┘           │
        │                   │
        v                   v
┌──────────────────────────────────────┐
│  Cortex AI_COMPLETE                  │
│  (diagnosis + chat + classification) │
└──────────────────┬───────────────────┘
                   │
                   v
┌─────────────────────────────────────────────────────┐
│  ACTION LAYER                                       │
│  ┌────────────────┐ ┌──────────────────┐ ┌────────┐ │
│  │INCIDENT_TICKETS│ │AUDIT_RESOLUTIONS │ │ Export │ │
│  │(ticket mgmt)   │ │(past fixes)      │ │ Report │ │
│  └────────────────┘ └──────────────────┘ └────────┘ │
└─────────────────────────────────────────────────────┘
```

**Data Flow:**
1. **Manual path:** User uploads log files via Streamlit sidebar → `PARSE_ANY_LOG` procedure extracts structured fields → rows stored in `PARSED_LOGS`
2. **Automated path:** Application writes `.log`/`.txt` files to `AUTO_INGEST_STAGE` → Snowpipe (`LOG_AUTO_PIPE`) loads raw lines into `RAW_LOG_LANDING` → `AUTO_PARSE_TASK` runs every 5 minutes, parses with regex, inserts into `PARSED_LOGS`
3. Cortex Search Service indexes `PARSED_LOGS.MESSAGE` for semantic search (1-minute refresh)
4. `SERVICE_REGISTRY` provides business context (tier, team, customers, deploy info)
5. AI Complete provides root cause analysis, chat responses, and severity classification
6. Users create incident tickets, log resolutions, and search past fixes for similar issues

## Prerequisites

- Snowflake account with **Cortex AI** enabled (AI_COMPLETE, Cortex Search)
- A warehouse (e.g., `COMPUTE_WH`)
- ACCOUNTADMIN or equivalent role for initial setup
- [Cortex Code (CoCo) CLI](https://docs.snowflake.com/en/user-guide/cortex-code) (for development and custom skills)

## Quick Start

1. **Clone the repository:**
   ```bash
   git clone https://github.com/donidolo/logsense-ai.git
   cd logsense-ai
   ```

2. **Run the SQL setup scripts in order:**
   ```sql
   -- Execute in Snowsight or via CoCo CLI
   -- 01: Create database and schema
   -- 02: Create all tables (PARSED_LOGS, RAW_LOG_LANDING, SERVICE_REGISTRY, INCIDENT_TICKETS, AUDIT_RESOLUTIONS)
   -- 03: Create Cortex Search Service
   -- 04: Create all stored procedures (PARSE_ANY_LOG, INGEST_LOG, DIAGNOSE_LOGS, CHECK_ERROR_FREQUENCY, SEARCH_POSTMORTEMS)
   -- 05: Create Snowpipe and AUTO_PARSE_TASK for automated ingestion
   -- 06: Deploy Streamlit app
   ```

3. **Upload Streamlit files to stage:**
   ```sql
   PUT file://streamlit_app.py @KAFKA_LOGS.RAW.STREAMLIT_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
   PUT file://environment.yml @KAFKA_LOGS.RAW.STREAMLIT_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
   ```

4. **Open the app** in Snowsight under Streamlit Apps, or access via the generated URL.

5. **(Optional) Set up automated ingestion:**
   See [docs/AUTOMATED_INGESTION.md](docs/AUTOMATED_INGESTION.md) for the Docker Kafka + PowerShell streaming script approach.

6. **(Optional) Install CoCo CLI custom skills:**
   See [AGENTS.md](AGENTS.md) for skill definitions.

## Project Structure

```
logsense-ai/
├── README.md                    # This file
├── AGENTS.md                    # CoCo CLI custom skill definitions
├── LICENSE                      # MIT License
├── streamlit_app.py             # Main Streamlit application (full-featured)
├── environment.yml              # Python dependencies for Streamlit
├── setup/
│   ├── 01_create_database.sql   # Database and schema creation
│   ├── 02_create_tables.sql     # All table DDL
│   ├── 03_create_search_service.sql  # Cortex Search Service
│   ├── 04_create_stored_procedures.sql  # All stored procedures
│   ├── 05_create_snowpipe.sql   # Snowpipe + Task for automation
│   └── 06_deploy_streamlit.sql  # Streamlit app deployment
├── docs/
│   ├── architecture.md          # Detailed architecture documentation
│   ├── AUTOMATED_INGESTION.md   # Snowpipe streaming setup guide
│   └── screenshots/             # App screenshots
└── sample_logs/
    ├── kafka_broker.log         # Sample Kafka broker log
    └── README.md                # Sample data description
```

## Built With

- **Snowflake AI Data Cloud** — Data platform, compute, and AI runtime
- **Cortex Code (CoCo) CLI** — Development, deployment, and custom agent skills
- **Cortex Search** — Semantic search with Arctic embeddings
- **Cortex AI Complete** — LLM-powered diagnosis and chat (llama3.3-70b, llama3.1-70b)
- **Snowpipe** — Automated file ingestion from internal stages
- **Streamlit in Snowflake** — Interactive dashboard UI

## Team

Built by **Team Espada**
- Doni
- Ivana
- Yunata

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
