# LogSense AI

AI-powered log diagnosis and analytics platform built on Snowflake Cortex Code.

## Features

- **Universal Log Ingestion** - Upload any log file (Kafka, Nginx, PostgreSQL, etc.) and auto-parse into structured data using regex pattern matching
- **Semantic Search** - Natural language search across all ingested logs powered by Cortex Search with Arctic embeddings
- **AI Diagnosis** - One-click root cause analysis using Cortex AI Complete (Mistral Large, Llama 3.1, Claude 3.5 Sonnet)
- **Severity Classification** - Automatic severity scoring (Critical/High/Medium/Low) based on log patterns
- **Multi-App Filtering** - Filter and analyze logs across multiple applications
- **Interactive Dashboard** - Paginated results, severity breakdown charts, and real-time ingestion progress

## Architecture

```
                    +-------------------+
                    |   Streamlit App   |
                    | (LogSense AI UI)  |
                    +--------+----------+
                             |
              +--------------+--------------+
              |              |              |
    +---------v--+   +------v------+  +----v--------+
    | PARSE_ANY  |   |   CORTEX    |  |   CORTEX    |
    | _LOG (SP)  |   |   SEARCH    |  | AI_COMPLETE |
    +-----+------+   +------+------+  +-------------+
          |                  |
          v                  v
    +-----+------------------+------+
    |      KAFKA_LOGS.RAW           |
    |        PARSED_LOGS            |
    +-------------------------------+
```

**Data Flow:**
1. User uploads log files via the Streamlit sidebar
2. `PARSE_ANY_LOG` procedure extracts timestamps, log levels, components, and messages using regex
3. Parsed rows are stored in `PARSED_LOGS` table
4. Cortex Search Service indexes messages for semantic search (auto-refreshed every 1 minute)
5. AI Complete provides root cause analysis and actionable fix steps

## Prerequisites

- Snowflake account with **Cortex AI** enabled (AI_COMPLETE, Cortex Search)
- A warehouse (e.g., `COMPUTE_WH`)
- ACCOUNTADMIN or equivalent role for initial setup
- [Cortex Code (CoCo) CLI](https://docs.snowflake.com/en/user-guide/cortex-code) (optional, for deployment)

## Quick Start

1. **Clone the repository:**
   ```bash
   git clone https://github.com/donidolo/logsense-ai.git
   cd logsense-ai
   ```

2. **Run the SQL setup scripts in order:**
   ```sql
   -- Execute in Snowsight or via CoCo CLI
   -- Script 01: Create database and schema
   -- Script 02: Create PARSED_LOGS table
   -- Script 03: Create Cortex Search Service
   -- Script 04: Create PARSE_ANY_LOG stored procedure
   -- Script 05: Deploy Streamlit app
   ```

3. **Upload Streamlit files to stage:**
   ```sql
   PUT file://streamlit_app.py @KAFKA_LOGS.RAW.STREAMLIT_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
   PUT file://environment.yml @KAFKA_LOGS.RAW.STREAMLIT_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
   ```

4. **Open the app** in Snowsight under Streamlit Apps, or access via the generated URL.

5. **(Optional) Load sample data:**
   Upload `sample_logs/kafka_broker.log` through the app's sidebar ingestion panel with App Name: `kafka`

## Built With

- **Snowflake AI Data Cloud** - Data platform and compute
- **Cortex Code (CoCo) CLI** - Development and deployment tooling
- **Cortex Search** - Semantic search with Arctic embeddings
- **Cortex AI Complete** - LLM-powered diagnosis (Mistral Large 2, Llama 3.1 70B, Claude 3.5 Sonnet)
- **Streamlit in Snowflake** - Interactive dashboard UI

## Team

Built by **Team Espada**
- Doni
- Ivana
- Yunata

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
