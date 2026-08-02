# Architecture

## Overview

LogSense AI is a fully Snowflake-native log analytics platform. All processing, storage, search, and AI inference run inside Snowflake with zero external dependencies.

## Components

### 1. Data Layer (`KAFKA_LOGS.RAW.PARSED_LOGS`)

Central table storing all parsed log entries with:
- Auto-incrementing `LOG_ID`
- `APP_NAME` for multi-application support
- Extracted `TIMESTAMP`, `LOG_LEVEL`, `COMPONENT`, `MESSAGE`
- `INGESTED_AT` for tracking ingestion time

### 2. Ingestion Layer (`PARSE_ANY_LOG` Stored Procedure)

A SQL stored procedure that:
- Splits raw log file content into individual lines
- Applies multiple regex patterns to extract structured fields
- Handles various timestamp formats (ISO 8601, syslog, bracketed)
- Auto-detects log levels (TRACE through FATAL)
- Identifies components from common log formats

### 3. Search Layer (Cortex Search Service)

- Uses `snowflake-arctic-embed-m-v1.5` for vector embeddings
- Indexes the `MESSAGE` column for semantic search
- Filters on `APP_NAME`, `LOG_LEVEL`, `COMPONENT`
- Auto-refreshes incrementally every 1 minute

### 4. AI Diagnosis Layer (Cortex AI Complete)

- Multi-model fallback: Mistral Large 2 -> Llama 3.1 70B -> Claude 3.5 Sonnet
- Structured output: ROOT CAUSE, SEVERITY, IMPACT, FIX STEPS, PREVENTION
- Context-aware analysis using search results + user query

### 5. Presentation Layer (Streamlit in Snowflake)

- Sidebar: File upload and ingestion with progress tracking
- Main: Search, filter, paginated results, severity charts, AI diagnosis
- Runs on `COMPUTE_WH` warehouse

## Data Flow Diagram

```
[Log Files] --> [Streamlit Upload] --> [PARSE_ANY_LOG SP]
                                             |
                                             v
                                      [PARSED_LOGS Table]
                                             |
                              +--------------+--------------+
                              |                             |
                              v                             v
                    [Cortex Search Service]        [Direct SQL Queries]
                              |                             |
                              v                             v
                    [Semantic Search Results]      [App Filter / Stats]
                              |
                              v
                    [Cortex AI Complete]
                              |
                              v
                    [Diagnosis Report]
```
