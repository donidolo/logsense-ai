# Automated Ingestion Setup

This guide explains how to set up automated real-time log streaming into LogSense AI using Snowpipe.

## Architecture

```
[Application / Kafka / Docker]
        │
        │  writes .log/.txt files
        v
┌─────────────────────────┐
│  AUTO_INGEST_STAGE       │  (Snowflake internal stage)
│  @KAFKA_LOGS.RAW.AUTO_  │
│  INGEST_STAGE            │
└───────────┬─────────────┘
            │
            v  (Snowpipe COPY INTO)
┌─────────────────────────┐
│  RAW_LOG_LANDING         │  (raw lines + source filename)
│  PROCESSED = FALSE       │
└───────────┬─────────────┘
            │
            v  (AUTO_PARSE_TASK every 5 min)
┌─────────────────────────┐
│  PARSED_LOGS             │  (structured: app, timestamp,
│                          │   level, component, message)
└─────────────────────────┘
```

## Components

### 1. AUTO_INGEST_STAGE

An internal Snowflake stage where log files are deposited. Any `.log` or `.txt` file placed here will be picked up by the pipe.

### 2. LOG_AUTO_PIPE

A Snowpipe that loads raw lines from files in the stage into `RAW_LOG_LANDING`. Each line becomes a row with:
- `RAW_LINE`: The full text of the log line
- `SOURCE_FILE`: The filename (used to derive `APP_NAME`)
- `LOADED_AT`: Timestamp when the row was loaded
- `PROCESSED`: Flag used by the task (starts as `FALSE`)

### 3. AUTO_PARSE_TASK

A scheduled task running every 5 minutes that:
1. Checks for unprocessed rows in `RAW_LOG_LANDING`
2. Applies regex to extract timestamp, log level, component, and message
3. Derives `APP_NAME` from the source filename
4. Inserts parsed rows into `PARSED_LOGS`
5. Marks processed rows as `PROCESSED = TRUE`

## Setup

Run `setup/05_create_snowpipe.sql` to create all three components.

## Streaming from Docker Kafka (PowerShell Script)

The recommended approach for streaming Kafka broker logs:

### Prerequisites
- Docker Desktop with a Kafka container running
- SnowSQL or Snowflake Python connector installed
- A PowerShell terminal

### PowerShell Streaming Script

```powershell
# stream_kafka_logs.ps1
# Streams Kafka broker logs to Snowflake AUTO_INGEST_STAGE in real-time

$ConnectionName = "your_connection_name"
$StagePath = "@KAFKA_LOGS.RAW.AUTO_INGEST_STAGE"
$KafkaContainer = "kafka-broker"
$LogPath = "/var/log/kafka/server.log"
$IntervalSeconds = 60

while ($true) {
    $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $localFile = "$env:TEMP\kafka_broker_${timestamp}.log"

    # Extract latest logs from Docker container
    docker logs $KafkaContainer --since "${IntervalSeconds}s" 2>&1 | Out-File -FilePath $localFile -Encoding utf8

    # Check if file has content
    if ((Get-Item $localFile).Length -gt 0) {
        # Upload to Snowflake stage
        snowsql -c $ConnectionName -q "PUT file://$localFile $StagePath AUTO_COMPRESS=FALSE OVERWRITE=FALSE;"

        # Trigger pipe refresh
        snowsql -c $ConnectionName -q "ALTER PIPE KAFKA_LOGS.RAW.LOG_AUTO_PIPE REFRESH;"

        Write-Host "[$(Get-Date)] Uploaded: kafka_broker_${timestamp}.log"
    }

    # Cleanup local temp file
    Remove-Item $localFile -ErrorAction SilentlyContinue

    Start-Sleep -Seconds $IntervalSeconds
}
```

### Running the Script

```powershell
# Start streaming (runs continuously)
.\stream_kafka_logs.ps1

# Stop with Ctrl+C
```

## Manual Pipe Operations

```sql
-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('KAFKA_LOGS.RAW.LOG_AUTO_PIPE');

-- Manually refresh pipe (pick up new files)
ALTER PIPE KAFKA_LOGS.RAW.LOG_AUTO_PIPE REFRESH;

-- Pause pipe (stop ingestion)
ALTER PIPE KAFKA_LOGS.RAW.LOG_AUTO_PIPE SET PIPE_EXECUTION_PAUSED = TRUE;

-- Resume pipe
ALTER PIPE KAFKA_LOGS.RAW.LOG_AUTO_PIPE SET PIPE_EXECUTION_PAUSED = FALSE;

-- Suspend/resume the parse task
ALTER TASK KAFKA_LOGS.RAW.AUTO_PARSE_TASK SUSPEND;
ALTER TASK KAFKA_LOGS.RAW.AUTO_PARSE_TASK RESUME;

-- Check task history
SELECT * FROM TABLE(KAFKA_LOGS.INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'AUTO_PARSE_TASK',
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())
));
```

## Monitoring

```sql
-- Files waiting to be processed
SELECT COUNT(*) FROM KAFKA_LOGS.RAW.RAW_LOG_LANDING WHERE PROCESSED = FALSE;

-- Recent ingestion activity
SELECT SOURCE_FILE, COUNT(*) AS ROWS, MAX(LOADED_AT) AS LAST_LOAD
FROM KAFKA_LOGS.RAW.RAW_LOG_LANDING
GROUP BY SOURCE_FILE
ORDER BY LAST_LOAD DESC
LIMIT 10;

-- Pipe usage credits
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
WHERE PIPE_NAME = 'LOG_AUTO_PIPE'
  AND START_TIME >= DATEADD('day', -1, CURRENT_TIMESTAMP())
ORDER BY START_TIME DESC;
```
