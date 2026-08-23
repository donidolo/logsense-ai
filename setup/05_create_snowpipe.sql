-- LogSense AI: Automated Ingestion (Snowpipe + Task)
-- Creates the stage, pipe, and scheduled task for real-time log streaming.

USE DATABASE KAFKA_LOGS;
USE SCHEMA RAW;

-- Internal stage for automated log file ingestion
CREATE STAGE IF NOT EXISTS AUTO_INGEST_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Internal stage for automated log ingestion. Drop .log/.txt files here.';

-- Snowpipe: loads raw log lines from stage into RAW_LOG_LANDING
CREATE OR REPLACE PIPE LOG_AUTO_PIPE
    AUTO_INGEST = FALSE
    COMMENT = 'Ingests .log/.txt files from AUTO_INGEST_STAGE into RAW_LOG_LANDING.'
AS
COPY INTO KAFKA_LOGS.RAW.RAW_LOG_LANDING (RAW_LINE, SOURCE_FILE)
FROM (
    SELECT $1, METADATA$FILENAME::VARCHAR
    FROM @KAFKA_LOGS.RAW.AUTO_INGEST_STAGE
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = NONE RECORD_DELIMITER = '\n' SKIP_HEADER = 0 ENCODING = 'ISO-8859-1')
PATTERN = '.*\.(log|txt)';

-- Scheduled task: parses raw lines every 5 minutes into PARSED_LOGS
CREATE OR REPLACE TASK AUTO_PARSE_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
    COMMENT = 'Picks up unprocessed rows from RAW_LOG_LANDING, parses with regex, inserts into PARSED_LOGS, marks as processed.'
AS
EXECUTE IMMEDIATE
$$
BEGIN
    LET row_count NUMBER DEFAULT 0;
    SELECT COUNT(*) INTO :row_count FROM KAFKA_LOGS.RAW.RAW_LOG_LANDING WHERE PROCESSED = FALSE;

    IF (:row_count > 0) THEN
        INSERT INTO KAFKA_LOGS.RAW.PARSED_LOGS (APP_NAME, TIMESTAMP, LOG_LEVEL, COMPONENT, MESSAGE, SOURCE_FILE, INGESTED_AT)
        SELECT
            COALESCE(
                REGEXP_SUBSTR(SOURCE_FILE, '([a-zA-Z][a-zA-Z0-9_-]+)\.(log|txt)', 1, 1, 'ie', 1),
                'auto_ingest'
            ),
            COALESCE(
                REGEXP_SUBSTR(RAW_LINE, '(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}[,.]?\d*)', 1, 1, 'e'),
                TO_CHAR(LOADED_AT, 'YYYY-MM-DD HH24:MI:SS')
            ),
            COALESCE(
                REGEXP_SUBSTR(RAW_LINE, '\b(TRACE|DEBUG|INFO|WARN|WARNING|ERROR|FATAL|CRITICAL|SEVERE)\b', 1, 1, 'ie'),
                'INFO'
            ),
            COALESCE(
                REGEXP_SUBSTR(RAW_LINE, '\[([A-Za-z][A-Za-z0-9_.\-]+)\]', 1, 1, 'e'),
                'unknown'
            ),
            RAW_LINE,
            SOURCE_FILE,
            CURRENT_TIMESTAMP()
        FROM KAFKA_LOGS.RAW.RAW_LOG_LANDING
        WHERE PROCESSED = FALSE
          AND TRIM(RAW_LINE) != '';

        UPDATE KAFKA_LOGS.RAW.RAW_LOG_LANDING
        SET PROCESSED = TRUE
        WHERE PROCESSED = FALSE;
    END IF;
END;
$$;

-- Resume the task (must be done after creation)
ALTER TASK AUTO_PARSE_TASK RESUME;
