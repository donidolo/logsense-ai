-- LogSense AI: Stored Procedures
-- PARSE_ANY_LOG: Parses raw log file content into structured rows using regex patterns.

USE DATABASE KAFKA_LOGS;
USE SCHEMA RAW;

CREATE OR REPLACE PROCEDURE PARSE_ANY_LOG(FILE_CONTENT VARCHAR, APP_NAME VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    rows_inserted NUMBER DEFAULT 0;
BEGIN
    CREATE OR REPLACE TEMPORARY TABLE KAFKA_LOGS.RAW._TEMP_INGEST_LINES (
        LINE_NUM NUMBER,
        RAW_LINE VARCHAR(65535)
    );

    INSERT INTO KAFKA_LOGS.RAW._TEMP_INGEST_LINES (LINE_NUM, RAW_LINE)
    SELECT SEQ4(), VALUE::VARCHAR
    FROM TABLE(SPLIT_TO_TABLE(:FILE_CONTENT, ''\\n''))
    WHERE TRIM(VALUE::VARCHAR) != '''';

    INSERT INTO KAFKA_LOGS.RAW.PARSED_LOGS (APP_NAME, TIMESTAMP, LOG_LEVEL, COMPONENT, MESSAGE, SOURCE_FILE)
    SELECT
        :APP_NAME,
        COALESCE(
            REGEXP_SUBSTR(RAW_LINE, ''(\\\\d{4}-\\\\d{2}-\\\\d{2}[T ]\\\\d{2}:\\\\d{2}:\\\\d{2}[,.]?\\\\d*)'', 1, 1, ''e''),
            REGEXP_SUBSTR(RAW_LINE, ''^([A-Z][a-z]{2} \\\\d{1,2} \\\\d{2}:\\\\d{2}:\\\\d{2})'', 1, 1, ''e''),
            REGEXP_SUBSTR(RAW_LINE, ''\\\\[(\\\\d{4}-\\\\d{2}-\\\\d{2} \\\\d{2}:\\\\d{2}:\\\\d{2}[,.]?\\\\d*)'', 1, 1, ''e''),
            ''unknown''
        ) AS TIMESTAMP,
        COALESCE(
            REGEXP_SUBSTR(RAW_LINE, ''\\\\b(TRACE|DEBUG|INFO|WARN|WARNING|ERROR|FATAL|CRITICAL|SEVERE)\\\\b'', 1, 1, ''ie''),
            ''INFO''
        ) AS LOG_LEVEL,
        COALESCE(
            REGEXP_SUBSTR(RAW_LINE, ''\\\\S+ (\\\\S+?)\\\\[\\\\d+\\\\]:'', 1, 1, ''e''),
            REGEXP_SUBSTR(RAW_LINE, ''\\\\(([\\\\w.]+):\\\\d+\\\\)'', 1, 1, ''e''),
            REGEXP_SUBSTR(RAW_LINE, ''\\\\[([A-Za-z][A-Za-z0-9_.\\\\-]+)\\\\]'', 1, 1, ''e''),
            :APP_NAME
        ) AS COMPONENT,
        COALESCE(
            REGEXP_SUBSTR(RAW_LINE, ''\\\\S+\\\\[\\\\d+\\\\]: (.*)$'', 1, 1, ''e''),
            RAW_LINE
        ) AS MESSAGE,
        :APP_NAME || ''_upload'' AS SOURCE_FILE
    FROM KAFKA_LOGS.RAW._TEMP_INGEST_LINES
    WHERE TRIM(RAW_LINE) != '''';

    SELECT COUNT(*) INTO :rows_inserted
    FROM KAFKA_LOGS.RAW._TEMP_INGEST_LINES
    WHERE TRIM(RAW_LINE) != '''';

    DROP TABLE IF EXISTS KAFKA_LOGS.RAW._TEMP_INGEST_LINES;

    RETURN rows_inserted::VARCHAR || '' rows parsed and inserted'';
END;
';
