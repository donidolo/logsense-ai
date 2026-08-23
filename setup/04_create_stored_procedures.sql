-- LogSense AI: Stored Procedures
-- All stored procedures for log ingestion, diagnosis, and analysis.

USE DATABASE KAFKA_LOGS;
USE SCHEMA RAW;

-- PARSE_ANY_LOG: Parses raw log file content into structured rows (used by Streamlit manual upload)
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

    INSERT INTO KAFKA_LOGS.RAW.PARSED_LOGS (APP_NAME, TIMESTAMP, LOG_LEVEL, COMPONENT, MESSAGE, SOURCE_FILE, INGESTED_AT)
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
        :APP_NAME || ''_upload'' AS SOURCE_FILE,
        CURRENT_TIMESTAMP() AS INGESTED_AT
    FROM KAFKA_LOGS.RAW._TEMP_INGEST_LINES
    WHERE TRIM(RAW_LINE) != '''';

    SELECT COUNT(*) INTO :rows_inserted
    FROM KAFKA_LOGS.RAW._TEMP_INGEST_LINES
    WHERE TRIM(RAW_LINE) != '''';

    DROP TABLE IF EXISTS KAFKA_LOGS.RAW._TEMP_INGEST_LINES;

    RETURN rows_inserted::VARCHAR || '' rows parsed and inserted'';
END;
';

-- INGEST_LOG: Simplified ingestion for CoCo CLI agent usage
CREATE OR REPLACE PROCEDURE INGEST_LOG(FILE_CONTENT VARCHAR, APP_NAME VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
BEGIN
    LET rows_inserted NUMBER DEFAULT 0;

    CREATE OR REPLACE TEMPORARY TABLE KAFKA_LOGS.RAW._TEMP_INGEST (
        LINE_NUM NUMBER,
        RAW_LINE VARCHAR(65535)
    );

    INSERT INTO KAFKA_LOGS.RAW._TEMP_INGEST (LINE_NUM, RAW_LINE)
    SELECT SEQ4(), VALUE::VARCHAR
    FROM TABLE(SPLIT_TO_TABLE(:FILE_CONTENT, ''\\n''))
    WHERE TRIM(VALUE::VARCHAR) != '''';

    INSERT INTO KAFKA_LOGS.RAW.PARSED_LOGS (APP_NAME, TIMESTAMP, LOG_LEVEL, COMPONENT, MESSAGE, SOURCE_FILE)
    SELECT
        :APP_NAME,
        COALESCE(
            REGEXP_SUBSTR(RAW_LINE, ''(\\\\d{4}-\\\\d{2}-\\\\d{2}[T ]\\\\d{2}:\\\\d{2}:\\\\d{2}[,.]?\\\\d*)'', 1, 1, ''e''),
            ''unknown''
        ),
        COALESCE(
            REGEXP_SUBSTR(RAW_LINE, ''\\\\b(TRACE|DEBUG|INFO|WARN|WARNING|ERROR|FATAL|CRITICAL|SEVERE)\\\\b'', 1, 1, ''ie''),
            ''INFO''
        ),
        COALESCE(
            REGEXP_SUBSTR(RAW_LINE, ''\\\\[([A-Za-z][A-Za-z0-9_.\\\\-]+)\\\\]'', 1, 1, ''e''),
            :APP_NAME
        ),
        RAW_LINE,
        :APP_NAME || ''_agent_ingest''
    FROM KAFKA_LOGS.RAW._TEMP_INGEST
    WHERE TRIM(RAW_LINE) != '''';

    SELECT COUNT(*) INTO :rows_inserted FROM KAFKA_LOGS.RAW._TEMP_INGEST WHERE TRIM(RAW_LINE) != '''';
    DROP TABLE IF EXISTS KAFKA_LOGS.RAW._TEMP_INGEST;

    RETURN :rows_inserted::VARCHAR || '' rows ingested for '' || :APP_NAME;
END;
';

-- DIAGNOSE_LOGS: AI-powered diagnosis with business context from Service Registry
CREATE OR REPLACE PROCEDURE DIAGNOSE_LOGS(SEARCH_QUERY VARCHAR, APP_NAME VARCHAR DEFAULT NULL)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    result VARIANT;
BEGIN
    CREATE OR REPLACE TEMPORARY TABLE KAFKA_LOGS.RAW._TEMP_DIAG_RESULTS (
        TIMESTAMP VARCHAR,
        LOG_LEVEL VARCHAR,
        COMPONENT VARCHAR,
        MESSAGE VARCHAR,
        APP_NAME VARCHAR,
        SCORE FLOAT
    );

    IF (:APP_NAME IS NOT NULL AND :APP_NAME != '''') THEN
        INSERT INTO KAFKA_LOGS.RAW._TEMP_DIAG_RESULTS
        SELECT TIMESTAMP, LOG_LEVEL, COMPONENT, MESSAGE, APP_NAME, 1.0 AS SCORE
        FROM KAFKA_LOGS.RAW.PARSED_LOGS
        WHERE UPPER(APP_NAME) = UPPER(:APP_NAME)
          AND (UPPER(MESSAGE) LIKE ''%'' || UPPER(:SEARCH_QUERY) || ''%''
               OR UPPER(COMPONENT) LIKE ''%'' || UPPER(:SEARCH_QUERY) || ''%'')
        ORDER BY TIMESTAMP DESC
        LIMIT 20;
    ELSE
        INSERT INTO KAFKA_LOGS.RAW._TEMP_DIAG_RESULTS
        SELECT TIMESTAMP, LOG_LEVEL, COMPONENT, MESSAGE, APP_NAME, 1.0 AS SCORE
        FROM KAFKA_LOGS.RAW.PARSED_LOGS
        WHERE UPPER(MESSAGE) LIKE ''%'' || UPPER(:SEARCH_QUERY) || ''%''
           OR UPPER(COMPONENT) LIKE ''%'' || UPPER(:SEARCH_QUERY) || ''%''
        ORDER BY TIMESTAMP DESC
        LIMIT 20;
    END IF;

    LET log_count NUMBER DEFAULT 0;
    SELECT COUNT(*) INTO :log_count FROM KAFKA_LOGS.RAW._TEMP_DIAG_RESULTS;

    IF (:log_count = 0) THEN
        result := PARSE_JSON(''{\"matched_logs\": 0, \"diagnosis\": \"No logs found matching the query.\", \"business_impact\": null}'');
    ELSE
        LET log_sample VARCHAR;
        SELECT LISTAGG(LOG_LEVEL || '' | '' || COMPONENT || '' | '' || LEFT(MESSAGE, 200), ''\\n'') WITHIN GROUP (ORDER BY TIMESTAMP DESC) INTO :log_sample
        FROM KAFKA_LOGS.RAW._TEMP_DIAG_RESULTS
        LIMIT 10;

        LET diagnosis VARCHAR;
        SELECT SNOWFLAKE.CORTEX.AI_COMPLETE(''llama3.3-70b'',
            ''You are a log diagnosis expert. Analyze these log entries and provide: 1) Root Cause, 2) Impact Assessment, 3) Recommended Fix.\\n\\nLogs:\\n'' || :log_sample
        ) INTO :diagnosis;

        LET biz_impact VARCHAR DEFAULT ''No service registry match'';
        SELECT LISTAGG(
            ''Service: '' || SERVICE_NAME || '' | Tier: '' || COALESCE(TIER, ''N/A'') || '' | Owner: '' || COALESCE(TEAM_OWNER, ''N/A''),
            ''; ''
        ) INTO :biz_impact
        FROM KAFKA_LOGS.RAW.SERVICE_REGISTRY SR
        WHERE EXISTS (
            SELECT 1 FROM KAFKA_LOGS.RAW._TEMP_DIAG_RESULTS D
            WHERE UPPER(D.COMPONENT) LIKE ''%'' || UPPER(SR.SERVICE_NAME) || ''%''
               OR UPPER(D.APP_NAME) LIKE ''%'' || UPPER(SR.SERVICE_NAME) || ''%''
        );

        SELECT OBJECT_CONSTRUCT(
            ''matched_logs'', :log_count,
            ''diagnosis'', :diagnosis,
            ''business_impact'', :biz_impact
        ) INTO :result;
    END IF;

    DROP TABLE IF EXISTS KAFKA_LOGS.RAW._TEMP_DIAG_RESULTS;
    RETURN result;
END;
';

-- CHECK_ERROR_FREQUENCY: Detects error spikes by hour for a given component
CREATE OR REPLACE PROCEDURE CHECK_ERROR_FREQUENCY(COMPONENT VARCHAR, HOURS_BACK NUMBER DEFAULT 24)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    result VARIANT;
    avg_count FLOAT DEFAULT 0;
BEGIN
    CREATE OR REPLACE TEMPORARY TABLE KAFKA_LOGS.RAW._TEMP_FREQ (
        HOUR_BUCKET TIMESTAMP,
        ERROR_COUNT NUMBER,
        IS_SPIKE BOOLEAN
    );

    INSERT INTO KAFKA_LOGS.RAW._TEMP_FREQ (HOUR_BUCKET, ERROR_COUNT, IS_SPIKE)
    SELECT
        DATE_TRUNC(''HOUR'', TRY_TO_TIMESTAMP(TIMESTAMP)) AS HOUR_BUCKET,
        COUNT(*) AS ERROR_COUNT,
        FALSE
    FROM KAFKA_LOGS.RAW.PARSED_LOGS
    WHERE UPPER(LOG_LEVEL) IN (''ERROR'', ''FATAL'', ''CRITICAL'', ''SEVERE'')
      AND (UPPER(COMPONENT) LIKE ''%'' || UPPER(:COMPONENT) || ''%''
           OR UPPER(APP_NAME) LIKE ''%'' || UPPER(:COMPONENT) || ''%'')
      AND TRY_TO_TIMESTAMP(TIMESTAMP) >= DATEADD(''HOUR'', -:HOURS_BACK, CURRENT_TIMESTAMP())
    GROUP BY HOUR_BUCKET;

    SELECT AVG(ERROR_COUNT) INTO :avg_count FROM KAFKA_LOGS.RAW._TEMP_FREQ;

    UPDATE KAFKA_LOGS.RAW._TEMP_FREQ
    SET IS_SPIKE = TRUE
    WHERE ERROR_COUNT > :avg_count * 2;

    LET total_errors NUMBER DEFAULT 0;
    LET spike_hours NUMBER DEFAULT 0;
    LET hourly_data VARIANT;

    SELECT COALESCE(SUM(ERROR_COUNT), 0) INTO :total_errors FROM KAFKA_LOGS.RAW._TEMP_FREQ;
    SELECT COUNT(*) INTO :spike_hours FROM KAFKA_LOGS.RAW._TEMP_FREQ WHERE IS_SPIKE = TRUE;
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(''hour'', HOUR_BUCKET::VARCHAR, ''count'', ERROR_COUNT, ''spike'', IS_SPIKE)) INTO :hourly_data FROM KAFKA_LOGS.RAW._TEMP_FREQ ORDER BY HOUR_BUCKET;

    result := OBJECT_CONSTRUCT(
        ''component'', :COMPONENT,
        ''hours_back'', :HOURS_BACK,
        ''avg_errors_per_hour'', ROUND(:avg_count, 1),
        ''hourly_data'', :hourly_data,
        ''total_errors'', :total_errors,
        ''spike_hours'', :spike_hours
    );

    DROP TABLE IF EXISTS KAFKA_LOGS.RAW._TEMP_FREQ;
    RETURN result;
END;
';

-- SEARCH_POSTMORTEMS: Search past resolutions for similar error patterns
CREATE OR REPLACE PROCEDURE SEARCH_POSTMORTEMS(ERROR_PATTERN VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    result VARIANT;
    match_count NUMBER DEFAULT 0;
BEGIN
    CREATE OR REPLACE TEMPORARY TABLE KAFKA_LOGS.RAW._TEMP_PM (
        TICKET_ID VARCHAR,
        RESOLVED_AT TIMESTAMP,
        ROOT_CAUSE VARCHAR,
        FIX_APPLIED VARCHAR,
        RESOLVED_BY VARCHAR,
        RELEVANCE_SCORE FLOAT
    );

    INSERT INTO KAFKA_LOGS.RAW._TEMP_PM
    SELECT
        TICKET_ID,
        RESOLVED_AT,
        ROOT_CAUSE,
        FIX_APPLIED,
        RESOLVED_BY,
        1.0 AS RELEVANCE_SCORE
    FROM KAFKA_LOGS.RAW.AUDIT_RESOLUTIONS
    WHERE ROOT_CAUSE IS NOT NULL
      AND FIX_APPLIED IS NOT NULL
      AND (UPPER(ROOT_CAUSE) LIKE ''%'' || UPPER(:ERROR_PATTERN) || ''%''
           OR UPPER(FIX_APPLIED) LIKE ''%'' || UPPER(:ERROR_PATTERN) || ''%''
           OR UPPER(TICKET_ID) LIKE ''%'' || UPPER(:ERROR_PATTERN) || ''%'')
    ORDER BY RESOLVED_AT DESC
    LIMIT 10;

    SELECT COUNT(*) INTO :match_count FROM KAFKA_LOGS.RAW._TEMP_PM;

    IF (:match_count = 0) THEN
        LET ai_context VARCHAR;
        SELECT COALESCE(LISTAGG(TICKET_ID || '': '' || COALESCE(ROOT_CAUSE, '''') || '' -> '' || COALESCE(FIX_APPLIED, ''''), ''\\n''), ''No resolutions available'')
        INTO :ai_context
        FROM KAFKA_LOGS.RAW.AUDIT_RESOLUTIONS
        WHERE ROOT_CAUSE IS NOT NULL AND FIX_APPLIED IS NOT NULL
        LIMIT 20;

        LET ai_match VARCHAR;
        SELECT SNOWFLAKE.CORTEX.AI_COMPLETE(''llama3.3-70b'',
            ''Given this error pattern: \"'' || :ERROR_PATTERN || ''\", which of these past resolutions is most relevant? Return the ticket ID or say NONE.\\n\\n'' || :ai_context
        ) INTO :ai_match;

        result := OBJECT_CONSTRUCT(
            ''matches'', 0,
            ''ai_suggestion'', :ai_match,
            ''data'', NULL
        );
    ELSE
        LET pm_data VARIANT;
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
            ''ticket_id'', TICKET_ID,
            ''resolved_at'', RESOLVED_AT::VARCHAR,
            ''root_cause'', ROOT_CAUSE,
            ''fix_applied'', FIX_APPLIED,
            ''resolved_by'', RESOLVED_BY
        )) INTO :pm_data FROM KAFKA_LOGS.RAW._TEMP_PM;

        result := OBJECT_CONSTRUCT(
            ''matches'', :match_count,
            ''ai_suggestion'', NULL,
            ''data'', :pm_data
        );
    END IF;

    DROP TABLE IF EXISTS KAFKA_LOGS.RAW._TEMP_PM;
    RETURN result;
END;
';
