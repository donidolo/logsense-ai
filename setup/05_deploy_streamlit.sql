-- LogSense AI: Streamlit Deployment
-- Creates the internal stage and deploys the Streamlit app.

USE DATABASE KAFKA_LOGS;
USE SCHEMA RAW;

-- Create stage for Streamlit files
CREATE STAGE IF NOT EXISTS STREAMLIT_STAGE
  DIRECTORY = (ENABLE = TRUE);

-- Upload files to stage (run these from SnowSQL or CoCo CLI):
-- PUT file://streamlit_app.py @KAFKA_LOGS.RAW.STREAMLIT_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
-- PUT file://environment.yml @KAFKA_LOGS.RAW.STREAMLIT_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- Create the Streamlit app
CREATE OR REPLACE STREAMLIT KAFKA_LOGS.RAW.LOG_DIAGNOSIS_APP
  ROOT_LOCATION = '@KAFKA_LOGS.RAW.STREAMLIT_STAGE'
  MAIN_FILE = 'streamlit_app.py'
  QUERY_WAREHOUSE = COMPUTE_WH;
