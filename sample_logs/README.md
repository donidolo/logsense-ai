# Sample Logs

This directory contains sample log files for testing LogSense AI.

## Files

### `kafka_broker.log`

100 lines of real Kafka broker and Kafka Connect log entries including:
- **Kafka Connect CDC source** - Debezium connector committing offsets and sending records
- **Kafka Connect sink** - Processing batches, serialization, and data writes
- **Kafka broker** - GroupCoordinator loading consumer offsets, partition management
- **Consumer rebalancing** - Group joins, partition assignments, offset resets

## Usage

1. Open the LogSense AI Streamlit app
2. In the sidebar, click "Upload log files"
3. Select `kafka_broker.log`
4. Set App Name to: `kafka`
5. Click "Ingest & Parse"

The logs will be parsed and available for semantic search and AI diagnosis.
