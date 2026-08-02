-- LogSense AI: Database and Schema Setup
-- Run this script first to create the required database and schema.

CREATE DATABASE IF NOT EXISTS KAFKA_LOGS;

CREATE SCHEMA IF NOT EXISTS KAFKA_LOGS.RAW;

USE DATABASE KAFKA_LOGS;
USE SCHEMA RAW;
