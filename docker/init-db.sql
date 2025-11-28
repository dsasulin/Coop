-- ============================================================================
-- PostgreSQL Initialization Script
-- ============================================================================
-- Creates databases for Hive Metastore and Airflow

-- Create Airflow database
CREATE DATABASE airflow;

-- Grant privileges to etl_user
GRANT ALL PRIVILEGES ON DATABASE metastore TO etl_user;
GRANT ALL PRIVILEGES ON DATABASE airflow TO etl_user;

-- Connect to metastore and create schema for Hive
\c metastore;

-- Hive metastore will auto-create its schema, but we can pre-create if needed
-- CREATE SCHEMA IF NOT EXISTS hive;
-- GRANT ALL ON SCHEMA hive TO etl_user;

-- Connect to airflow and setup
\c airflow;

-- Airflow will auto-create its schema
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO etl_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO etl_user;

-- Output confirmation
\echo 'Database initialization complete!'
\echo '- metastore: Database for Hive Metastore'
\echo '- airflow: Database for Airflow'
