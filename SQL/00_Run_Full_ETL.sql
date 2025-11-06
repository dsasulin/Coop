-- ============================================================================
-- MASTER ETL SCRIPT - Full Pipeline Execution
-- Description: Runs complete ETL process: Stage -> Bronze -> Silver -> Gold
-- Execution: Run in Hue SQL Editor
-- Duration: ~5-15 minutes (depending on data volume)
-- ============================================================================

-- ============================================================================
-- ETL PIPELINE EXECUTION GUIDE
-- ============================================================================
--
-- OPTION 1: Run all layers in sequence (recommended for initial load)
-- Execute this entire script in Hue to run the complete pipeline
--
-- OPTION 2: Run layers individually (for debugging or partial refresh)
-- Run each script separately in this order:
--   1. SQL/01_Load_Stage_to_Bronze.sql
--   2. SQL/02_Load_Bronze_to_Silver.sql
--   3. SQL/03_Load_Silver_to_Gold.sql
--
-- OPTION 3: Run specific tables only
-- Open individual layer scripts and execute only the sections you need
--
-- ============================================================================

-- Set Hive properties for better performance
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.max.dynamic.partitions.pernode=1000;
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=8;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.fetch.column.stats=true;
SET hive.stats.fetch.partition.stats=true;

-- ============================================================================
-- PRE-FLIGHT CHECK: Verify Source Data
-- ============================================================================
SELECT '============================================================' as message;
SELECT 'PRE-FLIGHT CHECK: Verifying Source Data' as step;
SELECT '============================================================' as message;

-- Check that source tables exist and have data
SELECT
    'test.clients' as table_name,
    COUNT(*) as record_count,
    CASE
        WHEN COUNT(*) > 0 THEN 'OK'
        ELSE 'ERROR: No data'
    END as status
FROM test.clients
UNION ALL
SELECT 'test.products', COUNT(*),
    CASE WHEN COUNT(*) > 0 THEN 'OK' ELSE 'ERROR: No data' END
FROM test.products
UNION ALL
SELECT 'test.transactions', COUNT(*),
    CASE WHEN COUNT(*) > 0 THEN 'OK' ELSE 'ERROR: No data' END
FROM test.transactions
UNION ALL
SELECT 'test.accounts', COUNT(*),
    CASE WHEN COUNT(*) > 0 THEN 'OK' ELSE 'ERROR: No data' END
FROM test.accounts
ORDER BY table_name;

-- If any table shows "ERROR: No data", stop and load data into test schema first
-- Use: Data/ folder CSV files -> Load via Hue Importer into test schema

-- ============================================================================
-- LAYER 1: STAGE -> BRONZE (Raw Data Load)
-- ============================================================================
SELECT '============================================================' as message;
SELECT 'LAYER 1: Loading Stage -> Bronze (Raw Data)' as step;
SELECT CONCAT('Start Time: ', CURRENT_TIMESTAMP) as timestamp;
SELECT '============================================================' as message;

-- Execute Bronze layer load
-- Note: In Hue, you cannot source external files, so copy/paste from:
-- SQL/01_Load_Stage_to_Bronze.sql

-- For demonstration, we'll show a summary approach:
-- Run the full 01_Load_Stage_to_Bronze.sql script manually

SELECT 'ACTION REQUIRED: Execute SQL/01_Load_Stage_to_Bronze.sql' as instruction;
SELECT 'This will load all tables from test schema to bronze layer' as description;

-- Pause here and run 01_Load_Stage_to_Bronze.sql in a separate Hue query window
-- Then come back and continue

-- Verify Bronze load (uncomment after running Bronze script)
/*
SELECT
    'clients' as table_name,
    COUNT(*) as bronze_count
FROM bronze.clients
UNION ALL
SELECT 'transactions', COUNT(*) FROM bronze.transactions
UNION ALL
SELECT 'accounts', COUNT(*) FROM bronze.accounts
UNION ALL
SELECT 'products', COUNT(*) FROM bronze.products
ORDER BY table_name;
*/

-- ============================================================================
-- LAYER 2: BRONZE -> SILVER (Data Cleaning & Transformation)
-- ============================================================================
SELECT '============================================================' as message;
SELECT 'LAYER 2: Transforming Bronze -> Silver (Clean Data)' as step;
SELECT CONCAT('Start Time: ', CURRENT_TIMESTAMP) as timestamp;
SELECT '============================================================' as message;

-- Execute Silver layer transformation
SELECT 'ACTION REQUIRED: Execute SQL/02_Load_Bronze_to_Silver.sql' as instruction;
SELECT 'This will clean and transform data from bronze to silver layer' as description;

-- Pause here and run 02_Load_Bronze_to_Silver.sql in a separate Hue query window
-- Then come back and continue

-- Verify Silver load (uncomment after running Silver script)
/*
SELECT
    'clients' as table_name,
    COUNT(*) as silver_count,
    ROUND(AVG(dq_score), 3) as avg_dq_score
FROM silver.clients
UNION ALL
SELECT 'transactions', COUNT(*), NULL FROM silver.transactions
UNION ALL
SELECT 'accounts', COUNT(*), NULL FROM silver.accounts
UNION ALL
SELECT 'products', COUNT(*), NULL FROM silver.products
ORDER BY table_name;
*/

-- ============================================================================
-- LAYER 3: SILVER -> GOLD (Aggregations & Analytics)
-- ============================================================================
SELECT '============================================================' as message;
SELECT 'LAYER 3: Building Silver -> Gold (Analytics Layer)' as step;
SELECT CONCAT('Start Time: ', CURRENT_TIMESTAMP) as timestamp;
SELECT '============================================================' as message;

-- Execute Gold layer aggregation
SELECT 'ACTION REQUIRED: Execute SQL/03_Load_Silver_to_Gold.sql' as instruction;
SELECT 'This will create dimensions, facts, and data marts in gold layer' as description;

-- Pause here and run 03_Load_Silver_to_Gold.sql in a separate Hue query window
-- Then come back and continue

-- Verify Gold load (uncomment after running Gold script)
/*
SELECT
    'dim_client' as table_name,
    COUNT(*) as gold_count,
    'Dimension' as table_type
FROM gold.dim_client
UNION ALL
SELECT 'dim_product', COUNT(*), 'Dimension' FROM gold.dim_product
UNION ALL
SELECT 'fact_transactions_daily', COUNT(*), 'Fact' FROM gold.fact_transactions_daily
UNION ALL
SELECT 'client_360_view', COUNT(*), 'Data Mart' FROM gold.client_360_view
UNION ALL
SELECT 'product_performance_summary', COUNT(*), 'Data Mart' FROM gold.product_performance_summary
ORDER BY table_type, table_name;
*/

-- ============================================================================
-- POST-ETL VALIDATION & STATISTICS
-- ============================================================================
SELECT '============================================================' as message;
SELECT 'POST-ETL VALIDATION: Data Quality Checks' as step;
SELECT '============================================================' as message;

-- Record counts across all layers (uncomment after running all scripts)
/*
SELECT
    'Stage (test)' as layer,
    'clients' as table_name,
    COUNT(*) as record_count
FROM test.clients
UNION ALL
SELECT 'Bronze', 'clients', COUNT(*) FROM bronze.clients
UNION ALL
SELECT 'Silver', 'clients', COUNT(*) FROM silver.clients
UNION ALL
SELECT 'Gold', 'dim_client', COUNT(*) FROM gold.dim_client
ORDER BY
    CASE layer
        WHEN 'Stage (test)' THEN 1
        WHEN 'Bronze' THEN 2
        WHEN 'Silver' THEN 3
        WHEN 'Gold' THEN 4
    END,
    table_name;
*/

-- Data lineage validation: Records should flow through layers
-- Stage count >= Bronze count >= Silver count (due to deduplication and filtering)
/*
WITH layer_counts AS (
    SELECT 'clients' as entity,
           (SELECT COUNT(*) FROM test.clients) as stage_count,
           (SELECT COUNT(*) FROM bronze.clients) as bronze_count,
           (SELECT COUNT(*) FROM silver.clients) as silver_count,
           (SELECT COUNT(*) FROM gold.dim_client) as gold_count
    UNION ALL
    SELECT 'transactions',
           (SELECT COUNT(*) FROM test.transactions),
           (SELECT COUNT(*) FROM bronze.transactions),
           (SELECT COUNT(*) FROM silver.transactions),
           (SELECT COUNT(*) FROM gold.fact_transactions_daily)
)
SELECT
    entity,
    stage_count,
    bronze_count,
    silver_count,
    gold_count,
    CASE
        WHEN bronze_count = stage_count AND silver_count <= bronze_count
        THEN 'PASS'
        ELSE 'WARNING: Check data flow'
    END as validation_status
FROM layer_counts;
*/

-- ============================================================================
-- DATA QUALITY REPORT
-- ============================================================================
SELECT '============================================================' as message;
SELECT 'DATA QUALITY REPORT' as step;
SELECT '============================================================' as message;

-- Silver layer data quality metrics (uncomment after Silver load)
/*
SELECT
    COUNT(*) as total_clients,
    ROUND(AVG(dq_score), 3) as avg_dq_score,
    MIN(dq_score) as min_dq_score,
    MAX(dq_score) as max_dq_score,
    SUM(CASE WHEN dq_score >= 0.9 THEN 1 ELSE 0 END) as excellent_quality,
    SUM(CASE WHEN dq_score >= 0.8 AND dq_score < 0.9 THEN 1 ELSE 0 END) as good_quality,
    SUM(CASE WHEN dq_score < 0.8 THEN 1 ELSE 0 END) as poor_quality
FROM silver.clients;
*/

-- Show records with data quality issues (uncomment after Silver load)
/*
SELECT
    client_id,
    full_name,
    dq_score,
    dq_issues
FROM silver.clients
WHERE dq_score < 0.8
ORDER BY dq_score ASC
LIMIT 10;
*/

-- ============================================================================
-- BUSINESS METRICS SUMMARY
-- ============================================================================
SELECT '============================================================' as message;
SELECT 'BUSINESS METRICS SUMMARY' as step;
SELECT '============================================================' as message;

-- Key business metrics from Gold layer (uncomment after Gold load)
/*
-- Client segmentation
SELECT
    client_segment,
    COUNT(*) as client_count,
    ROUND(AVG(credit_score), 0) as avg_credit_score,
    ROUND(AVG(total_balance), 2) as avg_balance,
    ROUND(AVG(transactions_30d), 1) as avg_monthly_transactions
FROM gold.client_360_view
GROUP BY client_segment
ORDER BY client_count DESC;
*/

/*
-- Transaction volume by month
SELECT
    year,
    month,
    SUM(transaction_count) as total_transactions,
    ROUND(SUM(total_amount), 2) as total_volume,
    ROUND(AVG(avg_amount), 2) as avg_transaction_amount
FROM gold.fact_transactions_daily
GROUP BY year, month
ORDER BY year DESC, month DESC
LIMIT 12;
*/

/*
-- Product performance
SELECT
    product_name,
    product_type,
    total_clients,
    active_clients,
    ROUND(total_contract_value, 2) as total_value,
    ROUND(total_monthly_revenue, 2) as monthly_revenue
FROM gold.product_performance_summary
ORDER BY total_contract_value DESC
LIMIT 10;
*/

/*
-- Branch performance
SELECT
    branch_name,
    city,
    total_accounts,
    total_clients,
    ROUND(total_deposits, 2) as total_deposits,
    ROUND(branch_performance_score, 2) as performance_score
FROM gold.branch_performance_dashboard
ORDER BY branch_performance_score DESC
LIMIT 10;
*/

-- ============================================================================
-- PERFORMANCE STATISTICS
-- ============================================================================
SELECT '============================================================' as message;
SELECT 'ETL PERFORMANCE STATISTICS' as step;
SELECT '============================================================' as message;

-- Table statistics (uncomment after running all layers)
/*
-- Analyze tables for query optimization
ANALYZE TABLE bronze.clients COMPUTE STATISTICS;
ANALYZE TABLE bronze.transactions COMPUTE STATISTICS;
ANALYZE TABLE silver.clients COMPUTE STATISTICS;
ANALYZE TABLE silver.transactions COMPUTE STATISTICS;
ANALYZE TABLE gold.dim_client COMPUTE STATISTICS;
ANALYZE TABLE gold.fact_transactions_daily COMPUTE STATISTICS;
ANALYZE TABLE gold.client_360_view COMPUTE STATISTICS;
*/

-- Show partition statistics for transactions
/*
SHOW PARTITIONS bronze.transactions;
SHOW PARTITIONS silver.transactions;
SHOW PARTITIONS gold.fact_transactions_daily;
*/

-- ============================================================================
-- ETL COMPLETION SUMMARY
-- ============================================================================
SELECT '============================================================' as message;
SELECT 'ETL PIPELINE EXECUTION COMPLETED' as status;
SELECT CONCAT('Completion Time: ', CURRENT_TIMESTAMP) as timestamp;
SELECT '============================================================' as message;

SELECT 'Next Steps:' as next_steps;
SELECT '1. Review data quality report above' as step_1;
SELECT '2. Run analytical queries from SQL/Analyse_Queries.sql' as step_2;
SELECT '3. Create visualizations in your BI tool' as step_3;
SELECT '4. Schedule regular ETL runs (daily/hourly)' as step_4;

-- ============================================================================
-- TROUBLESHOOTING GUIDE
-- ============================================================================
/*
-- If you encounter errors, check these:

-- 1. Verify database existence
SHOW DATABASES;

-- 2. Verify tables exist in each layer
SHOW TABLES IN test;
SHOW TABLES IN bronze;
SHOW TABLES IN silver;
SHOW TABLES IN gold;

-- 3. Check for failed partitions
SHOW PARTITIONS bronze.transactions;

-- 4. Verify permissions
-- In Hue: User must have SELECT on test, INSERT on bronze/silver/gold

-- 5. Check for null primary keys
SELECT COUNT(*) FROM test.clients WHERE client_id IS NULL;

-- 6. Monitor query execution
-- In Hue: Check Job Browser for failed queries

-- 7. Check available disk space
-- Contact admin if running out of HDFS space

-- 8. Re-run specific layer if needed
-- Just execute the individual layer script (01, 02, or 03)

-- 9. Full pipeline reset (if needed)
-- WARNING: This will delete all data in bronze, silver, gold
-- TRUNCATE TABLE bronze.clients;
-- TRUNCATE TABLE silver.clients;
-- TRUNCATE TABLE gold.dim_client;
-- Then re-run ETL scripts
*/

-- ============================================================================
-- MAINTENANCE QUERIES
-- ============================================================================
/*
-- Show table sizes
DESCRIBE EXTENDED bronze.clients;
DESCRIBE EXTENDED silver.clients;
DESCRIBE EXTENDED gold.dim_client;

-- Show table properties
SHOW TBLPROPERTIES bronze.clients;

-- Refresh metadata (if tables not appearing)
INVALIDATE METADATA;

-- Compact small files (if too many small files created)
ALTER TABLE bronze.transactions CONCATENATE;
ALTER TABLE silver.transactions CONCATENATE;

-- Drop and recreate partition (if partition corrupted)
ALTER TABLE bronze.transactions DROP IF EXISTS PARTITION (transaction_year=2025, transaction_month=1);
-- Then re-run Bronze load for that partition
*/

-- ============================================================================
-- SCHEDULING RECOMMENDATIONS
-- ============================================================================
/*
-- For Production Environment:

-- Daily Full Refresh (Recommended for small/medium datasets):
-- Schedule: Every day at 2:00 AM
-- 1. Run 01_Load_Stage_to_Bronze.sql
-- 2. Run 02_Load_Bronze_to_Silver.sql
-- 3. Run 03_Load_Silver_to_Gold.sql

-- Incremental Load (For large datasets):
-- Bronze: Load only new/changed records
-- Silver: Process only new records from Bronze
-- Gold: Refresh only affected aggregates

-- Real-time/Hourly (For transactional tables):
-- Transactions: Load every hour
-- Balances: Load every 4 hours
-- Clients/Products: Load daily (rarely change)

-- Implementation Options:
-- 1. Hue Scheduler: Schedule queries directly in Hue
-- 2. Cron + Beeline: Schedule via Linux cron calling beeline
-- 3. Airflow: Use when available (preferred)
-- 4. Oozie: Cloudera workflow scheduler
*/

-- ============================================================================
-- END OF MASTER ETL SCRIPT
-- ============================================================================
