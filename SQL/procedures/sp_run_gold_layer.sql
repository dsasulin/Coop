-- ============================================================================
-- Procedure: sp_run_gold_layer
-- Layer: Gold (orchestrator)
-- Reads: none directly
-- Writes: none directly
-- Calls: sp_build_dim_client, sp_build_client_360, sp_build_financial_kpi,
--        sp_build_dq_dashboard
-- Note: Also depends on SQL scripts for dim_product, dim_branch, dim_date,
--       and Spark for facts
-- ============================================================================
--
-- Gold Layer Build Orchestration
-- Execute steps in order. Each step must complete before the next begins.
-- Run via: beeline -f sp_run_gold_layer.sql
--
-- ============================================================================

-- ============================================================================
-- Step 1: Build dimensions (SQL scripts)
-- ============================================================================
-- SOURCE: SQL/03_Load_Silver_to_Gold.sql (dim_product, dim_branch, dim_date)
-- These dimension tables have no upstream SP dependencies.
--
-- Run externally:
--   beeline -f 03_Load_Silver_to_Gold.sql   (dim_product section)
--   beeline -f 03_Load_Silver_to_Gold.sql   (dim_branch section)
--   beeline -f 03_Load_Silver_to_Gold.sql   (dim_date section)
-- ============================================================================


-- ============================================================================
-- Step 2: Build dim_client
-- ============================================================================
-- Depends on: silver.clients, silver.accounts, silver.contracts,
--             silver.client_products, silver.cards, silver.loans
-- Output: gold.dim_client (37 columns)
-- ============================================================================

SOURCE /opt/sql/procedures/sp_build_dim_client.sql;


-- ============================================================================
-- Step 3: Build fact tables (SQL + Spark)
-- ============================================================================
-- Fact tables are built by a combination of SQL and Spark jobs.
-- SQL source: SQL/03_Load_Silver_to_Gold.sql (fact sections)
-- Spark jobs:
--   spark-submit spark_jobs/03_silver_to_gold.py   (fact_transactions_daily,
--                                                   fact_account_balance_daily,
--                                                   fact_loan_performance)
--   spark-submit spark_jobs/05_gold_aggregations.py (product_performance_summary,
--                                                    branch_performance_dashboard)
--
-- These must complete before Step 4.
-- ============================================================================


-- ============================================================================
-- Step 4: Build aggregate / analytics tables
-- ============================================================================
-- Depends on: gold.dim_client, gold.fact_transactions_daily,
--             gold.fact_loan_performance, gold.fact_account_balance_daily,
--             silver.dq_results
-- ============================================================================

-- 4a. client_360_view (51 columns)
SOURCE /opt/sql/procedures/sp_build_client_360.sql;

-- 4b. financial_kpi_summary (27 columns)
SOURCE /opt/sql/procedures/sp_build_financial_kpi.sql;

-- 4c. data_quality_dashboard (21 columns)
SOURCE /opt/sql/procedures/sp_build_dq_dashboard.sql;


-- ============================================================================
-- Execution summary
-- ============================================================================
-- After all steps complete, verify row counts:
SELECT 'gold.dim_client'              AS table_name, COUNT(*) AS row_count FROM gold.dim_client
UNION ALL
SELECT 'gold.client_360_view',        COUNT(*) FROM gold.client_360_view
UNION ALL
SELECT 'gold.financial_kpi_summary',  COUNT(*) FROM gold.financial_kpi_summary
UNION ALL
SELECT 'gold.data_quality_dashboard', COUNT(*) FROM gold.data_quality_dashboard;
