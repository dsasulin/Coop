# SQL ETL Scripts - Execution Guide

This folder contains SQL scripts to run the complete ETL pipeline in Hue without requiring Airflow or Spark.

## Overview

The ETL process is split into 3 layers following the Medallion Architecture:

```
test (Stage) → bronze (Raw) → silver (Clean) → gold (Analytics)
```

## Files

| File | Description | Duration | Status |
|------|-------------|----------|--------|
| `00_Run_Full_ETL.sql` | Master script with instructions | 1 min | ✅ Ready |
| `01_Load_Stage_to_Bronze.sql` | Load raw data from test to bronze | 2-5 min | ✅ Tested |
| `02_Load_Bronze_to_Silver.sql` | Clean and transform data | 5-10 min | ✅ Tested |
| `03_Load_Silver_to_Gold.sql` | Build dimensions, facts, and data marts | 3-7 min | ✅ Tested |
| `Analyse_Queries.sql` | Analytical queries for reporting | - | ✅ Ready |
| `FIXES_SUMMARY.md` | Detailed log of all schema fixes | - | 📋 Documentation |

## Important Notes

**⚠️ Scripts Updated and Tested (2025-01-06)**

All SQL ETL scripts have been updated to fix schema mismatches and runtime errors:

- ✅ **01_Load_Stage_to_Bronze.sql**: Fixed field references to match actual test schema (removed non-existent fields like `state`, `channel`, etc.)
- ✅ **02_Load_Bronze_to_Silver.sql**: Corrected column count mismatches for all 12 tables
- ✅ **03_Load_Silver_to_Gold.sql**: Fixed ambiguous column references and parsing errors

**See `FIXES_SUMMARY.md` for complete details of all corrections.**

All scripts are now validated against:
- ✅ DDL schema definitions
- ✅ Runtime execution in Hue
- ✅ Column count matching
- ✅ Proper table aliases in JOINs

## Prerequisites

### 1. Databases Must Exist

Run these DDL scripts first in Hue (if not already done):

```sql
-- Execute in this order:
1. DDL/Create_Tables.sql          -- Creates test schema
2. DDL/01_Create_Bronze_Layer.sql -- Creates bronze schema
3. DDL/02_Create_Silver_Layer.sql -- Creates silver schema
4. DDL/03_Create_Gold_Layer.sql   -- Creates gold schema
```

### 2. Load Data into Test Schema

Load CSV files from `Data/` folder into test schema using one of these methods:

**Option A: Hue Importer (Recommended)**
1. Open Hue → Importer
2. Select file: `Data/clients.csv`
3. Choose destination: `test.clients`
4. Set delimiter: `,`
5. Click Submit
6. Repeat for all tables

**Option B: LOAD DATA (if data in S3/HDFS)**
```sql
USE test;
LOAD DATA INPATH 's3a://your-bucket/data/clients.csv'
OVERWRITE INTO TABLE clients;
```

**Option C: INSERT from External Table**
```sql
-- Create external table pointing to CSV
CREATE EXTERNAL TABLE clients_ext (...)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3a://your-bucket/data/';

-- Insert into test table
INSERT INTO test.clients SELECT * FROM clients_ext;
```

### 3. Verify Data Loaded

```sql
SELECT 'clients' as table_name, COUNT(*) as records FROM test.clients
UNION ALL
SELECT 'transactions', COUNT(*) FROM test.transactions
UNION ALL
SELECT 'accounts', COUNT(*) FROM test.accounts
UNION ALL
SELECT 'products', COUNT(*) FROM test.products;
```

## Execution Instructions

### Method 1: Run Complete Pipeline (Recommended for First Time)

Execute scripts **in this order**, one at a time:

#### Step 1: Load Bronze Layer (5 minutes)
```
Open: SQL/01_Load_Stage_to_Bronze.sql
Action: Run entire script in Hue
Result: Data loaded from test → bronze
```

**What this does:**
- Copies all tables from test schema to bronze
- Adds technical metadata (load_timestamp, source_file)
- Partitions transactions by year/month

**Verify:**
```sql
SELECT COUNT(*) FROM bronze.clients;
SELECT COUNT(*) FROM bronze.transactions;
SHOW PARTITIONS bronze.transactions;
```

#### Step 2: Transform to Silver Layer (10 minutes)
```
Open: SQL/02_Load_Bronze_to_Silver.sql
Action: Run entire script in Hue
Result: Data cleaned and transformed bronze → silver
```

**What this does:**
- Removes duplicates
- Normalizes values (emails lowercase, status standardized)
- Calculates derived fields (age, tenure, risk category)
- Adds data quality score (dq_score)
- Masks sensitive data (card numbers)
- Flags suspicious transactions

**Verify:**
```sql
SELECT COUNT(*) FROM silver.clients;

-- Check data quality
SELECT
    AVG(dq_score) as avg_quality,
    MIN(dq_score) as min_quality,
    COUNT(CASE WHEN dq_score < 0.8 THEN 1 END) as low_quality_count
FROM silver.clients;

-- Show low quality records
SELECT client_id, full_name, dq_score, dq_issues
FROM silver.clients
WHERE dq_score < 0.8
LIMIT 10;
```

#### Step 3: Build Gold Layer (7 minutes)
```
Open: SQL/03_Load_Silver_to_Gold.sql
Action: Run entire script in Hue
Result: Analytical tables created in gold
```

**What this does:**
- Creates dimension tables (dim_client, dim_product, dim_branch, dim_date)
- Creates fact tables (fact_transactions_daily, fact_account_balance_daily, fact_loan_performance)
- Builds data marts (client_360_view, product_performance_summary, branch_performance_dashboard)

**Verify:**
```sql
-- Check all gold tables
SELECT 'dim_client' as table_name, COUNT(*) FROM gold.dim_client
UNION ALL
SELECT 'dim_product', COUNT(*) FROM gold.dim_product
UNION ALL
SELECT 'fact_transactions_daily', COUNT(*) FROM gold.fact_transactions_daily
UNION ALL
SELECT 'client_360_view', COUNT(*) FROM gold.client_360_view;

-- View Client 360
SELECT * FROM gold.client_360_view
WHERE client_segment = 'VIP'
LIMIT 10;
```

### Method 2: Use Master Script (Alternative)

The master script provides step-by-step guidance:

```
Open: SQL/00_Run_Full_ETL.sql
Action: Follow instructions in comments
Result: Guided execution with validation checks
```

This script includes:
- Pre-flight checks
- Validation queries
- Data quality reports
- Business metrics summary
- Troubleshooting guide

## Running Incremental Updates

### Daily Refresh (Recommended)

Run all three scripts daily to refresh all data:

```bash
# Schedule via Hue Scheduler or cron
# Daily at 2:00 AM

1. 01_Load_Stage_to_Bronze.sql    # 5 min
2. 02_Load_Bronze_to_Silver.sql   # 10 min
3. 03_Load_Silver_to_Gold.sql     # 7 min

Total: ~22 minutes
```

### Partial Refresh (Advanced)

Refresh only specific tables:

```sql
-- Refresh only transactions (in each layer)

-- Bronze
TRUNCATE TABLE bronze.transactions;
INSERT INTO TABLE bronze.transactions PARTITION (transaction_year, transaction_month)
SELECT *, YEAR(transaction_date), MONTH(transaction_date)
FROM test.transactions;

-- Silver
INSERT OVERWRITE TABLE silver.transactions PARTITION (transaction_year, transaction_month)
SELECT ... FROM bronze.transactions;

-- Gold
INSERT OVERWRITE TABLE gold.fact_transactions_daily PARTITION (year, month)
SELECT ... FROM silver.transactions;
```

## Analytical Queries

After ETL completes, run analytical queries:

```
Open: SQL/Analyse_Queries.sql
Action: Run individual queries or sections
Result: Business insights and reports
```

Available analyses:
- Client base and financial metrics
- Monthly transaction activity
- Product effectiveness
- Loan and risk analysis
- Balance distribution
- Branch performance
- Credit application statistics
- Client segmentation
- Card products analysis
- Data quality checks

## Performance Optimization

### 1. Compute Statistics (Run after each ETL)

```sql
-- Bronze layer
ANALYZE TABLE bronze.clients COMPUTE STATISTICS;
ANALYZE TABLE bronze.transactions COMPUTE STATISTICS;

-- Silver layer
ANALYZE TABLE silver.clients COMPUTE STATISTICS;
ANALYZE TABLE silver.transactions COMPUTE STATISTICS;
ANALYZE TABLE silver.accounts COMPUTE STATISTICS;

-- Gold layer
ANALYZE TABLE gold.dim_client COMPUTE STATISTICS;
ANALYZE TABLE gold.fact_transactions_daily COMPUTE STATISTICS;
ANALYZE TABLE gold.client_360_view COMPUTE STATISTICS;
```

### 2. Optimize for Query Performance

```sql
-- Enable performance settings before running ETL
SET hive.exec.parallel=true;
SET hive.vectorized.execution.enabled=true;
SET hive.cbo.enable=true;
```

### 3. Compact Small Files (if needed)

```sql
-- After many incremental loads
ALTER TABLE bronze.transactions CONCATENATE;
ALTER TABLE silver.transactions CONCATENATE;
```

## Troubleshooting

### Issue: "Table not found"

**Solution:**
```sql
-- Verify databases exist
SHOW DATABASES;

-- Verify tables exist
SHOW TABLES IN test;
SHOW TABLES IN bronze;

-- If missing, run DDL scripts first
```

### Issue: "No data in bronze after loading"

**Solution:**
```sql
-- Check source data
SELECT COUNT(*) FROM test.clients;

-- If 0, load data into test schema first
-- Use Hue Importer or LOAD DATA command
```

### Issue: "Partition not found"

**Solution:**
```sql
-- Check partitions
SHOW PARTITIONS bronze.transactions;

-- Drop and reload partition
ALTER TABLE bronze.transactions
DROP IF EXISTS PARTITION (transaction_year=2025, transaction_month=1);

-- Re-run Bronze load script
```

### Issue: "Query runs very slow"

**Solution:**
```sql
-- 1. Check if statistics are computed
DESCRIBE EXTENDED bronze.clients;

-- 2. Compute statistics
ANALYZE TABLE bronze.clients COMPUTE STATISTICS;

-- 3. Enable performance settings
SET hive.exec.parallel=true;
SET hive.vectorized.execution.enabled=true;
```

### Issue: "Permission denied"

**Solution:**
```
Contact Cloudera administrator to grant permissions:
- SELECT on test schema
- INSERT on bronze, silver, gold schemas
- CREATE on bronze, silver, gold schemas (for first run)
```

### Issue: "Out of memory / timeout"

**Solution:**
```sql
-- Process smaller batches
-- Instead of full table, process by partition or limit

-- Example: Load transactions one month at a time
INSERT INTO TABLE bronze.transactions
PARTITION (transaction_year=2025, transaction_month=1)
SELECT * FROM test.transactions
WHERE YEAR(transaction_date) = 2025
  AND MONTH(transaction_date) = 1;
```

## Data Quality Checks

### Check Completeness

```sql
-- Records should flow through layers
SELECT
    (SELECT COUNT(*) FROM test.clients) as stage_count,
    (SELECT COUNT(*) FROM bronze.clients) as bronze_count,
    (SELECT COUNT(*) FROM silver.clients) as silver_count,
    (SELECT COUNT(*) FROM gold.dim_client) as gold_count;

-- Should be: stage_count = bronze_count >= silver_count (due to dedup)
```

### Check Data Quality Scores

```sql
-- Distribution of DQ scores
SELECT
    CASE
        WHEN dq_score >= 0.9 THEN 'Excellent (0.9-1.0)'
        WHEN dq_score >= 0.8 THEN 'Good (0.8-0.9)'
        WHEN dq_score >= 0.7 THEN 'Fair (0.7-0.8)'
        ELSE 'Poor (<0.7)'
    END as quality_category,
    COUNT(*) as client_count,
    ROUND(AVG(dq_score), 3) as avg_score
FROM silver.clients
GROUP BY
    CASE
        WHEN dq_score >= 0.9 THEN 'Excellent (0.9-1.0)'
        WHEN dq_score >= 0.8 THEN 'Good (0.8-0.9)'
        WHEN dq_score >= 0.7 THEN 'Fair (0.7-0.8)'
        ELSE 'Poor (<0.7)'
    END
ORDER BY avg_score DESC;
```

### Identify Issues

```sql
-- Show records with quality issues
SELECT
    client_id,
    full_name,
    email,
    phone,
    dq_score,
    dq_issues
FROM silver.clients
WHERE dq_score < 0.8
ORDER BY dq_score ASC
LIMIT 20;
```

## Monitoring

### ETL Execution Time

Track execution time for each layer:

```sql
-- Add at start of script
SELECT CONCAT('Start Time: ', CURRENT_TIMESTAMP) as log;

-- Add at end of script
SELECT CONCAT('End Time: ', CURRENT_TIMESTAMP) as log;
```

### Data Volume Metrics

```sql
-- Track data growth
SELECT
    'bronze' as layer,
    COUNT(*) as client_count,
    (SELECT COUNT(*) FROM bronze.transactions) as transaction_count
FROM bronze.clients
UNION ALL
SELECT 'silver', COUNT(*), (SELECT COUNT(*) FROM silver.transactions)
FROM silver.clients
UNION ALL
SELECT 'gold', COUNT(*), (SELECT COUNT(*) FROM gold.fact_transactions_daily)
FROM gold.dim_client;
```

## Scheduling

### Option 1: Hue Scheduler

1. Open script in Hue Editor
2. Click **Schedule** button
3. Set schedule: `0 2 * * *` (Daily at 2 AM)
4. Save

### Option 2: Cron + Beeline

```bash
# Add to crontab
0 2 * * * beeline -u "jdbc:hive2://localhost:10000" -f /path/to/01_Load_Stage_to_Bronze.sql
10 2 * * * beeline -u "jdbc:hive2://localhost:10000" -f /path/to/02_Load_Bronze_to_Silver.sql
25 2 * * * beeline -u "jdbc:hive2://localhost:10000" -f /path/to/03_Load_Silver_to_Gold.sql
```

### Option 3: Oozie Workflow (Advanced)

Create Oozie workflow XML to orchestrate all three scripts.

## Best Practices

1. **Always verify source data** before running ETL
2. **Run scripts in order**: Bronze → Silver → Gold
3. **Check data quality** after each layer
4. **Compute statistics** after loading
5. **Monitor execution time** to detect issues
6. **Back up data** before full refreshes
7. **Test on subset** before full production run
8. **Document issues** and solutions for team

## Next Steps

After successful ETL execution:

1. ✅ Verify data in all layers
2. ✅ Run analytical queries
3. ✅ Create BI dashboards
4. ✅ Schedule regular ETL runs
5. ✅ Set up monitoring and alerts
6. ✅ Document for team

## Support

For issues or questions:
- Check troubleshooting section above
- Review `FIXES_SUMMARY.md` for detailed schema fix documentation
- Review Cloudera documentation
- Contact Data Engineering team

---

**Last Updated:** 2025-01-06
**Version:** 3.0 (All Runtime Errors Fixed)
**Compatibility:** Hue, Beeline, Cloudera Data Platform
**Testing Status:** ✅ Validated against DDL schemas and runtime execution

**Change Log:**
- v3.0 (2025-01-06): Fixed column count mismatches in Silver layer, ambiguous column errors in Gold layer, ParseException in dim_date
- v2.0 (2025-01-06): Fixed schema mismatches between test data and ETL scripts
- v1.0 (2025-01-06): Initial version
