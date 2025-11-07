# Banking Data Platform - Cloudera Pilot Project

This project demonstrates a modern Data Lakehouse architecture using Cloudera Data Platform (CDP) on AWS for the banking domain.

## Architecture

The project implements **Medallion Architecture** (Bronze-Silver-Gold) for data management:

```
┌─────────────────┐
│   Stage (test)  │  ← Source data
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Bronze Layer   │  ← Raw data "as is" + metadata
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Silver Layer   │  ← Cleaned, validated data
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Gold Layer    │  ← Aggregates, data marts, business metrics
└─────────────────┘
```

### Data Layers

1. **Stage (test schema)** - Tables for initial data loading
2. **Bronze** - Raw data with minimal processing, technical metadata
3. **Silver** - Cleaned data with business rules applied and validation
4. **Gold** - Aggregated data, dimensions, facts, analytical data marts

## Project Structure

```
Coop/
│
├── DDL/                                # SQL scripts for table creation
│   ├── Create_Tables.sql              # Stage tables (test schema)
│   ├── 01_Create_Bronze_Layer.sql     # Bronze layer DDL
│   ├── 02_Create_Silver_Layer.sql     # Silver layer DDL
│   └── 03_Create_Gold_Layer.sql       # Gold layer DDL (Star Schema)
│
├── spark_jobs/                         # Production PySpark ETL jobs ✅
│   ├── 01_stage_to_bronze.py          # Stage → Bronze (12 tables)
│   ├── 02_bronze_to_silver.py         # Bronze → Silver (cleaning, DQ scoring)
│   └── 03_silver_to_gold.py           # Silver → Gold (5 dims, 2 facts, 3 aggregates)
│
├── airflow_dags/                       # Airflow orchestration ✅
│   └── banking_etl_pipeline.py        # Complete ETL pipeline DAG
│
├── deploy_etl.sh                       # Automated deployment script ✅
├── DEPLOYMENT_GUIDE.md                 # Deployment documentation ✅
├── SPARK_AIRFLOW_SUMMARY.md            # Spark & Airflow summary ✅
│
├── Data/                               # Test data
│   ├── clients.csv                    # Clients
│   ├── accounts.csv                   # Accounts
│   ├── transactions.csv               # Transactions
│   ├── products.csv                   # Products
│   ├── contracts.csv                  # Contracts
│   ├── account_balances.csv           # Balances
│   ├── cards.csv                      # Cards
│   ├── branches.csv                   # Branches
│   ├── employees.csv                  # Employees
│   ├── loans.csv                      # Loans
│   ├── credit_applications.csv        # Credit applications
│   └── quality_test/                  # Data with quality issues
│       ├── clients_bad_quality.csv
│       ├── transactions_bad_quality.csv
│       └── accounts_bad_quality.csv
│
├── Generator/                          # Data generation utilities
│   ├── generate_banking_data.py       # Test data generator
│   └── generate_bad_quality_data.py   # Bad quality data generator
│
├── SQL/                                # SQL scripts for manual execution
│   ├── 01_Load_Stage_to_Bronze.sql    # Manual load: Stage → Bronze
│   ├── 02_Load_Bronze_to_Silver.sql   # Manual load: Bronze → Silver
│   ├── 03_Load_Silver_to_Gold.sql     # Manual load: Silver → Gold
│   └── Analyse_Queries.sql            # Sample analytical queries
│
└── README.md                           # This file
```

**Note:** Use `spark_jobs/` for production automation and `SQL/` scripts for manual execution in Hue.

## Data Model

### Main Entities

- **Clients** - Individual persons
- **Products** - Banking products
- **Contracts** - Client contracts for products
- **Accounts** - Bank accounts
- **Transactions** - Financial operations
- **Account Balances** - Current account balances
- **Cards** - Bank cards
- **Branches** - Bank branches
- **Employees** - Bank staff
- **Loans** - Loan products
- **Credit Applications** - Credit applications

### Table Relationships (Source Layers)

```
Clients (1) ────┬──── (N) Accounts
                │
                ├──── (N) Contracts
                │
                ├──── (N) Client_Products
                │
                └──── (N) Credit_Applications

Accounts (1) ───┬──── (N) Transactions
                │
                ├──── (1) Account_Balances
                │
                └──── (N) Cards

Contracts (1) ──┬──── (N) Accounts
                │
                └──── (N) Loans

Branches (1) ───┬──── (N) Accounts
                │
                └──── (N) Employees
```

---

## Gold Layer Model (Star Schema)

The Gold layer implements a **Star Schema** optimized for analytical queries and BI reporting. It consists of dimension tables, fact tables, and pre-aggregated summary tables.

### Architecture Overview

```
                    ┌──────────────────┐
                    │   dim_date       │
                    │  (Calendar)      │
                    └────────┬─────────┘
                             │
       ┌─────────────────────┼─────────────────────┐
       │                     │                     │
       │                     │                     │
┌──────▼──────┐      ┌──────▼──────────┐   ┌──────▼──────┐
│ dim_client  │      │  FACT TABLES    │   │ dim_product │
│  (Clients)  ├─────►│                 │◄──┤ (Products)  │
└─────────────┘      │ • fact_trans    │   └─────────────┘
                     │ • fact_balance  │
┌─────────────┐      │                 │   ┌─────────────┐
│ dim_branch  ├─────►│                 │◄──┤ dim_account │
│ (Branches)  │      └─────────────────┘   │ (Accounts)  │
└─────────────┘                            └─────────────┘
       │
       │          ┌─────────────────────────────────┐
       └─────────►│     AGGREGATE TABLES            │
                  │ • client_summary                │
                  │ • product_performance           │
                  │ • branch_performance            │
                  └─────────────────────────────────┘
```

### Dimension Tables (5)

#### 1. **dim_client** - Client Dimension (Client 360° View)

Complete client profile with demographics, financials, and relationship summary.

**Key Fields:**
- `client_key` (PK) - Surrogate key
- `client_id` - Business key
- Demographics: `first_name`, `last_name`, `full_name`, `age`, `age_group`
- Contact: `email`, `email_domain`, `phone`
- Location: `address`, `city`, `country`, `region`
- Financial Profile: `credit_score`, `credit_score_category`, `annual_income`, `income_category`, `risk_category`
- Segmentation: `client_segment` (VIP, PREMIUM, REGULAR, BASIC)
- Relationship Metrics: `customer_tenure_years`, `total_accounts`, `total_products`, `total_loans`, `total_cards`
- SCD Type 2 Support: `effective_date`, `end_date`, `is_current`

**Client Segments:**
- **VIP**: Income > $150K AND Credit Score > 750
- **PREMIUM**: Income > $100K AND Credit Score > 700
- **REGULAR**: Income > $50K AND Credit Score > 650
- **BASIC**: All other clients

---

#### 2. **dim_product** - Product Dimension

Banking products catalog with performance metrics.

**Key Fields:**
- `product_key` (PK) - Surrogate key
- `product_id` - Business key
- `product_name`, `product_type`, `product_category`
- `currency`, `active`
- Performance Metrics: `total_clients`, `total_contracts`, `total_revenue`
- SCD Type 2 Support: `effective_date`, `end_date`, `is_current`

**Product Types:** Loans, Deposits, Cards, Investments, Insurance

---

#### 3. **dim_branch** - Branch Dimension

Bank branches with operational metrics.

**Key Fields:**
- `branch_key` (PK) - Surrogate key
- `branch_id` - Business key
- `branch_name`, `branch_code`, `branch_type`
- Location: `address`, `city`, `region`, `country`
- Contact: `phone`, `email`, `manager_id`
- Operational Metrics: `total_employees`, `total_accounts`, `total_loans`, `total_contracts`
- `opening_date`
- SCD Type 2 Support: `effective_date`, `end_date`, `is_current`

---

#### 4. **dim_date** - Date Dimension (Calendar)

Time dimension for temporal analysis.

**Key Fields:**
- `date_key` (PK) - Integer in format YYYYMMDD
- `full_date` - Actual date
- `year`, `month`, `day`, `quarter`
- `day_of_week`, `day_name`, `month_name`
- `is_weekend`, `is_holiday`, `holiday_name`

**Usage:** Enables time-based analysis (daily, weekly, monthly, quarterly, yearly trends)

---

#### 5. **dim_account** - Account Dimension

Bank accounts with relationship to clients, products, and branches.

**Key Fields:**
- `account_key` (PK) - Surrogate key
- `account_id` - Business key
- `account_number`, `account_type`, `account_status`
- Foreign Keys: `client_id`, `product_id`, `branch_id`
- Financial: `current_balance`, `overdraft_limit`, `interest_rate`, `currency`
- Metrics: `account_age_years`, `total_transactions`, `total_cards`
- `opening_date`
- SCD Type 2 Support: `effective_date`, `end_date`, `is_current`

---

### Fact Tables (2)

Fact tables store measurable events with foreign keys to dimensions. Both are **partitioned** by year/month for performance.

#### 1. **fact_transaction** - Transaction Fact Table

Stores all financial transactions with dimensional context.

**Grain:** One row per transaction

**Key Fields:**
- `transaction_id` (PK)
- **Foreign Keys:**
  - `date_key` → dim_date
  - `account_key` → dim_account
  - `client_key` → dim_client
  - `product_key` → dim_product
  - `branch_key` → dim_branch
- **Measures:**
  - `amount` - Transaction amount (can be negative)
  - `amount_abs` - Absolute amount
  - `balance_after` - Balance after transaction
- **Attributes:**
  - `transaction_date`, `transaction_type`, `transaction_direction`
  - `channel` (ATM, Online, Branch, Mobile)
  - `transaction_status`, `is_completed`
  - `transaction_hour`, `transaction_day_of_week`
  - `currency`
- **Partitioning:** `transaction_year`, `transaction_month`

**Typical Queries:**
- Transaction volume by client segment
- Revenue by product type
- Channel performance analysis
- Hourly/daily transaction patterns
- Branch transaction activity

---

#### 2. **fact_account_balance** - Account Balance Fact Table

Stores daily account balance snapshots.

**Grain:** One row per account per day

**Key Fields:**
- `balance_id` (PK)
- **Foreign Keys:**
  - `date_key` → dim_date
  - `account_key` → dim_account
  - `client_key` → dim_client
  - `product_key` → dim_product
  - `branch_key` → dim_branch
- **Measures:**
  - `balance_amount` - Current balance
  - `available_balance` - Available for withdrawal
  - `available_percentage` - Available/Balance ratio
- **Attributes:**
  - `balance_date`, `currency`
- **Partitioning:** `balance_year`, `balance_month`

**Typical Queries:**
- Average balance trends by client segment
- Total deposits by branch/product
- Balance growth analysis
- Liquidity monitoring

---

### Aggregate Tables (3)

Pre-aggregated summary tables for fast dashboard queries.

#### 1. **client_summary** - Client Summary

Complete client activity summary with all financial metrics.

**Grain:** One row per client (current snapshot)

**Key Fields:**
- `client_id` (PK)
- `full_name`, `client_segment`
- **Account Metrics:** `total_accounts`, `total_balance`
- **Transaction Metrics:** `total_transactions`, `total_transaction_volume`
- **Product Metrics:** `total_cards`, `total_loans`, `total_loan_amount`
- **Risk Metrics:** `risk_category`, `credit_score`
- `summary_date`, `created_timestamp`

**Use Cases:**
- Client 360° dashboard
- VIP client identification
- Cross-sell/up-sell targeting
- Portfolio risk analysis

---

#### 2. **product_performance** - Product Performance

Product-level KPIs and adoption metrics.

**Grain:** One row per product (current snapshot)

**Key Fields:**
- `product_id` (PK)
- `product_name`, `product_type`, `product_category`
- **Adoption Metrics:** `total_clients`, `total_accounts`
- **Financial Metrics:** `total_balance`, `total_contract_value`, `total_loan_value`
- **Status:** `is_active`
- `performance_date`, `created_timestamp`

**Use Cases:**
- Product profitability analysis
- Product adoption trends
- Portfolio composition
- Revenue attribution by product

---

#### 3. **branch_performance** - Branch Performance

Branch-level operational and financial KPIs.

**Grain:** One row per branch (current snapshot)

**Key Fields:**
- `branch_id` (PK)
- `branch_name`, `branch_type`, `city`, `region`
- **HR Metrics:** `total_employees`, `total_active_employees`
- **Customer Metrics:** `total_accounts`, `total_balance`
- **Product Metrics:** `total_loans`, `total_loan_value`, `total_contracts`, `total_contract_value`
- `performance_date`, `created_timestamp`

**Use Cases:**
- Branch profitability comparison
- Regional performance analysis
- Resource allocation planning
- Branch efficiency metrics

---

### Star Schema Relationships

```
Fact Tables connect to Dimensions via Foreign Keys:

fact_transaction                    fact_account_balance
├─ date_key      → dim_date        ├─ date_key      → dim_date
├─ account_key   → dim_account     ├─ account_key   → dim_account
├─ client_key    → dim_client      ├─ client_key    → dim_client
├─ product_key   → dim_product     ├─ product_key   → dim_product
└─ branch_key    → dim_branch      └─ branch_key    → dim_branch
```

**Advantages of Star Schema:**
- ✅ Simple queries (easy JOINs)
- ✅ Fast aggregations
- ✅ Optimized for BI tools
- ✅ Denormalized dimensions (no snowflaking)
- ✅ Partition pruning on facts (year/month)

---

### Example Analytical Queries

#### Query 1: Revenue by Client Segment and Product Type

```sql
SELECT
    dc.client_segment,
    dp.product_type,
    COUNT(DISTINCT ft.transaction_id) as transaction_count,
    SUM(ft.amount_abs) as total_volume,
    AVG(ft.amount_abs) as avg_transaction_size
FROM gold.fact_transaction ft
INNER JOIN gold.dim_client dc ON ft.client_key = dc.client_key
INNER JOIN gold.dim_product dp ON ft.product_key = dp.product_key
INNER JOIN gold.dim_date dd ON ft.date_key = dd.date_key
WHERE dd.year = 2025
  AND dd.quarter = 1
  AND ft.is_completed = TRUE
GROUP BY dc.client_segment, dp.product_type
ORDER BY total_volume DESC;
```

#### Query 2: Top Performing Branches by Balance Growth

```sql
SELECT
    bp.branch_name,
    bp.region,
    bp.total_accounts,
    bp.total_balance,
    bp.total_employees,
    ROUND(bp.total_balance / NULLIF(bp.total_employees, 0), 2) as balance_per_employee,
    ROUND(bp.total_balance / NULLIF(bp.total_accounts, 0), 2) as avg_balance_per_account
FROM gold.branch_performance bp
WHERE bp.total_employees > 0
ORDER BY bp.total_balance DESC
LIMIT 10;
```

#### Query 3: VIP Client Portfolio Analysis

```sql
SELECT
    cs.client_id,
    cs.full_name,
    cs.client_segment,
    cs.total_accounts,
    cs.total_balance,
    cs.total_cards,
    cs.total_loans,
    cs.total_loan_amount,
    cs.credit_score,
    ROUND(cs.total_transaction_volume / NULLIF(cs.total_transactions, 0), 2) as avg_transaction_size
FROM gold.client_summary cs
WHERE cs.client_segment = 'VIP'
ORDER BY cs.total_balance DESC;
```

#### Query 4: Daily Transaction Trends with Weekday Analysis

```sql
SELECT
    dd.full_date,
    dd.day_name,
    dd.is_weekend,
    COUNT(ft.transaction_id) as daily_transactions,
    SUM(ft.amount_abs) as daily_volume,
    COUNT(DISTINCT ft.client_key) as unique_clients
FROM gold.fact_transaction ft
INNER JOIN gold.dim_date dd ON ft.date_key = dd.date_key
WHERE dd.year = 2025
  AND dd.month = 1
GROUP BY dd.full_date, dd.day_name, dd.is_weekend
ORDER BY dd.full_date;
```

---

### Data Lineage

```
Silver Layer                    Gold Layer
─────────────                  ─────────────

silver.clients        ──────► dim_client
                               client_summary

silver.products       ──────► dim_product
                               product_performance

silver.branches       ──────► dim_branch
                               branch_performance

silver.accounts       ──────► dim_account

silver.transactions   ──────► dim_date (distinct dates)
                               fact_transaction

silver.account_balances ────► fact_account_balance
```

---

### Performance Optimization

**Partitioning Strategy:**
- `fact_transaction`: Partitioned by `transaction_year`, `transaction_month`
- `fact_account_balance`: Partitioned by `balance_year`, `balance_month`
- **Benefit:** Partition pruning reduces scan size by 10-100x for date-filtered queries

**SCD Type 2 Ready:**
- All dimensions include `effective_date`, `end_date`, `is_current`
- Current implementation uses simple overwrite (Type 1)
- Can be enhanced to track historical changes (Type 2)

**Statistics:**
```sql
-- Compute statistics for query optimization
ANALYZE TABLE gold.fact_transaction COMPUTE STATISTICS;
ANALYZE TABLE gold.dim_client COMPUTE STATISTICS FOR COLUMNS;
```

---

## Technology Stack

- **Cloudera Data Platform (CDP)** - Big data platform
- **Apache Hive** - Data warehouse, metadata
- **Apache Spark** - Data processing (ETL)
- **Apache Airflow** - Pipeline orchestration
- **Hue** - Web UI for data work
- **Apache NiFi** - Data Flow (optional)
- **AWS S3** - Object storage
- **Python** - Programming language for ETL

## Quick Start

### 1. Environment Setup

```bash
# Clone repository
git clone <repo-url>
cd Coop

# Generate test data (optional)
python3 Generator/generate_banking_data.py

# Generate bad quality data
python3 Generator/generate_bad_quality_data.py
```

### 2. Create Structure in Hive

Open Hue and execute SQL scripts in order:

```sql
-- 1. Create stage tables
-- Execute DDL/Create_Tables.sql

-- 2. Create Bronze layer
-- Execute DDL/01_Create_Bronze_Layer.sql

-- 3. Create Silver layer
-- Execute DDL/02_Create_Silver_Layer.sql

-- 4. Create Gold layer
-- Execute DDL/03_Create_Gold_Layer.sql

-- 5. Verify creation
SHOW DATABASES;
USE bronze;
SHOW TABLES;
```

### 3. Load Data

**Via Hue Importer:**
1. Open Hue → Importer
2. Upload CSV files from Data/ folder
3. Specify destination: `test.<table_name>`

**Or via LOAD DATA (if data is in S3):**
```sql
USE test;
LOAD DATA INPATH 's3a://your-bucket/data/clients.csv'
OVERWRITE INTO TABLE clients;
```

### 4. Configure Spark Jobs in CDE

1. Open Cloudera Data Engineering
2. Create Resource with Python files:
   - `stage_to_bronze.py`
   - `bronze_to_silver.py`
   - `silver_to_gold.py`

3. Create three Jobs for each script

### 5. Configure Airflow

1. Upload DAG to CDE:
   - `banking_etl_pipeline.py`

2. Open Airflow UI
3. Enable DAG `banking_etl_pipeline`
4. Run first execution

## ETL Process

### Stage → Bronze

```python
# Spark Job: stage_to_bronze.py
# - Reads data from test schema
# - Adds technical fields (load_timestamp, source_file)
# - Saves to bronze layer
```

Features:
- Preserves all data "as is"
- Adds load metadata
- Transaction partitioning by year/month

### Bronze → Silver

```python
# Spark Job: bronze_to_silver.py
# - Removes duplicates
# - Cleans and normalizes data
# - Validates business rules
# - Standardizes formats
# - Calculates derived fields
# - Evaluates data quality (DQ score)
```

Applied transformations:
- Email and phone normalization
- Status and category standardization
- Calculation of age, tenure, and other metrics
- Categorization (age groups, income, credit ratings)
- Sensitive data masking (card numbers)
- Quality flags and suspicious operation flags

### Silver → Gold

```python
# Spark Job: silver_to_gold.py
# - Builds dimensions (SCD Type 2 ready)
# - Creates facts (aggregated metrics)
# - Builds business data marts (Client 360, Product Performance)
```

Created objects:
- **Dimensions**: dim_client, dim_product, dim_branch, dim_date
- **Facts**: fact_transactions_daily, fact_account_balance_daily, fact_loan_performance
- **Business Views**: client_360_view, product_performance_summary, branch_performance_dashboard

## Monitoring and Quality Assurance

### Data Verification by Layers

```sql
-- Record counts by layers
SELECT 'bronze' as layer, COUNT(*) FROM bronze.clients
UNION ALL
SELECT 'silver' as layer, COUNT(*) FROM silver.clients
UNION ALL
SELECT 'gold' as layer, COUNT(*) FROM gold.dim_client;

-- Data quality in Silver
SELECT
    AVG(dq_score) as avg_quality,
    MIN(dq_score) as min_quality,
    COUNT(*) as total_records,
    SUM(CASE WHEN dq_score < 0.8 THEN 1 ELSE 0 END) as low_quality_count
FROM silver.clients;

-- Client 360 View
SELECT * FROM gold.client_360_view
WHERE client_segment = 'VIP'
LIMIT 10;
```

### Monitoring in Airflow

1. Open Airflow UI
2. Check DAG status `banking_etl_pipeline`
3. View logs for each task
4. Check execution metrics

### Monitoring in CDE

1. Open CDE UI
2. Go to Job Runs
3. Check status and logs of Spark jobs
4. View resource consumption metrics

## Data Quality Testing

### Loading Test Data with Issues

```sql
-- Create temporary table
USE test;
CREATE TABLE clients_bad_quality LIKE clients;

-- Load data with issues
LOAD DATA LOCAL INPATH 'Data/quality_test/clients_bad_quality.csv'
INTO TABLE clients_bad_quality;

-- Run ETL and check results in Silver
-- Silver should have low DQ scores and populated dq_issues
```

### Checking Issue Handling

```sql
-- Problematic records in Silver
SELECT
    client_id,
    full_name,
    dq_score,
    dq_issues
FROM silver.clients
WHERE dq_score < 0.8
ORDER BY dq_score ASC;

-- Statistics by issues
SELECT
    dq_issues,
    COUNT(*) as issue_count
FROM silver.clients
WHERE dq_score < 1.0
GROUP BY dq_issues
ORDER BY issue_count DESC;
```

## Extending Functionality

### Adding a New Table

1. Create DDL in all layers (bronze, silver, gold)
2. Update Spark jobs to process the new table
3. Add new task to Airflow DAG
4. Test on test data

### Adding New Metrics in Gold

1. Update DDL in `03_Create_Gold_Layer.sql`
2. Add calculation logic in `silver_to_gold.py`
3. Restart ETL

### Configuring Data Quality Rules

1. Update logic in `bronze_to_silver.py`
2. Add new checks to dq_score calculation function
3. Add issue descriptions to dq_issues

## Performance

### Optimizing Spark Jobs

- Use partitioning for large tables
- Configure `spark.sql.adaptive.enabled=true`
- Use broadcast joins for small tables
- Cache frequently used dataframes

### Optimizing Hive Tables

```sql
-- Enable statistics
ANALYZE TABLE bronze.clients COMPUTE STATISTICS;
ANALYZE TABLE bronze.clients COMPUTE STATISTICS FOR COLUMNS;

-- Use bucketing for frequently joined tables
CREATE TABLE silver.clients_bucketed
CLUSTERED BY (client_id) INTO 32 BUCKETS
AS SELECT * FROM silver.clients;

-- Use ORC instead of Parquet for OLAP
-- (this project uses Parquet for S3 compatibility)
```

### Performance Monitoring

```sql
-- Table sizes
SELECT
    table_name,
    num_rows,
    total_size
FROM (
    SELECT 'bronze.clients' as table_name, COUNT(*) as num_rows FROM bronze.clients
) t;

-- Transaction partitions
SHOW PARTITIONS silver.transactions;

-- Partition statistics
SELECT
    transaction_year,
    transaction_month,
    COUNT(*) as record_count
FROM silver.transactions
GROUP BY transaction_year, transaction_month;
```

## Additional Documentation

- [CLOUDERA_SETUP_GUIDE.md](./CLOUDERA_SETUP_GUIDE.md) - Detailed Cloudera configuration guide
- [DDL/](./DDL/) - Table creation SQL scripts
- [SQL/Analyse_Queries.sql](./SQL/Analyse_Queries.sql) - Sample analytical queries

## Roadmap

- [ ] Add incremental loading
- [ ] Implement SCD Type 2 for dimensions
- [ ] Add CDC (Change Data Capture)
- [ ] Integrate with monitoring system (Grafana)
- [ ] Add automated Data Quality checks
- [ ] Implement Data Lineage tracking
- [ ] Add ML models for fraud detection
- [ ] Create BI dashboards (Tableau/PowerBI)

## FAQ

**Q: How often does ETL run?**
A: By default, daily at 2:00 UTC. Configurable in Airflow DAG.

**Q: How long is historical data stored?**
A: In the current version, all data is stored indefinitely. Retention policy can be configured via HDFS.

**Q: How to add a new user?**
A: Via Cloudera Manager → Ranger → Policies

**Q: How to recover data in case of error?**
A: All layers preserve load timestamp, can restore from bronze or previous partitions.

**Q: Is real-time processing supported?**
A: Current version has batch processing. For real-time, can add Kafka + Spark Streaming.

## License

This project is a pilot and is intended for demonstration purposes.

## Contact

For questions about the project, contact the Data Engineering team.
