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

### Fact Tables (3)

Fact tables store pre-aggregated metrics. All are **partitioned** by year/month for performance.

#### 1. **fact_transactions_daily** - Daily Transaction Aggregates

Daily aggregated transaction metrics by client, account, and branch.

**Grain:** One row per day per client per account per transaction type

**Key Fields:**
- `transaction_date` (PK part)
- `client_id`, `account_id`, `branch_code`
- `transaction_type`, `category`, `currency`
- **Aggregated Measures:**
  - `transaction_count` - Number of transactions
  - `total_amount` - Sum of all amounts
  - `avg_amount` - Average transaction amount
  - `min_amount`, `max_amount`
  - `successful_count`, `failed_count`, `pending_count`, `cancelled_count`
  - `success_rate` - Percentage of successful transactions
  - `has_suspicious_transactions` - Flag for suspicious activity
  - `suspicious_transaction_count`
- **Partitioning:** `year`, `month`

**Typical Queries:**
- Daily transaction volume trends
- Transaction success rate by channel
- Suspicious activity detection
- Revenue by transaction type

---

#### 2. **fact_account_balance_daily** - Daily Account Balance Snapshots

Daily balance snapshots for all accounts.

**Grain:** One row per day per account

**Key Fields:**
- `snapshot_date` (PK part)
- `account_id`, `client_id`, `branch_code`
- `account_type`, `currency`
- **Balance Measures:**
  - `current_balance` - End-of-day balance
  - `available_balance` - Available for withdrawal
  - `reserved_amount` - Reserved/held funds
  - `credit_limit`, `credit_utilization`
  - `balance_category` - HIGH/MEDIUM/LOW/NEGATIVE
- **Change Measures:**
  - `balance_change_daily` - Change from previous day
  - `balance_change_weekly` - Change from 7 days ago
  - `balance_change_monthly` - Change from 30 days ago
- **Partitioning:** `year`, `month`

**Typical Queries:**
- Balance growth trends
- Account liquidity analysis
- Credit utilization monitoring
- Low balance alerts

---

#### 3. **fact_loan_performance** - Loan Performance Tracking

Daily loan performance metrics and risk indicators.

**Grain:** One row per day per loan

**Key Fields:**
- `loan_id` (PK part)
- `contract_id`, `client_id`, `snapshot_date`
- **Loan Metrics:**
  - `loan_amount`, `outstanding_balance`, `paid_amount`
  - `payment_progress` - Percentage paid
  - `interest_rate`, `term_months`, `remaining_months`
- **Payment Metrics:**
  - `next_payment_date`, `next_payment_amount`, `days_to_next_payment`
  - `total_payments_made`, `missed_payments`, `late_payments`
  - `on_time_payment_rate`
- **Risk Metrics:**
  - `delinquency_status`, `is_delinquent`, `days_past_due`
  - `collateral_value`, `loan_to_value_ratio`
  - `default_probability` - Predictive score
- **Partitioning:** `year`, `month`

**Typical Queries:**
- Delinquency tracking
- Loan portfolio risk assessment
- Payment behavior analysis
- Early default prediction

---

### Aggregate Tables (6)

Wide pre-aggregated tables for dashboards and reporting. All are **partitioned** for performance.

#### 1. **client_360_view** - Complete Client Profile

Comprehensive 360-degree view of each client with all relationships and metrics.

**Grain:** One row per client per snapshot period

**Key Fields:**
- `client_id` (PK)
- `snapshot_date`
- **Demographics:** `full_name`, `email`, `phone`, `age`, `age_group`, `city`, `country`
- **Financial Profile:** `risk_category`, `credit_score`, `credit_score_category`, `annual_income`, `income_category`, `client_segment`
- **Account Summary:** `total_accounts`, `active_accounts`, `checking_accounts`, `savings_accounts`, `loan_accounts`, `total_balance`, `avg_balance`
- **Product Holdings:** `total_products`, `total_contracts`, `total_cards`, `active_cards`
- **Loan Summary:** `total_loans`, `active_loans`, `total_loan_amount`, `total_outstanding_balance`, `delinquent_loans`
- **Transaction Behavior (30d):** `transactions_30d`, `transaction_volume_30d`, `avg_transaction_30d`, `deposits_30d`, `withdrawals_30d`, `transfers_30d`
- **Engagement:** `last_transaction_date`, `days_since_last_transaction`, `transaction_frequency`, `channel_preference`
- **Value Metrics:** `customer_lifetime_value`, `total_revenue`, `total_fees_paid`, `profitability_score`
- **Risk Indicators:** `risk_score`, `fraud_alerts`, `suspicious_activities`
- **Partitioning:** `snapshot_year`, `snapshot_month`

**Use Cases:**
- Client 360° dashboard
- VIP client identification
- Churn prediction
- Cross-sell/up-sell targeting
- Relationship management

---

#### 2. **product_performance_summary** - Product KPIs

Product-level performance metrics and growth indicators.

**Grain:** One row per product per report period

**Key Fields:**
- `product_id` (PK)
- `report_date`
- **Product Details:** `product_name`, `product_type`, `product_category`
- **Customer Metrics:** `total_clients`, `new_clients_mtd`, `churned_clients_mtd`, `active_clients`, `retention_rate`
- **Financial Metrics:** `total_contracts`, `active_contracts`, `total_contract_value`, `avg_contract_value`, `total_revenue_mtd`, `total_revenue_ytd`
- **Growth Metrics:** `client_growth_rate`, `revenue_growth_rate`, `market_share`
- **Partitioning:** `year`, `month`

**Use Cases:**
- Product profitability analysis
- Product adoption trends
- Portfolio composition
- Revenue attribution
- Product lifecycle management

---

#### 3. **branch_performance_dashboard** - Branch KPIs

Branch-level operational and financial performance metrics.

**Grain:** One row per branch per report period

**Key Fields:**
- `branch_id`, `branch_code` (PK)
- `report_date`
- **Branch Details:** `branch_name`, `city`, `state`, `region`, `manager_name`
- **Staff Metrics:** `total_employees`, `active_employees`, `avg_employee_tenure_years`
- **Customer Metrics:** `total_clients`, `new_clients_mtd`, `active_clients`, `total_accounts`, `active_accounts`
- **Financial Metrics:** `total_deposits`, `total_loans_issued`, `total_transaction_volume`, `total_revenue_mtd`, `total_fees_collected`
- **Performance KPIs:** `avg_account_balance`, `loan_approval_rate`, `customer_satisfaction_score`, `nps_score`
- **Rankings:** `revenue_rank`, `customer_rank`, `efficiency_rank`
- **Partitioning:** `year`, `month`

**Use Cases:**
- Branch profitability comparison
- Regional performance analysis
- Resource allocation planning
- Manager performance evaluation
- Branch efficiency benchmarking

---

#### 4. **financial_kpi_summary** - Enterprise-Level Financial KPIs

Top-level financial metrics for executive reporting.

**Grain:** One row per report date per metric category

**Key Fields:**
- `report_date` (PK)
- `metric_category` - DEPOSITS/LOANS/TRANSACTIONS/REVENUE
- **Overall Metrics:** `total_clients`, `active_clients`, `total_accounts`, `active_accounts`
- **Financial Metrics:** `total_assets`, `total_liabilities`, `net_assets`, `total_deposits`, `total_loans`, `total_revenue_mtd`, `total_revenue_ytd`
- **Transaction Metrics:** `total_transactions`, `total_transaction_volume`, `avg_transaction_value`, `transaction_success_rate`
- **Risk Metrics:** `total_delinquent_loans`, `delinquency_rate`, `total_npl_amount`, `provision_coverage_ratio`
- **Growth Metrics:** `client_growth_rate`, `revenue_growth_rate`, `loan_growth_rate`, `deposit_growth_rate`
- **Partitioning:** `year`, `month`

**Use Cases:**
- Executive dashboards
- Board reporting
- Regulatory reporting
- Strategic planning
- Performance tracking against targets

---

#### 5. **data_quality_dashboard** - Data Quality Metrics

Data quality monitoring and issue tracking.

**Grain:** One row per table per report date

**Key Fields:**
- `report_date`, `table_name`, `database_name` (PK)
- **Volume Metrics:** `total_records`, `new_records_today`, `updated_records_today`, `deleted_records_today`
- **Quality Metrics:** `complete_records`, `incomplete_records`, `completeness_rate`, `duplicate_records`, `duplicate_rate`, `null_critical_fields`, `invalid_format_records`
- **Quality Score:** `data_quality_score` (0-100), `data_quality_grade` (A/B/C/D/F)
- **Issue Tracking:** `critical_issues`, `major_issues`, `minor_issues`, `issue_details` (JSON)
- **Partitioning:** `year`, `month`

**Use Cases:**
- Data quality monitoring
- Issue detection and alerting
- Data governance reporting
- SLA compliance tracking
- Root cause analysis

---

### Data Model Relationships

```
Fact/Aggregate Tables connect to Dimensions via Foreign Keys:

fact_transactions_daily             fact_account_balance_daily
├─ client_id     → dim_client      ├─ client_id     → dim_client
├─ account_id    → dim_account     ├─ account_id    → dim_account
└─ branch_code   → dim_branch      └─ branch_code   → dim_branch

fact_loan_performance               client_360_view
├─ client_id     → dim_client      ├─ client_id     → dim_client
└─ contract_id   → (silver layer)  └─ (denormalized, includes all relationships)

product_performance_summary         branch_performance_dashboard
├─ product_id    → dim_product     ├─ branch_id     → dim_branch
                                   └─ branch_code   → dim_branch
```

**Advantages of This Model:**
- ✅ Pre-aggregated facts = Fast queries
- ✅ Denormalized wide tables = No complex JOINs needed for dashboards
- ✅ Partition pruning on all facts (year/month)
- ✅ Optimized for BI tools (Tableau, PowerBI, etc.)
- ✅ Both detailed (facts) and summary (aggregates) available

---

### Example Analytical Queries

#### Query 1: Daily Transaction Volume by Client Segment

```sql
SELECT
    dc.client_segment,
    ftd.transaction_type,
    ftd.transaction_date,
    SUM(ftd.transaction_count) as total_transactions,
    ROUND(SUM(ftd.total_amount), 2) as total_volume,
    ROUND(AVG(ftd.avg_amount), 2) as avg_transaction_size,
    ROUND(AVG(ftd.success_rate), 2) as avg_success_rate
FROM gold.fact_transactions_daily ftd
INNER JOIN gold.dim_client dc ON ftd.client_id = dc.client_id
WHERE ftd.year = 2025
  AND ftd.month = 1
GROUP BY dc.client_segment, ftd.transaction_type, ftd.transaction_date
ORDER BY ftd.transaction_date, total_volume DESC;
```

#### Query 2: Top Performing Branches

```sql
SELECT
    bp.branch_name,
    bp.region,
    bp.total_employees,
    bp.active_employees,
    bp.total_clients,
    bp.total_accounts,
    ROUND(bp.total_deposits, 2) as total_deposits,
    ROUND(bp.total_loans_issued, 2) as total_loans_issued,
    ROUND(bp.total_revenue_mtd, 2) as revenue_mtd,
    ROUND(bp.avg_account_balance, 2) as avg_balance,
    ROUND(bp.total_deposits / NULLIF(bp.total_employees, 0), 2) as deposits_per_employee
FROM gold.branch_performance_dashboard bp
WHERE bp.year = 2025
  AND bp.month = 1
  AND bp.total_employees > 0
ORDER BY bp.total_revenue_mtd DESC
LIMIT 10;
```

#### Query 3: VIP Client 360° Analysis

```sql
SELECT
    cv.client_id,
    cv.full_name,
    cv.client_segment,
    cv.age,
    cv.city,
    cv.credit_score,
    cv.credit_score_category,
    cv.total_accounts,
    cv.active_accounts,
    ROUND(cv.total_balance, 2) as total_balance,
    cv.total_cards,
    cv.active_cards,
    cv.total_loans,
    cv.active_loans,
    ROUND(cv.total_loan_amount, 2) as total_loan_amount,
    ROUND(cv.total_outstanding_balance, 2) as outstanding_balance,
    cv.transactions_30d,
    ROUND(cv.transaction_volume_30d, 2) as volume_30d,
    cv.days_since_last_transaction,
    cv.channel_preference
FROM gold.client_360_view cv
WHERE cv.client_segment = 'VIP'
  AND cv.snapshot_year = 2025
  AND cv.snapshot_month = 1
ORDER BY cv.total_balance DESC
LIMIT 20;
```

#### Query 4: Product Performance Comparison

```sql
SELECT
    pps.product_name,
    pps.product_type,
    pps.product_category,
    pps.total_clients,
    pps.active_clients,
    pps.total_contracts,
    pps.active_contracts,
    ROUND(pps.total_contract_value, 2) as total_contract_value,
    ROUND(pps.avg_contract_value, 2) as avg_contract_value,
    ROUND(pps.total_revenue_mtd, 2) as revenue_mtd,
    ROUND(pps.total_revenue_ytd, 2) as revenue_ytd,
    ROUND(pps.retention_rate, 2) as retention_rate,
    ROUND(pps.client_growth_rate, 2) as client_growth_rate
FROM gold.product_performance_summary pps
WHERE pps.year = 2025
  AND pps.month = 1
ORDER BY pps.total_revenue_ytd DESC;
```

#### Query 5: Account Balance Trends

```sql
SELECT
    fabd.snapshot_date,
    fabd.account_type,
    fabd.branch_code,
    COUNT(DISTINCT fabd.account_id) as account_count,
    ROUND(SUM(fabd.current_balance), 2) as total_balance,
    ROUND(AVG(fabd.current_balance), 2) as avg_balance,
    ROUND(SUM(fabd.available_balance), 2) as total_available,
    ROUND(AVG(fabd.credit_utilization), 2) as avg_credit_utilization,
    ROUND(SUM(fabd.balance_change_daily), 2) as daily_change
FROM gold.fact_account_balance_daily fabd
WHERE fabd.year = 2025
  AND fabd.month = 1
GROUP BY fabd.snapshot_date, fabd.account_type, fabd.branch_code
ORDER BY fabd.snapshot_date, total_balance DESC;
```

---

### Data Lineage

```
Silver Layer                     Gold Layer
─────────────                   ─────────────

silver.clients         ──────► dim_client
                                client_360_view
                                financial_kpi_summary

silver.products        ──────► dim_product
                                product_performance_summary

silver.branches        ──────► dim_branch
                                branch_performance_dashboard

silver.accounts        ──────► dim_account
                                fact_account_balance_daily

silver.transactions    ──────► dim_date (distinct dates)
                                fact_transactions_daily

silver.loans           ──────► fact_loan_performance

silver.account_balances ─────► fact_account_balance_daily

All Silver tables      ──────► data_quality_dashboard
```

---

### Performance Optimization

**Partitioning Strategy:**
All fact and aggregate tables are partitioned by `year` and `month`:
- `fact_transactions_daily`
- `fact_account_balance_daily`
- `fact_loan_performance`
- `client_360_view` (by `snapshot_year`, `snapshot_month`)
- `product_performance_summary`
- `branch_performance_dashboard`
- `financial_kpi_summary`
- `data_quality_dashboard`

**Benefit:** Partition pruning reduces scan size by 10-100x for date-filtered queries

**SCD Type 2 Ready:**
- All dimension tables include `effective_date`, `end_date`, `is_current` fields
- Currently populated using Type 1 (overwrite) strategy
- Structure supports Type 2 (historical tracking) - can be enhanced as needed

**Query Optimization:**
```sql
-- Compute statistics for better query plans
ANALYZE TABLE gold.fact_transactions_daily COMPUTE STATISTICS;
ANALYZE TABLE gold.dim_client COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE gold.client_360_view COMPUTE STATISTICS;

-- Check partition metadata
SHOW PARTITIONS gold.fact_transactions_daily;

-- Optimize storage
ALTER TABLE gold.fact_transactions_daily CONCATENATE;
```

**Storage Format:**
- All Gold tables use **Parquet** format with **Snappy** compression
- Optimized for analytical queries (columnar storage)
- Efficient compression (typically 5:1 ratio)

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
