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
├── DDL/                              # SQL scripts for table creation
│   ├── Create_Tables.sql            # Stage tables (test schema)
│   ├── 01_Create_Bronze_Layer.sql   # Bronze layer DDL
│   ├── 02_Create_Silver_Layer.sql   # Silver layer DDL
│   └── 03_Create_Gold_Layer.sql     # Gold layer DDL
│
├── Spark/                            # PySpark ETL jobs
│   ├── stage_to_bronze.py           # Stage → Bronze
│   ├── bronze_to_silver.py          # Bronze → Silver (cleaning, validation)
│   └── silver_to_gold.py            # Silver → Gold (aggregation)
│
├── Airflow/                          # Airflow DAGs
│   └── dags/
│       └── banking_etl_pipeline.py  # ETL process orchestration
│
├── Data/                             # Test data
│   ├── clients.csv                  # Clients
│   ├── accounts.csv                 # Accounts
│   ├── transactions.csv             # Transactions
│   ├── products.csv                 # Products
│   ├── contracts.csv                # Contracts
│   ├── account_balances.csv         # Balances
│   ├── cards.csv                    # Cards
│   ├── branches.csv                 # Branches
│   ├── employees.csv                # Employees
│   ├── loans.csv                    # Loans
│   ├── credit_applications.csv      # Credit applications
│   └── quality_test/                # Data with quality issues
│       ├── clients_bad_quality.csv
│       ├── transactions_bad_quality.csv
│       └── accounts_bad_quality.csv
│
├── Generator/                        # Data generation utilities
│   ├── generate_banking_data.py     # Test data generator
│   └── generate_bad_quality_data.py # Bad quality data generator
│
├── SQL/                              # Analytical SQL queries
│   └── Analyse_Queries.sql          # Sample analytical queries
│
└── CLOUDERA_SETUP_GUIDE.md          # Setup guide
```

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

### Table Relationships

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
