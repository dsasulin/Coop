# Cloudera Data Platform Setup Guide

## Table of Contents
1. [Getting Connection Parameters](#getting-connection-parameters)
2. [Setting up Cloudera Data Engineering (CDE)](#setting-up-cloudera-data-engineering)
3. [Setting up Airflow](#setting-up-airflow)
4. [Setting up Hue for Table Operations](#setting-up-hue)
5. [Loading Data via Data Flow](#loading-data-via-data-flow)

---

## Getting Connection Parameters

### 1. Hive Connection Parameters via Hue

#### Through Hue Interface:

1. **Open Hue**
   - Navigate to your Cloudera Data Platform
   - Find the **Hue** service and open it

2. **Get connection information**
   ```sql
   -- Execute these queries in the Hive editor:

   -- Current database
   SELECT current_database();

   -- List all databases
   SHOW DATABASES;

   -- Hive configuration information
   SET;

   -- HDFS path to warehouse
   SET hive.metastore.warehouse.dir;

   -- Metastore information
   SET hive.metastore.uris;
   ```

3. **Get JDBC parameters**
   ```sql
   -- Hive version
   SELECT version();

   -- JDBC connection parameters can be found in:
   -- Cloudera Manager -> Hive -> Configuration -> HiveServer2
   ```

#### Through Cloudera Manager:

1. Open **Cloudera Manager**
2. Go to **Clusters** -> Your cluster
3. Select **Hive** service
4. Go to **Configuration**
5. Find parameters:
   - `hive.metastore.uris` - Hive Metastore address
   - `hive.server2.thrift.port` - HiveServer2 port (default 10000)
   - `hive.metastore.warehouse.dir` - HDFS warehouse path

### 2. Spark Connection Parameters

#### Getting via Cloudera Data Engineering UI:

1. **Open CDE (Cloudera Data Engineering)**
   - Go to CDP Home
   - Select **Data Engineering**
   - Select your Virtual Cluster

2. **Get Spark Submit parameters**
   - In CDE UI go to **Jobs** section
   - Click **Create Job**
   - In Spark Configuration section you'll see available parameters:
     ```
     spark.master: yarn
     spark.submit.deployMode: cluster
     spark.dynamicAllocation.enabled: true
     ```

3. **Get resource information**
   - Go to **Virtual Clusters** -> Your cluster
   - View available resources:
     - CPU Requests
     - Memory Requests
     - Executor instances

#### SQL queries to get information via Hue:

```sql
-- Spark SQL information
SET spark.sql.warehouse.dir;
SET spark.master;

-- Check available databases
SHOW DATABASES;

-- Check tables in test database
USE test;
SHOW TABLES;

-- Table information
DESCRIBE EXTENDED clients;

-- Table data path
DESCRIBE FORMATTED clients;
```

### 3. Airflow Parameters

#### Getting via Cloudera Data Engineering:

1. **Open CDE Virtual Cluster**
2. **Go to Airflow UI**
   - In CDE find your Virtual Cluster
   - Click on the **Airflow** icon (⚙️)
   - Airflow Web UI will open

3. **Get Connection parameters**
   - In Airflow UI go to: **Admin** -> **Connections**
   - Find existing connections:
     - `spark_default` - for Spark jobs
     - `hive_cli_default` - for Hive
     - `aws_default` - for S3 (if used)

4. **Create new connection for your project**
   - Connection Id: `banking_hive_connection`
   - Connection Type: `Hive Server 2 Thrift`
   - Host: `<hiveserver2-host>` (can get from CDE or Cloudera Manager)
   - Schema: `bronze` (or other default database)
   - Login: your username
   - Port: `10000` (or from Cloudera Manager)

### 4. AWS S3 Parameters (if used)

#### Through Cloudera Manager or CDP Environment:

1. **Open CDP Environments**
2. Find **Data Lake** settings
3. View S3 parameters:
   ```
   Bucket: s3://your-cdp-bucket/
   Data Location: s3://your-cdp-bucket/warehouse/
   Logs Location: s3://your-cdp-bucket/logs/
   ```

#### Through Hue/Hive:

```sql
-- Check S3 location for tables
DESCRIBE FORMATTED bronze.clients;

-- Result will show location like:
-- location: s3a://your-bucket/warehouse/bronze.db/clients
```

---

## Setting up Cloudera Data Engineering

### 1. Creating Virtual Cluster (if not already exists)

1. Open **Data Engineering** in CDP
2. Click **Create Virtual Cluster**
3. Fill in parameters:
   - Name: `banking-etl-cluster`
   - Environment: select your environment
   - CPU Quota: recommended minimum 10 CPU
   - Memory: recommended minimum 40 GB

### 2. Uploading Spark Jobs to CDE

1. **Create Resource for Python files**
   ```bash
   # In CDE CLI (if installed) or via UI
   ```

2. **Via CDE UI:**
   - Go to **Resources**
   - Click **Create Resource**
   - Select type: `Files`
   - Name: `banking-spark-jobs`
   - Upload files:
     - `stage_to_bronze.py`
     - `bronze_to_silver.py`
     - `silver_to_gold.py`

3. **Create Spark Jobs**
   - Go to **Jobs**
   - Click **Create Job**
   - Fill in parameters:

**Job 1: Stage to Bronze**
```
Name: stage_to_bronze_job
Type: Spark
Application File: stage_to_bronze.py (from Resources)
Main Class: (leave empty for Python)
Arguments: --execution-date ${execution_date}

Spark Configuration:
spark.executor.memory: 4g
spark.executor.cores: 2
spark.driver.memory: 2g

Resources:
- banking-spark-jobs
```

**Job 2: Bronze to Silver**
```
Name: bronze_to_silver_job
Type: Spark
Application File: bronze_to_silver.py
Arguments: --execution-date ${execution_date}

Spark Configuration:
spark.executor.memory: 4g
spark.executor.cores: 2
spark.driver.memory: 2g

Resources:
- banking-spark-jobs
```

**Job 3: Silver to Gold**
```
Name: silver_to_gold_job
Type: Spark
Application File: silver_to_gold.py
Arguments: --execution-date ${execution_date}

Spark Configuration:
spark.executor.memory: 4g
spark.executor.cores: 2
spark.driver.memory: 2g

Resources:
- banking-spark-jobs
```

---

## Setting up Airflow

### 1. Uploading DAG to CDE

1. **Create Resource for DAG**
   - In CDE UI go to **Resources**
   - Create new Resource of type `Files`
   - Name: `banking-airflow-dags`
   - Upload file: `banking_etl_pipeline.py`

2. **DAG will be automatically picked up by Airflow**
   - Go to Airflow UI
   - Find DAG: `banking_etl_pipeline`
   - Enable it (toggle switch)

### 2. Updating DAG for CDE

Update paths in DAG file:

```python
# At the beginning of banking_etl_pipeline.py replace:
SPARK_JOBS_PATH = "/path/to/spark/jobs"

# With:
SPARK_JOBS_PATH = "stage_to_bronze.py"  # CDE uses file names directly
```

For CDE use `CDEJobRunOperator` instead of `SparkSubmitOperator`:

```python
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator

# Example task
load_stage_to_bronze = CDEJobRunOperator(
    task_id='load_stage_to_bronze',
    job_name='stage_to_bronze_job',  # Name of created CDE Job
    variables={
        'execution_date': '{{ ds }}'
    },
)
```

### 3. Setting up Schedule

In Airflow UI:
1. Go to DAG `banking_etl_pipeline`
2. Click **Edit** -> **Schedule**
3. Configure schedule (default: daily at 2:00 UTC)

---

## Setting up Hue

### 1. Creating Databases

Open Hue and execute SQL scripts:

```sql
-- 1. Create databases
CREATE DATABASE IF NOT EXISTS bronze
COMMENT 'Bronze layer - raw data'
LOCATION '/user/hive/warehouse/bronze.db';

CREATE DATABASE IF NOT EXISTS silver
COMMENT 'Silver layer - cleaned data'
LOCATION '/user/hive/warehouse/silver.db';

CREATE DATABASE IF NOT EXISTS gold
COMMENT 'Gold layer - aggregated data'
LOCATION '/user/hive/warehouse/gold.db';

-- 2. Verify creation
SHOW DATABASES;

-- 3. Load DDL scripts from repository
-- Copy and execute contents of files:
-- - DDL/01_Create_Bronze_Layer.sql
-- - DDL/02_Create_Silver_Layer.sql
-- - DDL/03_Create_Gold_Layer.sql
```

### 2. Checking Tables

```sql
-- Check tables in bronze
USE bronze;
SHOW TABLES;

-- Check table structure
DESCRIBE FORMATTED clients;

-- Check data (if loaded)
SELECT * FROM clients LIMIT 10;

-- Check record count
SELECT COUNT(*) FROM clients;
```

### 3. Access Rights

```sql
-- Grant rights to databases (if needed)
GRANT ALL ON DATABASE bronze TO USER your_username;
GRANT ALL ON DATABASE silver TO USER your_username;
GRANT ALL ON DATABASE gold TO USER your_username;

-- Check rights
SHOW GRANT USER your_username ON DATABASE bronze;
```

---

## Loading Data via Data Flow

### Option 1: Loading via Hue

1. **Create tables in test schema** (from DDL/Create_Tables.sql)

2. **Load CSV via Hue**:
   - Open Hue
   - Go to **Importer**
   - Select file (e.g., `Data/clients.csv`)
   - Select destination: `test.clients`
   - Configure delimiter: `,`
   - Click **Submit**

### Option 2: Loading via NiFi (Cloudera Data Flow)

1. **Open DataFlow**
   - In CDP go to **DataFlow**
   - Create new Flow Definition

2. **Create simple flow for CSV loading**:
   ```
   GetFile -> SplitRecord -> ConvertRecord -> PutHiveStreaming
   ```

3. **Configure processors**:
   - **GetFile**: specify path to Data/
   - **SplitRecord**: specify CSV Reader
   - **ConvertRecord**: CSV -> Avro
   - **PutHiveStreaming**:
     - Hive Metastore URI: (from Cloudera Manager)
     - Database: test
     - Table: clients

### Option 3: Loading via S3 and LOAD DATA

```sql
-- 1. Upload CSV to S3 bucket
-- (via AWS Console or AWS CLI)

-- 2. Load data into Hive
USE test;

LOAD DATA INPATH 's3a://your-bucket/data/clients.csv'
OVERWRITE INTO TABLE clients;

-- 3. Verify loading
SELECT COUNT(*) FROM clients;
```

---

## Complete Deployment Process

### Step 1: Preparation

```bash
# 1. Upload entire repository to your S3 bucket or locally
git clone <your-repo>
cd Coop

# 2. Upload data to S3 (if using)
aws s3 cp Data/ s3://your-bucket/banking-data/Data/ --recursive
aws s3 cp Spark/ s3://your-bucket/banking-data/Spark/ --recursive
aws s3 cp Airflow/ s3://your-bucket/banking-data/Airflow/ --recursive
```

### Step 2: Creating Structure in Hive

1. Open Hue
2. Execute DDL scripts in order:
   - `DDL/Create_Tables.sql` (for test schema)
   - `DDL/01_Create_Bronze_Layer.sql`
   - `DDL/02_Create_Silver_Layer.sql`
   - `DDL/03_Create_Gold_Layer.sql`

### Step 3: Loading Data into test Schema

Choose one of the loading options (see "Loading Data" section)

### Step 4: Setting up CDE Jobs

1. Create Resource in CDE with Spark jobs
2. Create three Jobs (stage_to_bronze, bronze_to_silver, silver_to_gold)
3. Test each job manually

### Step 5: Setting up Airflow

1. Upload DAG to CDE
2. Update DAG to use CDEJobRunOperator
3. Enable DAG in Airflow UI
4. Run first execution manually

### Step 6: Monitoring

1. Check execution in Airflow UI
2. Check logs in CDE UI
3. Check data in Hue:

```sql
-- Check bronze
USE bronze;
SELECT COUNT(*) FROM clients;

-- Check silver
USE silver;
SELECT COUNT(*) FROM clients;

-- Check gold
USE gold;
SELECT COUNT(*) FROM dim_client;
SELECT COUNT(*) FROM client_360_view;
```

---

## Troubleshooting

### Problem: Cannot find Hive Metastore URI

**Solution via Hue:**
```sql
SET hive.metastore.uris;
```

**Solution via Cloudera Manager:**
1. Cloudera Manager -> Hive -> Configuration
2. Search: "metastore.uris"

### Problem: Spark job cannot find tables

**Solution:**
Ensure Spark uses correct configuration:
```python
spark = SparkSession.builder \
    .appName("your_app") \
    .enableHiveSupport() \  # IMPORTANT!
    .getOrCreate()
```

### Problem: Access rights errors

**Solution via Hue:**
```sql
-- Grant rights to all databases
GRANT ALL ON DATABASE test TO USER your_username;
GRANT ALL ON DATABASE bronze TO USER your_username;
GRANT ALL ON DATABASE silver TO USER your_username;
GRANT ALL ON DATABASE gold TO USER your_username;
```

### Problem: DAG doesn't appear in Airflow

**Solution:**
1. Check that file is uploaded to CDE Resource
2. Check DAG file syntax
3. Check Airflow Scheduler logs in CDE

---

## Useful SQL Queries for Monitoring

```sql
-- Statistics across all layers
SELECT
    'bronze' as layer,
    'clients' as table_name,
    COUNT(*) as record_count
FROM bronze.clients
UNION ALL
SELECT
    'silver' as layer,
    'clients' as table_name,
    COUNT(*) as record_count
FROM silver.clients
UNION ALL
SELECT
    'gold' as layer,
    'dim_client' as table_name,
    COUNT(*) as record_count
FROM gold.dim_client;

-- Check data quality in silver
SELECT
    AVG(dq_score) as avg_dq_score,
    MIN(dq_score) as min_dq_score,
    COUNT(*) as total_records,
    SUM(CASE WHEN dq_score < 0.8 THEN 1 ELSE 0 END) as low_quality_records
FROM silver.clients;

-- Transaction statistics
SELECT
    transaction_year,
    transaction_month,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount
FROM silver.transactions
GROUP BY transaction_year, transaction_month
ORDER BY transaction_year DESC, transaction_month DESC;
```

---

## Additional Resources

- [Cloudera Data Engineering Documentation](https://docs.cloudera.com/data-engineering/cloud/)
- [Cloudera Data Flow Documentation](https://docs.cloudera.com/dataflow/cloud/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)

---

## Contact and Support

For questions about the project, contact the Data Engineering team.
