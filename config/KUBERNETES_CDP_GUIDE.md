# Kubernetes-Based CDP Data Warehouse Guide

## Overview

Your Cloudera Data Platform (CDP) environment is running on **Kubernetes (EKS)**, not a traditional YARN cluster. This is a modern CDP Data Warehouse deployment that uses:

- **Storage**: Amazon S3 (instead of HDFS)
- **Compute**: Kubernetes pods (instead of YARN containers)
- **Metastore**: Kubernetes service (instead of standalone Hive Metastore)
- **Query Engine**: Hive-on-Tez with auto-scaling compute groups

## Key Differences from Traditional YARN Clusters

| Component | Traditional YARN Cluster | Your K8s-Based CDP DW |
|-----------|-------------------------|----------------------|
| **Storage** | HDFS (`hdfs://namenode:8020`) | S3 (`s3a://co-op-buk-39d7d9df/`) |
| **Compute** | YARN ResourceManager | Kubernetes API Server |
| **Metastore** | Standalone HMS (`thrift://hostname:9083`) | K8s Service (`metastore-service...svc.cluster.local:9083`) |
| **Spark Jobs** | `spark-submit --master yarn` | `spark-submit --master k8s://...` |
| **Scaling** | Static YARN capacity | Auto-scaling K8s pods |
| **Execution** | Fixed worker nodes | Dynamic pod scheduling |

## Your Cluster Configuration

### Extracted from Hue SQL Results

```yaml
# Cluster Identification
Cluster ID: compute-1762150223-2jsh
Environment: co-op-cdp-env
Data Warehouse: co-op-aw-dl-default
Kubernetes Namespace: warehouse-1761913838-c49g

# Storage (S3)
S3 Bucket: co-op-buk-39d7d9df
Managed Tables: s3a://co-op-buk-39d7d9df/data/warehouse/tablespace/managed/hive
External Tables: s3a://co-op-buk-39d7d9df/data/warehouse/tablespace/external/hive

# Metastore Service
Metastore URI: thrift://metastore-service.warehouse-1761913838-c49g.svc.cluster.local:9083

# Databases
test:   s3a://co-op-buk-39d7d9df/data/warehouse/tablespace/external/hive/test.db
bronze: s3a://co-op-buk-39d7d9df/user/hive/warehouse/bronze.db
silver: s3a://co-op-buk-39d7d9df/user/hive/warehouse/silver.db
gold:   s3a://co-op-buk-39d7d9df/user/hive/warehouse/gold.db

# Environment
Hadoop Home: /usr/lib/hadoop
Hive Version: 3.1.3000.2025.0.20.0-249
Java Version: OpenJDK 11
Kerberos: Enabled
ZooKeeper: zookeeper.cluster.svc.cluster.local:2181
```

## Running ETL Jobs on This Cluster

### Option 1: Run SQL via Hue (Current Approach)

**What you're doing now**: Running SQL scripts directly in Hue SQL Editor.

**Pros**:
- Simple and straightforward
- No additional configuration needed
- Built-in query editor with syntax highlighting
- Immediate feedback

**Cons**:
- Manual execution
- No scheduling/orchestration
- Limited to Hive SQL only
- No complex transformations

**How to use**:
```sql
-- You're already doing this successfully!
-- Just run the SQL scripts in Hue:
-- 1. 01_Load_Stage_to_Bronze.sql
-- 2. 02_Load_Bronze_to_Silver.sql
-- 3. 03_Load_Silver_to_Gold.sql
```

**Status**: ✅ **WORKING** - You've successfully run all three scripts!

---

### Option 2: Run PySpark Jobs on Kubernetes

**What it is**: Submit PySpark jobs that run as pods in Kubernetes.

**Architecture**:
```
┌─────────────────────────────────────────────┐
│  Submit PySpark Job                         │
│  (from your laptop or Airflow)              │
└──────────────┬──────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────┐
│  Kubernetes API Server                      │
│  (manages pod scheduling)                   │
└──────────────┬──────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────┐
│  Spark Driver Pod                           │
│  (orchestrates the job)                     │
└──────────────┬──────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────┐
│  Spark Executor Pods (auto-scaled)          │
│  Pod 1 │ Pod 2 │ Pod 3 │ ... │ Pod N        │
└──────────────┬──────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────┐
│  Read/Write Data from/to S3                 │
│  s3a://co-op-buk-39d7d9df/...              │
└─────────────────────────────────────────────┘
```

**Requirements**:
1. **Access to Kubernetes API**: Need credentials to submit jobs to K8s cluster
2. **Spark Container Image**: Need Docker image with Spark + dependencies
3. **Service Account**: Kubernetes service account with permissions
4. **Network Access**: Ability to reach K8s API server from where you submit jobs

**Example spark-submit command**:
```bash
spark-submit \
  --master k8s://https://kubernetes.default.svc.cluster.local:443 \
  --deploy-mode cluster \
  --name banking_etl_bronze_to_silver \
  --conf spark.kubernetes.container.image=cloudera/spark:latest \
  --conf spark.kubernetes.namespace=warehouse-1761913838-c49g \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.executor.instances=5 \
  --conf spark.executor.memory=8g \
  --conf spark.executor.cores=4 \
  --conf spark.driver.memory=4g \
  --conf spark.driver.cores=2 \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.sql.hive.metastore.version=3.1.3000 \
  --conf spark.hadoop.hive.metastore.uris=thrift://metastore-service.warehouse-1761913838-c49g.svc.cluster.local:9083 \
  s3a://co-op-buk-39d7d9df/scripts/02_bronze_to_silver.py
```

**Challenges**:
- ❌ Need K8s cluster admin access (may not have)
- ❌ Need to build/access Spark container images
- ❌ Need to configure Kerberos authentication
- ❌ Complex initial setup

**Status**: ⚠️ **NOT RECOMMENDED** unless you have K8s admin access

---

### Option 3: Use CDP Data Engineering (CDE)

**What it is**: Cloudera Data Engineering service - a managed Spark service on CDP.

**How it works**:
```
┌─────────────────────────────────────────────┐
│  CDE UI or CLI                              │
│  (upload PySpark job)                       │
└──────────────┬──────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────┐
│  CDE Virtual Cluster                        │
│  (auto-provisioned Spark environment)       │
└──────────────┬──────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────┐
│  Spark Jobs Run Automatically               │
│  (managed by CDE)                           │
└──────────────┬──────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────┐
│  Read/Write Data from/to S3                 │
└─────────────────────────────────────────────┘
```

**Advantages**:
- ✅ Fully managed Spark environment
- ✅ Auto-scaling built-in
- ✅ Job scheduling included
- ✅ Monitoring and logs included
- ✅ No K8s knowledge required

**How to check if you have CDE**:
1. Log in to Cloudera CDP console
2. Look for "Data Engineering" in the left menu
3. Check if you have a CDE virtual cluster

**Status**: ❓ **UNKNOWN** - Check if CDE is enabled in your CDP environment

---

### Option 4: Use CDP Data Flow (NiFi) + Hue SQL

**What it is**: Combine NiFi for data ingestion with Hue for SQL transformations.

**Workflow**:
```
1. NiFi ingests CSV files from source → S3
2. NiFi triggers Hive SQL (via ExecuteSQL processor)
3. SQL transformations run in Hive (like you're doing now)
4. NiFi monitors for completion and sends alerts
```

**Advantages**:
- ✅ Leverage your existing SQL scripts (no rewrite needed)
- ✅ Add automation and scheduling
- ✅ Built-in error handling and retry logic
- ✅ Email/Slack notifications

**Requirements**:
- Access to NiFi instance
- Ability to create NiFi flows
- JDBC connection to Hive

**Status**: ⚠️ **POSSIBLE** but requires NiFi access

---

## Recommended Approach for Your Environment

### **Phase 1: Continue with Hue SQL (Current)**

**What**: Keep using Hue SQL Editor for ETL

**Why**:
- ✅ It's working well
- ✅ No additional setup required
- ✅ All SQL scripts are tested and fixed
- ✅ Simple to understand and maintain

**How to improve**:
1. **Create a checklist** for manual execution:
   ```
   Daily ETL Checklist:
   [ ] Check if new CSV files arrived in S3
   [ ] Run 01_Load_Stage_to_Bronze.sql in Hue
   [ ] Verify row counts: SELECT COUNT(*) FROM bronze.clients;
   [ ] Run 02_Load_Bronze_to_Silver.sql in Hue
   [ ] Check data quality: SELECT AVG(dq_score) FROM silver.clients;
   [ ] Run 03_Load_Silver_to_Gold.sql in Hue
   [ ] Verify gold tables populated
   [ ] Check for errors in Hue query history
   ```

2. **Create a monitoring query**:
   ```sql
   -- Save this as a Hue snippet and run daily
   SELECT
     'bronze.clients' as table_name,
     COUNT(*) as row_count,
     MAX(load_timestamp) as last_load
   FROM bronze.clients
   UNION ALL
   SELECT
     'silver.clients',
     COUNT(*),
     MAX(process_timestamp)
   FROM silver.clients
   UNION ALL
   SELECT
     'gold.dim_client',
     COUNT(*),
     MAX(updated_timestamp)
   FROM gold.dim_client;
   ```

**Recommendation**: ✅ **USE THIS NOW** - It's the simplest and most reliable option

---

### **Phase 2: Add Scheduling (Future)**

**When**: After 2-4 weeks of successful manual runs

**Options**:

#### Option A: Use CDP Data Flow (NiFi)
```
Best if you have NiFi access

1. Create NiFi flow to schedule SQL execution
2. Use ExecuteSQL processor to run scripts
3. Set cron schedule (e.g., daily at 2 AM)
4. Add email alerts on failure
```

#### Option B: Use CDP Data Engineering (CDE)
```
Best if CDE is available

1. Convert SQL scripts to PySpark (we can help with this)
2. Upload to CDE virtual cluster
3. Create CDE job with schedule
4. Monitor via CDE UI
```

#### Option C: Use External Scheduler (cron + beeline)
```
Best if you have Linux access

1. Set up cron job on Linux machine
2. Use beeline to execute SQL scripts
3. Example:
   0 2 * * * /path/to/run_etl.sh >> /var/log/etl.log 2>&1

run_etl.sh:
#!/bin/bash
beeline -u "jdbc:hive2://metastore-service...:10000" \
  -f /path/to/01_Load_Stage_to_Bronze.sql
```

**Recommendation**: ⏱️ **PLAN FOR LATER** - Focus on stability first

---

## How to Access Your Cluster

### Via Hue (Web UI)
```
URL: https://login.cdpworkshops.cloudera.com/...
Login: user001
Status: ✅ WORKING (you're using this now)
```

### Via Beeline (Command Line)
```bash
# If you have SSH access to a cluster node:
beeline -u "jdbc:hive2://metastore-service.warehouse-1761913838-c49g.svc.cluster.local:10000" \
  -n user001 \
  -p <password> \
  -f script.sql

# Status: ❓ UNKNOWN - check if you have SSH access
```

### Via Spark Submit (Kubernetes)
```bash
# Requires K8s cluster access:
spark-submit --master k8s://... script.py

# Status: ❌ LIKELY NOT AVAILABLE - requires admin access
```

---

## Data Flow Diagram (Your Current Setup)

```
┌──────────────────────────────────────────────────────────────┐
│                     DATA SOURCES                             │
│                                                              │
│  CSV Files in S3:                                           │
│  s3a://co-op-buk-39d7d9df/data/input/banking/              │
│                                                              │
│  - clients.csv                                              │
│  - products.csv                                             │
│  - transactions.csv                                         │
│  - accounts.csv                                             │
│  - ... (12 files total)                                     │
└────────────────────┬─────────────────────────────────────────┘
                     │
                     │ (Manual: LOAD DATA or INSERT INTO)
                     │
                     ▼
┌──────────────────────────────────────────────────────────────┐
│                     TEST DATABASE                            │
│  Location: s3a://.../external/hive/test.db                  │
│                                                              │
│  Tables: clients, products, transactions, etc.              │
│  Owner: user001                                             │
│  Type: EXTERNAL (can be dropped without deleting data)      │
└────────────────────┬─────────────────────────────────────────┘
                     │
                     │ 01_Load_Stage_to_Bronze.sql
                     │ (SELECT ... FROM test.* INSERT INTO bronze.*)
                     │
                     ▼
┌──────────────────────────────────────────────────────────────┐
│                    BRONZE DATABASE                           │
│  Location: s3a://.../warehouse/bronze.db                    │
│                                                              │
│  Tables: 12 tables with raw data + metadata                 │
│  Added fields: load_timestamp, source_file                  │
│  Owner: user001                                             │
│  Type: MANAGED (data deleted if table dropped)              │
└────────────────────┬─────────────────────────────────────────┘
                     │
                     │ 02_Load_Bronze_to_Silver.sql
                     │ (Data cleaning, validation, DQ scoring)
                     │
                     ▼
┌──────────────────────────────────────────────────────────────┐
│                    SILVER DATABASE                           │
│  Location: s3a://.../warehouse/silver.db                    │
│                                                              │
│  Tables: 12 tables with cleaned data                        │
│  Added fields: dq_score, dq_issues, normalized fields       │
│  Transformations:                                           │
│    - TRIM(), UPPER(), email validation                      │
│    - Calculated fields (age, income_category)               │
│    - Data quality scoring (0.0 - 1.0)                       │
│  Owner: user001                                             │
│  Type: MANAGED                                              │
└────────────────────┬─────────────────────────────────────────┘
                     │
                     │ 03_Load_Silver_to_Gold.sql
                     │ (Aggregations, joins, business metrics)
                     │
                     ▼
┌──────────────────────────────────────────────────────────────┐
│                     GOLD DATABASE                            │
│  Location: s3a://.../warehouse/gold.db                      │
│                                                              │
│  Tables:                                                     │
│    - Dimensions: dim_client, dim_product, dim_branch, etc.  │
│    - Facts: fact_transaction, fact_account_balance          │
│    - Aggregates: client_summary, product_performance        │
│  Purpose: Analytics-ready, optimized for BI tools           │
│  Owner: user001                                             │
│  Type: MANAGED                                              │
└──────────────────────────────────────────────────────────────┘
```

---

## Storage Details (S3)

### S3 Bucket Structure
```
s3a://co-op-buk-39d7d9df/
├── data/
│   ├── input/
│   │   └── banking/              # CSV input files (TODO: verify)
│   │       ├── clients.csv
│   │       ├── products.csv
│   │       └── ...
│   ├── warehouse/
│   │   ├── tablespace/
│   │   │   ├── managed/
│   │   │   │   └── hive/         # Managed Hive tables
│   │   │   └── external/
│   │   │       └── hive/
│   │   │           └── test.db/  # Test database
│   │   └── cdw/
│   │       └── clusters/
│   │           └── co-op-cdp-env/
│   │               └── co-op-aw-dl-default/
├── user/
│   └── hive/
│       └── warehouse/
│           ├── bronze.db/        # Bronze database
│           ├── silver.db/        # Silver database
│           └── gold.db/          # Gold database
└── logs/                         # (suggested location for logs)
```

### Access Permissions

Your cluster is configured with **Kerberos authentication** (`USE_KERBEROS=true`).

**What this means**:
- You must authenticate with your credentials (user001)
- All data access is audited
- You cannot access data without valid Kerberos ticket
- Your credentials are managed by CDP

**Current Status**: ✅ Working - You're authenticated via CDP web console

---

## Next Steps

### Immediate (This Week)
1. ✅ **Continue running SQL scripts in Hue** - You're doing great!
2. ⬜ **Document your execution process** - Create a runbook
3. ⬜ **Set up monitoring queries** - Track row counts and data quality
4. ⬜ **Verify CSV input file locations** - Where are the source files?

### Short Term (Next 2-4 Weeks)
1. ⬜ **Test incremental loads** - Load only new/changed data
2. ⬜ **Set up alerts** - Email when data quality drops
3. ⬜ **Explore CDP Data Flow** - Check if NiFi is available
4. ⬜ **Explore CDP Data Engineering** - Check if CDE is available

### Long Term (1-3 Months)
1. ⬜ **Implement scheduling** - Automate daily runs
2. ⬜ **Add orchestration** - Use NiFi or CDE
3. ⬜ **Connect BI tools** - Tableau, PowerBI, etc. to gold layer
4. ⬜ **Optimize performance** - Partition large tables, add indexes

---

## Troubleshooting

### Common Issues

#### Issue 1: "Cannot connect to Hive"
```
Error: Could not open client transport...
```

**Solution**: Check if Kerberos ticket is valid. Re-login to CDP console.

#### Issue 2: "Permission denied on S3"
```
Error: Access Denied (Service: S3, Status Code: 403)
```

**Solution**:
1. Verify your user (user001) has S3 permissions
2. Check if IAM role is attached to CDP
3. Contact CDP admin

#### Issue 3: "Metastore not responding"
```
Error: Could not connect to metastore
```

**Solution**:
1. Check if metastore service is running in K8s
2. Use Cloudera Manager to verify HMS status
3. Contact CDP admin if down

---

## Useful Commands

### Check Database Sizes
```sql
-- Run in Hue to see table sizes
ANALYZE TABLE bronze.clients COMPUTE STATISTICS;
ANALYZE TABLE silver.clients COMPUTE STATISTICS;
ANALYZE TABLE gold.dim_client COMPUTE STATISTICS;

-- Then query statistics
SELECT
  table_name,
  row_count,
  data_size
FROM
  system.table_statistics
WHERE
  database_name IN ('bronze', 'silver', 'gold');
```

### Check S3 Data Location
```sql
-- See where each table's data is stored
DESCRIBE FORMATTED bronze.clients;
-- Look for "Location:" in output
```

### Check Partitions
```sql
-- See if table is partitioned
SHOW PARTITIONS silver.transactions;

-- Count rows per partition
SELECT
  transaction_year,
  transaction_month,
  COUNT(*) as row_count
FROM silver.transactions
GROUP BY transaction_year, transaction_month;
```

---

## Summary

**Your Current Setup**:
- ✅ Kubernetes-based CDP Data Warehouse
- ✅ Storage on S3 (not HDFS)
- ✅ Hive-on-Tez for SQL execution
- ✅ All databases created and working
- ✅ All 12 tables in each layer
- ✅ SQL ETL scripts tested and working

**Recommended Approach**:
1. **Keep using Hue SQL** - It's working well!
2. **Focus on data quality** - Monitor dq_score in silver layer
3. **Plan for automation** - But only after manual process is stable

**What You DON'T Need** (for now):
- ❌ Spark jobs on Kubernetes (too complex)
- ❌ Airflow orchestration (overkill for now)
- ❌ Direct K8s access (not necessary)

**What You CAN Add** (when ready):
- ⏱️ NiFi for scheduling (if available)
- ⏱️ CDE for managed Spark (if available)
- ⏱️ Cron + beeline for simple automation

---

## Questions?

If you need to:
- **Run SQL queries**: Use Hue (what you're doing now)
- **Schedule jobs**: Check if NiFi or CDE is available
- **Monitor data**: Create Hue saved queries
- **Get help**: Contact your CDP admin or Cloudera support

---

**Last Updated**: 2025-01-06
**Version**: 1.0
**Author**: Data Engineering Team
