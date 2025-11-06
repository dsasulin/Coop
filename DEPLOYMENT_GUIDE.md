# Banking ETL Pipeline - Deployment Guide

## Overview

This guide provides step-by-step instructions for deploying the Banking ETL pipeline (Spark jobs and Airflow DAGs) to your Kubernetes-based Cloudera Data Platform environment.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Architecture Overview](#architecture-overview)
3. [Files Overview](#files-overview)
4. [Configuration](#configuration)
5. [Deployment Steps](#deployment-steps)
6. [Testing](#testing)
7. [Monitoring](#monitoring)
8. [Troubleshooting](#troubleshooting)
9. [Rollback Procedures](#rollback-procedures)

---

## Prerequisites

### Required Access
- [ ] SSH access to CDP cluster or Airflow server
- [ ] Kubernetes access (for Spark-on-K8s) or service account credentials
- [ ] Airflow admin credentials
- [ ] S3 bucket permissions for `co-op-buk-39d7d9df`
- [ ] Hive Metastore access

### Required Software
- [ ] Python 3.7+
- [ ] PySpark 3.x
- [ ] Airflow 2.x
- [ ] kubectl (for K8s-based CDP)
- [ ] spark-submit command

### Environment Variables
```bash
export SPARK_HOME=/usr/lib/spark
export AIRFLOW_HOME=/opt/airflow
export METASTORE_URI="thrift://metastore-service.warehouse-1761913838-c49g.svc.cluster.local:9083"
export S3_BUCKET="co-op-buk-39d7d9df"
```

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────┐
│                     Airflow DAG Scheduler                    │
│                  (banking_etl_pipeline.py)                   │
└────────────────┬─────────────────────────────────────────────┘
                 │
                 │ Triggers Spark jobs via spark-submit
                 │
    ┌────────────┼────────────┬─────────────────┐
    │            │            │                 │
    ▼            ▼            ▼                 ▼
┌────────┐  ┌────────┐  ┌────────┐       ┌──────────┐
│ Spark  │  │ Spark  │  │ Spark  │       │  Email   │
│ Job 1  │  │ Job 2  │  │ Job 3  │       │  Alerts  │
│(bronze)│  │(silver)│  │ (gold) │       │          │
└────┬───┘  └────┬───┘  └────┬───┘       └──────────┘
     │           │           │
     │           │           │
     ▼           ▼           ▼
┌──────────────────────────────────────────────────────────────┐
│                 Hive Tables in S3                            │
│   test → bronze → silver → gold                              │
│   (s3a://co-op-buk-39d7d9df/...)                            │
└──────────────────────────────────────────────────────────────┘
```

**Data Flow**:
1. test (stage) → bronze (raw data)
2. bronze → silver (cleaned, validated)
3. silver → gold (dimensions, facts, aggregates)

**Execution**:
- Airflow DAG schedules and monitors the pipeline
- Spark jobs run on Kubernetes pods
- Data stored in S3 via Hive tables
- Metastore tracks table metadata

---

## Files Overview

### Spark Jobs (`spark_jobs/`)

| File | Purpose | Runtime | Output |
|------|---------|---------|--------|
| `01_stage_to_bronze.py` | Load raw data from test to bronze | ~5-10 min | 12 bronze tables |
| `02_bronze_to_silver.py` | Clean and validate to silver | ~10-20 min | 12 silver tables |
| `03_silver_to_gold.py` | Build dimensions and facts | ~15-30 min | 10 gold tables |

### Airflow DAGs (`airflow_dags/`)

| File | Purpose | Schedule | Dependencies |
|------|---------|----------|--------------|
| `banking_etl_pipeline.py` | Orchestrates all 3 Spark jobs | `@once` (manual) | All Spark jobs |

### Deployment Scripts

| File | Purpose |
|------|---------|
| `deploy_etl.sh` | Automated deployment script |
| `DEPLOYMENT_GUIDE.md` | This file |

---

## Configuration

### Step 1: Update Spark Job Configuration

Edit each Spark job and update these values:

```python
# In all 3 Spark jobs (01, 02, 03):

# Metastore URI (already set)
.config("hive.metastore.uris", "thrift://metastore-service.warehouse-1761913838-c49g.svc.cluster.local:9083")

# S3 warehouse directory (already set)
.config("spark.sql.warehouse.dir", "s3a://co-op-buk-39d7d9df/user/hive/warehouse")

# ✅ No changes needed if using the same cluster
```

### Step 2: Update Airflow DAG Configuration

Edit `airflow_dags/banking_etl_pipeline.py`:

```python
# Line 29-32: Update email addresses
default_args = {
    'owner': 'data_engineering',
    'email': ['YOUR_EMAIL@company.com'],  # ⚠️ UPDATE THIS
    ...
}

# Line 39: Update schedule (optional)
schedule_interval='@once',  # Change to '0 2 * * *' for daily at 2 AM

# Line 53-60: Update paths
SPARK_JOBS_DIR = "/opt/spark_jobs"  # ⚠️ UPDATE if different
SPARK_SUBMIT = "spark-submit"

# Line 63-67: Update Kubernetes config (if needed)
SPARK_MASTER = "k8s://https://kubernetes.default.svc.cluster.local:443"
SPARK_CONTAINER_IMAGE = "cloudera/spark:latest"  # ⚠️ Verify image name
SPARK_NAMESPACE = "warehouse-1761913838-c49g"  # ✅ Should be correct
```

### Step 3: Update Deployment Script

Edit `deploy_etl.sh`:

```bash
# Line 21-22: Update destination paths
SPARK_JOBS_DEST="/opt/spark_jobs"      # ⚠️ UPDATE if different
AIRFLOW_DAGS_DEST="/opt/airflow/dags"  # ⚠️ UPDATE if different

# Line 31: For remote deployment
REMOTE_SERVER=""  # e.g., "user001@cdp-server.company.com"
REMOTE_PORT="22"
```

---

## Deployment Steps

### Option 1: Local Deployment (Recommended)

If you're running this on the Airflow server:

```bash
# 1. Navigate to project directory
cd /path/to/Coop

# 2. Run deployment script
./deploy_etl.sh local

# Expected output:
# ============================================================================
#   Starting Local Deployment
# ============================================================================
#
# ============================================================================
#   Running Pre-deployment Checks
# ============================================================================
# ✅ Spark jobs directory found
# ✅ Airflow DAGs directory found
# ✅ All syntax checks passed
#
# ============================================================================
#   Backing Up Existing Files
# ============================================================================
# ✅ Backed up existing Spark jobs to /opt/backups/etl_YYYYMMDD_HHMMSS/spark_jobs_backup
# ✅ Backed up existing Airflow DAGs to /opt/backups/etl_YYYYMMDD_HHMMSS/airflow_dags_backup
#
# ============================================================================
#   Deploying Spark Jobs
# ============================================================================
# ✅ Spark jobs deployed successfully
#
# ============================================================================
#   Deploying Airflow DAGs
# ============================================================================
# ✅ Airflow DAGs deployed successfully
#
# ============================================================================
#   Deployment Summary
# ============================================================================
# ✅ All components deployed successfully!
```

### Option 2: Remote Deployment

If you're deploying from your local machine to a remote server:

```bash
# 1. Configure remote server in deploy_etl.sh
# Edit line 31: REMOTE_SERVER="user001@cdp-server.company.com"

# 2. Run remote deployment
./deploy_etl.sh remote

# This will:
# - Create deployment package (tar.gz)
# - Copy to remote server via SCP
# - Extract and deploy via SSH
```

### Option 3: Manual Deployment

If automated deployment doesn't work:

```bash
# 1. Copy Spark jobs
scp spark_jobs/*.py user001@cdp-server:/opt/spark_jobs/

# 2. Copy Airflow DAGs
scp airflow_dags/*.py user001@cdp-server:/opt/airflow/dags/

# 3. Set permissions
ssh user001@cdp-server "chmod +x /opt/spark_jobs/*.py"
ssh user001@cdp-server "chmod 644 /opt/airflow/dags/*.py"
```

---

## Testing

### Test 1: Syntax Validation

```bash
# Test Python syntax locally
python3 -m py_compile spark_jobs/01_stage_to_bronze.py
python3 -m py_compile spark_jobs/02_bronze_to_silver.py
python3 -m py_compile spark_jobs/03_silver_to_gold.py
python3 -m py_compile airflow_dags/banking_etl_pipeline.py
```

### Test 2: Individual Spark Job

```bash
# Test first Spark job locally (if you have local Spark)
spark-submit --master local[*] \
  --conf spark.sql.catalogImplementation=hive \
  --conf hive.metastore.uris="$METASTORE_URI" \
  spark_jobs/01_stage_to_bronze.py

# Check logs for errors
# Expected: "✅ ETL Job completed successfully"
```

### Test 3: Airflow DAG Visibility

```bash
# List all DAGs
airflow dags list | grep banking

# Expected output:
# banking_etl_pipeline | data_engineering | True  | @once | ...
```

### Test 4: Airflow DAG Testing

```bash
# Test DAG without actually running it
airflow dags test banking_etl_pipeline 2025-01-06

# This will:
# - Parse the DAG
# - Check task dependencies
# - Validate configurations
# - NOT actually execute the tasks
```

### Test 5: Manual DAG Trigger

```bash
# Trigger DAG manually (this actually runs it!)
airflow dags trigger banking_etl_pipeline

# Monitor in Airflow UI
# http://<airflow-host>:8080/dags/banking_etl_pipeline
```

---

## Monitoring

### Airflow UI

Access Airflow web interface:
```
URL: http://<airflow-host>:8080
Login: (your Airflow credentials)
```

Navigate to:
1. **DAGs** → `banking_etl_pipeline`
2. **Graph View** - See task dependencies
3. **Tree View** - See historical runs
4. **Logs** - Check task logs

### Command Line Monitoring

```bash
# Check DAG status
airflow dags state banking_etl_pipeline 2025-01-06

# Check specific task status
airflow tasks state banking_etl_pipeline 01_stage_to_bronze 2025-01-06

# View task logs
airflow tasks log banking_etl_pipeline 01_stage_to_bronze 2025-01-06

# List running tasks
airflow tasks list banking_etl_pipeline --tree
```

### Spark Job Monitoring

**Option 1: Spark UI (if available)**
```
URL: http://<spark-master>:4040
```

**Option 2: Kubernetes Dashboard**
```bash
# List Spark pods
kubectl get pods -n warehouse-1761913838-c49g | grep banking-etl

# Check pod logs
kubectl logs <pod-name> -n warehouse-1761913838-c49g

# Describe pod for details
kubectl describe pod <pod-name> -n warehouse-1761913838-c49g
```

**Option 3: Cloudera Manager**
- Navigate to YARN/Spark applications
- Find application by name: `banking_etl_*`

### Database Checks

```sql
-- Check row counts after each layer
SELECT 'bronze.clients' as table_name, COUNT(*) as row_count FROM bronze.clients
UNION ALL
SELECT 'silver.clients', COUNT(*) FROM silver.clients
UNION ALL
SELECT 'gold.dim_client', COUNT(*) FROM gold.dim_client;

-- Check data quality scores
SELECT AVG(dq_score) as avg_quality, MIN(dq_score) as min_quality
FROM silver.clients;

-- Check latest load times
SELECT MAX(load_timestamp) as last_load_bronze FROM bronze.clients
UNION ALL
SELECT MAX(process_timestamp) FROM silver.clients
UNION ALL
SELECT MAX(created_timestamp) FROM gold.dim_client;
```

---

## Troubleshooting

### Issue 1: "DAG not found in Airflow"

**Symptoms**:
```
airflow dags list | grep banking
# No output
```

**Solutions**:
1. Check file location:
   ```bash
   ls -la /opt/airflow/dags/banking_etl_pipeline.py
   ```

2. Check file permissions:
   ```bash
   chmod 644 /opt/airflow/dags/banking_etl_pipeline.py
   ```

3. Check Python syntax:
   ```bash
   python3 -m py_compile /opt/airflow/dags/banking_etl_pipeline.py
   ```

4. Restart Airflow scheduler:
   ```bash
   systemctl restart airflow-scheduler
   # OR
   airflow scheduler --daemon
   ```

5. Check Airflow logs:
   ```bash
   tail -f /opt/airflow/logs/scheduler/latest/*.log
   ```

---

### Issue 2: "Connection refused to metastore"

**Symptoms**:
```
pyspark.sql.utils.AnalysisException: java.net.ConnectException:
Connection refused to thrift://metastore-service...
```

**Solutions**:
1. Check metastore service:
   ```bash
   kubectl get svc -n warehouse-1761913838-c49g | grep metastore
   ```

2. Test connectivity:
   ```bash
   telnet metastore-service.warehouse-1761913838-c49g.svc.cluster.local 9083
   ```

3. Verify metastore URI in Spark config:
   ```python
   # Should be:
   hive.metastore.uris = "thrift://metastore-service.warehouse-1761913838-c49g.svc.cluster.local:9083"
   ```

4. Check Kerberos authentication (if enabled)

---

### Issue 3: "Access Denied to S3"

**Symptoms**:
```
com.amazonaws.services.s3.model.AmazonS3Exception: Access Denied
(Service: Amazon S3; Status Code: 403)
```

**Solutions**:
1. Check S3 bucket permissions in AWS IAM

2. Verify IAM role is attached to CDP

3. Test S3 access:
   ```bash
   aws s3 ls s3://co-op-buk-39d7d9df/user/hive/warehouse/
   ```

4. Add S3 credentials to Spark config (if not using IAM roles):
   ```python
   .config("spark.hadoop.fs.s3a.access.key", "YOUR_KEY")
   .config("spark.hadoop.fs.s3a.secret.key", "YOUR_SECRET")
   ```

---

### Issue 4: "Spark job timeout"

**Symptoms**:
```
airflow.exceptions.AirflowTaskTimeout:
Task exceeded maximum timeout of 7200 seconds
```

**Solutions**:
1. Increase timeout in DAG:
   ```python
   default_args = {
       'execution_timeout': timedelta(hours=4),  # Increase from 2 to 4 hours
   }
   ```

2. Check Spark resource allocation:
   ```python
   EXECUTOR_INSTANCES = "10"  # Increase from 5
   EXECUTOR_MEMORY = "16g"     # Increase from 8g
   ```

3. Monitor Spark job progress:
   ```bash
   kubectl logs <spark-pod> -n warehouse-1761913838-c49g --follow
   ```

---

### Issue 5: "Image pull failed" (K8s)

**Symptoms**:
```
Failed to pull image "cloudera/spark:latest":
rpc error: code = Unknown desc = Error response from daemon:
pull access denied for cloudera/spark
```

**Solutions**:
1. Verify correct image name:
   ```bash
   kubectl get pods -n warehouse-1761913838-c49g -o jsonpath='{.items[0].spec.containers[0].image}'
   ```

2. Update in DAG:
   ```python
   SPARK_CONTAINER_IMAGE = "correct-registry/spark:version"
   ```

3. Add image pull secret (if using private registry):
   ```bash
   kubectl create secret docker-registry my-secret \
     --docker-server=my-registry.com \
     --docker-username=user \
     --docker-password=pass
   ```

---

## Rollback Procedures

### Automatic Rollback

If you used the deployment script, backups were created automatically:

```bash
# Check backup location
ls -la /opt/backups/etl_*/

# Rollback to previous version
./deploy_etl.sh rollback

# This will restore files from the latest backup
```

### Manual Rollback

If you need to rollback to a specific version:

```bash
# 1. Find backup directory
ls -la /opt/backups/

# 2. Restore Spark jobs
cp /opt/backups/etl_20250106_120000/spark_jobs_backup/* /opt/spark_jobs/

# 3. Restore Airflow DAGs
cp /opt/backups/etl_20250106_120000/airflow_dags_backup/* /opt/airflow/dags/

# 4. Restart Airflow
systemctl restart airflow-scheduler
```

### Emergency Rollback (Stop All Jobs)

```bash
# 1. Pause DAG in Airflow
airflow dags pause banking_etl_pipeline

# 2. Kill running tasks
airflow tasks clear banking_etl_pipeline --start-date 2025-01-06 --end-date 2025-01-06 --yes

# 3. Kill Spark jobs
kubectl delete pod -n warehouse-1761913838-c49g -l app=banking-etl

# 4. Restore previous version (see above)

# 5. Unpause DAG
airflow dags unpause banking_etl_pipeline
```

---

## Next Steps After Deployment

### 1. Verify Deployment
- [ ] Check Airflow UI for DAG visibility
- [ ] Run syntax validation on all files
- [ ] Test individual Spark job

### 2. Initial Run
- [ ] Trigger DAG manually
- [ ] Monitor execution in Airflow UI
- [ ] Check Spark job logs
- [ ] Verify data in gold layer

### 3. Schedule Automation
- [ ] Update schedule_interval in DAG
- [ ] Set up email notifications
- [ ] Configure Slack alerts (optional)

### 4. Monitoring Setup
- [ ] Create monitoring queries
- [ ] Set up data quality alerts
- [ ] Configure log aggregation

### 5. Documentation
- [ ] Document any configuration changes
- [ ] Update runbook with actual paths
- [ ] Record backup locations

---

## Support and Contacts

**For deployment issues**:
- Check troubleshooting section above
- Review Airflow logs
- Contact: data-engineering@company.com

**For cluster issues**:
- Check Cloudera Manager
- Contact: cdp-admin@company.com

**For urgent issues**:
- On-call: +1-XXX-XXX-XXXX

---

## Appendix

### File Permissions

Required permissions for deployed files:

```
/opt/spark_jobs/
├── 01_stage_to_bronze.py      (755 - rwxr-xr-x)
├── 02_bronze_to_silver.py     (755 - rwxr-xr-x)
└── 03_silver_to_gold.py       (755 - rwxr-xr-x)

/opt/airflow/dags/
└── banking_etl_pipeline.py    (644 - rw-r--r--)
```

### Environment Variables

Set these in your profile or CI/CD:

```bash
# Add to ~/.bashrc or ~/.profile
export SPARK_HOME=/usr/lib/spark
export AIRFLOW_HOME=/opt/airflow
export METASTORE_URI="thrift://metastore-service.warehouse-1761913838-c49g.svc.cluster.local:9083"
export S3_BUCKET="co-op-buk-39d7d9df"
```

### Useful Commands Reference

```bash
# Airflow
airflow dags list
airflow dags trigger <dag_id>
airflow tasks list <dag_id>
airflow tasks log <dag_id> <task_id> <execution_date>

# Kubernetes
kubectl get pods -n <namespace>
kubectl logs <pod-name> -n <namespace>
kubectl describe pod <pod-name> -n <namespace>

# Spark
spark-submit --help
spark-submit --master local[*] <script>

# Hive
beeline -u "jdbc:hive2://...:10000" -e "SELECT COUNT(*) FROM bronze.clients"
```

---

**Last Updated**: 2025-01-06
**Version**: 1.0
**Maintained By**: Data Engineering Team
