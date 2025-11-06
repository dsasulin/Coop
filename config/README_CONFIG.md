# Environment Configuration Guide

This document provides instructions for filling out `environment_config.yaml` and regenerating Spark jobs and Airflow DAGs.

## Overview

The `environment_config.yaml` file is the **single source of truth** for all environment-specific parameters used by:
- ✅ PySpark ETL Jobs
- ✅ Airflow DAGs
- ✅ NiFi Flows (configuration reference)

## Quick Start

### 1. Fill in Configuration

```bash
# Copy the template
cd config/
cp environment_config.yaml environment_config.yaml.backup

# Edit the file
vim environment_config.yaml
# OR
nano environment_config.yaml
```

### 2. Required Fields

All fields marked with `<FILL_IN>` must be populated before regenerating code.

**Critical fields to fill:**
- `cdp.cluster.name`
- `cdp.hdfs.namenode`
- `cdp.hive.metastore_uri`
- `spark.executor.instances`
- `spark.executor.memory`
- `airflow.default_args.email`
- `data_sources.csv_input.base_path`

### 3. Regenerate Code

After filling in the configuration:

```bash
# Regenerate Spark jobs and Airflow DAGs
python scripts/generate_from_config.py

# This will update:
# - spark_jobs/*.py
# - airflow_dags/*.py
```

## Configuration Sections

### Section 1: CDP Configuration

**Purpose**: Define Cloudera cluster connection details

**Example**:
```yaml
cdp:
  cluster:
    name: "production-aws-cluster"
    environment: "production"
    region: "us-east-1"

  hdfs:
    namenode: "hdfs://ip-10-0-1-100.ec2.internal:8020"
    base_path: "/user/hive/warehouse"
    temp_path: "/tmp/banking_etl"

  hive:
    metastore_uri: "thrift://ip-10-0-1-101.ec2.internal:9083"

  yarn:
    resource_manager: "ip-10-0-1-100.ec2.internal:8032"
    queue: "production"
```

**Where to find values**:
- Cloudera Manager → Clusters → Service Details
- HDFS namenode: CM → HDFS → Instances → NameNode
- Hive metastore: CM → Hive → Configuration → hive.metastore.uris
- YARN RM: CM → YARN → Instances → ResourceManager

---

### Section 2: Database Configuration

**Purpose**: Define Hive database names and locations

**Example**:
```yaml
databases:
  test:
    name: "test"
    location: "/user/hive/warehouse/test.db"

  bronze:
    name: "bronze"
    location: "/user/hive/warehouse/bronze.db"
```

**Default**: Keep standard names unless your organization has naming conventions.

---

### Section 3: Data Sources

**Purpose**: Define where input CSV files are located

**Example for S3**:
```yaml
data_sources:
  csv_input:
    base_path: "s3a://banking-data-prod/input"

  files:
    clients: "s3a://banking-data-prod/input/clients.csv"
    products: "s3a://banking-data-prod/input/products.csv"
    # ... etc
```

**Example for HDFS**:
```yaml
data_sources:
  csv_input:
    base_path: "/data/banking/input"

  files:
    clients: "/data/banking/input/clients.csv"
    products: "/data/banking/input/products.csv"
    # ... etc
```

**Example for Local (Development)**:
```yaml
data_sources:
  csv_input:
    base_path: "file:///opt/data/input"

  files:
    clients: "file:///opt/data/input/clients.csv"
    # ... etc
```

---

### Section 4: Spark Configuration

**Purpose**: Define Spark cluster resource allocation

**Small Cluster (10 nodes, 16GB RAM each)**:
```yaml
spark:
  master: "yarn"
  deploy_mode: "cluster"

  driver:
    memory: "4g"
    cores: 2

  executor:
    memory: "8g"
    cores: 4
    instances: 10

  conf:
    spark.sql.shuffle.partitions: 200
    spark.default.parallelism: 200
    spark.dynamicAllocation.enabled: false
```

**Medium Cluster (20 nodes, 32GB RAM each)**:
```yaml
spark:
  executor:
    memory: "16g"
    cores: 4
    instances: 20

  conf:
    spark.sql.shuffle.partitions: 400
    spark.default.parallelism: 400
```

**Large Cluster (50+ nodes, 64GB RAM each)**:
```yaml
spark:
  executor:
    memory: "32g"
    cores: 8
    instances: 50

  conf:
    spark.sql.shuffle.partitions: 800
    spark.default.parallelism: 800
    spark.dynamicAllocation.enabled: true
    spark.dynamicAllocation.minExecutors: 10
    spark.dynamicAllocation.maxExecutors: 100
```

**How to size Spark resources**:

1. **Executor Memory**:
   - Start with 60-70% of node RAM
   - Example: 32GB node → 16-20GB executor memory

2. **Executor Cores**:
   - Typically 4-8 cores per executor
   - More cores = better parallelism, but watch CPU contention

3. **Number of Executors**:
   - Total cores available / cores per executor
   - Example: 20 nodes × 16 cores = 320 cores / 4 cores per executor = 80 executors

4. **Shuffle Partitions**:
   - Rule of thumb: 2-4× number of executors × cores
   - Example: 20 executors × 4 cores × 2 = 160 partitions (round to 200)

---

### Section 5: Airflow Configuration

**Purpose**: Define DAG scheduling and execution parameters

**Example - Production**:
```yaml
airflow:
  default_args:
    owner: "data_engineering"
    email: ["etl-alerts@company.com", "data-team@company.com"]
    retries: 3
    retry_delay_minutes: 10
    execution_timeout_minutes: 180  # 3 hours

  schedules:
    full_etl: "0 2 * * *"            # Daily at 2 AM UTC
    stage_to_bronze: "0 1 * * *"     # Daily at 1 AM UTC
    bronze_to_silver: "30 1 * * *"   # Daily at 1:30 AM UTC
    silver_to_gold: "0 2 * * *"      # Daily at 2 AM UTC

  max_active_runs: 1                 # Only one DAG run at a time
  concurrency: 16                    # Max parallel tasks
```

**Example - Development**:
```yaml
airflow:
  default_args:
    owner: "developer"
    email: ["dev-team@company.com"]
    retries: 1
    retry_delay_minutes: 5
    execution_timeout_minutes: 60

  schedules:
    full_etl: "@once"                # Manual trigger only
    stage_to_bronze: "@once"
    bronze_to_silver: "@once"
    silver_to_gold: "@once"

  max_active_runs: 3                 # Allow multiple test runs
```

**Cron Schedule Examples**:
```
"0 2 * * *"      # Daily at 2:00 AM
"0 */6 * * *"    # Every 6 hours
"0 2 * * 1-5"    # Weekdays at 2:00 AM
"0 2 1 * *"      # First day of month at 2:00 AM
"@hourly"        # Every hour
"@daily"         # Daily at midnight
"@weekly"        # Weekly on Sunday at midnight
"@once"          # Manual trigger only (no schedule)
```

---

### Section 6: NiFi Configuration

**Purpose**: Reference for NiFi flow configuration (manual setup required)

**Example**:
```yaml
nifi:
  url: "https://nifi.company.com:8443"
  api_url: "https://nifi.company.com:8443/nifi-api"

  processors:
    get_file:
      source_directory: "/data/incoming"
      file_filter: ".*\\.csv"
      polling_interval: "60 sec"

    put_hdfs:
      hdfs_url: "hdfs://namenode:8020"
      directory: "/data/landing/banking"
```

**Note**: NiFi configuration is provided as reference. Flows must be configured manually in NiFi UI.

---

### Section 7: Data Quality

**Purpose**: Define data quality thresholds and rules

**Example - Strict**:
```yaml
data_quality:
  thresholds:
    min_data_quality_score: 0.95     # 95% quality required
    max_null_percentage: 0.05        # Max 5% nulls
    max_duplicate_percentage: 0.01   # Max 1% duplicates
```

**Example - Lenient**:
```yaml
data_quality:
  thresholds:
    min_data_quality_score: 0.80     # 80% quality required
    max_null_percentage: 0.15        # Max 15% nulls
    max_duplicate_percentage: 0.05   # Max 5% duplicates
```

---

### Section 8: Notifications

**Purpose**: Configure email and Slack alerts

**Example - Gmail SMTP**:
```yaml
notifications:
  email:
    enabled: true
    smtp_host: "smtp.gmail.com"
    smtp_port: 587
    smtp_user: "etl-alerts@gmail.com"
    smtp_password: "<APP_PASSWORD>"  # Use app-specific password
    use_tls: true

    recipients:
      success: ["data-team@company.com"]
      failure: ["data-team@company.com", "oncall@company.com"]
      data_quality_issues: ["dq-team@company.com"]
```

**Example - Corporate SMTP**:
```yaml
notifications:
  email:
    smtp_host: "smtp.company.com"
    smtp_port: 25
    smtp_user: "airflow@company.com"
    smtp_password: "<PASSWORD>"
    use_tls: false
```

**Example - Slack**:
```yaml
  slack:
    enabled: true
    webhook_url: "https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXX"
    channel: "#etl-alerts"
```

---

## Environment-Specific Configurations

Use the `environments` section to override values per environment:

```yaml
environments:
  development:
    spark.executor.instances: 2
    spark.executor.memory: "4g"
    airflow.schedules.full_etl: "@once"

  production:
    spark.executor.instances: 20
    spark.executor.memory: "16g"
    airflow.schedules.full_etl: "0 2 * * *"
```

**Usage**:
```bash
# Generate for production
python scripts/generate_from_config.py --env production

# Generate for development
python scripts/generate_from_config.py --env development
```

---

## Security Best Practices

### 1. Secrets Management

**DO NOT** commit passwords and keys to Git!

**Option A: Environment Variables**
```yaml
smtp_password: ${SMTP_PASSWORD}
spark.hadoop.fs.s3a.access.key: ${AWS_ACCESS_KEY}
```

Then set environment variables:
```bash
export SMTP_PASSWORD="your_password"
export AWS_ACCESS_KEY="your_key"
```

**Option B: AWS Secrets Manager**
```yaml
smtp_password: "arn:aws:secretsmanager:us-east-1:123456789012:secret:etl/smtp-password"
```

**Option C: Airflow Connections/Variables**
- Store sensitive data in Airflow UI → Admin → Connections
- Reference in config as connection IDs

### 2. Kerberos

If your cluster uses Kerberos:
```yaml
cdp:
  kerberos:
    enabled: true
    principal: "hive/hostname@REALM.COM"
    keytab: "/etc/security/keytabs/hive.keytab"
```

---

## Validation

Before regenerating code, validate your configuration:

```bash
# Check YAML syntax
python -c "import yaml; yaml.safe_load(open('environment_config.yaml'))"

# Validate configuration
python scripts/validate_config.py

# Expected output:
# ✅ All required fields filled
# ✅ YAML syntax valid
# ✅ Values within acceptable ranges
```

---

## Regeneration Process

### Step 1: Update Configuration

```bash
vim config/environment_config.yaml
```

### Step 2: Run Generator Script

```bash
cd /Users/dsasulin/Documents/GitHub/Coop

# Generate for production
python scripts/generate_from_config.py --env production

# Generate for development
python scripts/generate_from_config.py --env development
```

### Step 3: Review Generated Files

The script will update:

**PySpark Jobs** (`spark_jobs/`):
- `01_stage_to_bronze.py`
- `02_bronze_to_silver.py`
- `03_silver_to_gold.py`

**Airflow DAGs** (`airflow_dags/`):
- `full_etl_pipeline.py`
- `incremental_etl.py`

**Changes**:
- Connection strings
- Resource allocations
- File paths
- Schedule intervals
- Email recipients

### Step 4: Test Generated Code

```bash
# Dry run Spark job
spark-submit --help spark_jobs/01_stage_to_bronze.py

# Validate Airflow DAG
airflow dags test full_etl_pipeline 2025-01-06

# Check syntax
python -m py_compile spark_jobs/*.py
python -m py_compile airflow_dags/*.py
```

### Step 5: Deploy

```bash
# Copy to Airflow dags folder
cp airflow_dags/*.py /opt/airflow/dags/

# Copy Spark jobs to shared location
cp spark_jobs/*.py /opt/spark_jobs/

# Restart Airflow (if needed)
systemctl restart airflow-scheduler
systemctl restart airflow-webserver
```

---

## Troubleshooting

### Issue: "KeyError: missing required field"

**Solution**: Check that all `<FILL_IN>` placeholders are replaced.

```bash
# Find remaining placeholders
grep -r "FILL_IN" environment_config.yaml
```

### Issue: "YAML parsing error"

**Solution**: Check indentation (use spaces, not tabs).

```bash
# Validate YAML
python -c "import yaml; yaml.safe_load(open('environment_config.yaml'))"
```

### Issue: "Spark job fails with OutOfMemoryError"

**Solution**: Increase executor memory or reduce executor cores.

```yaml
spark:
  executor:
    memory: "16g"  # Increase from 8g
    cores: 4       # Reduce from 8
```

### Issue: "Airflow DAG not appearing"

**Solution**:
1. Check DAG syntax: `airflow dags list`
2. Check logs: `tail -f /opt/airflow/logs/scheduler/latest/*.log`
3. Verify file permissions: `ls -la /opt/airflow/dags/`

---

## Example: Complete Configuration

See `environment_config.example.yaml` for a fully filled example configuration for a medium-sized cluster.

---

## Support

For questions or issues:
- Check this README
- Review example configuration
- Contact Data Engineering team: `<YOUR_TEAM_EMAIL>`

---

**Last Updated**: 2025-01-06
**Version**: 1.0
**Maintained By**: Data Engineering Team
