# Configuration Quick Start Checklist

Use this checklist to quickly fill out `environment_config.yaml`.

## ✅ Prerequisites

- [ ] Access to Cloudera Manager
- [ ] Access to HDFS/S3 data location
- [ ] Airflow admin credentials
- [ ] SMTP server details (for email alerts)

---

## 📋 Configuration Checklist

### 1. CDP Cluster Information (5 min)

**Get from Cloudera Manager → Clusters**

```yaml
cdp:
  cluster:
    name: ________________              # Your cluster name
    environment: ________________       # production/staging/development
    region: ________________            # AWS region or datacenter

  hdfs:
    namenode: ________________          # hdfs://hostname:8020

  hive:
    metastore_uri: ________________     # thrift://hostname:9083

  yarn:
    resource_manager: ________________  # hostname:8032
    queue: ________________             # default/production/etc
```

**Where to find**:
- CM → HDFS → Instances → NameNode → hostname + port 8020
- CM → Hive → Configuration → search "metastore.uris"
- CM → YARN → Instances → ResourceManager → hostname + port 8032

---

### 2. Data Locations (3 min)

**Choose one**: S3, HDFS, or Local

```yaml
data_sources:
  csv_input:
    base_path: ________________         # s3a://bucket/path OR /hdfs/path

  files:
    clients: ________________/clients.csv
    products: ________________/products.csv
    transactions: ________________/transactions.csv
    # ... (use same base_path for all)
```

**Examples**:
- S3: `s3a://my-banking-data/input`
- HDFS: `/data/banking/input`
- Local: `file:///opt/data/input` (dev only)

---

### 3. Spark Resources (10 min)

**Based on your cluster size**:

**Small cluster (10 nodes, 16GB each)**:
```yaml
spark:
  executor:
    memory: "8g"
    cores: 4
    instances: 10

  conf:
    spark.sql.shuffle.partitions: 200
```

**Medium cluster (20 nodes, 32GB each)**:
```yaml
spark:
  executor:
    memory: "16g"
    cores: 4
    instances: 20

  conf:
    spark.sql.shuffle.partitions: 400
```

**Large cluster (50+ nodes, 64GB each)**:
```yaml
spark:
  executor:
    memory: "32g"
    cores: 8
    instances: 50

  conf:
    spark.sql.shuffle.partitions: 800
```

**Fill in**:
```yaml
spark:
  master: "yarn"
  deploy_mode: "cluster"

  driver:
    memory: ________________            # 4g/8g/16g
    cores: ________________             # 2/4/8

  executor:
    memory: ________________            # 8g/16g/32g
    cores: ________________             # 4/8
    instances: ________________         # 10/20/50
```

---

### 4. Airflow Configuration (5 min)

```yaml
airflow:
  default_args:
    owner: ________________              # Your team name
    email: [________________]            # Alert email(s)
    retries: ________________            # 2 or 3
    retry_delay_minutes: ________________ # 5 or 10

  schedules:
    full_etl: ________________           # "0 2 * * *" or "@once"
```

**Common schedules**:
- Daily at 2 AM: `"0 2 * * *"`
- Every 6 hours: `"0 */6 * * *"`
- Manual only: `"@once"`
- Weekdays at 2 AM: `"0 2 * * 1-5"`

---

### 5. Email Notifications (5 min)

```yaml
notifications:
  email:
    smtp_host: ________________         # smtp.gmail.com or smtp.company.com
    smtp_port: ________________         # 587 (TLS) or 25
    smtp_user: ________________         # email address
    smtp_password: ________________     # password or ${SMTP_PASSWORD}

    recipients:
      success: [________________]
      failure: [________________]
      data_quality_issues: [________________]
```

**Gmail example**:
- Host: `smtp.gmail.com`
- Port: `587`
- User: `your-email@gmail.com`
- Password: Use [App Password](https://support.google.com/accounts/answer/185833)

**Corporate SMTP**:
- Ask IT department for SMTP settings

---

### 6. Optional: Slack Alerts (2 min)

```yaml
  slack:
    enabled: ________________           # true or false
    webhook_url: ________________       # Get from Slack → Apps → Incoming Webhooks
    channel: ________________           # "#etl-alerts"
```

---

### 7. Data Quality Thresholds (2 min)

```yaml
data_quality:
  thresholds:
    min_data_quality_score: ________________  # 0.80-0.95 (80%-95%)
    max_null_percentage: ________________     # 0.05-0.15 (5%-15%)
    max_duplicate_percentage: ________________ # 0.01-0.05 (1%-5%)
```

**Recommended**:
- Production: 0.95 / 0.05 / 0.01
- Staging: 0.85 / 0.10 / 0.03
- Development: 0.70 / 0.15 / 0.05

---

## 🚀 Quick Fill Template

Copy and paste this, then fill in the blanks:

```yaml
# CLUSTER
cdp.cluster.name: "________________"
cdp.hdfs.namenode: "hdfs://________________:8020"
cdp.hive.metastore_uri: "thrift://________________:9083"
cdp.yarn.resource_manager: "________________:8032"
cdp.yarn.queue: "________________"

# DATA SOURCES
data_sources.csv_input.base_path: "________________"

# SPARK
spark.driver.memory: "________________"
spark.executor.memory: "________________"
spark.executor.cores: ________________
spark.executor.instances: ________________
spark.conf.spark.sql.shuffle.partitions: ________________

# AIRFLOW
airflow.default_args.owner: "________________"
airflow.default_args.email: ["________________"]
airflow.default_args.retries: ________________
airflow.schedules.full_etl: "________________"

# NOTIFICATIONS
notifications.email.smtp_host: "________________"
notifications.email.smtp_port: ________________
notifications.email.smtp_user: "________________"
notifications.email.smtp_password: "________________"
notifications.email.recipients.failure: ["________________"]

# DATA QUALITY
data_quality.thresholds.min_data_quality_score: ________________
```

---

## ✅ Validation Steps

After filling in values:

### Step 1: Check YAML Syntax
```bash
python -c "import yaml; yaml.safe_load(open('config/environment_config.yaml'))"
```

Expected: No errors

### Step 2: Find Remaining Placeholders
```bash
grep -n "FILL_IN" config/environment_config.yaml
```

Expected: No results

### Step 3: Validate Configuration
```bash
python scripts/validate_config.py
```

Expected: ✅ All checks passed

---

## 🔄 Next Steps

Once configuration is complete:

1. **Generate Spark jobs and Airflow DAGs**:
   ```bash
   python scripts/generate_from_config.py --env production
   ```

2. **Test generated code**:
   ```bash
   python -m py_compile spark_jobs/*.py
   python -m py_compile airflow_dags/*.py
   ```

3. **Deploy to Airflow**:
   ```bash
   cp airflow_dags/*.py /opt/airflow/dags/
   cp spark_jobs/*.py /opt/spark_jobs/
   ```

4. **Verify in Airflow UI**:
   - Navigate to Airflow UI
   - Check that DAGs appear
   - Trigger test run

---

## 📞 Need Help?

- **Documentation**: See `config/README_CONFIG.md`
- **Examples**: See `config/environment_config.example.yaml`
- **Team**: Contact data engineering team

---

**Estimated time to complete**: 30-45 minutes

**Last Updated**: 2025-01-06
