# Configuration Files Index

This directory contains all environment configuration files for the Banking ETL project.

> **Important:** the `environment_config*.yaml` files here are documentation and planning templates. No code in this repo reads them. The actual runtime configuration comes from environment variables (an `.env` file) loaded by `utils/config.py`. Any workflow below that mentions `scripts/generate_from_config.py` or `scripts/validate_config.py` is not implemented: there is no `scripts/` directory in this repo and those files do not exist.

## 📁 File Structure

```
config/
├── INDEX.md                           ← You are here
├── CONFIGURATION_SUMMARY.md           ← 🆕 START HERE! Analysis results
├── KUBERNETES_CDP_GUIDE.md            ← 🆕 K8s-based CDP explanation
├── environment_config.filled.yaml     ← 🆕 YOUR ACTUAL CONFIG (from cluster)
├── environment_config.yaml            ← Template (empty)
├── environment_config.example.yaml    ← Example with "HOW TO GET" commands
├── README_CONFIG.md                   ← Detailed documentation
├── QUICKSTART_CHECKLIST.md            ← 30-minute quick start guide
└── current.txt                        ← Raw Hue query results
```

## 🎯 Quick Start

**🆕 If you just received config analysis results:**

1. **Read the summary** → `CONFIGURATION_SUMMARY.md` (10 min)
2. **Understand your cluster** → `KUBERNETES_CDP_GUIDE.md` (15 min)
3. **Review your config** → `environment_config.filled.yaml` (5 min)
4. **Update TODO items** → Fill in missing values (emails, region, etc.)
5. **Continue using Hue SQL** → Keep running your ETL scripts!

**📝 If you're setting up from scratch:**

1. **Read the checklist** → `QUICKSTART_CHECKLIST.md` (5 min)
2. **Copy the template** → `cp environment_config.yaml my_config.yaml`
3. **Fill in values** → Edit `my_config.yaml` using the checklist (30 min)
4. Validate and generate steps below are NOT IMPLEMENTED: `scripts/validate_config.py` and `scripts/generate_from_config.py` do not exist. Filling in the YAML does not change any Spark job or DAG.

## 📄 File Descriptions

### 🆕 NEW FILES (2025-01-06)

#### `CONFIGURATION_SUMMARY.md` ⭐ **START HERE**

**Purpose**: Summary of cluster configuration analysis

**Status**: ✅ Complete analysis

**Contents**:
- What type of cluster you have (K8s-based CDP)
- Key findings from Hue query results
- Extracted configuration parameters
- Comparison: traditional YARN vs your K8s cluster
- Description of all files created
- TODO items that need your input
- Next steps (immediate, short-term, long-term)

**Length**: ~500 lines

**Read this if**:
- You just received the config analysis
- You want to understand what was found
- You need to know what files to read next

---

#### `KUBERNETES_CDP_GUIDE.md` ⭐ **IMPORTANT**

**Purpose**: Comprehensive guide for Kubernetes-based CDP

**Status**: ✅ Complete

**Contents**:
- Explanation of K8s-based CDP vs traditional YARN
- 4 options for running ETL jobs on your cluster
- Recommended approach (Hue SQL - what you're doing now!)
- S3 bucket structure diagram
- Data flow diagram (test → bronze → silver → gold)
- How to access your cluster (Hue, Beeline, Spark)
- Troubleshooting guide
- Useful SQL queries for monitoring

**Length**: ~620 lines

**Read this if**:
- You want to understand your cluster architecture
- You're planning to add automation
- You need to decide between Hue/NiFi/CDE/Spark options

---

#### `environment_config.filled.yaml` ⭐ **YOUR ACTUAL CONFIG**

**Purpose**: Configuration file with YOUR actual cluster values

**Status**: ✅ Filled with real values from current.txt

**Contents**:
- All 14 configuration sections populated
- Actual S3 bucket: `co-op-buk-39d7d9df`
- Actual metastore URI: `thrift://metastore-service...`
- Actual database locations in S3
- Kubernetes-specific configuration
- All 12 bronze tables listed
- Security settings (Kerberos enabled)
- TODO items for values needing verification

**Length**: 674 lines (19KB)

**Use this for**:
- Reference when writing Spark jobs (future)
- Configuring Airflow DAGs (future)
- Setting up NiFi flows (future)
- Understanding your cluster parameters

**Validation**: ✅ YAML syntax valid

---

#### `current.txt`

**Purpose**: Raw output from Hue SQL queries

**Status**: ✅ Analyzed

**Contents**:
- Hive configuration parameters (SET commands)
- Database descriptions (DESCRIBE DATABASE)
- Table listings (SHOW TABLES)
- System properties and environment variables

**Length**: 1,979 lines (260KB)

**Use this for**: Reference only (already analyzed)

---

### ORIGINAL FILES

### 1. `environment_config.yaml`

**Purpose**: Single source of truth for all environment parameters

**Status**: 🔴 Template - needs to be filled

**Sections** (14 total):
1. CDP Configuration (cluster details)
2. Database Configuration (Hive databases)
3. Data Sources (CSV file locations)
4. Spark Configuration (resources, tuning)
5. Airflow Configuration (scheduling, retries)
6. NiFi Configuration (data ingestion)
7. Data Quality (thresholds, rules)
8. Monitoring and Logging
9. Notifications (email, Slack)
10. Performance Tuning
11. Security Configuration
12. Backup and Recovery
13. Testing Configuration
14. Environment-Specific Overrides

**Required fields**: ~50 parameters marked with `<FILL_IN>`

**Usage**:
```bash
# Fill out this file first (for reference only, nothing reads it)
vim environment_config.yaml

# NOT IMPLEMENTED - scripts/generate_from_config.py does not exist
python scripts/generate_from_config.py
```

---

### 2. `environment_config.example.yaml`

**Purpose**: Fully filled example for reference

**Status**: ✅ Complete example

**Use this for**:
- Understanding what values look like
- Copying sections to your config
- Troubleshooting configuration issues

**Example environment**: Medium-sized production cluster
- 20 nodes, 32GB RAM each
- S3 data sources
- Daily ETL schedule
- Email + Slack alerts

**Copy sections**:
```bash
# Copy Spark config from example
sed -n '/^spark:/,/^[a-z]/p' environment_config.example.yaml
```

---

### 3. `README_CONFIG.md`

**Purpose**: Comprehensive documentation

**Status**: ✅ Complete

**Contents**:
- Detailed explanation of each section
- How to find values in Cloudera Manager
- Sizing guidelines for Spark resources
- Cron schedule examples
- Security best practices
- Troubleshooting guide

**Length**: ~500 lines

**Read this if**:
- You need detailed explanations
- You're sizing Spark resources
- You're troubleshooting configuration errors
- You need security guidelines

---

### 4. `QUICKSTART_CHECKLIST.md`

**Purpose**: Fast 30-minute setup guide

**Status**: ✅ Complete

**Format**: Checklist with fill-in-the-blank template

**Use this if**:
- You want to get started quickly
- You already know your cluster details
- You prefer step-by-step instructions

**Time**: 30-45 minutes to complete

---

## 🔄 Configuration Workflow

```
┌─────────────────────────────────────────────────────────┐
│ 1. READ DOCUMENTATION                                   │
│    - QUICKSTART_CHECKLIST.md (quick start)              │
│    - README_CONFIG.md (detailed guide)                  │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│ 2. FILL CONFIGURATION                                   │
│    - Copy environment_config.yaml                       │
│    - Fill in <FILL_IN> placeholders                     │
│    - Reference environment_config.example.yaml          │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│ 3. VALIDATE  (NOT IMPLEMENTED)                          │
│    scripts/validate_config.py does not exist            │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│ 4. GENERATE CODE  (NOT IMPLEMENTED)                     │
│    scripts/generate_from_config.py does not exist       │
│    Spark jobs and DAGs are edited by hand               │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│ 5. TEST & DEPLOY                                        │
│    - Test Spark jobs locally                            │
│    - Test Airflow DAGs                                  │
│    - Deploy to production                               │
└─────────────────────────────────────────────────────────┘
```

Steps 3 and 4 are aspirational. The YAML template is not wired to any generator or validator. Runtime configuration is environment variables read by `utils/config.py`.

## 📊 Configuration Sections Overview

| Section | Parameters | Complexity | Time to Fill |
|---------|-----------|------------|--------------|
| 1. CDP Config | 10 | Medium | 5 min |
| 2. Databases | 4 | Easy | 2 min |
| 3. Data Sources | 15 | Easy | 5 min |
| 4. Spark | 20 | High | 10 min |
| 5. Airflow | 15 | Medium | 5 min |
| 6. NiFi | 10 | Low | 3 min |
| 7. Data Quality | 5 | Easy | 2 min |
| 8. Monitoring | 5 | Easy | 2 min |
| 9. Notifications | 8 | Medium | 5 min |
| 10. Performance | 6 | Medium | 3 min |
| 11. Security | 5 | Easy | 2 min |
| 12. Backup | 5 | Easy | 2 min |
| 13. Testing | 4 | Easy | 2 min |
| 14. Environments | 3 | Easy | 2 min |
| **TOTAL** | **~115** | - | **~50 min** |

## 🎓 Learning Path

### Beginner
1. Start with `QUICKSTART_CHECKLIST.md`
2. Use `environment_config.example.yaml` as reference
3. Fill only required fields (marked `<FILL_IN>`)
4. Use default values for optional fields

### Intermediate
1. Read `README_CONFIG.md` sections for areas you're configuring
2. Customize Spark resources based on cluster size
3. Configure data quality thresholds
4. Set up email notifications

### Advanced
1. Read full `README_CONFIG.md`
2. Fine-tune Spark performance settings
3. Configure environment-specific overrides
4. Set up comprehensive monitoring and alerts
5. Implement security best practices

## 🔑 Key Configuration Parameters

**Most Important** (must fill):
```yaml
cdp.hdfs.namenode                      # Where data is stored
cdp.hive.metastore_uri                 # Where metadata is stored
data_sources.csv_input.base_path       # Where input files are
spark.executor.memory                  # Spark resource allocation
spark.executor.instances               # Number of Spark executors
airflow.default_args.email             # Where to send alerts
airflow.schedules.full_etl             # When to run ETL
```

**Important** (should fill):
```yaml
cdp.yarn.queue                         # YARN queue for jobs
spark.conf.spark.sql.shuffle.partitions # Parallelism tuning
airflow.default_args.retries           # Retry behavior
notifications.email.smtp_*             # Email server settings
data_quality.thresholds.*              # Quality thresholds
```

**Optional** (use defaults if unsure):
```yaml
performance.*                          # Performance tuning
security.*                             # Security settings
monitoring.*                           # Monitoring config
backup.*                               # Backup settings
```

## 🔍 Common Questions

**Q: Which file should I edit?**
A: Edit `environment_config.yaml`. The other files are for reference only.

**Q: Can I use multiple config files?**
A: Yes! Create separate files for dev/staging/prod:
```bash
environment_config.dev.yaml
environment_config.staging.yaml
environment_config.prod.yaml
```

Then generate with:
```bash
python scripts/generate_from_config.py --config environment_config.prod.yaml
```

**Q: What if I don't know a value?**
A: Check these sources in order:
1. `environment_config.example.yaml` - see examples
2. `README_CONFIG.md` - detailed explanations
3. Cloudera Manager - cluster details
4. Ask your data engineering team

**Q: Can I use environment variables?**
A: Yes! Use `${VAR_NAME}` syntax:
```yaml
smtp_password: ${SMTP_PASSWORD}
```

**Q: How do I validate my config?**
A: Run the validation script:
```bash
python scripts/validate_config.py environment_config.yaml
```

## 📞 Support

**Documentation Issues**: Create issue in GitHub
**Configuration Help**: Contact data engineering team
**Example Needed**: Check `environment_config.example.yaml`
**Quick Question**: See `QUICKSTART_CHECKLIST.md`

## 📝 Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2025-01-06 | Initial configuration templates created |
| 1.1 | 2025-01-06 | 🆕 Added cluster analysis results: filled config, K8s guide, summary |

---

## 🎯 Quick Access

**Just getting started?**
1. 📖 Read `CONFIGURATION_SUMMARY.md` (analysis results)
2. 📚 Read `KUBERNETES_CDP_GUIDE.md` (understand your cluster)
3. 🔧 Review `environment_config.filled.yaml` (your actual config)

**Setting up from scratch?**
1. 📖 Read `QUICKSTART_CHECKLIST.md`
2. ✏️ Fill `environment_config.yaml`
3. ✅ Validate with `validate_config.py`
4. 🚀 Generate code with `generate_from_config.py`

---

**Last Updated**: 2025-01-06 (v1.1)
**Maintained By**: Data Engineering Team
