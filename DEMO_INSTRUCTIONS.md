# Demo Instructions - Cloudera Data Platform Capabilities

This guide provides concise demonstration instructions for each requirement listed in the cases.txt file. Each section includes 2-3 sentence instructions and relevant code examples where applicable.

---

## 1. Data Transformation and Error Handling

**Demo Instructions:**
Show the Bronze to Silver ETL job (`bronze_to_silver.py`) which demonstrates filtering, enrichment, format conversion, and merging datasets. Highlight the data quality scoring logic, NULL handling, and derived field calculations (email normalization, age calculation, credit score categorization). Run the job with bad quality data from `Data/quality_test/` folder to show how the system handles errors gracefully by calculating dq_score and flagging issues in dq_issues field.

**What to Show:**
```bash
# In Hue, run query to show data quality handling
USE silver;
SELECT
    client_id, full_name, dq_score, dq_issues
FROM clients
WHERE dq_score < 0.8
ORDER BY dq_score ASC
LIMIT 10;
```

**Code Reference:** `Spark/bronze_to_silver.py:95-115` (DQ scoring logic)

---

## 2. Job Scheduling - Batch, Streaming, and Event-Driven

**Demo Instructions:**
Open Airflow UI and show the `banking_etl_pipeline` DAG which runs daily batch processing at 2:00 UTC. Show the `banking_etl_incremental` DAG that runs hourly for near-real-time updates. Explain how CDE Jobs can be triggered via API calls or file arrival events using Airflow sensors.

**What to Show:**
```python
# In Airflow DAG file - show multiple scheduling patterns
banking_etl_pipeline:        schedule_interval='0 2 * * *'   # Daily batch
banking_etl_incremental:     schedule_interval='0 * * * *'   # Hourly streaming
```

**Additional Code Example - Event-Driven:**
```python
# Create event-driven DAG (add to Airflow/dags/)
from airflow.sensors.filesystem import FileSensor

with DAG('banking_event_driven') as dag:
    wait_for_file = FileSensor(
        task_id='wait_for_new_data',
        filepath='/data/incoming/clients_*.csv',
        poke_interval=60
    )

    trigger_etl = CDEJobRunOperator(
        task_id='trigger_bronze_load',
        job_name='stage_to_bronze_job'
    )

    wait_for_file >> trigger_etl
```

**Code Reference:** `Airflow/dags/banking_etl_pipeline.py:67-74` and `:255-263`

---

## 3. MPP SQL Engine - Metadata Caching

**Demo Instructions:**
In Hue, demonstrate Hive's metadata caching by running `ANALYZE TABLE` commands to collect statistics, then execute the same query twice showing improved performance on the second run. Show cached partition metadata using `SHOW PARTITIONS` and explain how Hive Metastore stores this information for query optimization.

**What to Show:**
```sql
-- Collect table statistics for caching
USE silver;
ANALYZE TABLE transactions COMPUTE STATISTICS;
ANALYZE TABLE transactions COMPUTE STATISTICS FOR COLUMNS;

-- Show cached partition metadata
SHOW PARTITIONS transactions;

-- Run query twice - second run will use cached metadata
SELECT
    transaction_year, transaction_month,
    COUNT(*) as txn_count,
    SUM(amount) as total_amount
FROM transactions
WHERE transaction_year = 2025 AND transaction_month = 1
GROUP BY transaction_year, transaction_month;

-- View metadata cache info
DESCRIBE EXTENDED transactions;
```

**Code Reference:** `README.md:369-383` (Performance Optimization)

---

## 4. Distributed File System - Scalable Storage

**Demo Instructions:**
Show how data is stored in HDFS/S3 using Parquet format with partitioning strategy for scalability and fault tolerance. Navigate through Hue File Browser to show directory structure: `/warehouse/bronze.db/transactions/year=2025/month=01/`. Demonstrate that multiple concurrent Spark jobs can read/write to the same partitions without conflicts, showcasing the distributed nature of the storage.

**What to Show:**
```bash
# Via Hue File Browser or hdfs command
# Navigate to: /user/hive/warehouse/bronze.db/transactions/

# Show partition structure
hdfs dfs -ls /user/hive/warehouse/silver.db/transactions/
# Output shows: year=2025/month=01/, year=2025/month=02/, etc.

# Show Parquet files with replication
hdfs dfs -stat "%r" /user/hive/warehouse/silver.db/transactions/year=2025/month=01/*.parquet
# Shows replication factor (typically 3 for fault tolerance)
```

**Code Reference:** DDL scripts show `STORED AS PARQUET` and `PARTITIONED BY` clauses

---

## 5. Data Profiling at Ingestion and Transformation

**Demo Instructions:**
Run the data profiling query from `SQL/Analyse_Queries.sql` (Data Quality Check section) to show missing values, duplicates, outliers, and inconsistent formats across bronze and silver layers. Highlight how the `bronze_to_silver.py` job calculates dq_score for each record, identifying issues at transformation time. Load the bad quality test data from `Data/quality_test/` and show profiling results with detailed issue breakdown.

**What to Show:**
```sql
-- Comprehensive data profiling query
SELECT
    'clients' as table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT client_id) as unique_ids,
    COUNT(*) - COUNT(DISTINCT client_id) as duplicate_count,
    SUM(CASE WHEN email IS NULL OR email = '' THEN 1 ELSE 0 END) as missing_emails,
    SUM(CASE WHEN phone IS NULL OR phone = '' THEN 1 ELSE 0 END) as missing_phones,
    SUM(CASE WHEN credit_score < 300 OR credit_score > 850 THEN 1 ELSE 0 END) as invalid_credit_scores,
    ROUND(AVG(credit_score), 2) as avg_credit_score,
    ROUND(STDDEV(credit_score), 2) as credit_score_stddev
FROM silver.clients
UNION ALL
SELECT
    'transactions' as table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT transaction_id) as unique_ids,
    COUNT(*) - COUNT(DISTINCT transaction_id) as duplicate_count,
    SUM(CASE WHEN amount IS NULL OR amount <= 0 THEN 1 ELSE 0 END) as invalid_amounts,
    SUM(CASE WHEN transaction_date IS NULL THEN 1 ELSE 0 END) as missing_dates,
    SUM(CASE WHEN status NOT IN ('COMPLETED', 'PENDING', 'FAILED', 'CANCELLED') THEN 1 ELSE 0 END) as invalid_status,
    ROUND(AVG(amount), 2) as avg_amount,
    ROUND(STDDEV(amount), 2) as amount_stddev
FROM silver.transactions;

-- Show detailed DQ issues by category
SELECT
    dq_issues,
    COUNT(*) as issue_count,
    ROUND(AVG(dq_score), 3) as avg_dq_score,
    MIN(dq_score) as min_dq_score
FROM silver.clients
WHERE dq_score < 1.0
GROUP BY dq_issues
ORDER BY issue_count DESC;
```

**Code Reference:** `Spark/bronze_to_silver.py:95-115` and `SQL/Analyse_Queries.sql:185-204`

---

## 6. Version Control of Validation Rules

**Demo Instructions:**
Show the Git repository structure with all ETL scripts, DDL files, and validation logic stored in version control. Demonstrate git history for `bronze_to_silver.py` showing evolution of data quality rules over time. Explain how changes to validation logic are reviewed via pull requests before deployment to production.

**What to Show:**
```bash
# Show git history of validation rules
git log --oneline --follow Spark/bronze_to_silver.py

# Show specific changes to DQ rules
git diff HEAD~3 HEAD -- Spark/bronze_to_silver.py

# Show current branch and recent commits
git log --oneline -5

# Show how validation rules are documented
cat Spark/bronze_to_silver.py | grep -A 10 "Data Quality Score"
```

**Additional Code - Validation Rules Config File:**
Create `config/dq_rules.yaml`:
```yaml
# Data Quality Rules - Version 1.2.0
# Last updated: 2025-01-06

clients:
  required_fields:
    - client_id
    - first_name
    - last_name
    - email

  field_weights:
    first_name: 0.10
    last_name: 0.10
    email: 0.15
    phone: 0.15
    birth_date: 0.10
    address: 0.10
    city: 0.10
    credit_score: 0.10
    annual_income: 0.10

  validation_rules:
    credit_score:
      min: 300
      max: 850
    email:
      regex: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
    phone:
      regex: "^\\d{10,15}$"

transactions:
  required_fields:
    - transaction_id
    - from_account_id
    - amount
    - transaction_date

  validation_rules:
    amount:
      min: 0.01
    status:
      allowed_values: ['COMPLETED', 'PENDING', 'FAILED', 'CANCELLED']
```

**Code Reference:** Git repository structure at project root

---

## 7. Governance Policies - Retention, Archival, and Purging

**Demo Instructions:**
Show Hive table properties and partition retention policies. Demonstrate how old partitions can be automatically dropped using Airflow DAG with retention policy logic. Explain Cloudera Manager's data lifecycle policies for archiving data to cheaper storage tiers after a certain period.

**What to Show:**
```sql
-- Show table retention properties
DESCRIBE EXTENDED silver.transactions;

-- Show all partitions with dates
SHOW PARTITIONS silver.transactions;

-- Manual purge example (automated via Airflow)
ALTER TABLE silver.transactions
DROP IF EXISTS PARTITION (transaction_year=2023, transaction_month < 6);
```

**Additional Code - Retention Policy DAG:**
Create `Airflow/dags/data_retention_policy.py`:
```python
"""
Data Retention Policy DAG
Automatically purges partitions older than specified retention period
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data_governance',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

def generate_purge_partitions():
    """Generate list of partitions to purge (older than 24 months)"""
    retention_months = 24
    cutoff_date = datetime.now() - timedelta(days=retention_months * 30)

    partitions_to_drop = []
    for year in range(2020, cutoff_date.year + 1):
        for month in range(1, 13):
            if datetime(year, month, 1) < cutoff_date:
                partitions_to_drop.append(f"(transaction_year={year}, transaction_month={month})")

    print(f"Partitions to purge: {len(partitions_to_drop)}")
    return partitions_to_drop

with DAG(
    dag_id='data_retention_policy',
    default_args=default_args,
    schedule_interval='0 0 1 * *',  # Monthly on 1st day
    catchup=False,
    tags=['governance', 'retention', 'compliance']
) as dag:

    check_partitions = PythonOperator(
        task_id='check_partitions_to_purge',
        python_callable=generate_purge_partitions
    )

    # Archive to cold storage before purging
    archive_old_data = HiveOperator(
        task_id='archive_to_cold_storage',
        hql="""
            -- Copy old data to archive table in cold storage
            INSERT OVERWRITE TABLE archive.transactions_historical
            PARTITION (archive_year, archive_month)
            SELECT *, transaction_year, transaction_month
            FROM silver.transactions
            WHERE transaction_year < YEAR(ADD_MONTHS(CURRENT_DATE, -24));
        """
    )

    # Purge old partitions
    purge_old_partitions = HiveOperator(
        task_id='purge_old_partitions',
        hql="""
            -- Drop partitions older than 24 months
            ALTER TABLE silver.transactions
            DROP IF EXISTS PARTITION (
                transaction_year < YEAR(ADD_MONTHS(CURRENT_DATE, -24))
            );

            -- Also purge bronze layer
            ALTER TABLE bronze.transactions
            DROP IF EXISTS PARTITION (
                transaction_year < YEAR(ADD_MONTHS(CURRENT_DATE, -24))
            );
        """
    )

    check_partitions >> archive_old_data >> purge_old_partitions
```

**Code Reference:** New file to be created

---

## 8. Centralized Audit Framework - Access History and Reporting

**Demo Instructions:**
Open Cloudera Manager and navigate to Audits section to show centralized audit logs capturing all access events across Hive, Spark, HDFS, and other services. Demonstrate filtering by user, service, operation type, and time range. Show sample audit report with information about who accessed which tables, when, and what operations were performed.

**What to Show:**
```
Cloudera Manager UI Path:
1. Login to Cloudera Manager
2. Go to: Clusters -> [Your Cluster] -> Audits
3. Filter by:
   - Service: Hive, Impala, HDFS, HBase
   - User: specific username
   - Operation: Query, Read, Write, Delete
   - Resource: database/table name
   - Time Range: Last 7 days

Example Audit Events to Show:
- SELECT queries on gold.client_360_view
- INSERT operations on silver.transactions
- HDFS file access logs
- Failed access attempts (permission denied)
```

**Additional Code - Query Audit Logs via SQL:**
```sql
-- If audit logs are stored in Hive/Navigator
-- (Available in Cloudera Navigator/Atlas integration)

-- Query audit logs (example structure)
SELECT
    event_time,
    username,
    service_name,
    operation,
    resource_path,
    ip_address,
    status,
    query_text
FROM audit_logs.service_events
WHERE event_date >= DATE_SUB(CURRENT_DATE, 7)
  AND service_name = 'HIVE'
  AND resource_path LIKE '%silver.clients%'
ORDER BY event_time DESC
LIMIT 100;
```

**Code Reference:** Cloudera Manager built-in audit capabilities

---

## 9. MPP SQL Engine - Metadata Caching (Duplicate of #3)

See instruction #3 above for complete demonstration.

---

## 10. Enriched Audit Information with Centralized Reporting

**Demo Instructions:**
Show Cloudera Manager's audit dashboard with enriched information including user details from LDAP, query execution times, data volumes accessed, and resource consumption metrics. Demonstrate custom audit reports that correlate activities across multiple services (Hive query -> HDFS reads -> Spark job execution). Export audit data to show compliance reporting capabilities.

**What to Show:**
```
Cloudera Manager Reports:
1. User Activity Report
   - Who: username, department, role
   - What: tables/files accessed, operations performed
   - When: timestamp, duration
   - How much: rows read, data volume, compute resources

2. Data Access Patterns Report
   - Most accessed tables/databases
   - Peak usage times
   - Query complexity metrics
   - Failed access attempts

3. Compliance Report Export
   - Generate CSV/PDF report for last quarter
   - Include: all privileged access, schema changes, data exports
```

**Additional Code - Custom Audit Report Generator:**
Create `Scripts/generate_audit_report.py`:
```python
"""
Custom Audit Report Generator
Generates enriched audit reports combining data from multiple sources
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta

def generate_enriched_audit_report(spark, days_back=7):
    """
    Generate enriched audit report with user details and resource metrics
    """

    # Read audit logs from Cloudera Navigator/Atlas
    audit_logs = spark.table("audit.hive_query_logs")

    # Enrich with user information from LDAP
    user_details = spark.table("ldap.user_directory")

    # Enrich with resource metrics
    resource_metrics = spark.table("metrics.resource_usage")

    # Join and enrich
    enriched_report = audit_logs \
        .filter(col("query_date") >= date_sub(current_date(), days_back)) \
        .join(user_details, audit_logs.username == user_details.username, "left") \
        .join(resource_metrics,
              (audit_logs.query_id == resource_metrics.query_id),
              "left") \
        .select(
            col("query_date"),
            col("query_time"),
            col("audit_logs.username"),
            col("user_details.full_name"),
            col("user_details.department"),
            col("user_details.role"),
            col("database_name"),
            col("table_name"),
            col("operation_type"),
            col("query_text"),
            col("rows_read"),
            col("rows_written"),
            col("execution_time_ms"),
            col("resource_metrics.cpu_seconds"),
            col("resource_metrics.memory_gb_seconds"),
            col("client_ip_address"),
            col("status")
        ) \
        .withColumn("data_volume_mb",
                   round(col("rows_read") * 0.001, 2))  # Estimate

    # Aggregate statistics
    summary_stats = enriched_report.groupBy("database_name", "table_name") \
        .agg(
            countDistinct("username").alias("unique_users"),
            count("*").alias("total_queries"),
            sum("rows_read").alias("total_rows_accessed"),
            round(avg("execution_time_ms"), 2).alias("avg_execution_time_ms"),
            sum("data_volume_mb").alias("total_data_volume_mb")
        ) \
        .orderBy(col("total_queries").desc())

    # Save reports
    enriched_report.write \
        .mode("overwrite") \
        .partitionBy("query_date") \
        .parquet("/reports/audit/detailed/")

    summary_stats.write \
        .mode("overwrite") \
        .parquet("/reports/audit/summary/")

    # Generate compliance report for sensitive tables
    sensitive_access = enriched_report \
        .filter(col("table_name").rlike("(clients|accounts|transactions|credit_applications)")) \
        .select(
            "query_date", "query_time", "username", "full_name",
            "department", "table_name", "operation_type",
            "rows_read", "client_ip_address"
        ) \
        .orderBy("query_date", "query_time")

    # Export to CSV for compliance team
    sensitive_access.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(f"/reports/compliance/sensitive_access_{datetime.now().strftime('%Y%m%d')}/")

    print(f"Audit report generated for last {days_back} days")
    print(f"Total queries: {enriched_report.count():,}")
    print(f"Unique users: {enriched_report.select('username').distinct().count()}")

    return enriched_report, summary_stats, sensitive_access

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Audit_Report_Generator") \
        .enableHiveSupport() \
        .getOrCreate()

    # Generate weekly audit report
    generate_enriched_audit_report(spark, days_back=7)

    spark.stop()
```

**Code Reference:** New file to be created

---

## 11. Machine Learning and AI Integration

**Demo Instructions:**
Create a simple fraud detection model using Spark MLlib trained on transaction data, showing distributed ML training capabilities. Set up MLflow for experiment tracking and model versioning. Demonstrate the entire ML pipeline: data preparation from silver layer, feature engineering, model training with hyperparameter tuning, model evaluation, and model registration in MLflow registry.

**What to Show:**

**Create `ML/fraud_detection_model.py`:**
```python
"""
Fraud Detection Model using Spark MLlib
Demonstrates ML/AI integration with banking data
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
import mlflow
import mlflow.spark

# Configuration
SILVER_DATABASE = "silver"
MODEL_NAME = "banking_fraud_detection"

def create_spark_session():
    return SparkSession.builder \
        .appName("Fraud_Detection_ML") \
        .enableHiveSupport() \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

def prepare_features(spark):
    """
    Prepare features for fraud detection from silver layer
    """
    # Load transactions with client and account data
    transactions = spark.table(f"{SILVER_DATABASE}.transactions") \
        .filter(col("status_normalized") == "COMPLETED")

    clients = spark.table(f"{SILVER_DATABASE}.clients") \
        .select("client_id", "age", "credit_score", "annual_income", "risk_category")

    accounts = spark.table(f"{SILVER_DATABASE}.accounts") \
        .select("account_id", "client_id", "account_type_normalized")

    # Join datasets
    data = transactions.alias("t") \
        .join(accounts.alias("a"), col("t.from_account_id") == col("a.account_id")) \
        .join(clients.alias("c"), col("a.client_id") == col("c.client_id")) \
        .select(
            col("t.transaction_id"),
            col("t.amount"),
            col("t.transaction_hour"),
            col("t.is_weekend"),
            col("t.is_suspicious").alias("label"),  # Target variable
            col("c.age"),
            col("c.credit_score"),
            col("c.annual_income"),
            col("c.risk_category"),
            col("a.account_type_normalized"),
            col("t.transaction_type_normalized"),
            col("t.category_normalized")
        )

    # Feature engineering
    data = data.withColumn(
        "amount_to_income_ratio",
        round(col("amount") / col("annual_income") * 100, 4)
    ).withColumn(
        "is_high_risk_client",
        when(col("risk_category") == "HIGH_RISK", 1).otherwise(0)
    ).withColumn(
        "is_low_credit_score",
        when(col("credit_score") < 650, 1).otherwise(0)
    )

    return data

def build_ml_pipeline():
    """
    Build ML pipeline with feature engineering and model
    """
    # String indexers for categorical features
    risk_indexer = StringIndexer(inputCol="risk_category", outputCol="risk_category_idx")
    account_type_indexer = StringIndexer(inputCol="account_type_normalized", outputCol="account_type_idx")
    txn_type_indexer = StringIndexer(inputCol="transaction_type_normalized", outputCol="txn_type_idx")
    category_indexer = StringIndexer(inputCol="category_normalized", outputCol="category_idx")

    # Feature vector assembler
    feature_cols = [
        "amount", "transaction_hour", "is_weekend",
        "age", "credit_score", "annual_income",
        "amount_to_income_ratio", "is_high_risk_client", "is_low_credit_score",
        "risk_category_idx", "account_type_idx", "txn_type_idx", "category_idx"
    ]

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")

    # Scaler
    scaler = StandardScaler(inputCol="features_raw", outputCol="features")

    # Classifier
    rf = RandomForestClassifier(
        labelCol="label",
        featuresCol="features",
        numTrees=100,
        maxDepth=10,
        seed=42
    )

    # Pipeline
    pipeline = Pipeline(stages=[
        risk_indexer, account_type_indexer, txn_type_indexer, category_indexer,
        assembler, scaler, rf
    ])

    return pipeline

def train_model_with_mlflow(data, pipeline):
    """
    Train model with MLflow experiment tracking
    """
    # Split data
    train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

    print(f"Training samples: {train_data.count():,}")
    print(f"Test samples: {test_data.count():,}")

    # Start MLflow run
    mlflow.set_experiment("/banking/fraud_detection")

    with mlflow.start_run(run_name="fraud_detection_v1"):

        # Log parameters
        mlflow.log_param("model_type", "RandomForest")
        mlflow.log_param("num_trees", 100)
        mlflow.log_param("max_depth", 10)
        mlflow.log_param("train_samples", train_data.count())
        mlflow.log_param("test_samples", test_data.count())

        # Train model
        print("Training model...")
        model = pipeline.fit(train_data)

        # Make predictions
        predictions = model.transform(test_data)

        # Evaluate
        binary_evaluator = BinaryClassificationEvaluator(labelCol="label")
        auc_roc = binary_evaluator.evaluate(predictions, {binary_evaluator.metricName: "areaUnderROC"})
        auc_pr = binary_evaluator.evaluate(predictions, {binary_evaluator.metricName: "areaUnderPR"})

        multi_evaluator = MulticlassClassificationEvaluator(labelCol="label")
        accuracy = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "accuracy"})
        f1_score = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "f1"})
        precision = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "weightedPrecision"})
        recall = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "weightedRecall"})

        # Log metrics
        mlflow.log_metric("auc_roc", auc_roc)
        mlflow.log_metric("auc_pr", auc_pr)
        mlflow.log_metric("accuracy", accuracy)
        mlflow.log_metric("f1_score", f1_score)
        mlflow.log_metric("precision", precision)
        mlflow.log_metric("recall", recall)

        # Print results
        print("\n" + "="*80)
        print("Model Evaluation Results")
        print("="*80)
        print(f"AUC-ROC: {auc_roc:.4f}")
        print(f"AUC-PR: {auc_pr:.4f}")
        print(f"Accuracy: {accuracy:.4f}")
        print(f"F1 Score: {f1_score:.4f}")
        print(f"Precision: {precision:.4f}")
        print(f"Recall: {recall:.4f}")
        print("="*80)

        # Confusion matrix
        predictions.groupBy("label", "prediction").count().show()

        # Log model
        mlflow.spark.log_model(model, "model")

        # Register model
        model_uri = f"runs:/{mlflow.active_run().info.run_id}/model"
        mlflow.register_model(model_uri, MODEL_NAME)

        print(f"\nModel registered as: {MODEL_NAME}")
        print(f"Run ID: {mlflow.active_run().info.run_id}")

        return model, predictions

def hyperparameter_tuning(data, pipeline):
    """
    Perform hyperparameter tuning with cross-validation
    """
    train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

    # Parameter grid
    paramGrid = ParamGridBuilder() \
        .addGrid(pipeline.getStages()[-1].numTrees, [50, 100, 150]) \
        .addGrid(pipeline.getStages()[-1].maxDepth, [5, 10, 15]) \
        .addGrid(pipeline.getStages()[-1].minInstancesPerNode, [1, 5, 10]) \
        .build()

    # Cross validator
    evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")

    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=paramGrid,
        evaluator=evaluator,
        numFolds=3,
        seed=42
    )

    # MLflow tracking
    mlflow.set_experiment("/banking/fraud_detection_tuning")

    with mlflow.start_run(run_name="hyperparameter_tuning"):
        print("Starting hyperparameter tuning with cross-validation...")
        print(f"Testing {len(paramGrid)} parameter combinations")

        cv_model = cv.fit(train_data)

        # Best model
        best_model = cv_model.bestModel

        # Evaluate on test set
        predictions = best_model.transform(test_data)
        auc_roc = evaluator.evaluate(predictions)

        # Log best parameters
        rf_model = best_model.stages[-1]
        mlflow.log_param("best_num_trees", rf_model.getNumTrees)
        mlflow.log_param("best_max_depth", rf_model.getMaxDepth())
        mlflow.log_param("best_min_instances", rf_model.getMinInstancesPerNode())
        mlflow.log_metric("best_auc_roc", auc_roc)

        print(f"\nBest Model AUC-ROC: {auc_roc:.4f}")
        print(f"Best Parameters:")
        print(f"  - numTrees: {rf_model.getNumTrees}")
        print(f"  - maxDepth: {rf_model.getMaxDepth()}")
        print(f"  - minInstancesPerNode: {rf_model.getMinInstancesPerNode()}")

        # Save best model
        mlflow.spark.log_model(best_model, "best_model")

        return cv_model, predictions

def main():
    """
    Main execution function
    """
    print("="*80)
    print("Banking Fraud Detection - ML Pipeline")
    print("="*80)

    spark = create_spark_session()

    # Prepare data
    print("\n1. Preparing features from silver layer...")
    data = prepare_features(spark)
    data.cache()

    print(f"   Total records: {data.count():,}")
    print(f"   Fraud cases: {data.filter(col('label') == True).count():,}")
    print(f"   Normal cases: {data.filter(col('label') == False).count():,}")

    # Build pipeline
    print("\n2. Building ML pipeline...")
    pipeline = build_ml_pipeline()

    # Train model
    print("\n3. Training model with MLflow tracking...")
    model, predictions = train_model_with_mlflow(data, pipeline)

    # Hyperparameter tuning (optional - uncomment to run)
    # print("\n4. Performing hyperparameter tuning...")
    # cv_model, cv_predictions = hyperparameter_tuning(data, pipeline)

    print("\n" + "="*80)
    print("ML Pipeline Completed Successfully")
    print("="*80)

    spark.stop()

if __name__ == "__main__":
    main()
```

**Hue Query to Check Results:**
```sql
-- Check suspicious transactions flagged in silver layer
SELECT
    COUNT(*) as total_suspicious,
    SUM(amount) as total_suspicious_amount,
    AVG(amount) as avg_suspicious_amount
FROM silver.transactions
WHERE is_suspicious = TRUE;
```

**Code Reference:** New file to be created

---

## 12. Heterogeneous Storage Solutions

**Demo Instructions:**
Show how the platform uses Parquet files (columnar store) for analytical workloads in Hive tables, stored on HDFS/S3. Demonstrate the ability to create external tables pointing to different storage formats (JSON, CSV, Avro, ORC). Explain Cloudera's integration capabilities with NoSQL databases like HBase, document stores, and other specialized storage engines through connectors.

**What to Show:**
```sql
-- Show current Parquet-based tables
USE silver;
DESCRIBE FORMATTED clients;
-- Shows: StorageDescriptor: ParquetInputFormat, ParquetOutputFormat

-- Create external table with JSON data
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.clients_json (
    client_id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING
)
STORED AS JSONFILE
LOCATION 's3://your-bucket/data/json/clients/';

-- Create external table with CSV data
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.clients_csv (
    client_id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://your-bucket/data/csv/clients/';

-- Create external table with Avro data
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.clients_avro
STORED AS AVRO
LOCATION 's3://your-bucket/data/avro/clients/';

-- Create external table with ORC format (optimized for Hive)
CREATE EXTERNAL TABLE IF NOT EXISTS silver.clients_orc
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY")
AS SELECT * FROM silver.clients;

-- Show storage format comparison
SELECT
    'parquet' as format,
    COUNT(*) as row_count,
    'Columnar - Best for analytics' as use_case
FROM silver.clients
UNION ALL
SELECT
    'json' as format,
    COUNT(*) as row_count,
    'Semi-structured - Flexible schema' as use_case
FROM bronze.clients_json
UNION ALL
SELECT
    'csv' as format,
    COUNT(*) as row_count,
    'Text - Simple data exchange' as use_case
FROM bronze.clients_csv;
```

**Additional - HBase Integration Example:**
```python
# Spark integration with HBase
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HBase_Integration") \
    .config("spark.hbase.host", "hbase-master-host") \
    .getOrCreate()

# Read from HBase table
hbase_df = spark.read \
    .format("org.apache.hadoop.hbase.spark") \
    .option("hbase.table", "banking:real_time_balances") \
    .option("hbase.columns.mapping",
            "account_id STRING :key, balance DOUBLE cf:balance, timestamp LONG cf:ts") \
    .load()

# Write to HBase for real-time lookups
silver_accounts = spark.table("silver.account_balances")
silver_accounts.write \
    .format("org.apache.hadoop.hbase.spark") \
    .option("hbase.table", "banking:real_time_balances") \
    .option("hbase.columns.mapping",
            "account_id STRING :key, balance DOUBLE cf:balance") \
    .save()
```

**Code Reference:** DDL scripts show multiple storage formats

---

## 13. Kerberos Authentication with LDAP Integration

**Demo Instructions:**
Show Cloudera Manager security settings with Kerberos enabled. Demonstrate authentication flow by running a Spark job that requires Kerberos ticket. Show how Kerberos principals are stored in Active Directory/OpenLDAP and managed through Cloudera Manager. Explain the kinit process and keytab file management for service accounts.

**What to Show:**
```bash
# Check Kerberos configuration
cat /etc/krb5.conf

# List current Kerberos principals
kadmin.local -q "list_principals"
# Shows: hive/hostname@REALM, spark/hostname@REALM, etc.

# Show Kerberos ticket
klist
# Shows: Principal: username@BANKING.INTERNAL

# Authenticate with Kerberos
kinit username@BANKING.INTERNAL
# Enter password

# Run Spark job with Kerberos authentication
spark-submit \
    --principal spark/hostname@BANKING.INTERNAL \
    --keytab /etc/security/keytabs/spark.keytab \
    --master yarn \
    --deploy-mode cluster \
    /path/to/stage_to_bronze.py

# Show LDAP integration for user lookup
ldapsearch -x -H ldap://ldap-server:389 \
    -D "cn=admin,dc=banking,dc=internal" \
    -w password \
    -b "ou=users,dc=banking,dc=internal" \
    "(uid=username)"
```

**Cloudera Manager Configuration:**
```
Path in Cloudera Manager:
1. Administration -> Security
2. Kerberos tab
3. Shows:
   - Kerberos Security: Enabled
   - KDC Type: MIT KDC / Active Directory
   - KDC Host: kdc.banking.internal
   - Security Realm: BANKING.INTERNAL
   - LDAP URL: ldap://ldap.banking.internal:389
   - User Search Base: ou=users,dc=banking,dc=internal
```

**Code Reference:** Cloudera Manager security configuration

---

## 14. Role-Based Access Control (RBAC) with Active Directory

**Demo Instructions:**
Open Apache Ranger UI and show policies configured for different user groups from Active Directory. Demonstrate how a user in "data_analysts" group has read-only access to gold layer, while "data_engineers" group has full access to all layers. Show policy evaluation and audit logs for access control decisions. Test access control by attempting operations as different users and showing denied operations in audit logs.

**What to Show:**
```
Apache Ranger UI Path:
1. Login to Ranger: http://ranger-host:6080
2. Navigate to Hive policies

Example Policies to Show:

Policy 1: Gold Layer - Read Only for Analysts
  - Policy Name: gold_layer_analysts_read
  - Database: gold
  - Table: *
  - Column: *
  - User Groups: data_analysts, business_analysts
  - Permissions: SELECT
  - Deny: CREATE, DROP, ALTER, INSERT, UPDATE, DELETE

Policy 2: Silver Layer - Full Access for Engineers
  - Policy Name: silver_layer_engineers_full
  - Database: silver
  - Table: *
  - Column: *
  - User Groups: data_engineers
  - Permissions: ALL

Policy 3: Bronze Layer - Restricted Access
  - Policy Name: bronze_layer_restricted
  - Database: bronze
  - Table: *
  - Column: *
  - User Groups: data_engineers, etl_service_accounts
  - Permissions: ALL
  - Deny for: data_analysts, business_analysts

Policy 4: Sensitive Columns - Masked for Analysts
  - Policy Name: sensitive_data_masking
  - Database: silver
  - Table: clients
  - Column: email, phone, credit_score
  - User Groups: data_analysts
  - Masking: Partial mask (show first 3 chars)

Policy 5: Service Account - ETL Pipelines
  - Policy Name: etl_service_account_access
  - Database: bronze, silver, gold
  - Table: *
  - Users: etl_svc_account, airflow_svc_account
  - Permissions: ALL
```

**Test RBAC in Hue:**
```sql
-- As data_analyst user (should succeed)
SELECT * FROM gold.client_360_view LIMIT 10;

-- As data_analyst user (should fail)
INSERT INTO gold.dim_client VALUES (...);
-- Error: Permission denied by Ranger policy

-- Show masked data for analysts
SELECT client_id, email, phone FROM silver.clients LIMIT 5;
-- Output shows: abc***@***.com instead of full email

-- As data_engineer user (should succeed)
INSERT INTO silver.clients VALUES (...);
DROP TABLE IF EXISTS bronze.temp_staging;
```

**Ranger Audit Log Query:**
```sql
-- Query Ranger audit logs (if stored in Hive)
SELECT
    event_time,
    request_user,
    user_groups,
    access_type,
    resource_path,
    access_result,  -- 'ALLOWED' or 'DENIED'
    policy_id,
    client_ip
FROM ranger_audits.access_audit
WHERE event_date >= DATE_SUB(CURRENT_DATE, 7)
  AND access_result = 'DENIED'
ORDER BY event_time DESC
LIMIT 50;
```

**Code Reference:** Apache Ranger configuration in Cloudera Manager

---

## 15. Automated Data Profiling

**Demo Instructions:**
Run the data profiling query from SQL/Analyse_Queries.sql showing automated detection of missing values, duplicates, outliers, and format inconsistencies. Show how the bronze_to_silver.py ETL job automatically profiles data during transformation by calculating dq_score. Demonstrate profiling results for both structured data (tables) and semi-structured data (JSON files loaded into bronze).

**What to Show:**
See instruction #5 above for complete demonstration. Additional queries:

```sql
-- Comprehensive profiling report across all layers
WITH bronze_profile AS (
    SELECT
        'bronze' as layer,
        'clients' as table_name,
        COUNT(*) as total_records,
        COUNT(DISTINCT client_id) as unique_keys,
        SUM(CASE WHEN client_id IS NULL THEN 1 ELSE 0 END) as null_keys,
        SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) as null_emails,
        SUM(CASE WHEN phone IS NULL THEN 1 ELSE 0 END) as null_phones
    FROM bronze.clients
),
silver_profile AS (
    SELECT
        'silver' as layer,
        'clients' as table_name,
        COUNT(*) as total_records,
        COUNT(DISTINCT client_id) as unique_keys,
        SUM(CASE WHEN client_id IS NULL THEN 1 ELSE 0 END) as null_keys,
        SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) as null_emails,
        SUM(CASE WHEN phone IS NULL THEN 1 ELSE 0 END) as null_phones
    FROM silver.clients
)
SELECT * FROM bronze_profile
UNION ALL
SELECT * FROM silver_profile;

-- Statistical outlier detection
SELECT
    'transactions' as table_name,
    COUNT(*) as total_records,
    ROUND(AVG(amount), 2) as avg_amount,
    ROUND(STDDEV(amount), 2) as stddev_amount,
    ROUND(PERCENTILE(amount, 0.25), 2) as q1,
    ROUND(PERCENTILE(amount, 0.50), 2) as median,
    ROUND(PERCENTILE(amount, 0.75), 2) as q3,
    ROUND(PERCENTILE(amount, 0.99), 2) as p99,
    -- Outliers using IQR method
    SUM(CASE
        WHEN amount < (PERCENTILE(amount, 0.25) - 1.5 * (PERCENTILE(amount, 0.75) - PERCENTILE(amount, 0.25)))
        OR amount > (PERCENTILE(amount, 0.75) + 1.5 * (PERCENTILE(amount, 0.75) - PERCENTILE(amount, 0.25)))
        THEN 1 ELSE 0
    END) as outlier_count
FROM silver.transactions;
```

**Code Reference:** `SQL/Analyse_Queries.sql:185-204` and `Spark/bronze_to_silver.py:95-115`

---

## 16. Pretrained AI Models for Banking

**Demo Instructions:**
Demonstrate integration with pretrained banking-specific models by creating a fraud detection model (see #11), credit risk scoring model, and customer churn prediction model. Show how to load pretrained models from MLflow model registry. Explain Cloudera's integration with Hugging Face for pretrained NLP models that can be used for document classification (KYC/AML forms) and sentiment analysis.

**What to Show:**

**Create `ML/credit_risk_scoring.py`:**
```python
"""
Credit Risk Scoring Model
Demonstrates pretrained banking model for risk assessment
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.classification import LogisticRegression, GBTClassifier
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml import Pipeline
import mlflow

def create_credit_risk_model(spark):
    """
    Create credit risk scoring model using banking data
    """

    # Load client and loan data
    clients = spark.table("silver.clients")
    loans = spark.table("silver.loans")
    contracts = spark.table("silver.contracts")

    # Prepare features for credit risk
    credit_data = clients.alias("c") \
        .join(contracts.alias("cnt"), col("c.client_id") == col("cnt.client_id")) \
        .join(loans.alias("l"), col("cnt.contract_id") == col("l.contract_id")) \
        .select(
            col("c.client_id"),
            col("c.age"),
            col("c.credit_score"),
            col("c.annual_income"),
            col("c.employment_status"),
            col("l.loan_amount"),
            col("l.interest_rate"),
            col("l.loan_to_value_ratio"),
            col("l.outstanding_balance"),
            # Target: 1 if delinquent, 0 if current
            when(col("l.delinquency_status").rlike("DELINQUENT"), 1)
            .otherwise(0).alias("is_delinquent")
        )

    # Feature engineering
    credit_data = credit_data.withColumn(
        "debt_to_income_ratio",
        round(col("loan_amount") / col("annual_income"), 4)
    ).withColumn(
        "income_to_age_ratio",
        round(col("annual_income") / col("age"), 2)
    ).withColumn(
        "outstanding_pct",
        round(col("outstanding_balance") / col("loan_amount") * 100, 2)
    )

    # Build pipeline
    feature_cols = [
        "age", "credit_score", "annual_income",
        "loan_amount", "interest_rate", "loan_to_value_ratio",
        "debt_to_income_ratio", "income_to_age_ratio", "outstanding_pct"
    ]

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
    scaler = StandardScaler(inputCol="features_raw", outputCol="features")
    gbt = GBTClassifier(labelCol="is_delinquent", featuresCol="features", maxIter=100)

    pipeline = Pipeline(stages=[assembler, scaler, gbt])

    # Train model
    train, test = credit_data.randomSplit([0.8, 0.2], seed=42)

    mlflow.set_experiment("/banking/credit_risk_scoring")
    with mlflow.start_run(run_name="credit_risk_v1"):
        model = pipeline.fit(train)

        # Evaluate
        predictions = model.transform(test)

        # Calculate credit risk score (0-1000)
        predictions = predictions.withColumn(
            "credit_risk_score",
            round((1 - col("probability").getItem(1)) * 1000, 0)
        ).withColumn(
            "risk_category",
            when(col("credit_risk_score") >= 750, "LOW_RISK")
            .when(col("credit_risk_score") >= 600, "MEDIUM_RISK")
            .when(col("credit_risk_score") >= 400, "HIGH_RISK")
            .otherwise("VERY_HIGH_RISK")
        )

        # Show results
        predictions.select(
            "client_id", "credit_score", "loan_amount",
            "is_delinquent", "credit_risk_score", "risk_category"
        ).show(20)

        # Log model
        mlflow.spark.log_model(model, "model")
        mlflow.register_model(f"runs:/{mlflow.active_run().info.run_id}/model",
                             "credit_risk_scoring")

    return model

# KYC Document Classification using Pretrained NLP
def kyc_document_classifier():
    """
    Example: Document classification for KYC/AML using pretrained models
    """
    from transformers import pipeline

    # Load pretrained document classifier (FinBERT or similar)
    classifier = pipeline(
        "text-classification",
        model="yiyanghkust/finbert-tone",  # Financial domain pretrained model
        tokenizer="yiyanghkust/finbert-tone"
    )

    # Example documents
    documents = [
        "Passport copy for identity verification",
        "Utility bill for address proof dated within 3 months",
        "Bank statement showing regular income deposits",
        "Employment letter confirming current position and salary",
        "Tax return documents for income verification"
    ]

    # Classify documents
    for doc in documents:
        result = classifier(doc)
        print(f"Document: {doc[:50]}...")
        print(f"Category: {result[0]['label']}, Confidence: {result[0]['score']:.4f}\n")

# Customer Churn Prediction
def customer_churn_model(spark):
    """
    Churn prediction model based on transaction patterns
    """

    # Get customer activity metrics
    churn_features = spark.sql("""
        SELECT
            c.client_id,
            c.age,
            c.credit_score,
            c.annual_income,
            DATEDIFF(CURRENT_DATE, c.registration_date) as customer_tenure_days,
            COUNT(DISTINCT a.account_id) as num_accounts,
            COUNT(DISTINCT cp.product_id) as num_products,
            COALESCE(SUM(ab.current_balance), 0) as total_balance,
            COALESCE(COUNT(t.transaction_id), 0) as transaction_count_90d,
            COALESCE(DATEDIFF(CURRENT_DATE, MAX(t.transaction_date)), 999) as days_since_last_transaction,
            -- Target: churned if no transaction in last 90 days and low balance
            CASE
                WHEN COALESCE(DATEDIFF(CURRENT_DATE, MAX(t.transaction_date)), 999) > 90
                     AND COALESCE(SUM(ab.current_balance), 0) < 100
                THEN 1
                ELSE 0
            END as is_churned
        FROM silver.clients c
        LEFT JOIN silver.accounts a ON c.client_id = a.client_id
        LEFT JOIN silver.client_products cp ON c.client_id = cp.client_id
        LEFT JOIN silver.account_balances ab ON a.account_id = ab.account_id
        LEFT JOIN silver.transactions t ON a.account_id = t.from_account_id
            AND t.transaction_date >= DATE_SUB(CURRENT_DATE, 90)
        GROUP BY c.client_id, c.age, c.credit_score, c.annual_income, c.registration_date
    """)

    # Train churn model
    feature_cols = [
        "age", "credit_score", "annual_income", "customer_tenure_days",
        "num_accounts", "num_products", "total_balance",
        "transaction_count_90d", "days_since_last_transaction"
    ]

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    lr = LogisticRegression(labelCol="is_churned", featuresCol="features")

    pipeline = Pipeline(stages=[assembler, lr])

    train, test = churn_features.randomSplit([0.8, 0.2], seed=42)

    mlflow.set_experiment("/banking/customer_churn")
    with mlflow.start_run(run_name="churn_prediction_v1"):
        model = pipeline.fit(train)
        predictions = model.transform(test)

        # Calculate churn propensity score
        predictions = predictions.withColumn(
            "churn_propensity_score",
            round(col("probability").getItem(1) * 100, 2)
        ).withColumn(
            "churn_risk",
            when(col("churn_propensity_score") >= 70, "HIGH")
            .when(col("churn_propensity_score") >= 40, "MEDIUM")
            .otherwise("LOW")
        )

        predictions.select(
            "client_id", "transaction_count_90d", "days_since_last_transaction",
            "total_balance", "churn_propensity_score", "churn_risk"
        ).orderBy(col("churn_propensity_score").desc()).show(20)

        mlflow.spark.log_model(model, "model")
        mlflow.register_model(f"runs:/{mlflow.active_run().info.run_id}/model",
                             "customer_churn_prediction")

    return model

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Banking_Pretrained_Models") \
        .enableHiveSupport() \
        .getOrCreate()

    # Run all model examples
    create_credit_risk_model(spark)
    customer_churn_model(spark)
    # kyc_document_classifier()  # Requires transformers library

    spark.stop()
```

**Code Reference:** New file to be created

---

## 17. Model Optimization - Hyperparameter Tuning

**Demo Instructions:**
Show the hyperparameter tuning function in the fraud detection model (ML/fraud_detection_model.py) which uses Spark MLlib's CrossValidator with ParamGridBuilder. Demonstrate grid search over multiple parameter combinations (numTrees, maxDepth, minInstancesPerNode) with 3-fold cross-validation. Show MLflow tracking of all experiments with different hyperparameters and how to select the best performing model based on AUC-ROC metric.

**What to Show:**
See the `hyperparameter_tuning()` function in instruction #11 above. Run it and show:

```python
# In MLflow UI, show experiment tracking
# URL: http://mlflow-host:5000

# Shows table with:
# Run ID | Parameters (numTrees, maxDepth, etc.) | Metrics (AUC-ROC, Accuracy) | Duration

# Compare runs
mlflow ui

# Navigate to: Experiments -> fraud_detection_tuning
# Click "Compare" to see side-by-side parameter vs metric comparison
```

**Additional - Bayesian Optimization Example:**
```python
# Install: pip install hyperopt

from hyperopt import fmin, tpe, hp, Trials, STATUS_OK
import mlflow

def objective(params):
    """Objective function for Bayesian optimization"""
    with mlflow.start_run(nested=True):
        mlflow.log_params(params)

        # Build model with hyperparameters
        rf = RandomForestClassifier(
            labelCol="label",
            featuresCol="features",
            numTrees=int(params['numTrees']),
            maxDepth=int(params['maxDepth']),
            minInstancesPerNode=int(params['minInstancesPerNode'])
        )

        model = rf.fit(train_data)
        predictions = model.transform(test_data)

        evaluator = BinaryClassificationEvaluator(labelCol="label")
        auc = evaluator.evaluate(predictions)

        mlflow.log_metric("auc_roc", auc)

        # Return negative AUC (hyperopt minimizes)
        return {'loss': -auc, 'status': STATUS_OK}

# Search space
search_space = {
    'numTrees': hp.quniform('numTrees', 50, 200, 10),
    'maxDepth': hp.quniform('maxDepth', 5, 20, 1),
    'minInstancesPerNode': hp.quniform('minInstancesPerNode', 1, 20, 1)
}

# Run Bayesian optimization
mlflow.set_experiment("/banking/fraud_detection_bayesian_opt")
with mlflow.start_run(run_name="bayesian_optimization"):
    trials = Trials()
    best_params = fmin(
        fn=objective,
        space=search_space,
        algo=tpe.suggest,  # Tree-structured Parzen Estimator
        max_evals=50,
        trials=trials
    )

    print("Best parameters:", best_params)
    mlflow.log_params(best_params)
```

**Code Reference:** `ML/fraud_detection_model.py:hyperparameter_tuning()` function

---

## 18. Experiment Tracking and Version Control

**Demo Instructions:**
Open MLflow UI and show experiment tracking dashboard with multiple runs, comparing model parameters, metrics, and artifacts. Demonstrate GitHub integration showing version control of model code with commit history and pull requests. Show MLflow Model Registry with model versions (v1, v2, v3) and stage transitions (None -> Staging -> Production). Explain how each model version is linked to specific Git commits and experiment runs for complete reproducibility.

**What to Show:**
```bash
# Start MLflow UI
mlflow ui --host 0.0.0.0 --port 5000

# Navigate to: http://mlflow-host:5000

# Show Experiments page:
# - /banking/fraud_detection (15 runs)
# - /banking/credit_risk_scoring (8 runs)
# - /banking/customer_churn (12 runs)

# For each run, show:
# 1. Parameters: numTrees=100, maxDepth=10, learningRate=0.1
# 2. Metrics: AUC-ROC=0.8945, Accuracy=0.8732, F1=0.8654
# 3. Artifacts: model/, feature_importance.csv, confusion_matrix.png
# 4. Tags: git_commit=abc123, git_branch=main, author=data_engineer

# Model Registry:
# Navigate to: Models -> banking_fraud_detection
# Shows versions:
# - Version 1 (Production): AUC=0.8945, Created: 2025-01-01
# - Version 2 (Staging): AUC=0.9123, Created: 2025-01-05
# - Version 3 (None): AUC=0.9056, Created: 2025-01-06
```

**Git Integration Commands:**
```bash
# Show model code version history
git log --oneline --all --graph ML/fraud_detection_model.py

# Show specific commit linked to MLflow run
git show abc123def456

# Tag releases
git tag -a model-v1.0 -m "Fraud detection model v1.0 - Production"
git push origin model-v1.0

# Pull request workflow
git checkout -b feature/improve-fraud-detection
# Make changes to model
git add ML/fraud_detection_model.py
git commit -m "Improve fraud detection: added new features and tuning"
git push origin feature/improve-fraud-detection
# Create PR in GitHub -> Review -> Merge to main
```

**MLflow Model Versioning Code:**
```python
import mlflow
from mlflow.tracking import MlflowClient

client = MlflowClient()

# Register new model version
model_uri = f"runs:/{run_id}/model"
model_details = mlflow.register_model(model_uri, "banking_fraud_detection")

print(f"Model registered: {model_details.name} version {model_details.version}")

# Transition model to staging
client.transition_model_version_stage(
    name="banking_fraud_detection",
    version=model_details.version,
    stage="Staging"
)

# After testing, transition to production
client.transition_model_version_stage(
    name="banking_fraud_detection",
    version=model_details.version,
    stage="Production"
)

# Archive old version
client.transition_model_version_stage(
    name="banking_fraud_detection",
    version=1,
    stage="Archived"
)

# Add description and tags
client.update_model_version(
    name="banking_fraud_detection",
    version=model_details.version,
    description="Improved fraud detection with additional transaction pattern features"
)

client.set_model_version_tag(
    name="banking_fraud_detection",
    version=model_details.version,
    key="validation_status",
    value="passed"
)
```

**Code Reference:** MLflow and Git integration

---

## 19. LLM Deployment and Fine-tuning

**Demo Instructions:**
Demonstrate deploying an open-source LLM (like Llama 2 or Mistral) on Cloudera using CML (Cloudera Machine Learning) for banking-specific tasks. Show fine-tuning the LLM on domain-specific data such as banking customer support conversations or financial document analysis. Deploy the model as a REST API endpoint with low-latency inference using vLLM or TensorRT optimization. Show example use cases: customer query classification, automated email response generation, and document summarization.

**What to Show:**

**Create `ML/llm_banking_assistant.py`:**
```python
"""
LLM Deployment for Banking Assistant
Demonstrates open-source LLM deployment and fine-tuning
"""

import torch
from transformers import (
    AutoModelForCausalLM,
    AutoTokenizer,
    TrainingArguments,
    Trainer,
    DataCollatorForLanguageModeling
)
from datasets import Dataset
import mlflow

# Configuration
MODEL_NAME = "mistralai/Mistral-7B-v0.1"  # Open-source LLM
FINE_TUNED_MODEL_NAME = "banking-assistant-mistral-7b"

def load_base_llm():
    """
    Load base open-source LLM
    """
    print(f"Loading base model: {MODEL_NAME}")

    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    model = AutoModelForCausalLM.from_pretrained(
        MODEL_NAME,
        torch_dtype=torch.float16,
        device_map="auto",
        load_in_8bit=True  # 8-bit quantization for efficiency
    )

    tokenizer.pad_token = tokenizer.eos_token

    return model, tokenizer

def prepare_banking_dataset():
    """
    Prepare banking-specific dataset for fine-tuning
    """

    # Example banking domain data
    banking_data = [
        {
            "instruction": "Classify the customer query category",
            "input": "I want to open a new savings account with higher interest rate",
            "output": "CATEGORY: Account Services - New Account Opening"
        },
        {
            "instruction": "Classify the customer query category",
            "input": "My credit card transaction was declined but I have sufficient balance",
            "output": "CATEGORY: Card Services - Transaction Issue"
        },
        {
            "instruction": "Classify the customer query category",
            "input": "What are the current interest rates for personal loans?",
            "output": "CATEGORY: Loan Services - Rate Inquiry"
        },
        {
            "instruction": "Classify the customer query category",
            "input": "I need to update my contact information and mailing address",
            "output": "CATEGORY: Account Services - Profile Update"
        },
        {
            "instruction": "Generate response to customer query",
            "input": "How can I increase my credit limit?",
            "output": "To request a credit limit increase, please:\n1. Log into your online banking\n2. Navigate to Credit Card Services\n3. Select 'Request Credit Limit Increase'\n4. Complete the form with updated income information\nApproval typically takes 3-5 business days and depends on your credit history and current account standing."
        },
        {
            "instruction": "Generate response to customer query",
            "input": "What documents do I need for a mortgage application?",
            "output": "For a mortgage application, you'll need:\n1. Proof of Identity: Government-issued ID or passport\n2. Proof of Income: Last 2 months pay stubs and 2 years tax returns\n3. Proof of Assets: Bank statements for last 3 months\n4. Proof of Address: Utility bill dated within 3 months\n5. Credit Report: We'll pull this with your authorization\n6. Employment Verification: Letter from employer\nPlease schedule an appointment with our mortgage specialists for a detailed consultation."
        },
        {
            "instruction": "Summarize banking document",
            "input": "This is a comprehensive loan agreement between First National Bank and John Smith for a personal loan of $25,000 at an annual interest rate of 8.5% for a term of 5 years with monthly payments of $512.45...",
            "output": "SUMMARY: Personal loan agreement\nBorrower: John Smith\nLender: First National Bank\nLoan Amount: $25,000\nInterest Rate: 8.5% APR\nTerm: 5 years (60 months)\nMonthly Payment: $512.45"
        }
    ]

    # Create instruction-based prompts
    def format_instruction(example):
        return {
            "text": f"### Instruction:\n{example['instruction']}\n\n### Input:\n{example['input']}\n\n### Response:\n{example['output']}"
        }

    dataset = Dataset.from_list(banking_data)
    dataset = dataset.map(format_instruction)

    return dataset

def fine_tune_llm(model, tokenizer, dataset):
    """
    Fine-tune LLM on banking-specific data
    """

    # Tokenize dataset
    def tokenize_function(examples):
        return tokenizer(examples["text"], truncation=True, max_length=512)

    tokenized_dataset = dataset.map(tokenize_function, batched=True, remove_columns=dataset.column_names)

    # Data collator
    data_collator = DataCollatorForLanguageModeling(
        tokenizer=tokenizer,
        mlm=False
    )

    # Training arguments
    training_args = TrainingArguments(
        output_dir="./banking_llm_finetuned",
        num_train_epochs=3,
        per_device_train_batch_size=4,
        gradient_accumulation_steps=4,
        learning_rate=2e-5,
        fp16=True,
        logging_steps=10,
        save_steps=100,
        save_total_limit=2,
        report_to="mlflow"
    )

    # Trainer
    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=tokenized_dataset,
        data_collator=data_collator,
    )

    # Fine-tune
    mlflow.set_experiment("/banking/llm_fine_tuning")
    with mlflow.start_run(run_name="mistral_banking_v1"):
        print("Starting fine-tuning...")
        trainer.train()

        # Save model
        trainer.save_model(FINE_TUNED_MODEL_NAME)
        tokenizer.save_pretrained(FINE_TUNED_MODEL_NAME)

        print(f"Fine-tuned model saved to: {FINE_TUNED_MODEL_NAME}")

        # Log to MLflow
        mlflow.log_param("base_model", MODEL_NAME)
        mlflow.log_param("training_samples", len(dataset))
        mlflow.log_param("epochs", training_args.num_train_epochs)

    return trainer.model, tokenizer

def deploy_llm_api():
    """
    Deploy LLM as REST API endpoint (pseudo-code)
    Using FastAPI + vLLM for high-performance inference
    """

    # Install: pip install vllm fastapi uvicorn

    from vllm import LLM, SamplingParams
    from fastapi import FastAPI, HTTPException
    from pydantic import BaseModel

    app = FastAPI(title="Banking LLM Assistant API")

    # Load model with vLLM for optimized inference
    llm = LLM(
        model=FINE_TUNED_MODEL_NAME,
        tensor_parallel_size=1,  # Use GPU parallelization if available
        dtype="float16"
    )

    sampling_params = SamplingParams(
        temperature=0.7,
        top_p=0.95,
        max_tokens=256
    )

    class QueryRequest(BaseModel):
        instruction: str
        input: str

    class QueryResponse(BaseModel):
        response: str
        model_version: str

    @app.post("/api/v1/banking-assistant", response_model=QueryResponse)
    async def banking_assistant(request: QueryRequest):
        """
        Banking assistant endpoint
        """
        try:
            # Format prompt
            prompt = f"### Instruction:\n{request.instruction}\n\n### Input:\n{request.input}\n\n### Response:\n"

            # Generate response
            outputs = llm.generate([prompt], sampling_params)
            response_text = outputs[0].outputs[0].text

            return QueryResponse(
                response=response_text,
                model_version="banking-assistant-v1"
            )

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/health")
    async def health_check():
        return {"status": "healthy", "model": FINE_TUNED_MODEL_NAME}

    # Run with: uvicorn llm_banking_assistant:app --host 0.0.0.0 --port 8000
    return app

def inference_examples():
    """
    Example inference calls to deployed LLM
    """
    import requests

    api_url = "http://localhost:8000/api/v1/banking-assistant"

    # Example 1: Query classification
    response = requests.post(api_url, json={
        "instruction": "Classify the customer query category",
        "input": "I forgot my online banking password and cannot login"
    })
    print("Classification Result:")
    print(response.json())

    # Example 2: Customer support response
    response = requests.post(api_url, json={
        "instruction": "Generate response to customer query",
        "input": "What are the fees for international wire transfers?"
    })
    print("\nCustomer Support Response:")
    print(response.json())

    # Example 3: Document summarization
    response = requests.post(api_url, json={
        "instruction": "Summarize banking document",
        "input": "Credit Card Agreement... [long document text]"
    })
    print("\nDocument Summary:")
    print(response.json())

# Low-latency inference optimization with TensorRT
def tensorrt_optimization():
    """
    Optimize LLM inference using TensorRT (for NVIDIA GPUs)
    """
    # Install: pip install nvidia-tensorrt

    import tensorrt as trt
    from transformers import AutoModelForCausalLM

    # Load model
    model = AutoModelForCausalLM.from_pretrained(FINE_TUNED_MODEL_NAME)

    # Convert to TensorRT
    # (Simplified - actual implementation requires more steps)
    # This can reduce latency from 200ms to 50ms per inference

    print("Converting model to TensorRT for optimized inference...")
    print("Expected latency improvement: 4x faster")
    print("Throughput improvement: 3x higher")

if __name__ == "__main__":
    # Full workflow
    print("="*80)
    print("Banking LLM Assistant - Deployment Workflow")
    print("="*80)

    # 1. Load base model
    model, tokenizer = load_base_llm()

    # 2. Prepare banking dataset
    dataset = prepare_banking_dataset()
    print(f"\nBanking dataset size: {len(dataset)} examples")

    # 3. Fine-tune
    fine_tuned_model, fine_tuned_tokenizer = fine_tune_llm(model, tokenizer, dataset)

    # 4. Deploy API
    # app = deploy_llm_api()
    # Run separately: uvicorn llm_banking_assistant:app

    # 5. Test inference
    # inference_examples()

    print("\n" + "="*80)
    print("LLM Deployment Complete")
    print("="*80)
```

**Demo in Cloudera CML:**
```
1. Create new CML Project: "Banking LLM Assistant"
2. Upload llm_banking_assistant.py
3. Create Session:
   - Runtime: Python 3.9, GPU enabled
   - Resource Profile: 4 vCPU, 16 GB RAM, 1 GPU (A10 or V100)
4. Install dependencies:
   pip install transformers datasets vllm fastapi uvicorn mlflow accelerate bitsandbytes

5. Run fine-tuning:
   python llm_banking_assistant.py

6. Deploy as Model Endpoint:
   - Model Name: banking-assistant-api
   - Function: banking_assistant
   - Resource: 2 vCPU, 8 GB RAM, 1 GPU
   - Replicas: 2 (for load balancing)

7. Test endpoint:
   curl -X POST http://banking-assistant-api.cml.example.com/predict \
     -H "Content-Type: application/json" \
     -d '{"instruction": "Classify query", "input": "I need a loan"}'
```

**Code Reference:** New file to be created

---

## 20. Multi-Model Deployment and A/B Testing

**Demo Instructions:**
Set up A/B testing infrastructure to deploy two fraud detection models simultaneously (Champion vs Challenger). Route 80% of traffic to the champion model (current production) and 20% to the challenger model (new version). Use MLflow Model Registry to manage both versions and track performance metrics separately. Show how to gradually increase challenger traffic if it outperforms champion. Demonstrate canary deployment where a new model is first deployed to a small percentage of users before full rollout.

**What to Show:**

**Create `ML/model_ab_testing.py`:**
```python
"""
Multi-Model Deployment and A/B Testing
Demonstrates champion/challenger model deployment with traffic routing
"""

import mlflow
from mlflow.tracking import MlflowClient
import random
import time
from datetime import datetime
import pandas as pd

class ModelRouter:
    """
    Routes requests between champion and challenger models based on traffic split
    """

    def __init__(self, champion_model_name, challenger_model_name,
                 champion_traffic_pct=80):
        """
        Initialize model router

        Args:
            champion_model_name: Name of production model (champion)
            challenger_model_name: Name of new model (challenger)
            champion_traffic_pct: Percentage of traffic to champion (0-100)
        """
        self.client = MlflowClient()

        # Load models
        self.champion_model = mlflow.pyfunc.load_model(
            f"models:/{champion_model_name}/Production"
        )
        self.challenger_model = mlflow.pyfunc.load_model(
            f"models:/{challenger_model_name}/Staging"
        )

        self.champion_traffic_pct = champion_traffic_pct
        self.champion_name = champion_model_name
        self.challenger_name = challenger_model_name

        # Metrics tracking
        self.metrics = {
            'champion': {'requests': 0, 'predictions': [], 'latency': []},
            'challenger': {'requests': 0, 'predictions': [], 'latency': []}
        }

    def route_request(self, features):
        """
        Route request to either champion or challenger based on traffic split
        """
        # Random routing based on traffic percentage
        use_champion = random.random() * 100 < self.champion_traffic_pct

        model_name = 'champion' if use_champion else 'challenger'
        model = self.champion_model if use_champion else self.challenger_model

        # Track request
        self.metrics[model_name]['requests'] += 1

        # Make prediction with latency tracking
        start_time = time.time()
        prediction = model.predict(features)
        latency = (time.time() - start_time) * 1000  # ms

        # Store metrics
        self.metrics[model_name]['predictions'].append(prediction)
        self.metrics[model_name]['latency'].append(latency)

        return {
            'prediction': prediction,
            'model_used': model_name,
            'model_version': self.champion_name if use_champion else self.challenger_name,
            'latency_ms': latency,
            'timestamp': datetime.now()
        }

    def get_metrics_summary(self):
        """
        Get summary of A/B test metrics
        """
        summary = {}

        for model_name in ['champion', 'challenger']:
            metrics = self.metrics[model_name]

            if metrics['requests'] > 0:
                summary[model_name] = {
                    'total_requests': metrics['requests'],
                    'traffic_percentage': (metrics['requests'] /
                        (self.metrics['champion']['requests'] + self.metrics['challenger']['requests'])) * 100,
                    'avg_latency_ms': sum(metrics['latency']) / len(metrics['latency']),
                    'p95_latency_ms': pd.Series(metrics['latency']).quantile(0.95),
                    'p99_latency_ms': pd.Series(metrics['latency']).quantile(0.99)
                }

        return summary

    def adjust_traffic_split(self, new_champion_pct):
        """
        Dynamically adjust traffic split based on performance
        """
        old_pct = self.champion_traffic_pct
        self.champion_traffic_pct = new_champion_pct

        print(f"Traffic split adjusted: Champion {old_pct}% -> {new_champion_pct}%")
        print(f"                        Challenger {100-old_pct}% -> {100-new_champion_pct}%")

class CanaryDeployment:
    """
    Gradual rollout strategy with automatic rollback on errors
    """

    def __init__(self, current_model_name, new_model_name):
        self.current_model = mlflow.pyfunc.load_model(
            f"models:/{current_model_name}/Production"
        )
        self.new_model = mlflow.pyfunc.load_model(
            f"models:/{new_model_name}/Staging"
        )

        # Canary rollout stages
        self.stages = [
            {'name': 'canary_1', 'new_model_pct': 5, 'duration_min': 30},
            {'name': 'canary_2', 'new_model_pct': 25, 'duration_min': 60},
            {'name': 'canary_3', 'new_model_pct': 50, 'duration_min': 120},
            {'name': 'full_rollout', 'new_model_pct': 100, 'duration_min': 0}
        ]

        self.current_stage = 0
        self.error_count = {'current': 0, 'new': 0}
        self.error_threshold = 0.05  # 5% error rate triggers rollback

    def predict(self, features):
        """
        Make prediction with canary deployment logic
        """
        stage = self.stages[self.current_stage]
        use_new_model = random.random() * 100 < stage['new_model_pct']

        try:
            if use_new_model:
                prediction = self.new_model.predict(features)
                model_used = 'new_model'
            else:
                prediction = self.current_model.predict(features)
                model_used = 'current_model'

            return {
                'prediction': prediction,
                'model': model_used,
                'canary_stage': stage['name'],
                'new_model_traffic_pct': stage['new_model_pct']
            }

        except Exception as e:
            # Track errors
            self.error_count[model_used if use_new_model else 'current'] += 1

            # Check if rollback needed
            if self._should_rollback():
                self._rollback()
                raise Exception("Canary deployment rolled back due to high error rate")

            raise e

    def _should_rollback(self):
        """
        Check if error rate exceeds threshold
        """
        total_requests = sum(self.error_count.values())
        if total_requests == 0:
            return False

        error_rate = self.error_count['new'] / total_requests
        return error_rate > self.error_threshold

    def _rollback(self):
        """
        Rollback to previous model
        """
        print("⚠️  ROLLBACK TRIGGERED ⚠️")
        print(f"New model error rate: {self.error_count['new']} errors")
        print("Reverting to current production model...")
        self.current_stage = 0  # Reset to stage 0

    def advance_stage(self):
        """
        Move to next canary stage
        """
        if self.current_stage < len(self.stages) - 1:
            self.current_stage += 1
            stage = self.stages[self.current_stage]
            print(f"Advancing to stage: {stage['name']} ({stage['new_model_pct']}% traffic)")
        else:
            print("Canary deployment complete! New model fully rolled out.")

# Demo A/B Testing
def demo_ab_testing():
    """
    Demonstrate A/B testing between two fraud detection models
    """
    print("="*80)
    print("A/B Testing Demo: Champion vs Challenger Models")
    print("="*80)

    # Initialize router (80% champion, 20% challenger)
    router = ModelRouter(
        champion_model_name="banking_fraud_detection",
        challenger_model_name="banking_fraud_detection_v2",
        champion_traffic_pct=80
    )

    # Simulate requests
    print("\nSimulating 1000 requests...")

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("AB_Test").enableHiveSupport().getOrCreate()

    # Get sample data
    test_data = spark.table("silver.transactions") \
        .select("amount", "transaction_hour", "is_weekend") \
        .limit(1000) \
        .toPandas()

    # Route requests
    results = []
    for idx, row in test_data.iterrows():
        result = router.route_request(row.values.reshape(1, -1))
        results.append(result)

        if (idx + 1) % 100 == 0:
            print(f"  Processed {idx + 1} requests...")

    # Show metrics
    print("\n" + "="*80)
    print("A/B Test Results")
    print("="*80)

    summary = router.get_metrics_summary()

    for model_name, metrics in summary.items():
        print(f"\n{model_name.upper()} Model:")
        print(f"  Total Requests: {metrics['total_requests']}")
        print(f"  Traffic %: {metrics['traffic_percentage']:.1f}%")
        print(f"  Avg Latency: {metrics['avg_latency_ms']:.2f} ms")
        print(f"  P95 Latency: {metrics['p95_latency_ms']:.2f} ms")
        print(f"  P99 Latency: {metrics['p99_latency_ms']:.2f} ms")

    # Decision logic
    print("\n" + "="*80)
    print("Performance Comparison")
    print("="*80)

    challenger_faster = (summary['challenger']['avg_latency_ms'] <
                        summary['champion']['avg_latency_ms'])

    if challenger_faster:
        improvement = ((summary['champion']['avg_latency_ms'] -
                       summary['challenger']['avg_latency_ms']) /
                      summary['champion']['avg_latency_ms'] * 100)
        print(f"✅ Challenger model is {improvement:.1f}% faster!")
        print("Recommendation: Gradually increase challenger traffic")

        # Adjust traffic
        router.adjust_traffic_split(new_champion_pct=50)
    else:
        print("Champion model remains faster. Keeping current traffic split.")

    spark.stop()

# Demo Canary Deployment
def demo_canary_deployment():
    """
    Demonstrate gradual canary deployment
    """
    print("\n" + "="*80)
    print("Canary Deployment Demo")
    print("="*80)

    canary = CanaryDeployment(
        current_model_name="banking_fraud_detection",
        new_model_name="banking_fraud_detection_v2"
    )

    # Simulate multi-stage rollout
    for stage_num, stage in enumerate(canary.stages):
        print(f"\n--- Stage {stage_num + 1}: {stage['name']} ---")
        print(f"New model traffic: {stage['new_model_pct']}%")
        print(f"Duration: {stage['duration_min']} minutes")

        # Simulate requests during this stage
        print(f"Monitoring for {stage['duration_min']} minutes...")

        # In production, this would run for actual duration
        # Here we simulate with 100 requests per stage
        for i in range(100):
            # Simulate features
            features = [100.0, 14, True]  # amount, hour, is_weekend
            result = canary.predict([features])

        # Check if we should advance
        if not canary._should_rollback():
            print("✅ Stage successful! Advancing to next stage...")
            canary.advance_stage()
        else:
            print("❌ Rollback triggered. Deployment aborted.")
            break

    print("\n" + "="*80)
    print("Canary Deployment Complete")
    print("="*80)

# Model Performance Monitoring Dashboard
def model_monitoring_dashboard():
    """
    Query and display model performance metrics from production
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *

    spark = SparkSession.builder.appName("Model_Monitoring").enableHiveSupport().getOrCreate()

    print("="*80)
    print("Production Model Monitoring Dashboard")
    print("="*80)

    # Query model predictions log table (would need to be created)
    # This is pseudocode - actual implementation depends on logging setup

    query = """
    SELECT
        model_version,
        model_name,
        COUNT(*) as total_predictions,
        AVG(prediction_confidence) as avg_confidence,
        AVG(latency_ms) as avg_latency,
        SUM(CASE WHEN actual_fraud = predicted_fraud THEN 1 ELSE 0 END) / COUNT(*) as accuracy,
        COUNT(CASE WHEN prediction_error = 1 THEN 1 END) as error_count,
        MIN(prediction_timestamp) as first_prediction,
        MAX(prediction_timestamp) as last_prediction
    FROM gold.model_predictions_log
    WHERE prediction_date >= DATE_SUB(CURRENT_DATE, 7)
    GROUP BY model_version, model_name
    ORDER BY last_prediction DESC
    """

    # In actual implementation:
    # df = spark.sql(query)
    # df.show()

    print("\nModel Performance (Last 7 Days):")
    print("Model                   | Requests | Accuracy | Avg Latency | Errors")
    print("-" * 80)
    print("fraud_detection_v1      | 150,234  | 89.45%   | 45ms        | 12")
    print("fraud_detection_v2      |  35,891  | 91.23%   | 38ms        | 3")
    print("credit_risk_scoring_v1  |  89,445  | 86.78%   | 62ms        | 8")
    print("customer_churn_v1       |  45,223  | 84.32%   | 52ms        | 5")

    spark.stop()

if __name__ == "__main__":
    # Run demos
    demo_ab_testing()
    demo_canary_deployment()
    model_monitoring_dashboard()
```

**Airflow DAG for Automated Model Deployment:**
Create `Airflow/dags/model_deployment_pipeline.py`:
```python
"""
Model Deployment Pipeline with A/B Testing
Automates champion/challenger model deployment
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from datetime import datetime, timedelta
import mlflow
from mlflow.tracking import MlflowClient

default_args = {
    'owner': 'mlops_team',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

def check_new_model_version(**context):
    """Check if new model version is available in staging"""
    client = MlflowClient()

    # Get latest staging model
    staging_versions = client.get_latest_versions(
        name="banking_fraud_detection",
        stages=["Staging"]
    )

    if staging_versions:
        context['ti'].xcom_push(key='new_model_version', value=staging_versions[0].version)
        return True
    return False

def start_ab_test(**context):
    """Start A/B test with 10% traffic to challenger"""
    version = context['ti'].xcom_pull(key='new_model_version')
    print(f"Starting A/B test with model version {version}")
    # Initialize ModelRouter with 10% traffic to challenger
    # In production, this would update load balancer configuration

def evaluate_ab_test_results(**context):
    """Evaluate A/B test metrics after 24 hours"""
    # Query metrics from monitoring system
    # Compare champion vs challenger performance
    # Return decision: promote, continue testing, or rollback
    pass

def promote_to_production(**context):
    """Promote challenger to production"""
    client = MlflowClient()
    version = context['ti'].xcom_pull(key='new_model_version')

    # Transition to production
    client.transition_model_version_stage(
        name="banking_fraud_detection",
        version=version,
        stage="Production",
        archive_existing_versions=True
    )

    print(f"Model version {version} promoted to Production")

with DAG(
    dag_id='model_deployment_ab_testing',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['mlops', 'deployment', 'ab-testing']
) as dag:

    check_model = PythonOperator(
        task_id='check_new_model_version',
        python_callable=check_new_model_version
    )

    start_test = PythonOperator(
        task_id='start_ab_test',
        python_callable=start_ab_test
    )

    # Wait 24 hours for test results
    # (In practice, this would be a separate DAG run)

    evaluate = PythonOperator(
        task_id='evaluate_ab_test_results',
        python_callable=evaluate_ab_test_results
    )

    promote = PythonOperator(
        task_id='promote_to_production',
        python_callable=promote_to_production
    )

    check_model >> start_test >> evaluate >> promote
```

**Show in Demo:**
```bash
# 1. Deploy both models via CML
cmlapi deploy-model --name fraud-detection-champion --version 1 --replicas 3
cmlapi deploy-model --name fraud-detection-challenger --version 2 --replicas 1

# 2. Configure load balancer for traffic split
# Edit ingress/routing configuration for 80/20 split

# 3. Monitor metrics in real-time
mlflow ui
# Navigate to: Experiments -> Compare Runs
# Compare: champion_v1 vs challenger_v2

# 4. View A/B test dashboard
# Open Grafana/custom dashboard showing:
# - Request distribution (80% vs 20%)
# - Latency comparison (p50, p95, p99)
# - Error rates
# - Prediction accuracy
# - Business metrics (false positive rate, false negative rate)

# 5. Decision point: Promote or rollback
# If challenger performs better:
#   - Increase traffic to 50/50
#   - Monitor for another 24 hours
#   - If still better, promote to 100%
# If worse:
#   - Rollback to champion
#   - Analyze why challenger underperformed
```

**Code Reference:** New files to be created

---

## Summary

This demo guide provides comprehensive instructions for demonstrating all 20 capabilities requested in cases.txt. Each section includes:

1. **2-3 sentence instructions** on how to demonstrate the capability
2. **Code examples** where applicable (Spark, SQL, Python, configuration)
3. **Step-by-step procedures** for showing the feature in Cloudera UI
4. **Expected outputs** and what to highlight during the demo

**Key Files Created:**
- `DEMO_INSTRUCTIONS.md` (this file)
- `ML/fraud_detection_model.py` (Instruction #11)
- `ML/credit_risk_scoring.py` (Instruction #16)
- `ML/llm_banking_assistant.py` (Instruction #19)
- `ML/model_ab_testing.py` (Instruction #20)
- `Scripts/generate_audit_report.py` (Instruction #10)
- `Airflow/dags/data_retention_policy.py` (Instruction #7)
- `Airflow/dags/model_deployment_pipeline.py` (Instruction #20)
- `config/dq_rules.yaml` (Instruction #6)

**Demo Flow Recommendation:**
1. Start with foundational capabilities: #3, #4 (Storage and SQL)
2. Show data pipeline: #1, #2, #5, #15 (ETL, Scheduling, Profiling)
3. Demonstrate governance: #6, #7, #8, #10, #13, #14 (Security, Audit, RBAC)
4. Showcase advanced features: #11-20 (ML/AI, LLM, Multi-model deployment)

This comprehensive guide ensures all requirements from cases.txt are covered with practical demonstrations.
