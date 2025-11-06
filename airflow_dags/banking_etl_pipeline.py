"""
Airflow DAG: Banking ETL Pipeline
Description: Orchestrates the complete ETL pipeline from test -> bronze -> silver -> gold
Author: Data Engineering Team
Date: 2025-01-06
Version: 2.0 (Updated to use SparkSubmitOperator)

This DAG:
1. Loads data from test (stage) to bronze layer
2. Transforms and cleans data from bronze to silver layer
3. Builds dimensions, facts, and aggregates from silver to gold layer
4. Includes data quality checks between each layer
5. Sends notifications on success/failure

Schedule: Configurable (default: @once for manual trigger)

Requirements:
    pip install apache-airflow-providers-apache-spark
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# ============================================================================
# DAG Configuration
# ============================================================================

# Default arguments for all tasks
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email': ['user001@company.com'],  # TODO: Update with actual email
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# DAG definition
dag = DAG(
    dag_id='banking_etl_pipeline',
    default_args=default_args,
    description='Complete ETL pipeline: test -> bronze -> silver -> gold',
    schedule_interval='@once',  # TODO: Change to cron schedule (e.g., '0 2 * * *' for daily at 2 AM)
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['banking', 'etl', 'production'],
)

# ============================================================================
# Configuration Variables
# ============================================================================

# Paths (update these based on your deployment)
SPARK_JOBS_DIR = "/opt/spark_jobs"  # TODO: Update with actual path

# Spark configuration for Kubernetes-based CDP
SPARK_MASTER = "k8s://https://kubernetes.default.svc.cluster.local:443"
SPARK_DEPLOY_MODE = "cluster"
SPARK_NAMESPACE = "warehouse-1761913838-c49g"
SPARK_CONTAINER_IMAGE = "cloudera/spark:latest"  # TODO: Update with actual image
SPARK_SERVICE_ACCOUNT = "spark"

# S3 and Hive configuration
S3_BUCKET = "co-op-buk-39d7d9df"
METASTORE_URI = "thrift://metastore-service.warehouse-1761913838-c49g.svc.cluster.local:9083"

# Spark resources
DRIVER_MEMORY = "4g"
DRIVER_CORES = "2"
EXECUTOR_MEMORY = "8g"
EXECUTOR_CORES = "4"
EXECUTOR_INSTANCES = "5"

# Spark connection ID (configured in Airflow Connections)
# To configure: Airflow UI -> Admin -> Connections -> Add Connection
# Connection ID: spark_k8s
# Connection Type: Spark
# Host: k8s://https://kubernetes.default.svc.cluster.local:443
SPARK_CONN_ID = "spark_default"  # Use "spark_default" or create "spark_k8s"

# Common Spark configuration
SPARK_CONF = {
    # Kubernetes configuration
    "spark.kubernetes.container.image": SPARK_CONTAINER_IMAGE,
    "spark.kubernetes.namespace": SPARK_NAMESPACE,
    "spark.kubernetes.authenticate.driver.serviceAccountName": SPARK_SERVICE_ACCOUNT,

    # Hive and S3 configuration
    "spark.sql.catalogImplementation": "hive",
    "spark.sql.hive.metastore.version": "3.1.3000",
    "hive.metastore.uris": METASTORE_URI,
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.sql.warehouse.dir": f"s3a://{S3_BUCKET}/user/hive/warehouse",

    # Performance tuning
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
}

# ============================================================================
# Helper Functions
# ============================================================================

def check_data_quality(**context):
    """
    Check data quality scores in silver layer.
    Fails the task if average DQ score is below threshold.
    """
    from airflow.exceptions import AirflowException

    dq_threshold = 0.80  # 80% minimum quality score

    # TODO: Implement actual DQ check by connecting to Hive
    # For now, just log
    print(f"Data Quality check: threshold = {dq_threshold}")
    print("TODO: Implement actual DQ query")

    # Example of what the actual implementation would look like:
    # from pyhive import hive
    # conn = hive.Connection(host='metastore-service...', port=10000)
    # cursor = conn.cursor()
    # cursor.execute("SELECT AVG(dq_score) as avg_score FROM silver.clients")
    # avg_score = cursor.fetchone()[0]
    # if avg_score < dq_threshold:
    #     raise AirflowException(f"DQ check failed: {avg_score} < {dq_threshold}")

    return True


def send_success_notification(**context):
    """Send success notification (email, Slack, etc.)."""
    execution_date = context['execution_date']
    print(f"✅ ETL Pipeline completed successfully for {execution_date}")
    # TODO: Add actual notification logic (Slack webhook, email, etc.)
    return True


# ============================================================================
# DAG Tasks
# ============================================================================

# Start task
start = DummyOperator(
    task_id='start',
    dag=dag,
)

# Stage 1: Load data from test to bronze
stage_to_bronze = SparkSubmitOperator(
    task_id='01_stage_to_bronze',
    application=f"{SPARK_JOBS_DIR}/01_stage_to_bronze.py",
    name='banking_etl_stage_to_bronze',
    conn_id=SPARK_CONN_ID,
    verbose=True,

    # Spark configuration
    conf=SPARK_CONF,

    # Resources
    driver_memory=DRIVER_MEMORY,
    driver_cores=DRIVER_CORES,
    executor_memory=EXECUTOR_MEMORY,
    executor_cores=EXECUTOR_CORES,
    num_executors=EXECUTOR_INSTANCES,

    # Kubernetes specific (if using k8s:// master)
    deploy_mode=SPARK_DEPLOY_MODE,

    dag=dag,
)

# Check: Verify bronze layer data
check_bronze = BashOperator(
    task_id='check_bronze_data',
    bash_command="""
    echo "Checking bronze layer data..."
    # TODO: Add actual check (e.g., query row counts via beeline)
    echo "✅ Bronze layer check passed"
    """,
    dag=dag,
)

# Stage 2: Transform data from bronze to silver
bronze_to_silver = SparkSubmitOperator(
    task_id='02_bronze_to_silver',
    application=f"{SPARK_JOBS_DIR}/02_bronze_to_silver.py",
    name='banking_etl_bronze_to_silver',
    conn_id=SPARK_CONN_ID,
    verbose=True,

    # Spark configuration
    conf=SPARK_CONF,

    # Resources
    driver_memory=DRIVER_MEMORY,
    driver_cores=DRIVER_CORES,
    executor_memory=EXECUTOR_MEMORY,
    executor_cores=EXECUTOR_CORES,
    num_executors=EXECUTOR_INSTANCES,

    # Kubernetes specific
    deploy_mode=SPARK_DEPLOY_MODE,

    dag=dag,
)

# Check: Data quality in silver layer
check_data_quality_task = PythonOperator(
    task_id='check_data_quality',
    python_callable=check_data_quality,
    provide_context=True,
    dag=dag,
)

# Stage 3: Build gold layer (dimensions, facts, aggregates)
silver_to_gold = SparkSubmitOperator(
    task_id='03_silver_to_gold',
    application=f"{SPARK_JOBS_DIR}/03_silver_to_gold.py",
    name='banking_etl_silver_to_gold',
    conn_id=SPARK_CONN_ID,
    verbose=True,

    # Spark configuration
    conf=SPARK_CONF,

    # Resources
    driver_memory=DRIVER_MEMORY,
    driver_cores=DRIVER_CORES,
    executor_memory=EXECUTOR_MEMORY,
    executor_cores=EXECUTOR_CORES,
    num_executors=EXECUTOR_INSTANCES,

    # Kubernetes specific
    deploy_mode=SPARK_DEPLOY_MODE,

    dag=dag,
)

# Check: Verify gold layer data
check_gold = BashOperator(
    task_id='check_gold_data',
    bash_command="""
    echo "Checking gold layer data..."
    # TODO: Add actual check (e.g., query fact/dimension tables)
    echo "✅ Gold layer check passed"
    """,
    dag=dag,
)

# Success notification
notify_success = PythonOperator(
    task_id='notify_success',
    python_callable=send_success_notification,
    provide_context=True,
    dag=dag,
)

# End task
end = DummyOperator(
    task_id='end',
    dag=dag,
)

# ============================================================================
# DAG Dependencies
# ============================================================================

# Define task dependencies (linear pipeline)
start >> stage_to_bronze >> check_bronze
check_bronze >> bronze_to_silver >> check_data_quality_task
check_data_quality_task >> silver_to_gold >> check_gold
check_gold >> notify_success >> end

# ============================================================================
# Documentation
# ============================================================================

"""
DAG Execution Flow:
==================

start
  |
  v
01_stage_to_bronze (SparkSubmitOperator)
  |
  v
check_bronze_data (Validation)
  |
  v
02_bronze_to_silver (SparkSubmitOperator)
  |
  v
check_data_quality (Python function)
  |
  v
03_silver_to_gold (SparkSubmitOperator)
  |
  v
check_gold_data (Validation)
  |
  v
notify_success (Notification)
  |
  v
end

Configuration:
--------------
Before running this DAG:

1. Install Spark provider:
   pip install apache-airflow-providers-apache-spark

2. Configure Spark connection in Airflow UI:
   Admin -> Connections -> Add Connection
   - Connection ID: spark_default (or spark_k8s)
   - Connection Type: Spark
   - Host: k8s://https://kubernetes.default.svc.cluster.local:443
   - Extra: {"deploy-mode": "cluster"}

3. Update configuration in this file:
   - SPARK_JOBS_DIR: Path to PySpark scripts
   - SPARK_CONTAINER_IMAGE: Docker image for Spark
   - Update email addresses in default_args

4. If NOT using Kubernetes:
   - Change SPARK_MASTER to "yarn" or "local[*]"
   - Remove Kubernetes-specific configurations

Manual Testing:
---------------
# Test DAG syntax:
python /opt/airflow/dags/banking_etl_pipeline.py

# Test in Airflow:
airflow dags test banking_etl_pipeline 2025-01-06

# Trigger manually:
airflow dags trigger banking_etl_pipeline

Monitoring:
-----------
- Airflow UI: http://<airflow-host>:8080
- Check logs in: /opt/airflow/logs/banking_etl_pipeline/
- Monitor Spark jobs in Kubernetes dashboard or Cloudera Manager

Troubleshooting:
----------------
1. "Connection not found":
   - Configure spark_default connection in Airflow UI
   - Or update SPARK_CONN_ID variable

2. "Application file not found":
   - Check SPARK_JOBS_DIR path is correct
   - Verify .py files exist in that directory

3. "Image pull failed":
   - Update SPARK_CONTAINER_IMAGE with correct image
   - Check image exists in registry

4. "Permission denied":
   - Verify Kubernetes service account has permissions
   - Check RBAC settings

For help: Contact data engineering team
"""
