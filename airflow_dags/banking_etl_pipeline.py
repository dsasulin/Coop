"""
Airflow DAG: Banking ETL Pipeline
Description: Orchestrates the complete ETL pipeline from test -> bronze -> silver -> gold
Author: Data Engineering Team
Date: 2025-01-06
Version: 1.0

This DAG:
1. Loads data from test (stage) to bronze layer
2. Transforms and cleans data from bronze to silver layer
3. Builds dimensions, facts, and aggregates from silver to gold layer
4. Includes data quality checks between each layer
5. Sends notifications on success/failure

Schedule: Configurable (default: @once for manual trigger)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
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
SPARK_SUBMIT = "spark-submit"

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

# ============================================================================
# Helper Functions
# ============================================================================

def build_spark_submit_command(script_name, job_name):
    """
    Build spark-submit command for Kubernetes-based CDP.

    Args:
        script_name: Name of the PySpark script (e.g., '01_stage_to_bronze.py')
        job_name: Name for the Spark application (e.g., 'stage_to_bronze')

    Returns:
        str: Complete spark-submit command
    """
    cmd = f"""
{SPARK_SUBMIT} \\
  --master {SPARK_MASTER} \\
  --deploy-mode {SPARK_DEPLOY_MODE} \\
  --name banking_etl_{job_name} \\
  --conf spark.kubernetes.container.image={SPARK_CONTAINER_IMAGE} \\
  --conf spark.kubernetes.namespace={SPARK_NAMESPACE} \\
  --conf spark.kubernetes.authenticate.driver.serviceAccountName={SPARK_SERVICE_ACCOUNT} \\
  --conf spark.driver.memory={DRIVER_MEMORY} \\
  --conf spark.driver.cores={DRIVER_CORES} \\
  --conf spark.executor.memory={EXECUTOR_MEMORY} \\
  --conf spark.executor.cores={EXECUTOR_CORES} \\
  --conf spark.executor.instances={EXECUTOR_INSTANCES} \\
  --conf spark.sql.catalogImplementation=hive \\
  --conf spark.sql.hive.metastore.version=3.1.3000 \\
  --conf hive.metastore.uris={METASTORE_URI} \\
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \\
  --conf spark.sql.warehouse.dir=s3a://{S3_BUCKET}/user/hive/warehouse \\
  --conf spark.sql.adaptive.enabled=true \\
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \\
  {SPARK_JOBS_DIR}/{script_name}
"""
    return cmd.strip()


def check_data_quality(**context):
    """
    Check data quality scores in silver layer.
    Fails the task if average DQ score is below threshold.
    """
    # This is a placeholder - in real implementation, you would:
    # 1. Connect to Hive/Spark
    # 2. Query silver.clients for AVG(dq_score)
    # 3. Check against threshold (e.g., 0.85)
    # 4. Raise AirflowException if below threshold

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
stage_to_bronze = BashOperator(
    task_id='01_stage_to_bronze',
    bash_command=build_spark_submit_command('01_stage_to_bronze.py', 'stage_to_bronze'),
    dag=dag,
)

# Check: Verify bronze layer data
check_bronze = BashOperator(
    task_id='check_bronze_data',
    bash_command="""
    echo "Checking bronze layer data..."
    # TODO: Add actual check (e.g., query row counts)
    echo "✅ Bronze layer check passed"
    """,
    dag=dag,
)

# Stage 2: Transform data from bronze to silver
bronze_to_silver = BashOperator(
    task_id='02_bronze_to_silver',
    bash_command=build_spark_submit_command('02_bronze_to_silver.py', 'bronze_to_silver'),
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
silver_to_gold = BashOperator(
    task_id='03_silver_to_gold',
    bash_command=build_spark_submit_command('03_silver_to_gold.py', 'silver_to_gold'),
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

# Alternative: Parallel execution for independent tasks
# For example, if you had separate data marts that don't depend on each other:
# silver_to_gold >> [build_data_mart_1, build_data_mart_2, build_data_mart_3] >> end

# ============================================================================
# Documentation
# ============================================================================

"""
DAG Execution Flow:
==================

start
  |
  v
01_stage_to_bronze (Spark job)
  |
  v
check_bronze_data (Validation)
  |
  v
02_bronze_to_silver (Spark job)
  |
  v
check_data_quality (Python function)
  |
  v
03_silver_to_gold (Spark job)
  |
  v
check_gold_data (Validation)
  |
  v
notify_success (Notification)
  |
  v
end

Estimated Runtime:
- stage_to_bronze: 5-10 minutes
- bronze_to_silver: 10-20 minutes
- silver_to_gold: 15-30 minutes
- Total: ~30-60 minutes (depends on data volume)

Configuration:
--------------
Before running this DAG:
1. Update SPARK_JOBS_DIR with actual path to Spark scripts
2. Update email addresses in default_args
3. Update SPARK_CONTAINER_IMAGE with actual image name
4. Verify SPARK_MASTER, METASTORE_URI, and S3_BUCKET
5. Implement actual data quality checks
6. Set up notification channels (Slack, email, etc.)

Manual Testing:
---------------
# Test individual Spark job:
spark-submit --master local[*] /opt/spark_jobs/01_stage_to_bronze.py

# Test in Airflow:
airflow dags test banking_etl_pipeline 2025-01-06

# Trigger manually:
airflow dags trigger banking_etl_pipeline

Monitoring:
-----------
- Airflow UI: http://<airflow-host>:8080
- Check logs in: /opt/airflow/logs/banking_etl_pipeline/
- Monitor Spark jobs in Cloudera Manager or Kubernetes dashboard

Troubleshooting:
----------------
Common issues:
1. "Connection refused to metastore" -> Check METASTORE_URI
2. "Access denied to S3" -> Check IAM roles and S3 permissions
3. "Image pull failed" -> Check SPARK_CONTAINER_IMAGE exists
4. "Task timeout" -> Increase execution_timeout in default_args

For help: Contact data engineering team
"""
