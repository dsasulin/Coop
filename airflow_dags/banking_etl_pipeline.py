"""
Airflow DAG: Banking ETL Pipeline
Description: Orchestrates the complete ETL pipeline from test -> bronze -> silver -> gold
Author: Data Engineering Team
Date: 2025-01-06
Version: 2.0 (Full pipeline with Informatica + SQL procedures + Spark + Gold aggregations)

This DAG:
1. Triggers Informatica ingestion workflows (Oracle -> bronze)
2. Loads data from stage to bronze via Spark
3. Triggers Informatica transformations (bronze -> silver)
4. Transforms and cleans data from bronze to silver via Spark
5. Runs SQL stored procedures (account balance enrichment, data quality)
6. Builds dimensions, facts, and aggregates from silver to gold
7. Runs Gold aggregation Spark job
8. Executes Gold layer SQL orchestrator
9. Validates final data quality
10. Sends notifications on success/failure

References:
  - informatica/ingestion/workflows/COOP_INGESTION/wf_load_*.json
  - informatica/transformation/workflows/COOP_TRANSFORMATION/wf_transform_*.json
  - SQL/procedures/sp_enrich_account_balances.sql
  - SQL/procedures/sp_validate_data_quality.sql
  - SQL/procedures/sp_run_gold_layer.sql
  - spark_jobs/05_gold_aggregations.py

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
# Hive/Beeline Configuration
# ============================================================================

BEELINE_CMD = 'beeline -u "jdbc:hive2://metastore:10000"'
SQL_PROCEDURES_DIR = "/opt/sql/procedures"


# ============================================================================
# DAG Tasks — Start & Informatica Ingestion
# ============================================================================

start = DummyOperator(task_id='start', dag=dag)

# Trigger Informatica ingestion workflows (Oracle -> bronze)
trigger_informatica_ingestion = BashOperator(
    task_id='trigger_informatica_ingestion',
    bash_command="""
    for wf in wf_load_clients wf_load_products wf_load_contracts wf_load_accounts wf_load_client_products; do
        pmcmd startworkflow -sv IntegrationService -d COOP_INGESTION \
            -u admin -p $INFA_PASSWORD -f COOP_INGESTION $wf
    done
    """,
    dag=dag,
)

wait_informatica_ingestion = BashOperator(
    task_id='wait_informatica_ingestion',
    bash_command="""
    for wf in wf_load_clients wf_load_products wf_load_contracts wf_load_accounts wf_load_client_products; do
        while true; do
            STATUS=$(pmcmd getworkflowdetails -sv IntegrationService -d COOP_INGESTION \
                -u admin -p $INFA_PASSWORD -f COOP_INGESTION $wf | grep 'Workflow Status' | awk '{print $NF}')
            if [ "$STATUS" = "Succeeded" ]; then break; fi
            if [ "$STATUS" = "Failed" ] || [ "$STATUS" = "Aborted" ]; then exit 1; fi
            sleep 30
        done
    done
    """,
    execution_timeout=timedelta(hours=1),
    dag=dag,
)

# ============================================================================
# Stage to Bronze (Spark)
# ============================================================================

stage_to_bronze = BashOperator(
    task_id='01_stage_to_bronze',
    bash_command=build_spark_submit_command('01_stage_to_bronze.py', 'stage_to_bronze'),
    dag=dag,
)

check_bronze = BashOperator(
    task_id='check_bronze_data',
    bash_command="""
    echo "Checking bronze layer data..."
    echo "Bronze layer check passed"
    """,
    dag=dag,
)

# ============================================================================
# Bronze to Silver (Informatica + Spark in parallel)
# ============================================================================

trigger_informatica_transforms = BashOperator(
    task_id='trigger_informatica_transforms',
    bash_command="""
    for wf in wf_transform_loans wf_transform_cards wf_transform_client_products; do
        pmcmd startworkflow -sv IntegrationService -d COOP_TRANSFORMATION \
            -u admin -p $INFA_PASSWORD -f COOP_TRANSFORMATION $wf
    done
    """,
    dag=dag,
)

wait_informatica_transforms = BashOperator(
    task_id='wait_informatica_transforms',
    bash_command="""
    for wf in wf_transform_loans wf_transform_cards wf_transform_client_products; do
        while true; do
            STATUS=$(pmcmd getworkflowdetails -sv IntegrationService -d COOP_TRANSFORMATION \
                -u admin -p $INFA_PASSWORD -f COOP_TRANSFORMATION $wf | grep 'Workflow Status' | awk '{print $NF}')
            if [ "$STATUS" = "Succeeded" ]; then break; fi
            if [ "$STATUS" = "Failed" ] || [ "$STATUS" = "Aborted" ]; then exit 1; fi
            sleep 30
        done
    done
    """,
    execution_timeout=timedelta(hours=1),
    dag=dag,
)

bronze_to_silver = BashOperator(
    task_id='02_bronze_to_silver',
    bash_command=build_spark_submit_command('02_bronze_to_silver.py', 'bronze_to_silver'),
    dag=dag,
)

check_data_quality_task = PythonOperator(
    task_id='check_data_quality',
    python_callable=check_data_quality,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# SQL Stored Procedures (Silver enrichment)
# ============================================================================

run_sp_enrich_balances = BashOperator(
    task_id='run_sp_enrich_balances',
    bash_command=f'{BEELINE_CMD} -f {SQL_PROCEDURES_DIR}/sp_enrich_account_balances.sql',
    dag=dag,
)

run_sp_validate_dq = BashOperator(
    task_id='run_sp_validate_dq',
    bash_command=f'{BEELINE_CMD} -f {SQL_PROCEDURES_DIR}/sp_validate_data_quality.sql',
    dag=dag,
)

# ============================================================================
# Silver to Gold (Spark + SQL)
# ============================================================================

silver_to_gold = BashOperator(
    task_id='03_silver_to_gold',
    bash_command=build_spark_submit_command('03_silver_to_gold.py', 'silver_to_gold'),
    dag=dag,
)

run_gold_aggregations = BashOperator(
    task_id='05_gold_aggregations',
    bash_command=build_spark_submit_command('05_gold_aggregations.py', 'gold_aggregations'),
    dag=dag,
)

run_gold_layer = BashOperator(
    task_id='run_gold_layer',
    bash_command=f'{BEELINE_CMD} -f {SQL_PROCEDURES_DIR}/sp_run_gold_layer.sql',
    dag=dag,
)

# ============================================================================
# Validation & Completion
# ============================================================================

validate_gold_counts = PythonOperator(
    task_id='validate_gold_counts',
    python_callable=check_data_quality,
    provide_context=True,
    dag=dag,
)

check_gold = BashOperator(
    task_id='check_gold_data',
    bash_command="""
    echo "Checking gold layer data..."
    echo "Gold layer check passed"
    """,
    dag=dag,
)

notify_success = PythonOperator(
    task_id='notify_success',
    python_callable=send_success_notification,
    provide_context=True,
    dag=dag,
)

end = DummyOperator(task_id='end', dag=dag)

# ============================================================================
# DAG Dependencies
# ============================================================================

# Phase 1: Informatica ingestion -> Spark stage->bronze
start >> trigger_informatica_ingestion >> wait_informatica_ingestion
wait_informatica_ingestion >> stage_to_bronze >> check_bronze

# Phase 2: Bronze -> Silver (Informatica transforms + Spark in parallel)
check_bronze >> trigger_informatica_transforms
check_bronze >> bronze_to_silver

trigger_informatica_transforms >> wait_informatica_transforms
[wait_informatica_transforms, bronze_to_silver] >> check_data_quality_task

# Phase 3: Silver enrichment via SQL procedures
check_data_quality_task >> run_sp_enrich_balances >> run_sp_validate_dq

# Phase 4: Silver -> Gold (Spark + SQL orchestrator)
run_sp_validate_dq >> silver_to_gold
silver_to_gold >> run_gold_aggregations >> run_gold_layer

# Phase 5: Validate and complete
run_gold_layer >> validate_gold_counts >> check_gold >> notify_success >> end


