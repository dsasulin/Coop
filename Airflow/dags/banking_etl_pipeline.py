"""
Airflow DAG: Banking Data ETL Pipeline
Orchestrates data loading process through all layers: Stage -> Bronze -> Silver -> Gold
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

# Default configuration
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email': ['data-engineering@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# Path to Spark jobs (needs to be configured for Cloudera)
SPARK_JOBS_PATH = "/path/to/spark/jobs"  # TODO: Update with actual path

# Spark configuration
SPARK_CONF = {
    'spark.executor.memory': '4g',
    'spark.executor.cores': '2',
    'spark.driver.memory': '2g',
    'spark.sql.adaptive.enabled': 'true',
    'spark.sql.adaptive.coalescePartitions.enabled': 'true',
}

def log_pipeline_start(**context):
    """Log pipeline start"""
    execution_date = context['execution_date']
    print(f"=" * 80)
    print(f"Starting Banking ETL Pipeline")
    print(f"Execution Date: {execution_date}")
    print(f"DAG Run ID: {context['run_id']}")
    print(f"=" * 80)

def log_pipeline_end(**context):
    """Log pipeline completion"""
    execution_date = context['execution_date']
    print(f"=" * 80)
    print(f"Banking ETL Pipeline Completed Successfully")
    print(f"Execution Date: {execution_date}")
    print(f"Duration: {context['task_instance'].duration}")
    print(f"=" * 80)

def check_data_quality(**context):
    """Data quality check (placeholder)"""
    print("Running data quality checks...")
    # TODO: Implement actual DQ checks
    # - Check record counts
    # - Check for nulls in critical fields
    # - Check data freshness
    # - Check referential integrity
    print("Data quality checks passed!")

# Create DAG
with DAG(
    dag_id='banking_etl_pipeline',
    default_args=default_args,
    description='End-to-end ETL pipeline for banking data (Stage -> Bronze -> Silver -> Gold)',
    schedule_interval='0 2 * * *',  # Ежедневно в 2:00 UTC
    catchup=False,
    max_active_runs=1,
    tags=['banking', 'etl', 'data-pipeline'],
) as dag:

    # =========================================================================
    # Start Task
    # =========================================================================
    start_pipeline = PythonOperator(
        task_id='start_pipeline',
        python_callable=log_pipeline_start,
        provide_context=True,
    )

    # =========================================================================
    # BRONZE LAYER - Load raw data from Stage
    # =========================================================================
    with TaskGroup(group_id='bronze_layer', tooltip='Load data into Bronze layer') as bronze_layer:

        load_stage_to_bronze = SparkSubmitOperator(
            task_id='load_stage_to_bronze',
            application=f'{SPARK_JOBS_PATH}/stage_to_bronze.py',
            name='stage_to_bronze_etl',
            conf=SPARK_CONF,
            verbose=True,
            application_args=[
                '--execution-date', '{{ ds }}',
            ],
        )

        check_bronze_data = PythonOperator(
            task_id='check_bronze_data_quality',
            python_callable=check_data_quality,
            provide_context=True,
            op_kwargs={'layer': 'bronze'},
        )

        load_stage_to_bronze >> check_bronze_data

    # =========================================================================
    # SILVER LAYER - Clean and validate data
    # =========================================================================
    with TaskGroup(group_id='silver_layer', tooltip='Clean and validate data in Silver layer') as silver_layer:

        # Clients and reference data
        process_clients = SparkSubmitOperator(
            task_id='process_clients',
            application=f'{SPARK_JOBS_PATH}/bronze_to_silver.py',
            name='bronze_to_silver_clients',
            conf=SPARK_CONF,
            verbose=True,
            application_args=[
                '--execution-date', '{{ ds }}',
                '--table', 'clients',
            ],
        )

        # Transactional data
        process_transactions = SparkSubmitOperator(
            task_id='process_transactions',
            application=f'{SPARK_JOBS_PATH}/bronze_to_silver.py',
            name='bronze_to_silver_transactions',
            conf=SPARK_CONF,
            verbose=True,
            application_args=[
                '--execution-date', '{{ ds }}',
                '--table', 'transactions',
            ],
        )

        # Accounts and balances
        process_accounts = SparkSubmitOperator(
            task_id='process_accounts',
            application=f'{SPARK_JOBS_PATH}/bronze_to_silver.py',
            name='bronze_to_silver_accounts',
            conf=SPARK_CONF,
            verbose=True,
            application_args=[
                '--execution-date', '{{ ds }}',
                '--table', 'accounts',
            ],
        )

        check_silver_data = PythonOperator(
            task_id='check_silver_data_quality',
            python_callable=check_data_quality,
            provide_context=True,
            op_kwargs={'layer': 'silver'},
        )

        # Clients processed first, then transactions and accounts in parallel
        process_clients >> [process_transactions, process_accounts] >> check_silver_data

    # =========================================================================
    # GOLD LAYER - Build analytical aggregates
    # =========================================================================
    with TaskGroup(group_id='gold_layer', tooltip='Build Gold layer aggregates') as gold_layer:

        # Dimensions
        build_dimensions = SparkSubmitOperator(
            task_id='build_dimensions',
            application=f'{SPARK_JOBS_PATH}/silver_to_gold.py',
            name='silver_to_gold_dimensions',
            conf=SPARK_CONF,
            verbose=True,
            application_args=[
                '--execution-date', '{{ ds }}',
                '--job-type', 'dimensions',
            ],
        )

        # Facts
        build_facts = SparkSubmitOperator(
            task_id='build_facts',
            application=f'{SPARK_JOBS_PATH}/silver_to_gold.py',
            name='silver_to_gold_facts',
            conf=SPARK_CONF,
            verbose=True,
            application_args=[
                '--execution-date', '{{ ds }}',
                '--job-type', 'facts',
            ],
        )

        # Client 360 View
        build_client_360 = SparkSubmitOperator(
            task_id='build_client_360_view',
            application=f'{SPARK_JOBS_PATH}/silver_to_gold.py',
            name='silver_to_gold_client_360',
            conf=SPARK_CONF,
            verbose=True,
            application_args=[
                '--execution-date', '{{ ds }}',
                '--job-type', 'client_360',
            ],
        )

        check_gold_data = PythonOperator(
            task_id='check_gold_data_quality',
            python_callable=check_data_quality,
            provide_context=True,
            op_kwargs={'layer': 'gold'},
        )

        # Dimensions built first, then facts and aggregates
        build_dimensions >> [build_facts, build_client_360] >> check_gold_data

    # =========================================================================
    # Data Quality and Monitoring
    # =========================================================================
    with TaskGroup(group_id='data_quality', tooltip='Final data quality checks') as data_quality:

        final_dq_check = PythonOperator(
            task_id='final_data_quality_check',
            python_callable=check_data_quality,
            provide_context=True,
            op_kwargs={'layer': 'all'},
        )

        # TODO: Add more specific DQ tasks
        # - Check record counts across layers
        # - Check for data anomalies
        # - Validate business rules
        # - Update DQ dashboard

    # =========================================================================
    # End Task
    # =========================================================================
    end_pipeline = PythonOperator(
        task_id='end_pipeline',
        python_callable=log_pipeline_end,
        provide_context=True,
    )

    # =========================================================================
    # Define DAG Flow
    # =========================================================================
    start_pipeline >> bronze_layer >> silver_layer >> gold_layer >> data_quality >> end_pipeline


# =========================================================================
# Alternative DAG for Incremental Loads
# =========================================================================
with DAG(
    dag_id='banking_etl_incremental',
    default_args=default_args,
    description='Incremental ETL pipeline for banking data',
    schedule_interval='0 * * * *',  # Ежечасно
    catchup=False,
    max_active_runs=1,
    tags=['banking', 'etl', 'incremental'],
) as dag_incremental:

    start = DummyOperator(task_id='start')

    # Incremental load только для транзакционных таблиц
    load_transactions_incremental = SparkSubmitOperator(
        task_id='load_transactions_incremental',
        application=f'{SPARK_JOBS_PATH}/stage_to_bronze.py',
        name='incremental_transactions',
        conf=SPARK_CONF,
        verbose=True,
        application_args=[
            '--execution-date', '{{ ds }}',
            '--mode', 'incremental',
            '--table', 'transactions',
        ],
    )

    process_transactions_silver = SparkSubmitOperator(
        task_id='process_transactions_silver',
        application=f'{SPARK_JOBS_PATH}/bronze_to_silver.py',
        name='incremental_transactions_silver',
        conf=SPARK_CONF,
        verbose=True,
        application_args=[
            '--execution-date', '{{ ds }}',
            '--mode', 'incremental',
            '--table', 'transactions',
        ],
    )

    update_gold_facts = SparkSubmitOperator(
        task_id='update_gold_facts',
        application=f'{SPARK_JOBS_PATH}/silver_to_gold.py',
        name='incremental_facts',
        conf=SPARK_CONF,
        verbose=True,
        application_args=[
            '--execution-date', '{{ ds }}',
            '--mode', 'incremental',
            '--job-type', 'facts',
        ],
    )

    end = DummyOperator(task_id='end')

    start >> load_transactions_incremental >> process_transactions_silver >> update_gold_facts >> end
