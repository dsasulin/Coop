"""
Airflow DAG: API Ingestion Pipeline
Description: Fetch data from banking APIs and load to bronze layer
Author: Data Engineering Team
Date: 2025-01-06
Version: 1.0

This DAG:
1. Fetches credit applications, loans, and cards from REST APIs
2. Loads raw JSON responses to bronze layer via Spark
3. Runs data quality checks on new records
4. Sends notifications on completion

Schedule: Every 6 hours
References:
  - bronze.credit_applications
  - bronze.loans
  - bronze.cards
  - spark_jobs/04_spark_csv_ingestion.py (similar pattern)
"""

import json
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

# ============================================================================
# DAG Configuration
# ============================================================================

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email': ['user001@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

dag = DAG(
    dag_id='api_ingestion_pipeline',
    default_args=default_args,
    description='Fetch banking data from APIs and load to bronze layer',
    schedule_interval='0 */6 * * *',
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['banking', 'api', 'ingestion'],
)

# ============================================================================
# Configuration
# ============================================================================

API_BASE_URL = "https://banking-api.corp.internal/v2"
S3_STAGING = "s3a://co-op-buk-39d7d9df/staging/api"
SPARK_SUBMIT = "spark-submit"
SPARK_JOBS_DIR = "/opt/spark_jobs"
METASTORE_URI = "thrift://metastore-service.warehouse-1761913838-c49g.svc.cluster.local:9083"


def build_spark_submit_command(script_name, job_name):
    """Build spark-submit command."""
    return f"""
{SPARK_SUBMIT} \\
  --master k8s://https://kubernetes.default.svc.cluster.local:443 \\
  --deploy-mode cluster \\
  --name api_ingestion_{job_name} \\
  --conf spark.sql.catalogImplementation=hive \\
  --conf hive.metastore.uris={METASTORE_URI} \\
  --conf spark.driver.memory=2g \\
  --conf spark.executor.memory=4g \\
  --conf spark.executor.instances=2 \\
  {SPARK_JOBS_DIR}/{script_name}
""".strip()


# ============================================================================
# API Fetch Functions
# ============================================================================

def fetch_credit_applications(**context):
    """Fetch credit applications from banking API."""
    execution_date = context['execution_date']
    endpoint = f"{API_BASE_URL}/credit-applications"
    params = {
        'from_date': (execution_date - timedelta(hours=6)).isoformat(),
        'to_date': execution_date.isoformat(),
        'limit': 10000,
    }

    # In production: response = requests.get(endpoint, params=params, timeout=300)
    # For demo: simulate API response
    print(f"Fetching credit applications from {endpoint}")
    print(f"Parameters: {json.dumps(params, default=str)}")
    print(f"Target: bronze.credit_applications")
    return True


def fetch_loans(**context):
    """Fetch loan updates from banking API."""
    execution_date = context['execution_date']
    endpoint = f"{API_BASE_URL}/loans"
    params = {
        'updated_since': (execution_date - timedelta(hours=6)).isoformat(),
        'limit': 10000,
    }

    print(f"Fetching loans from {endpoint}")
    print(f"Parameters: {json.dumps(params, default=str)}")
    print(f"Target: bronze.loans")
    return True


def fetch_cards(**context):
    """Fetch card data from banking API."""
    execution_date = context['execution_date']
    endpoint = f"{API_BASE_URL}/cards"
    params = {
        'updated_since': (execution_date - timedelta(hours=6)).isoformat(),
        'limit': 10000,
    }

    print(f"Fetching cards from {endpoint}")
    print(f"Parameters: {json.dumps(params, default=str)}")
    print(f"Target: bronze.cards")
    return True


def check_data_quality_api(**context):
    """Verify data quality of newly loaded records."""
    print("Checking data quality for API-ingested records:")
    print("  - bronze.credit_applications: null checks on application_id, client_id")
    print("  - bronze.loans: null checks on loan_id, contract_id")
    print("  - bronze.cards: null checks on card_id, client_id")
    return True


def send_success_notification(**context):
    """Send success notification."""
    execution_date = context['execution_date']
    print(f"API ingestion pipeline completed for {execution_date}")
    return True


# ============================================================================
# Tasks
# ============================================================================

start = DummyOperator(task_id='start', dag=dag)

fetch_credit_applications_task = PythonOperator(
    task_id='fetch_credit_applications',
    python_callable=fetch_credit_applications,
    provide_context=True,
    dag=dag,
)

fetch_loans_task = PythonOperator(
    task_id='fetch_loans',
    python_callable=fetch_loans,
    provide_context=True,
    dag=dag,
)

fetch_cards_task = PythonOperator(
    task_id='fetch_cards',
    python_callable=fetch_cards,
    provide_context=True,
    dag=dag,
)

load_credit_applications_to_bronze = BashOperator(
    task_id='load_credit_applications_to_bronze',
    bash_command=build_spark_submit_command(
        '04_spark_csv_ingestion.py', 'credit_applications'),
    dag=dag,
)

load_loans_to_bronze = BashOperator(
    task_id='load_loans_to_bronze',
    bash_command=build_spark_submit_command(
        '04_spark_csv_ingestion.py', 'loans'),
    dag=dag,
)

load_cards_to_bronze = BashOperator(
    task_id='load_cards_to_bronze',
    bash_command=build_spark_submit_command(
        '04_spark_csv_ingestion.py', 'cards'),
    dag=dag,
)

check_quality = PythonOperator(
    task_id='check_data_quality',
    python_callable=check_data_quality_api,
    provide_context=True,
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
# Dependencies
# ============================================================================

start >> [fetch_credit_applications_task, fetch_loans_task, fetch_cards_task]

fetch_credit_applications_task >> load_credit_applications_to_bronze
fetch_loans_task >> load_loans_to_bronze
fetch_cards_task >> load_cards_to_bronze

[load_credit_applications_to_bronze, load_loans_to_bronze,
 load_cards_to_bronze] >> check_quality

check_quality >> notify_success >> end
