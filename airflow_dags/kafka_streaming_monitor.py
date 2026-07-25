"""
Airflow DAG: Kafka Streaming Monitor
Description: Monitor Kafka consumer lag and Spark Streaming health
Author: Data Engineering Team
Date: 2025-01-06
Version: 1.0

This DAG:
1. Checks Kafka consumer group lag for banking topics
2. Monitors Spark Streaming job health
3. Alerts if lag exceeds threshold or streaming job is down

Schedule: Every 15 minutes
References:
  - spark_jobs/00_kafka_to_bronze.py
  - Kafka topics: banking.transactions, banking.account_balances
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

# ============================================================================
# DAG Configuration
# ============================================================================

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email': ['oncall-data@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=10),
}

dag = DAG(
    dag_id='kafka_streaming_monitor',
    default_args=default_args,
    description='Monitor Kafka consumer lag and Spark Streaming health',
    schedule_interval='*/15 * * * *',
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['banking', 'kafka', 'monitoring', 'streaming'],
)

# ============================================================================
# Configuration
# ============================================================================

KAFKA_BOOTSTRAP = "kafka-broker.warehouse.svc:9092"
KAFKA_BIN = "/opt/kafka/bin"
CONSUMER_GROUP = "banking_etl_kafka_to_bronze"
SPARK_UI_URL = "http://spark-master.warehouse.svc:4040"
LAG_THRESHOLD = 10000

# ============================================================================
# Tasks
# ============================================================================

start = DummyOperator(task_id='start', dag=dag)

check_kafka_lag_transactions = BashOperator(
    task_id='check_kafka_lag_transactions',
    bash_command=f"""
    {KAFKA_BIN}/kafka-consumer-groups.sh \\
        --bootstrap-server {KAFKA_BOOTSTRAP} \\
        --describe \\
        --group {CONSUMER_GROUP} \\
        2>/dev/null | grep 'banking.transactions' | awk '{{sum += $6}} END {{print "TRANSACTIONS_LAG=" sum}}'
    """,
    dag=dag,
)

check_kafka_lag_balances = BashOperator(
    task_id='check_kafka_lag_balances',
    bash_command=f"""
    {KAFKA_BIN}/kafka-consumer-groups.sh \\
        --bootstrap-server {KAFKA_BOOTSTRAP} \\
        --describe \\
        --group {CONSUMER_GROUP} \\
        2>/dev/null | grep 'banking.account_balances' | awk '{{sum += $6}} END {{print "BALANCES_LAG=" sum}}'
    """,
    dag=dag,
)

check_spark_streaming_health = BashOperator(
    task_id='check_spark_streaming_health',
    bash_command=f"""
    RESPONSE=$(curl -s -o /dev/null -w "%{{http_code}}" {SPARK_UI_URL}/api/v1/applications/ 2>/dev/null || echo "000")
    if [ "$RESPONSE" = "200" ]; then
        ACTIVE=$(curl -s {SPARK_UI_URL}/api/v1/applications/ | python3 -c "
import sys, json
apps = json.load(sys.stdin)
active = [a for a in apps if 'Kafka_to_Bronze' in a.get('name', '')]
print(len(active))
" 2>/dev/null || echo "0")
        echo "SPARK_STREAMING_ACTIVE=$ACTIVE"
    else
        echo "SPARK_STREAMING_ACTIVE=0"
        echo "WARNING: Spark UI not reachable (HTTP $RESPONSE)"
    fi
    """,
    dag=dag,
)


def evaluate_health_status(**context):
    """Evaluate all health checks and decide if alerting is needed."""
    ti = context['ti']

    txn_lag_output = ti.xcom_pull(task_ids='check_kafka_lag_transactions') or ""
    bal_lag_output = ti.xcom_pull(task_ids='check_kafka_lag_balances') or ""
    spark_output = ti.xcom_pull(task_ids='check_spark_streaming_health') or ""

    issues = []

    try:
        txn_lag = int(txn_lag_output.split("=")[1].strip())
        if txn_lag > LAG_THRESHOLD:
            issues.append(f"Transaction topic lag: {txn_lag} > {LAG_THRESHOLD}")
    except (IndexError, ValueError):
        issues.append("Could not parse transaction lag")

    try:
        bal_lag = int(bal_lag_output.split("=")[1].strip())
        if bal_lag > LAG_THRESHOLD:
            issues.append(f"Balance topic lag: {bal_lag} > {LAG_THRESHOLD}")
    except (IndexError, ValueError):
        issues.append("Could not parse balance lag")

    try:
        active = int(spark_output.split("SPARK_STREAMING_ACTIVE=")[1].split("\n")[0].strip())
        if active == 0:
            issues.append("Spark Streaming job not active (00_kafka_to_bronze.py)")
    except (IndexError, ValueError):
        issues.append("Could not check Spark Streaming status")

    if issues:
        context['ti'].xcom_push(key='issues', value=json.dumps(issues))
        return 'alert_on_issues'
    return 'no_issues'


import json

evaluate_health = BranchPythonOperator(
    task_id='evaluate_health',
    python_callable=evaluate_health_status,
    provide_context=True,
    dag=dag,
)


def send_alert(**context):
    """Send alert for streaming issues."""
    ti = context['ti']
    issues_json = ti.xcom_pull(task_ids='evaluate_health', key='issues') or "[]"
    issues = json.loads(issues_json)

    print("ALERT: Kafka Streaming Issues Detected")
    print("=" * 50)
    for issue in issues:
        print(f"  - {issue}")
    print("=" * 50)
    print("Action: Check spark_jobs/00_kafka_to_bronze.py and Kafka cluster health")
    return True


alert_on_issues = PythonOperator(
    task_id='alert_on_issues',
    python_callable=send_alert,
    provide_context=True,
    dag=dag,
)

no_issues = DummyOperator(task_id='no_issues', dag=dag)

end = DummyOperator(
    task_id='end',
    trigger_rule='none_failed_min_one_success',
    dag=dag,
)

# ============================================================================
# Dependencies
# ============================================================================

start >> [check_kafka_lag_transactions, check_kafka_lag_balances,
          check_spark_streaming_health]

[check_kafka_lag_transactions, check_kafka_lag_balances,
 check_spark_streaming_health] >> evaluate_health

evaluate_health >> [alert_on_issues, no_issues]

[alert_on_issues, no_issues] >> end
