#!/bin/bash
# ============================================================================
# Spark Docker Entrypoint Script
# ============================================================================

set -e

# Determine Spark mode from environment variable
SPARK_MODE=${SPARK_MODE:-master}

echo "Starting Spark in $SPARK_MODE mode..."

if [ "$SPARK_MODE" = "master" ]; then
    echo "Starting Spark Master..."
    /opt/spark/sbin/start-master.sh
    tail -f /dev/null
elif [ "$SPARK_MODE" = "worker" ]; then
    echo "Starting Spark Worker..."
    echo "Connecting to master: $SPARK_MASTER_URL"
    /opt/spark/sbin/start-worker.sh $SPARK_MASTER_URL
    tail -f /dev/null
else
    echo "Unknown SPARK_MODE: $SPARK_MODE"
    echo "Valid modes: master, worker"
    exit 1
fi
