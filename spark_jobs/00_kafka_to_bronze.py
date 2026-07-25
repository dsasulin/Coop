#!/usr/bin/env python3
"""
ETL Job: Kafka -> Bronze Layer (Structured Streaming)
Description: Real-time ingestion of transactions and account balances from Kafka
Author: Data Engineering Team
Date: 2025-01-06
Version: 1.0

Reads: Kafka topics banking.transactions, banking.account_balances
Writes: bronze.transactions, bronze.account_balances
"""

import sys
import logging
import signal
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType,
    TimestampType, DateType, LongType
)

# ============================================================================
# Logging Configuration
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# Kafka Configuration
# ============================================================================
KAFKA_BOOTSTRAP_SERVERS = "kafka-broker.warehouse.svc:9092"
KAFKA_TOPIC_TRANSACTIONS = "banking.transactions"
KAFKA_TOPIC_BALANCES = "banking.account_balances"
CHECKPOINT_BASE = "s3a://co-op-buk-39d7d9df/checkpoints/kafka_to_bronze"

# ============================================================================
# Schemas matching bronze DDL exactly
# ============================================================================
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", IntegerType(), False),
    StructField("transaction_uuid", StringType(), True),
    StructField("from_account_id", IntegerType(), True),
    StructField("to_account_id", IntegerType(), True),
    StructField("from_account_number", StringType(), True),
    StructField("to_account_number", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("amount", DecimalType(18, 2), True),
    StructField("currency", StringType(), True),
    StructField("transaction_date", TimestampType(), True),
    StructField("description", StringType(), True),
    StructField("status", StringType(), True),
    StructField("category", StringType(), True),
    StructField("merchant_name", StringType(), True),
])

BALANCE_SCHEMA = StructType([
    StructField("balance_id", IntegerType(), False),
    StructField("account_id", IntegerType(), True),
    StructField("current_balance", DecimalType(18, 2), True),
    StructField("available_balance", DecimalType(18, 2), True),
    StructField("currency", StringType(), True),
    StructField("last_updated", TimestampType(), True),
    StructField("credit_limit", DecimalType(18, 2), True),
])


# ============================================================================
# Spark Session Configuration
# ============================================================================
def create_spark_session():
    """Create Spark session with Kafka and Hive support."""
    logger.info("Creating Spark session for Kafka streaming...")

    spark = SparkSession.builder \
        .appName("Banking_ETL_Kafka_to_Bronze") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("hive.metastore.uris",
                "thrift://metastore-service.warehouse-1761913838-c49g.svc.cluster.local:9083") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.warehouse.dir", "s3a://co-op-buk-39d7d9df/user/hive/warehouse") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_BASE) \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sql("SET hive.exec.dynamic.partition=true")
    spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")

    logger.info(f"Spark session created: {spark.version}")
    return spark


# ============================================================================
# Streaming: Transactions
# ============================================================================
def stream_transactions(spark):
    """Read transactions from Kafka and write to bronze.transactions."""
    logger.info(f"Starting transaction stream from {KAFKA_TOPIC_TRANSACTIONS}")

    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC_TRANSACTIONS) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    parsed = raw_stream \
        .select(from_json(col("value").cast("string"), TRANSACTION_SCHEMA).alias("data")) \
        .select("data.*") \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("source_file", lit("kafka_streaming"))

    query = parsed.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", "s3a://co-op-buk-39d7d9df/user/hive/warehouse/bronze.db/transactions") \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/transactions") \
        .trigger(processingTime="5 minutes") \
        .start()

    logger.info("Transaction streaming query started")
    return query


# ============================================================================
# Streaming: Account Balances
# ============================================================================
def stream_account_balances(spark):
    """Read account balances from Kafka and write to bronze.account_balances."""
    logger.info(f"Starting balance stream from {KAFKA_TOPIC_BALANCES}")

    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC_BALANCES) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    parsed = raw_stream \
        .select(from_json(col("value").cast("string"), BALANCE_SCHEMA).alias("data")) \
        .select("data.*") \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("source_file", lit("kafka_streaming"))

    query = parsed.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", "s3a://co-op-buk-39d7d9df/user/hive/warehouse/bronze.db/account_balances") \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/account_balances") \
        .trigger(processingTime="5 minutes") \
        .start()

    logger.info("Account balance streaming query started")
    return query


# ============================================================================
# Main
# ============================================================================
def main():
    """Start all streaming queries and await termination."""
    spark = None
    try:
        spark = create_spark_session()

        txn_query = stream_transactions(spark)
        bal_query = stream_account_balances(spark)

        logger.info("All streaming queries started. Awaiting termination...")

        def shutdown_handler(signum, frame):
            logger.info("Shutdown signal received. Stopping streams...")
            txn_query.stop()
            bal_query.stop()
            sys.exit(0)

        signal.signal(signal.SIGTERM, shutdown_handler)
        signal.signal(signal.SIGINT, shutdown_handler)

        spark.streams.awaitAnyTermination()

    except Exception as e:
        logger.error(f"Streaming job failed: {e}")
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
