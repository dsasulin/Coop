#!/usr/bin/env python3
"""
ETL Job: CSV Files -> Bronze Layer
Description: Load reference data (branches, employees) from CSV files to bronze layer
Author: Data Engineering Team
Date: 2025-01-06
Version: 1.0

Reads: Data/branches.csv, Data/employees.csv
Writes: bronze.branches, bronze.employees
"""

import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

# ============================================================================
# Logging Configuration
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# Configuration
# ============================================================================
CSV_BASE_PATH = "/opt/data"
S3_BUCKET = "co-op-buk-39d7d9df"

EXPECTED_COLUMNS = {
    "branches": [
        "branch_id", "branch_code", "branch_name", "address", "city",
        "state", "zip_code", "phone", "manager_name", "opening_date"
    ],
    "employees": [
        "employee_id", "branch_id", "first_name", "last_name", "email",
        "phone", "position", "department", "hire_date", "salary", "status"
    ],
}


# ============================================================================
# Spark Session
# ============================================================================
def create_spark_session():
    """Create Spark session with Hive support and S3 configuration."""
    logger.info("Creating Spark session...")

    spark = SparkSession.builder \
        .appName("Banking_ETL_CSV_Ingestion") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("hive.metastore.uris",
                "thrift://metastore-service.warehouse-1761913838-c49g.svc.cluster.local:9083") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.warehouse.dir", f"s3a://{S3_BUCKET}/user/hive/warehouse") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sql("SET hive.exec.dynamic.partition=true")
    spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")

    logger.info(f"Spark session created: {spark.version}")
    return spark


# ============================================================================
# Load Functions
# ============================================================================
def load_branches(spark):
    """Load branches.csv into bronze.branches."""
    csv_path = f"{CSV_BASE_PATH}/branches.csv"
    logger.info(f"Loading branches from {csv_path}")

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(csv_path)

    actual_cols = set(df.columns)
    expected_cols = set(EXPECTED_COLUMNS["branches"])
    missing = expected_cols - actual_cols
    if missing:
        logger.error(f"Missing columns in branches.csv: {missing}")
        raise ValueError(f"Schema mismatch: missing columns {missing}")

    df_with_meta = df \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("source_file", lit("branches.csv"))

    df_with_meta.write \
        .mode("overwrite") \
        .format("parquet") \
        .saveAsTable("bronze.branches")

    row_count = spark.sql("SELECT COUNT(*) FROM bronze.branches").collect()[0][0]
    logger.info(f"bronze.branches loaded: {row_count} rows")
    return row_count


def load_employees(spark):
    """Load employees.csv into bronze.employees."""
    csv_path = f"{CSV_BASE_PATH}/employees.csv"
    logger.info(f"Loading employees from {csv_path}")

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(csv_path)

    actual_cols = set(df.columns)
    expected_cols = set(EXPECTED_COLUMNS["employees"])
    missing = expected_cols - actual_cols
    if missing:
        logger.error(f"Missing columns in employees.csv: {missing}")
        raise ValueError(f"Schema mismatch: missing columns {missing}")

    df_with_meta = df \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("source_file", lit("employees.csv"))

    df_with_meta.write \
        .mode("overwrite") \
        .format("parquet") \
        .saveAsTable("bronze.employees")

    row_count = spark.sql("SELECT COUNT(*) FROM bronze.employees").collect()[0][0]
    logger.info(f"bronze.employees loaded: {row_count} rows")
    return row_count


# ============================================================================
# Main
# ============================================================================
def main():
    """Load all CSV reference data into bronze layer."""
    spark = None
    try:
        spark = create_spark_session()

        logger.info("=" * 60)
        logger.info("Starting CSV ingestion to bronze layer")
        logger.info("=" * 60)

        branches_count = load_branches(spark)
        employees_count = load_employees(spark)

        logger.info("=" * 60)
        logger.info("CSV ingestion complete:")
        logger.info(f"  bronze.branches:  {branches_count} rows")
        logger.info(f"  bronze.employees: {employees_count} rows")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"CSV ingestion failed: {e}")
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
