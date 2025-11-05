"""
ETL Job: Stage (test schema) -> Bronze Layer
Loads raw data from stage tables into bronze layer
Adds technical metadata (timestamp, source_file)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, year, month
from datetime import datetime
import sys

# Configuration
STAGE_DATABASE = "test"
BRONZE_DATABASE = "bronze"
SOURCE_FILE = "stage_initial_load"

# List of tables to load
TABLES = [
    "clients",
    "products",
    "contracts",
    "accounts",
    "client_products",
    "transactions",
    "account_balances",
    "cards",
    "branches",
    "employees",
    "loans",
    "credit_applications"
]

def create_spark_session(app_name="Stage_to_Bronze_ETL"):
    """Create Spark session"""
    return SparkSession.builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .getOrCreate()

def load_table_to_bronze(spark, table_name):
    """
    Load one table from stage to bronze

    Args:
        spark: SparkSession
        table_name: table name
    """
    print(f"Processing table: {table_name}")

    try:
        # Read from stage
        df_stage = spark.table(f"{STAGE_DATABASE}.{table_name}")
        record_count = df_stage.count()
        print(f"  - Read {record_count} records from {STAGE_DATABASE}.{table_name}")

        # Add technical fields
        df_bronze = df_stage \
            .withColumn("load_timestamp", current_timestamp()) \
            .withColumn("source_file", lit(SOURCE_FILE))

        # For transactions add partitions
        if table_name == "transactions":
            df_bronze = df_bronze \
                .withColumn("transaction_year", year("transaction_date")) \
                .withColumn("transaction_month", month("transaction_date"))

            # Write with partitioning
            df_bronze.write \
                .mode("append") \
                .partitionBy("transaction_year", "transaction_month") \
                .format("parquet") \
                .saveAsTable(f"{BRONZE_DATABASE}.{table_name}")
        else:
            # Write without partitions
            df_bronze.write \
                .mode("append") \
                .format("parquet") \
                .saveAsTable(f"{BRONZE_DATABASE}.{table_name}")

        print(f"  - Successfully loaded {record_count} records to {BRONZE_DATABASE}.{table_name}")
        return True, record_count

    except Exception as e:
        print(f"  - ERROR loading {table_name}: {str(e)}")
        return False, 0

def main():
    """Main ETL function"""
    print("=" * 80)
    print("Starting Stage to Bronze ETL Job")
    print(f"Start time: {datetime.now()}")
    print("=" * 80)

    # Create Spark session
    spark = create_spark_session()

    # Statistics
    success_count = 0
    fail_count = 0
    total_records = 0

    # Process each table
    for table_name in TABLES:
        success, records = load_table_to_bronze(spark, table_name)
        if success:
            success_count += 1
            total_records += records
        else:
            fail_count += 1

    # Final statistics
    print("\n" + "=" * 80)
    print("ETL Job Summary")
    print("=" * 80)
    print(f"Total tables processed: {len(TABLES)}")
    print(f"  - Successful: {success_count}")
    print(f"  - Failed: {fail_count}")
    print(f"Total records loaded: {total_records:,}")
    print(f"End time: {datetime.now()}")
    print("=" * 80)

    spark.stop()

    # Return error code if there were failures
    if fail_count > 0:
        sys.exit(1)

if __name__ == "__main__":
    main()
