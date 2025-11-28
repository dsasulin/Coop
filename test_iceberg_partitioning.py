"""
Test Iceberg Partitioning Syntax
=================================
This script tests different partitioning syntax options for Apache Iceberg tables.
"""

from pyspark.sql import SparkSession
from datetime import datetime
import sys

def create_spark_session():
    """Create Spark session with Iceberg support"""

    spark = (SparkSession.builder
        .appName("Iceberg Partitioning Test")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", "/tmp/iceberg-warehouse")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3")
        .enableHiveSupport()
        .getOrCreate()
    )

    return spark

def test_partitioning_syntax():
    """Test different partitioning syntax options"""

    print("=" * 80)
    print("Testing Iceberg Partitioning Syntax")
    print("=" * 80)

    spark = create_spark_session()

    # Test 1: PARTITIONED BY (days(event_ts))
    print("\n[TEST 1] Testing: PARTITIONED BY (days(event_ts))")
    print("-" * 80)

    try:
        spark.sql("DROP TABLE IF EXISTS local.test_db.events_test1")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS local.test_db.events_test1 (
                event_id STRING,
                event_ts TIMESTAMP,
                user_id STRING,
                event_type STRING
            )
            USING iceberg
            PARTITIONED BY (days(event_ts))
        """)
        print("✓ SUCCESS: PARTITIONED BY (days(event_ts)) syntax works!")

        # Show table properties
        spark.sql("DESCRIBE EXTENDED local.test_db.events_test1").show(100, False)

    except Exception as e:
        print(f"✗ FAILED: {str(e)}")


    # Test 2: PARTITIONED BY SPEC (days(event_ts))
    print("\n[TEST 2] Testing: PARTITIONED BY SPEC (days(event_ts))")
    print("-" * 80)

    try:
        spark.sql("DROP TABLE IF EXISTS local.test_db.events_test2")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS local.test_db.events_test2 (
                event_id STRING,
                event_ts TIMESTAMP,
                user_id STRING,
                event_type STRING
            )
            USING iceberg
            PARTITIONED BY SPEC (days(event_ts))
        """)
        print("✓ SUCCESS: PARTITIONED BY SPEC (days(event_ts)) syntax works!")

        # Show table properties
        spark.sql("DESCRIBE EXTENDED local.test_db.events_test2").show(100, False)

    except Exception as e:
        print(f"✗ FAILED: {str(e)}")


    # Test 3: Simple column partitioning
    print("\n[TEST 3] Testing: PARTITIONED BY (event_date)")
    print("-" * 80)

    try:
        spark.sql("DROP TABLE IF EXISTS local.test_db.events_test3")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS local.test_db.events_test3 (
                event_id STRING,
                event_ts TIMESTAMP,
                user_id STRING,
                event_type STRING,
                event_date DATE
            )
            USING iceberg
            PARTITIONED BY (event_date)
        """)
        print("✓ SUCCESS: PARTITIONED BY (event_date) syntax works!")

        # Show table properties
        spark.sql("DESCRIBE EXTENDED local.test_db.events_test3").show(100, False)

    except Exception as e:
        print(f"✗ FAILED: {str(e)}")


    # Test 4: Multiple partition transforms
    print("\n[TEST 4] Testing: PARTITIONED BY (days(event_ts), bucket(10, user_id))")
    print("-" * 80)

    try:
        spark.sql("DROP TABLE IF EXISTS local.test_db.events_test4")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS local.test_db.events_test4 (
                event_id STRING,
                event_ts TIMESTAMP,
                user_id STRING,
                event_type STRING
            )
            USING iceberg
            PARTITIONED BY (days(event_ts), bucket(10, user_id))
        """)
        print("✓ SUCCESS: Multiple partition transforms work!")

        # Show table properties
        spark.sql("DESCRIBE EXTENDED local.test_db.events_test4").show(100, False)

    except Exception as e:
        print(f"✗ FAILED: {str(e)}")


    # Test 5: Insert data and verify partitioning
    print("\n[TEST 5] Inserting test data into partitioned table")
    print("-" * 80)

    try:
        from pyspark.sql.functions import lit, current_timestamp
        from datetime import datetime, timedelta

        # Create test data
        test_data = []
        base_date = datetime(2024, 1, 1)

        for i in range(10):
            event_date = base_date + timedelta(days=i)
            test_data.append((
                f"event_{i}",
                event_date,
                f"user_{i % 3}",
                "click"
            ))

        df = spark.createDataFrame(
            test_data,
            ["event_id", "event_ts", "user_id", "event_type"]
        )

        # Insert into table
        df.writeTo("local.test_db.events_test1").append()

        print("✓ SUCCESS: Data inserted successfully!")

        # Show data
        print("\nData in table:")
        spark.sql("SELECT * FROM local.test_db.events_test1 ORDER BY event_ts").show()

        # Show partition info
        print("\nPartition information:")
        spark.sql("SHOW PARTITIONS local.test_db.events_test1").show()

    except Exception as e:
        print(f"✗ FAILED: {str(e)}")


    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print("""
    For Apache Iceberg with Spark 3.5:

    ✓ RECOMMENDED: PARTITIONED BY (days(event_ts))
      - This is the standard and preferred syntax
      - Cleaner and more readable
      - Works with transform functions: days(), months(), years(), hours(), bucket(), truncate()

    ? ALTERNATIVE: PARTITIONED BY SPEC (days(event_ts))
      - May work depending on Spark/Iceberg version
      - Less common syntax

    ✓ SIMPLE: PARTITIONED BY (column_name)
      - Works for direct column partitioning
      - No transform function applied

    ✓ MULTIPLE: PARTITIONED BY (transform1(col1), transform2(col2))
      - Multiple partition transforms are supported
    """)

    spark.stop()

if __name__ == "__main__":
    test_partitioning_syntax()
