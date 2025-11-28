#!/usr/bin/env python3
"""
Quick Iceberg Partitioning Test (Local Mode)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from datetime import datetime, timedelta

print("\n" + "="*80)
print("Creating Spark Session with Iceberg support...")
print("="*80)

try:
    spark = (SparkSession.builder
        .appName("IcebergTest")
        .master("local[*]")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", "/tmp/iceberg-test")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    print("\n✓ Spark session created successfully!")

    # Test 1: PARTITIONED BY (days(event_ts))
    print("\n" + "="*80)
    print("TEST: PARTITIONED BY (days(event_ts))")
    print("="*80)

    spark.sql("DROP TABLE IF EXISTS local.default.events")

    create_sql = """
        CREATE TABLE IF NOT EXISTS local.default.events (
            event_id STRING,
            event_ts TIMESTAMP,
            user_id STRING,
            event_type STRING
        )
        USING iceberg
        PARTITIONED BY (days(event_ts))
    """

    print("\nExecuting SQL:")
    print(create_sql)

    spark.sql(create_sql)

    print("\n✓ SUCCESS: Table created with PARTITIONED BY (days(event_ts))")

    # Show table details
    print("\n" + "-"*80)
    print("Table Details:")
    print("-"*80)
    spark.sql("DESCRIBE EXTENDED local.default.events").show(50, False)

    # Insert test data
    print("\n" + "="*80)
    print("Inserting test data...")
    print("="*80)

    test_data = []
    for i in range(5):
        test_data.append((
            f"evt_{i}",
            datetime(2024, 1, i+1, 12, 0, 0),
            f"user_{i%2}",
            "click"
        ))

    df = spark.createDataFrame(test_data, ["event_id", "event_ts", "user_id", "event_type"])
    df.writeTo("local.default.events").append()

    print("✓ Data inserted successfully!")

    # Query data
    print("\n" + "-"*80)
    print("Data in table:")
    print("-"*80)
    spark.sql("SELECT * FROM local.default.events ORDER BY event_ts").show()

    # Show partition info
    print("\n" + "-"*80)
    print("Partition Information:")
    print("-"*80)
    result = spark.sql("SELECT event_ts, days(event_ts) as partition_key FROM local.default.events")
    result.show()

    print("\n" + "="*80)
    print("✓ ALL TESTS PASSED!")
    print("="*80)
    print("""
ВЫВОД:
------
Синтаксис PARTITIONED BY (days(event_ts)) работает корректно!

Используйте:
    PARTITIONED BY (days(event_ts))

НЕ используйте:
    PARTITIONED BY SPEC (days(event_ts))  ← старый/нестандартный синтаксис
    """)

    spark.stop()

except Exception as e:
    print(f"\n✗ ERROR: {e}")
    import traceback
    traceback.print_exc()
