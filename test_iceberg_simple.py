"""
Simple Iceberg Partitioning Test
================================
Test if PARTITIONED BY (days(event_ts)) works in Iceberg
"""

from pyspark.sql import SparkSession

# Create Spark session with Iceberg
spark = (SparkSession.builder
    .appName("IcebergPartitionTest")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "/tmp/iceberg-warehouse")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3")
    .getOrCreate()
)

print("\n" + "="*80)
print("TEST 1: PARTITIONED BY (days(event_ts))")
print("="*80)

try:
    spark.sql("DROP TABLE IF EXISTS local.default.test_table1")
    spark.sql("""
        CREATE TABLE local.default.test_table1 (
            event_id STRING,
            event_ts TIMESTAMP,
            user_id STRING
        )
        USING iceberg
        PARTITIONED BY (days(event_ts))
    """)
    print("✓ SUCCESS: PARTITIONED BY (days(event_ts)) works!")
except Exception as e:
    print(f"✗ FAILED: {e}")

print("\n" + "="*80)
print("TEST 2: PARTITIONED BY SPEC (days(event_ts))")
print("="*80)

try:
    spark.sql("DROP TABLE IF EXISTS local.default.test_table2")
    spark.sql("""
        CREATE TABLE local.default.test_table2 (
            event_id STRING,
            event_ts TIMESTAMP,
            user_id STRING
        )
        USING iceberg
        PARTITIONED BY SPEC (days(event_ts))
    """)
    print("✓ SUCCESS: PARTITIONED BY SPEC (days(event_ts)) works!")
except Exception as e:
    print(f"✗ FAILED: {e}")

spark.stop()
