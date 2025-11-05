"""
ETL Job: Bronze -> Silver Layer
Cleans and validates data from bronze layer
Applies business rules, standardizes formats, removes duplicates
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, year, month,
    concat_ws, upper, lower, regexp_replace, trim,
    datediff, current_date, when, coalesce,
    round as spark_round, abs as spark_abs,
    to_date, date_format, hour
)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from datetime import datetime
import sys

# Configuration
BRONZE_DATABASE = "bronze"
SILVER_DATABASE = "silver"

def create_spark_session(app_name="Bronze_to_Silver_ETL"):
    """Create Spark session"""
    return SparkSession.builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

def process_clients(spark):
    """Process clients table"""
    print("Processing clients...")

    df = spark.table(f"{BRONZE_DATABASE}.clients")

    # Remove duplicates by client_id (take last load)
    window = Window.partitionBy("client_id").orderBy(col("load_timestamp").desc())
    df = df.withColumn("rn", row_number().over(window)) \
           .filter(col("rn") == 1) \
           .drop("rn")

    # Cleaning and transformations
    df_silver = df.select(
        col("client_id"),
        trim(col("first_name")).alias("first_name"),
        trim(col("last_name")).alias("last_name"),
        concat_ws(" ", trim(col("first_name")), trim(col("last_name"))).alias("full_name"),
        lower(trim(col("email"))).alias("email"),
        regexp_replace(lower(trim(col("email"))), ".*@", "").alias("email_domain"),
        trim(col("phone")).alias("phone"),
        regexp_replace(regexp_replace(trim(col("phone")), "[^0-9+]", ""), "^00", "+").alias("phone_normalized"),
        col("birth_date"),
        spark_round((datediff(current_date(), col("birth_date")) / 365.25), 0).cast("int").alias("age"),
        col("registration_date"),
        trim(col("address")).alias("address"),
        trim(col("city")).alias("city"),
        trim(col("country")).alias("country"),
        upper(trim(col("risk_category"))).alias("risk_category"),
        col("credit_score"),
        # Credit score categories
        when(col("credit_score") < 580, "POOR")
        .when(col("credit_score") < 670, "FAIR")
        .when(col("credit_score") < 740, "GOOD")
        .when(col("credit_score") < 800, "VERY_GOOD")
        .otherwise("EXCELLENT").alias("credit_score_category"),
        upper(trim(col("employment_status"))).alias("employment_status"),
        col("annual_income"),
        # Income categories
        when(col("annual_income") < 30000, "LOW")
        .when(col("annual_income") < 75000, "MEDIUM")
        .when(col("annual_income") < 150000, "HIGH")
        .otherwise("VERY_HIGH").alias("income_category"),
        # Activity flag
        when(
            (col("email").isNotNull()) &
            (col("phone").isNotNull()) &
            (col("credit_score").isNotNull()),
            True
        ).otherwise(False).alias("is_active"),
        # Data Quality Score (0-1)
        spark_round(
            (
                when(col("first_name").isNotNull(), 0.1).otherwise(0) +
                when(col("last_name").isNotNull(), 0.1).otherwise(0) +
                when(col("email").isNotNull(), 0.15).otherwise(0) +
                when(col("phone").isNotNull(), 0.15).otherwise(0) +
                when(col("birth_date").isNotNull(), 0.1).otherwise(0) +
                when(col("address").isNotNull(), 0.1).otherwise(0) +
                when(col("city").isNotNull(), 0.1).otherwise(0) +
                when(col("credit_score").isNotNull(), 0.1).otherwise(0) +
                when(col("annual_income").isNotNull(), 0.1).otherwise(0)
            ), 2
        ).alias("dq_score"),
        # List of quality issues
        concat_ws(",",
            when(col("first_name").isNull(), lit("missing_first_name")),
            when(col("last_name").isNull(), lit("missing_last_name")),
            when(col("email").isNull(), lit("missing_email")),
            when(col("phone").isNull(), lit("missing_phone")),
            when(col("credit_score").isNull(), lit("missing_credit_score"))
        ).alias("dq_issues"),
        col("load_timestamp"),
        current_timestamp().alias("process_timestamp"),
        lit("bronze.clients").alias("source_system")
    )

    # Partition by registration year
    df_silver = df_silver.withColumn("registration_year", year("registration_date"))

    # Write to silver
    df_silver.write \
        .mode("overwrite") \
        .partitionBy("registration_year") \
        .format("parquet") \
        .saveAsTable(f"{SILVER_DATABASE}.clients")

    count = df_silver.count()
    print(f"  - Loaded {count:,} records to silver.clients")
    return count

def process_products(spark):
    """Process products table"""
    print("Processing products...")

    df = spark.table(f"{BRONZE_DATABASE}.products")

    # Remove duplicates
    window = Window.partitionBy("product_id").orderBy(col("load_timestamp").desc())
    df = df.withColumn("rn", row_number().over(window)) \
           .filter(col("rn") == 1) \
           .drop("rn")

    df_silver = df.select(
        col("product_id"),
        trim(col("product_name")).alias("product_name"),
        upper(trim(col("product_type"))).alias("product_type"),
        # Standardize categories
        when(upper(col("product_type")).isin("LOAN", "CREDIT"), "LENDING")
        .when(upper(col("product_type")).isin("SAVINGS", "CHECKING"), "DEPOSITS")
        .when(upper(col("product_type")).isin("CARD", "DEBIT", "CREDIT_CARD"), "CARDS")
        .otherwise("OTHER").alias("product_category"),
        upper(trim(col("currency"))).alias("currency"),
        coalesce(col("active"), lit(True)).alias("active"),
        col("load_timestamp"),
        current_timestamp().alias("process_timestamp"),
        lit("bronze.products").alias("source_system")
    )

    df_silver.write \
        .mode("overwrite") \
        .format("parquet") \
        .saveAsTable(f"{SILVER_DATABASE}.products")

    count = df_silver.count()
    print(f"  - Loaded {count:,} records to silver.products")
    return count

def process_accounts(spark):
    """Process accounts table"""
    print("Processing accounts...")

    df = spark.table(f"{BRONZE_DATABASE}.accounts")

    # Remove duplicates
    window = Window.partitionBy("account_id").orderBy(col("load_timestamp").desc())
    df = df.withColumn("rn", row_number().over(window)) \
           .filter(col("rn") == 1) \
           .drop("rn")

    df_silver = df.select(
        col("account_id"),
        col("client_id"),
        col("contract_id"),
        trim(col("account_number")).alias("account_number"),
        upper(trim(col("account_type"))).alias("account_type"),
        # Standardize types
        when(upper(col("account_type")).isin("CHECKING", "CURRENT"), "CHECKING")
        .when(upper(col("account_type")).isin("SAVINGS", "DEPOSIT"), "SAVINGS")
        .when(upper(col("account_type")).isin("LOAN", "CREDIT"), "LOAN")
        .otherwise(upper(col("account_type"))).alias("account_type_normalized"),
        upper(trim(col("currency"))).alias("currency"),
        col("open_date"),
        col("close_date"),
        datediff(coalesce(col("close_date"), current_date()), col("open_date")).alias("account_age_days"),
        upper(trim(col("status"))).alias("status"),
        # Standardize status
        when(upper(col("status")).isin("ACTIVE", "OPEN"), "ACTIVE")
        .when(upper(col("status")).isin("CLOSED", "TERMINATED"), "CLOSED")
        .when(upper(col("status")).isin("SUSPENDED", "BLOCKED", "FROZEN"), "SUSPENDED")
        .when(upper(col("status")).isin("DORMANT", "INACTIVE"), "DORMANT")
        .otherwise(upper(col("status"))).alias("status_normalized"),
        trim(col("branch_code")).alias("branch_code"),
        when(upper(col("status")).isin("ACTIVE", "OPEN"), True).otherwise(False).alias("is_active"),
        col("load_timestamp"),
        current_timestamp().alias("process_timestamp"),
        lit("bronze.accounts").alias("source_system")
    )

    # Partition by opening year
    df_silver = df_silver.withColumn("open_year", year("open_date"))

    df_silver.write \
        .mode("overwrite") \
        .partitionBy("open_year") \
        .format("parquet") \
        .saveAsTable(f"{SILVER_DATABASE}.accounts")

    count = df_silver.count()
    print(f"  - Loaded {count:,} records to silver.accounts")
    return count

def process_transactions(spark):
    """Process transactions table"""
    print("Processing transactions...")

    df = spark.table(f"{BRONZE_DATABASE}.transactions")

    # Remove duplicates by transaction_id
    window = Window.partitionBy("transaction_id").orderBy(col("load_timestamp").desc())
    df = df.withColumn("rn", row_number().over(window)) \
           .filter(col("rn") == 1) \
           .drop("rn")

    df_silver = df.select(
        col("transaction_id").cast("bigint"),
        col("transaction_uuid"),
        col("from_account_id"),
        col("to_account_id"),
        trim(col("from_account_number")).alias("from_account_number"),
        trim(col("to_account_number")).alias("to_account_number"),
        upper(trim(col("transaction_type"))).alias("transaction_type"),
        # Standardize transaction type
        when(upper(col("transaction_type")).isin("DEPOSIT", "CREDIT"), "DEPOSIT")
        .when(upper(col("transaction_type")).isin("WITHDRAWAL", "DEBIT"), "WITHDRAWAL")
        .when(upper(col("transaction_type")).isin("TRANSFER", "P2P"), "TRANSFER")
        .when(upper(col("transaction_type")).isin("PAYMENT", "PURCHASE"), "PAYMENT")
        .otherwise(upper(col("transaction_type"))).alias("transaction_type_normalized"),
        col("amount"),
        spark_abs(col("amount")).alias("amount_abs"),
        upper(trim(col("currency"))).alias("currency"),
        col("transaction_date"),
        to_date(col("transaction_date")).alias("transaction_date_only"),
        hour(col("transaction_date")).alias("transaction_hour"),
        trim(col("description")).alias("description"),
        upper(trim(col("status"))).alias("status"),
        # Standardize status
        when(upper(col("status")).isin("COMPLETED", "SUCCESS", "SUCCESSFUL"), "COMPLETED")
        .when(upper(col("status")).isin("PENDING", "PROCESSING"), "PENDING")
        .when(upper(col("status")).isin("FAILED", "ERROR"), "FAILED")
        .when(upper(col("status")).isin("CANCELLED", "CANCELED"), "CANCELLED")
        .otherwise(upper(col("status"))).alias("status_normalized"),
        upper(trim(col("category"))).alias("category"),
        # Standardize category
        when(upper(col("category")).isin("SALARY", "INCOME"), "SALARY")
        .when(upper(col("category")).isin("SHOPPING", "RETAIL"), "SHOPPING")
        .when(upper(col("category")).isin("FOOD", "GROCERIES", "RESTAURANT"), "FOOD_DINING")
        .when(upper(col("category")).isin("TRANSPORT", "TRANSPORTATION"), "TRANSPORT")
        .when(upper(col("category")).isin("ENTERTAINMENT", "LEISURE"), "ENTERTAINMENT")
        .otherwise(upper(col("category"))).alias("category_normalized"),
        trim(col("merchant_name")).alias("merchant_name"),
        # Internal transfer flag
        when(
            (col("from_account_id").isNotNull()) &
            (col("to_account_id").isNotNull()),
            True
        ).otherwise(False).alias("is_internal_transfer"),
        # Simple suspicious flag (can be enhanced)
        when(
            (col("amount") > 10000) |
            (hour(col("transaction_date")).between(0, 5)),
            True
        ).otherwise(False).alias("is_suspicious"),
        col("load_timestamp"),
        current_timestamp().alias("process_timestamp"),
        lit("bronze.transactions").alias("source_system")
    )

    # Partitions
    df_silver = df_silver \
        .withColumn("transaction_year", year("transaction_date")) \
        .withColumn("transaction_month", month("transaction_date"))

    df_silver.write \
        .mode("overwrite") \
        .partitionBy("transaction_year", "transaction_month") \
        .format("parquet") \
        .saveAsTable(f"{SILVER_DATABASE}.transactions")

    count = df_silver.count()
    print(f"  - Loaded {count:,} records to silver.transactions")
    return count

def process_account_balances(spark):
    """Process account_balances table"""
    print("Processing account_balances...")

    df = spark.table(f"{BRONZE_DATABASE}.account_balances")

    # Take latest balance for each account
    window = Window.partitionBy("account_id").orderBy(col("last_updated").desc())
    df = df.withColumn("rn", row_number().over(window)) \
           .filter(col("rn") == 1) \
           .drop("rn")

    df_silver = df.select(
        col("balance_id"),
        col("account_id"),
        col("current_balance"),
        col("available_balance"),
        (col("current_balance") - col("available_balance")).alias("reserved_amount"),
        upper(trim(col("currency"))).alias("currency"),
        col("last_updated"),
        coalesce(col("credit_limit"), lit(0)).alias("credit_limit"),
        # Credit utilization (if limit exists)
        when(
            col("credit_limit") > 0,
            spark_round((col("current_balance") / col("credit_limit")) * 100, 2)
        ).otherwise(lit(0)).alias("credit_utilization"),
        # Balance category
        when(col("current_balance") < 0, "NEGATIVE")
        .when(col("current_balance") < 1000, "LOW")
        .when(col("current_balance") < 10000, "MEDIUM")
        .when(col("current_balance") < 100000, "HIGH")
        .otherwise("VERY_HIGH").alias("balance_category"),
        col("load_timestamp"),
        current_timestamp().alias("process_timestamp"),
        lit("bronze.account_balances").alias("source_system")
    )

    df_silver.write \
        .mode("overwrite") \
        .format("parquet") \
        .saveAsTable(f"{SILVER_DATABASE}.account_balances")

    count = df_silver.count()
    print(f"  - Loaded {count:,} records to silver.account_balances")
    return count

def main():
    """Main ETL function"""
    print("=" * 80)
    print("Starting Bronze to Silver ETL Job")
    print(f"Start time: {datetime.now()}")
    print("=" * 80)

    spark = create_spark_session()

    total_records = 0

    try:
        # Process tables
        total_records += process_clients(spark)
        total_records += process_products(spark)
        total_records += process_accounts(spark)
        total_records += process_transactions(spark)
        total_records += process_account_balances(spark)

        # Final statistics
        print("\n" + "=" * 80)
        print("ETL Job Summary")
        print("=" * 80)
        print(f"Total records processed: {total_records:,}")
        print(f"End time: {datetime.now()}")
        print("=" * 80)

        spark.stop()

    except Exception as e:
        print(f"\nERROR: {str(e)}")
        spark.stop()
        sys.exit(1)

if __name__ == "__main__":
    main()
