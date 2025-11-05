"""
ETL Job: Silver -> Gold Layer
Creates aggregated data marts and analytical tables
Builds dimensions and facts for analytics
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, year, month, current_date,
    sum as spark_sum, count as spark_count, avg as spark_avg,
    max as spark_max, min as spark_min, countDistinct,
    datediff, when, coalesce, round as spark_round,
    to_date, dense_rank
)
from pyspark.sql.window import Window
from datetime import datetime
import sys

# Configuration
SILVER_DATABASE = "silver"
GOLD_DATABASE = "gold"

def create_spark_session(app_name="Silver_to_Gold_ETL"):
    """Create Spark session"""
    return SparkSession.builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

def build_dim_client(spark):
    """Build client dimension table"""
    print("Building dim_client...")

    # Main client information
    clients = spark.table(f"{SILVER_DATABASE}.clients")

    # Aggregated metrics by accounts
    accounts = spark.table(f"{SILVER_DATABASE}.accounts") \
        .groupBy("client_id") \
        .agg(
            spark_count("*").alias("total_accounts"),
            spark_sum(when(col("is_active"), 1).otherwise(0)).alias("total_active_accounts")
        )

    # Product metrics
    client_products = spark.table(f"{SILVER_DATABASE}.client_products") \
        .groupBy("client_id") \
        .agg(
            countDistinct("product_id").alias("total_products")
        )

    # Join all data
    dim_client = clients.alias("c") \
        .join(accounts.alias("a"), col("c.client_id") == col("a.client_id"), "left") \
        .join(client_products.alias("cp"), col("c.client_id") == col("cp.client_id"), "left") \
        .select(
            # Use client_id as surrogate key (in reality need auto-increment)
            col("c.client_id").alias("client_key"),
            col("c.client_id"),
            col("c.first_name"),
            col("c.last_name"),
            col("c.full_name"),
            col("c.email"),
            col("c.email_domain"),
            col("c.phone"),
            col("c.birth_date"),
            col("c.age"),
            # Age groups
            when(col("c.age") < 25, "18-24")
            .when(col("c.age") < 35, "25-34")
            .when(col("c.age") < 45, "35-44")
            .when(col("c.age") < 55, "45-54")
            .when(col("c.age") < 65, "55-64")
            .otherwise("65+").alias("age_group"),
            col("c.registration_date"),
            datediff(current_date(), col("c.registration_date")).alias("customer_tenure_days"),
            spark_round(datediff(current_date(), col("c.registration_date")) / 365.25, 2).alias("customer_tenure_years"),
            col("c.address"),
            col("c.city"),
            col("c.country"),
            # Region (simplified version)
            when(col("c.country") == "US", "NORTH_AMERICA")
            .when(col("c.country").isin("UK", "DE", "FR", "IT", "ES"), "EUROPE")
            .otherwise("OTHER").alias("region"),
            col("c.risk_category"),
            col("c.credit_score"),
            col("c.credit_score_category"),
            col("c.employment_status"),
            col("c.annual_income"),
            col("c.income_category"),
            # Client Segment (simplified logic)
            when(
                (col("c.annual_income") > 150000) &
                (col("c.credit_score") > 750),
                "VIP"
            ).when(
                (col("c.annual_income") > 100000) &
                (col("c.credit_score") > 700),
                "PREMIUM"
            ).when(
                (col("c.annual_income") > 50000) &
                (col("c.credit_score") > 650),
                "REGULAR"
            ).otherwise("BASIC").alias("client_segment"),
            # Placeholder for CLV (needs more complex logic)
            lit(0.0).alias("client_lifetime_value"),
            coalesce(col("a.total_accounts"), lit(0)).alias("total_accounts"),
            coalesce(col("a.total_active_accounts"), lit(0)).alias("total_active_accounts"),
            coalesce(col("cp.total_products"), lit(0)).alias("total_products"),
            lit(0).alias("total_contracts"),  # Placeholder
            lit(0).alias("total_cards"),      # Placeholder
            lit(0).alias("total_loans"),      # Placeholder
            # SCD fields
            col("c.registration_date").alias("effective_date"),
            lit(None).cast("date").alias("end_date"),
            lit(True).alias("is_current"),
            current_timestamp().alias("created_timestamp"),
            current_timestamp().alias("updated_timestamp")
        )

    dim_client.write \
        .mode("overwrite") \
        .format("parquet") \
        .saveAsTable(f"{GOLD_DATABASE}.dim_client")

    count = dim_client.count()
    print(f"  - Created dim_client with {count:,} records")
    return count

def build_fact_transactions_daily(spark):
    """Build fact table with daily transaction aggregates"""
    print("Building fact_transactions_daily...")

    transactions = spark.table(f"{SILVER_DATABASE}.transactions")
    accounts = spark.table(f"{SILVER_DATABASE}.accounts").select(
        col("account_id"),
        col("client_id"),
        col("branch_code")
    )

    # Join transactions with accounts to get client_id and branch_code
    trans_enriched = transactions.alias("t") \
        .join(accounts.alias("a"), col("t.from_account_id") == col("a.account_id"), "left") \
        .select(
            col("t.transaction_date_only").alias("transaction_date"),
            coalesce(col("a.client_id"), lit(-1)).alias("client_id"),
            col("t.from_account_id").alias("account_id"),
            coalesce(col("a.branch_code"), lit("UNKNOWN")).alias("branch_code"),
            col("t.transaction_type_normalized").alias("transaction_type"),
            col("t.category_normalized").alias("category"),
            col("t.currency"),
            col("t.amount"),
            col("t.status_normalized").alias("status"),
            col("t.is_suspicious")
        )

    # Aggregate by day, client, account, transaction type
    fact_daily = trans_enriched.groupBy(
        "transaction_date",
        "client_id",
        "account_id",
        "branch_code",
        "transaction_type",
        "category",
        "currency"
    ).agg(
        spark_count("*").alias("transaction_count"),
        spark_sum("amount").alias("total_amount"),
        spark_avg("amount").alias("avg_amount"),
        spark_min("amount").alias("min_amount"),
        spark_max("amount").alias("max_amount"),
        spark_sum(when(col("status") == "COMPLETED", 1).otherwise(0)).alias("successful_count"),
        spark_sum(when(col("status") == "FAILED", 1).otherwise(0)).alias("failed_count"),
        spark_sum(when(col("status") == "PENDING", 1).otherwise(0)).alias("pending_count"),
        spark_sum(when(col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
        spark_sum(when(col("is_suspicious"), 1).otherwise(0)).alias("suspicious_transaction_count")
    ).withColumn(
        "success_rate",
        spark_round(
            (col("successful_count") / col("transaction_count")) * 100,
            2
        )
    ).withColumn(
        "has_suspicious_transactions",
        when(col("suspicious_transaction_count") > 0, True).otherwise(False)
    ).withColumn(
        "created_timestamp",
        current_timestamp()
    ).withColumn(
        "updated_timestamp",
        current_timestamp()
    )

    # Add partitions
    fact_daily = fact_daily \
        .withColumn("year", year("transaction_date")) \
        .withColumn("month", month("transaction_date"))

    fact_daily.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .format("parquet") \
        .saveAsTable(f"{GOLD_DATABASE}.fact_transactions_daily")

    count = fact_daily.count()
    print(f"  - Created fact_transactions_daily with {count:,} records")
    return count

def build_client_360_view(spark):
    """Build Client 360 View data mart"""
    print("Building client_360_view...")

    # Main client information
    clients = spark.table(f"{SILVER_DATABASE}.clients")

    # Client accounts
    accounts = spark.table(f"{SILVER_DATABASE}.accounts") \
        .groupBy("client_id") \
        .agg(
            spark_count("*").alias("total_accounts"),
            spark_sum(when(col("is_active"), 1).otherwise(0)).alias("active_accounts"),
            spark_sum(when(col("account_type_normalized") == "CHECKING", 1).otherwise(0)).alias("checking_accounts"),
            spark_sum(when(col("account_type_normalized") == "SAVINGS", 1).otherwise(0)).alias("savings_accounts"),
            spark_sum(when(col("account_type_normalized") == "LOAN", 1).otherwise(0)).alias("loan_accounts")
        )

    # Client balances
    balances = spark.table(f"{SILVER_DATABASE}.account_balances").alias("b") \
        .join(
            spark.table(f"{SILVER_DATABASE}.accounts").select("account_id", "client_id").alias("a"),
            col("b.account_id") == col("a.account_id")
        ) \
        .groupBy(col("a.client_id")) \
        .agg(
            spark_sum("b.current_balance").alias("total_balance"),
            spark_avg("b.current_balance").alias("avg_balance")
        )

    # Transactions for last 30 days
    transactions_30d = spark.table(f"{SILVER_DATABASE}.transactions") \
        .join(
            spark.table(f"{SILVER_DATABASE}.accounts").select("account_id", "client_id"),
            col("from_account_id") == col("account_id")
        ) \
        .filter(col("transaction_date") >= date_add(current_date(), -30)) \
        .groupBy("client_id") \
        .agg(
            spark_count("*").alias("transactions_30d"),
            spark_sum("amount").alias("transaction_volume_30d"),
            spark_avg("amount").alias("avg_transaction_30d"),
            spark_sum(when(col("transaction_type_normalized") == "DEPOSIT", 1).otherwise(0)).alias("deposits_30d"),
            spark_sum(when(col("transaction_type_normalized") == "WITHDRAWAL", 1).otherwise(0)).alias("withdrawals_30d"),
            spark_sum(when(col("transaction_type_normalized") == "TRANSFER", 1).otherwise(0)).alias("transfers_30d"),
            spark_max("transaction_date").alias("last_transaction_date")
        )

    # Combine all data
    client_360 = clients.alias("c") \
        .join(accounts.alias("a"), col("c.client_id") == col("a.client_id"), "left") \
        .join(balances.alias("b"), col("c.client_id") == col("b.client_id"), "left") \
        .join(transactions_30d.alias("t"), col("c.client_id") == col("t.client_id"), "left") \
        .select(
            col("c.client_id"),
            current_date().alias("snapshot_date"),
            col("c.full_name"),
            col("c.email"),
            col("c.phone"),
            col("c.age"),
            # Age group
            when(col("c.age") < 25, "18-24")
            .when(col("c.age") < 35, "25-34")
            .when(col("c.age") < 45, "35-44")
            .when(col("c.age") < 55, "45-54")
            .when(col("c.age") < 65, "55-64")
            .otherwise("65+").alias("age_group"),
            col("c.city"),
            col("c.country"),
            col("c.risk_category"),
            col("c.credit_score"),
            col("c.credit_score_category"),
            col("c.annual_income"),
            col("c.income_category"),
            # Client Segment
            when(
                (col("c.annual_income") > 150000) &
                (col("c.credit_score") > 750),
                "VIP"
            ).when(
                (col("c.annual_income") > 100000) &
                (col("c.credit_score") > 700),
                "PREMIUM"
            ).otherwise("REGULAR").alias("client_segment"),
            # Account summary
            coalesce(col("a.total_accounts"), lit(0)).alias("total_accounts"),
            coalesce(col("a.active_accounts"), lit(0)).alias("active_accounts"),
            coalesce(col("a.checking_accounts"), lit(0)).alias("checking_accounts"),
            coalesce(col("a.savings_accounts"), lit(0)).alias("savings_accounts"),
            coalesce(col("a.loan_accounts"), lit(0)).alias("loan_accounts"),
            coalesce(col("b.total_balance"), lit(0)).alias("total_balance"),
            coalesce(col("b.avg_balance"), lit(0)).alias("avg_balance"),
            # Product holdings (placeholders)
            lit(0).alias("total_products"),
            lit(0).alias("total_contracts"),
            lit(0).alias("total_cards"),
            lit(0).alias("active_cards"),
            # Loan summary (placeholders)
            lit(0).alias("total_loans"),
            lit(0).alias("active_loans"),
            lit(0.0).alias("total_loan_amount"),
            lit(0.0).alias("total_outstanding_balance"),
            lit(0.0).alias("total_loan_payment"),
            lit(0).alias("delinquent_loans"),
            # Transaction behavior
            coalesce(col("t.transactions_30d"), lit(0)).alias("transactions_30d"),
            coalesce(col("t.transaction_volume_30d"), lit(0)).alias("transaction_volume_30d"),
            coalesce(col("t.avg_transaction_30d"), lit(0)).alias("avg_transaction_30d"),
            coalesce(col("t.deposits_30d"), lit(0)).alias("deposits_30d"),
            coalesce(col("t.withdrawals_30d"), lit(0)).alias("withdrawals_30d"),
            coalesce(col("t.transfers_30d"), lit(0)).alias("transfers_30d"),
            # Engagement metrics
            col("t.last_transaction_date"),
            coalesce(datediff(current_date(), col("t.last_transaction_date")), lit(999)).alias("days_since_last_transaction"),
            when(coalesce(col("t.transactions_30d"), lit(0)) >= 20, "DAILY")
            .when(coalesce(col("t.transactions_30d"), lit(0)) >= 4, "WEEKLY")
            .when(coalesce(col("t.transactions_30d"), lit(0)) >= 1, "MONTHLY")
            .otherwise("INACTIVE").alias("transaction_frequency"),
            lit("MOBILE").alias("channel_preference"),  # Placeholder
            # Lifetime value (placeholders)
            lit(0.0).alias("customer_lifetime_value"),
            lit(0.0).alias("total_revenue"),
            lit(0.0).alias("total_fees_paid"),
            lit(0.0).alias("profitability_score"),
            # Risk indicators (placeholders)
            lit(0.0).alias("risk_score"),
            lit(0).alias("fraud_alerts"),
            lit(0).alias("suspicious_activities"),
            current_timestamp().alias("created_timestamp"),
            current_timestamp().alias("updated_timestamp")
        )

    # Partitions
    client_360 = client_360 \
        .withColumn("snapshot_year", year("snapshot_date")) \
        .withColumn("snapshot_month", month("snapshot_date"))

    client_360.write \
        .mode("overwrite") \
        .partitionBy("snapshot_year", "snapshot_month") \
        .format("parquet") \
        .saveAsTable(f"{GOLD_DATABASE}.client_360_view")

    count = client_360.count()
    print(f"  - Created client_360_view with {count:,} records")
    return count

def main():
    """Main ETL function"""
    print("=" * 80)
    print("Starting Silver to Gold ETL Job")
    print(f"Start time: {datetime.now()}")
    print("=" * 80)

    spark = create_spark_session()

    total_records = 0

    try:
        # Build dimensions and facts
        total_records += build_dim_client(spark)
        total_records += build_fact_transactions_daily(spark)
        total_records += build_client_360_view(spark)

        # Final statistics
        print("\n" + "=" * 80)
        print("ETL Job Summary")
        print("=" * 80)
        print(f"Total records created: {total_records:,}")
        print(f"End time: {datetime.now()}")
        print("=" * 80)

        spark.stop()

    except Exception as e:
        print(f"\nERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        spark.stop()
        sys.exit(1)

if __name__ == "__main__":
    main()
