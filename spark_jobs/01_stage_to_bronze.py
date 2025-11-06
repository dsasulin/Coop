#!/usr/bin/env python3
"""
ETL Job: Stage (test) -> Bronze Layer
Description: Load raw data from stage tables to bronze layer
Author: Data Engineering Team
Date: 2025-01-06
Version: 1.0
"""

import sys
import logging
from datetime import datetime
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
# Spark Session Configuration
# ============================================================================
def create_spark_session(app_name="Banking_ETL_Stage_to_Bronze"):
    """
    Create Spark session with Hive support and S3 configuration.

    This is configured for Kubernetes-based CDP with S3 storage.
    """
    logger.info("Creating Spark session...")

    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("hive.metastore.uris", "thrift://metastore-service.warehouse-1761913838-c49g.svc.cluster.local:9083") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.warehouse.dir", "s3a://co-op-buk-39d7d9df/user/hive/warehouse") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    # Set dynamic partitioning
    spark.sql("SET hive.exec.dynamic.partition=true")
    spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("SET hive.exec.max.dynamic.partitions=1000")
    spark.sql("SET hive.exec.max.dynamic.partitions.pernode=1000")

    logger.info(f"Spark session created: {spark.version}")
    logger.info(f"Metastore: {spark.conf.get('hive.metastore.uris')}")

    return spark


# ============================================================================
# ETL Functions
# ============================================================================

def load_clients(spark):
    """Load clients from test to bronze layer."""
    logger.info("Loading clients: test.clients -> bronze.clients")

    # Truncate bronze table
    spark.sql("TRUNCATE TABLE bronze.clients")

    # Read from test and write to bronze
    df = spark.sql("""
        SELECT
            client_id,
            first_name,
            last_name,
            email,
            phone,
            birth_date,
            registration_date,
            address,
            city,
            country,
            risk_category,
            credit_score,
            employment_status,
            annual_income
        FROM test.clients
    """)

    # Add technical fields
    df_bronze = df \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("source_file", lit("test.clients"))

    # Write to bronze
    df_bronze.write \
        .mode("overwrite") \
        .format("hive") \
        .saveAsTable("bronze.clients")

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM bronze.clients").collect()[0]['cnt']
    logger.info(f"✅ Clients loaded: {row_count} rows")

    return row_count


def load_products(spark):
    """Load products from test to bronze layer."""
    logger.info("Loading products: test.products -> bronze.products")

    spark.sql("TRUNCATE TABLE bronze.products")

    df = spark.sql("""
        SELECT
            product_id,
            product_name,
            product_type,
            product_category,
            interest_rate,
            currency,
            active
        FROM test.products
    """)

    df_bronze = df \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("source_file", lit("test.products"))

    df_bronze.write \
        .mode("overwrite") \
        .format("hive") \
        .saveAsTable("bronze.products")

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM bronze.products").collect()[0]['cnt']
    logger.info(f"✅ Products loaded: {row_count} rows")

    return row_count


def load_branches(spark):
    """Load branches from test to bronze layer."""
    logger.info("Loading branches: test.branches -> bronze.branches")

    spark.sql("TRUNCATE TABLE bronze.branches")

    df = spark.sql("""
        SELECT
            branch_id,
            branch_name,
            branch_code,
            branch_type,
            address,
            city,
            region,
            country,
            phone,
            email,
            manager_id,
            opening_date
        FROM test.branches
    """)

    df_bronze = df \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("source_file", lit("test.branches"))

    df_bronze.write \
        .mode("overwrite") \
        .format("hive") \
        .saveAsTable("bronze.branches")

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM bronze.branches").collect()[0]['cnt']
    logger.info(f"✅ Branches loaded: {row_count} rows")

    return row_count


def load_employees(spark):
    """Load employees from test to bronze layer."""
    logger.info("Loading employees: test.employees -> bronze.employees")

    spark.sql("TRUNCATE TABLE bronze.employees")

    df = spark.sql("""
        SELECT
            employee_id,
            first_name,
            last_name,
            email,
            phone,
            hire_date,
            job_title,
            department,
            branch_id,
            manager_id,
            salary,
            employment_status
        FROM test.employees
    """)

    df_bronze = df \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("source_file", lit("test.employees"))

    df_bronze.write \
        .mode("overwrite") \
        .format("hive") \
        .saveAsTable("bronze.employees")

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM bronze.employees").collect()[0]['cnt']
    logger.info(f"✅ Employees loaded: {row_count} rows")

    return row_count


def load_accounts(spark):
    """Load accounts from test to bronze layer."""
    logger.info("Loading accounts: test.accounts -> bronze.accounts")

    spark.sql("TRUNCATE TABLE bronze.accounts")

    df = spark.sql("""
        SELECT
            account_id,
            client_id,
            account_number,
            account_type,
            product_id,
            branch_id,
            opening_date,
            account_status,
            current_balance,
            currency,
            overdraft_limit,
            interest_rate
        FROM test.accounts
    """)

    df_bronze = df \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("source_file", lit("test.accounts"))

    df_bronze.write \
        .mode("overwrite") \
        .format("hive") \
        .saveAsTable("bronze.accounts")

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM bronze.accounts").collect()[0]['cnt']
    logger.info(f"✅ Accounts loaded: {row_count} rows")

    return row_count


def load_transactions(spark):
    """Load transactions from test to bronze layer with partitioning."""
    logger.info("Loading transactions: test.transactions -> bronze.transactions")

    spark.sql("TRUNCATE TABLE bronze.transactions")

    df = spark.sql("""
        SELECT
            transaction_id,
            account_id,
            transaction_date,
            transaction_type,
            amount,
            currency,
            balance_after,
            description,
            channel,
            transaction_status,
            YEAR(transaction_date) as transaction_year,
            MONTH(transaction_date) as transaction_month
        FROM test.transactions
    """)

    df_bronze = df \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("source_file", lit("test.transactions"))

    # Write with dynamic partitioning
    df_bronze.write \
        .mode("overwrite") \
        .format("hive") \
        .partitionBy("transaction_year", "transaction_month") \
        .saveAsTable("bronze.transactions")

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM bronze.transactions").collect()[0]['cnt']
    logger.info(f"✅ Transactions loaded: {row_count} rows")

    return row_count


def load_cards(spark):
    """Load cards from test to bronze layer."""
    logger.info("Loading cards: test.cards -> bronze.cards")

    spark.sql("TRUNCATE TABLE bronze.cards")

    df = spark.sql("""
        SELECT
            card_id,
            account_id,
            card_number,
            card_type,
            card_status,
            issue_date,
            expiry_date,
            credit_limit,
            available_credit,
            cvv,
            pin_set
        FROM test.cards
    """)

    df_bronze = df \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("source_file", lit("test.cards"))

    df_bronze.write \
        .mode("overwrite") \
        .format("hive") \
        .saveAsTable("bronze.cards")

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM bronze.cards").collect()[0]['cnt']
    logger.info(f"✅ Cards loaded: {row_count} rows")

    return row_count


def load_loans(spark):
    """Load loans from test to bronze layer."""
    logger.info("Loading loans: test.loans -> bronze.loans")

    spark.sql("TRUNCATE TABLE bronze.loans")

    df = spark.sql("""
        SELECT
            loan_id,
            client_id,
            product_id,
            branch_id,
            loan_amount,
            interest_rate,
            loan_term_months,
            start_date,
            end_date,
            loan_status,
            outstanding_balance,
            monthly_payment,
            currency
        FROM test.loans
    """)

    df_bronze = df \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("source_file", lit("test.loans"))

    df_bronze.write \
        .mode("overwrite") \
        .format("hive") \
        .saveAsTable("bronze.loans")

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM bronze.loans").collect()[0]['cnt']
    logger.info(f"✅ Loans loaded: {row_count} rows")

    return row_count


def load_contracts(spark):
    """Load contracts from test to bronze layer."""
    logger.info("Loading contracts: test.contracts -> bronze.contracts")

    spark.sql("TRUNCATE TABLE bronze.contracts")

    df = spark.sql("""
        SELECT
            contract_id,
            client_id,
            product_id,
            contract_number,
            contract_type,
            start_date,
            end_date,
            contract_amount,
            contract_status,
            signed_date,
            branch_id
        FROM test.contracts
    """)

    df_bronze = df \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("source_file", lit("test.contracts"))

    df_bronze.write \
        .mode("overwrite") \
        .format("hive") \
        .saveAsTable("bronze.contracts")

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM bronze.contracts").collect()[0]['cnt']
    logger.info(f"✅ Contracts loaded: {row_count} rows")

    return row_count


def load_client_products(spark):
    """Load client_products from test to bronze layer."""
    logger.info("Loading client_products: test.client_products -> bronze.client_products")

    spark.sql("TRUNCATE TABLE bronze.client_products")

    df = spark.sql("""
        SELECT
            client_product_id,
            client_id,
            product_id,
            subscription_date,
            subscription_status,
            last_usage_date
        FROM test.client_products
    """)

    df_bronze = df \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("source_file", lit("test.client_products"))

    df_bronze.write \
        .mode("overwrite") \
        .format("hive") \
        .saveAsTable("bronze.client_products")

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM bronze.client_products").collect()[0]['cnt']
    logger.info(f"✅ Client Products loaded: {row_count} rows")

    return row_count


def load_credit_applications(spark):
    """Load credit_applications from test to bronze layer."""
    logger.info("Loading credit_applications: test.credit_applications -> bronze.credit_applications")

    spark.sql("TRUNCATE TABLE bronze.credit_applications")

    df = spark.sql("""
        SELECT
            application_id,
            client_id,
            product_id,
            application_date,
            requested_amount,
            application_status,
            decision_date,
            approved_amount,
            interest_rate,
            rejection_reason
        FROM test.credit_applications
    """)

    df_bronze = df \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("source_file", lit("test.credit_applications"))

    df_bronze.write \
        .mode("overwrite") \
        .format("hive") \
        .saveAsTable("bronze.credit_applications")

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM bronze.credit_applications").collect()[0]['cnt']
    logger.info(f"✅ Credit Applications loaded: {row_count} rows")

    return row_count


def load_account_balances(spark):
    """Load account_balances from test to bronze layer with partitioning."""
    logger.info("Loading account_balances: test.account_balances -> bronze.account_balances")

    spark.sql("TRUNCATE TABLE bronze.account_balances")

    df = spark.sql("""
        SELECT
            balance_id,
            account_id,
            balance_date,
            balance_amount,
            available_balance,
            currency,
            YEAR(balance_date) as balance_year,
            MONTH(balance_date) as balance_month
        FROM test.account_balances
    """)

    df_bronze = df \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("source_file", lit("test.account_balances"))

    # Write with dynamic partitioning
    df_bronze.write \
        .mode("overwrite") \
        .format("hive") \
        .partitionBy("balance_year", "balance_month") \
        .saveAsTable("bronze.account_balances")

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM bronze.account_balances").collect()[0]['cnt']
    logger.info(f"✅ Account Balances loaded: {row_count} rows")

    return row_count


# ============================================================================
# Main ETL Pipeline
# ============================================================================

def main():
    """Main ETL pipeline execution."""
    start_time = datetime.now()
    logger.info("="*80)
    logger.info("Starting ETL Job: Stage (test) -> Bronze Layer")
    logger.info("="*80)

    spark = None
    exit_code = 0

    try:
        # Create Spark session
        spark = create_spark_session()

        # Load all tables
        tables_loaded = {}

        tables_loaded['clients'] = load_clients(spark)
        tables_loaded['products'] = load_products(spark)
        tables_loaded['branches'] = load_branches(spark)
        tables_loaded['employees'] = load_employees(spark)
        tables_loaded['accounts'] = load_accounts(spark)
        tables_loaded['transactions'] = load_transactions(spark)
        tables_loaded['cards'] = load_cards(spark)
        tables_loaded['loans'] = load_loans(spark)
        tables_loaded['contracts'] = load_contracts(spark)
        tables_loaded['client_products'] = load_client_products(spark)
        tables_loaded['credit_applications'] = load_credit_applications(spark)
        tables_loaded['account_balances'] = load_account_balances(spark)

        # Summary
        logger.info("="*80)
        logger.info("ETL Job Summary:")
        logger.info("-"*80)
        total_rows = 0
        for table, count in tables_loaded.items():
            logger.info(f"  {table:30s}: {count:>10,} rows")
            total_rows += count
        logger.info("-"*80)
        logger.info(f"  {'TOTAL':30s}: {total_rows:>10,} rows")
        logger.info("="*80)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"✅ ETL Job completed successfully in {duration:.2f} seconds")

    except Exception as e:
        logger.error("="*80)
        logger.error(f"❌ ETL Job failed with error: {str(e)}")
        logger.error("="*80)
        import traceback
        traceback.print_exc()
        exit_code = 1

    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
