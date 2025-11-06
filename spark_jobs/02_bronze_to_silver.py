#!/usr/bin/env python3
"""
ETL Job: Bronze -> Silver Layer
Description: Transform and clean data from bronze to silver layer with data quality scoring
Author: Data Engineering Team
Date: 2025-01-06
Version: 1.0
"""

import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, lower, concat, lit, when, substring, length,
    regexp_replace, current_timestamp, current_date, datediff, floor,
    round as spark_round, concat_ws, coalesce, abs as spark_abs, year, month
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
# Spark Session Configuration
# ============================================================================
def create_spark_session(app_name="Banking_ETL_Bronze_to_Silver"):
    """Create Spark session with Hive support and S3 configuration."""
    logger.info("Creating Spark session...")

    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.sql.hive.metastore.version", "3.1.3000") \
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
    return spark


# ============================================================================
# Data Quality Functions
# ============================================================================

def calculate_client_dq_score(df):
    """Calculate data quality score for clients."""
    return df.withColumn(
        "dq_score",
        spark_round(
            (when(col("first_name").isNotNull(), 0.10).otherwise(0) +
             when(col("last_name").isNotNull(), 0.10).otherwise(0) +
             when(col("email").isNotNull(), 0.15).otherwise(0) +
             when(col("phone").isNotNull(), 0.15).otherwise(0) +
             when(col("birth_date").isNotNull(), 0.10).otherwise(0) +
             when(col("address").isNotNull(), 0.10).otherwise(0) +
             when(col("city").isNotNull(), 0.10).otherwise(0) +
             when(col("credit_score").isNotNull(), 0.10).otherwise(0) +
             when(col("annual_income").isNotNull(), 0.10).otherwise(0)),
            2
        )
    ).withColumn(
        "dq_issues",
        concat_ws("; ",
            when(col("first_name").isNull(), lit("missing_first_name")).otherwise(lit("")),
            when(col("last_name").isNull(), lit("missing_last_name")).otherwise(lit("")),
            when(col("email").isNull(), lit("missing_email")).otherwise(lit("")),
            when(col("email").isNotNull() & ~col("email").like("%@%"), lit("invalid_email_format")).otherwise(lit("")),
            when(col("phone").isNull(), lit("missing_phone")).otherwise(lit("")),
            when(col("birth_date").isNull(), lit("missing_birth_date")).otherwise(lit("")),
            when(col("credit_score").isNull(), lit("missing_credit_score")).otherwise(lit("")),
            when((col("credit_score") < 300) | (col("credit_score") > 850), lit("invalid_credit_score")).otherwise(lit("")),
            when(col("annual_income").isNull(), lit("missing_annual_income")).otherwise(lit("")),
            when(col("annual_income") < 0, lit("negative_income")).otherwise(lit(""))
        )
    )


# ============================================================================
# ETL Functions
# ============================================================================

def transform_clients(spark):
    """Transform clients from bronze to silver."""
    logger.info("Transforming clients: bronze.clients -> silver.clients")

    # Use Spark SQL for complex transformations
    df = spark.sql("""
        SELECT
            -- Primary Key
            client_id,

            -- Original fields
            TRIM(first_name) AS first_name,
            TRIM(last_name) AS last_name,

            -- Derived: Full name
            CONCAT(TRIM(first_name), ' ', TRIM(last_name)) AS full_name,

            -- Email normalization
            LOWER(TRIM(email)) AS email,

            -- Email domain extraction
            CASE
                WHEN email IS NOT NULL AND email LIKE '%@%'
                THEN SUBSTRING(LOWER(email), LOCATE('@', LOWER(email)) + 1)
                ELSE NULL
            END AS email_domain,

            -- Phone (original)
            phone,

            -- Phone normalized (remove special characters)
            REGEXP_REPLACE(phone, '[^0-9+]', '') AS phone_normalized,

            birth_date,

            -- Age calculation
            CAST(FLOOR(DATEDIFF(CURRENT_DATE, birth_date) / 365.25) AS INT) AS age,

            registration_date,

            -- Address fields
            TRIM(address) AS address,
            TRIM(city) AS city,
            UPPER(TRIM(country)) AS country,

            -- Risk category
            UPPER(TRIM(risk_category)) AS risk_category,

            -- Credit score
            credit_score,

            -- Credit score category
            CASE
                WHEN credit_score IS NULL THEN 'UNKNOWN'
                WHEN credit_score < 580 THEN 'POOR'
                WHEN credit_score >= 580 AND credit_score < 670 THEN 'FAIR'
                WHEN credit_score >= 670 AND credit_score < 740 THEN 'GOOD'
                WHEN credit_score >= 740 AND credit_score < 800 THEN 'VERY_GOOD'
                WHEN credit_score >= 800 THEN 'EXCELLENT'
                ELSE 'UNKNOWN'
            END AS credit_score_category,

            -- Employment and income
            UPPER(TRIM(employment_status)) AS employment_status,
            annual_income,

            -- Income category
            CASE
                WHEN annual_income < 30000 THEN 'LOW'
                WHEN annual_income >= 30000 AND annual_income < 60000 THEN 'LOWER_MIDDLE'
                WHEN annual_income >= 60000 AND annual_income < 100000 THEN 'MIDDLE'
                WHEN annual_income >= 100000 AND annual_income < 150000 THEN 'UPPER_MIDDLE'
                WHEN annual_income >= 150000 THEN 'HIGH'
                ELSE 'UNKNOWN'
            END AS income_category,

            -- Is active (client has recent activity)
            CASE
                WHEN registration_date IS NOT NULL
                     AND DATEDIFF(CURRENT_DATE, registration_date) <= 365
                THEN TRUE
                ELSE FALSE
            END AS is_active,

            load_timestamp,

            YEAR(registration_date) AS registration_year

        FROM bronze.clients
        WHERE client_id IS NOT NULL
    """)

    # Calculate DQ score using Python function
    df = calculate_client_dq_score(df)

    # Add process metadata
    df = df.withColumn("process_timestamp", current_timestamp()) \
           .withColumn("source_system", lit("bronze.clients"))

    # Write to silver
    spark.sql("TRUNCATE TABLE silver.clients")
    df.write.mode("overwrite").format("hive").saveAsTable("silver.clients")

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM silver.clients").collect()[0]['cnt']
    logger.info(f"✅ Clients transformed: {row_count} rows")
    return row_count


def transform_products(spark):
    """Transform products from bronze to silver."""
    logger.info("Transforming products: bronze.products -> silver.products")

    spark.sql("TRUNCATE TABLE silver.products")

    spark.sql("""
        INSERT INTO TABLE silver.products
        SELECT
            product_id,
            TRIM(product_name) AS product_name,
            UPPER(TRIM(product_type)) AS product_type,
            UPPER(TRIM(product_category)) AS product_category,
            interest_rate,
            UPPER(TRIM(currency)) AS currency,
            active,
            load_timestamp,
            CURRENT_TIMESTAMP AS process_timestamp,
            'bronze.products' AS source_system
        FROM bronze.products
        WHERE product_id IS NOT NULL
    """)

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM silver.products").collect()[0]['cnt']
    logger.info(f"✅ Products transformed: {row_count} rows")
    return row_count


def transform_branches(spark):
    """Transform branches from bronze to silver."""
    logger.info("Transforming branches: bronze.branches -> silver.branches")

    spark.sql("TRUNCATE TABLE silver.branches")

    spark.sql("""
        INSERT INTO TABLE silver.branches
        SELECT
            branch_id,
            TRIM(branch_name) AS branch_name,
            UPPER(TRIM(branch_code)) AS branch_code,
            UPPER(TRIM(branch_type)) AS branch_type,
            TRIM(address) AS address,
            TRIM(city) AS city,
            UPPER(TRIM(region)) AS region,
            UPPER(TRIM(country)) AS country,
            phone,
            LOWER(TRIM(email)) AS email,
            manager_id,
            opening_date,
            load_timestamp,
            CURRENT_TIMESTAMP AS process_timestamp,
            'bronze.branches' AS source_system
        FROM bronze.branches
        WHERE branch_id IS NOT NULL
    """)

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM silver.branches").collect()[0]['cnt']
    logger.info(f"✅ Branches transformed: {row_count} rows")
    return row_count


def transform_employees(spark):
    """Transform employees from bronze to silver."""
    logger.info("Transforming employees: bronze.employees -> silver.employees")

    spark.sql("TRUNCATE TABLE silver.employees")

    spark.sql("""
        INSERT INTO TABLE silver.employees
        SELECT
            employee_id,
            TRIM(first_name) AS first_name,
            TRIM(last_name) AS last_name,
            CONCAT(TRIM(first_name), ' ', TRIM(last_name)) AS full_name,
            LOWER(TRIM(email)) AS email,
            phone,
            hire_date,
            CAST(FLOOR(DATEDIFF(CURRENT_DATE, hire_date) / 365.25) AS INT) AS tenure_years,
            UPPER(TRIM(job_title)) AS job_title,
            UPPER(TRIM(department)) AS department,
            branch_id,
            manager_id,
            salary,
            UPPER(TRIM(employment_status)) AS employment_status,
            CASE
                WHEN employment_status = 'ACTIVE' THEN TRUE
                ELSE FALSE
            END AS is_active,
            load_timestamp,
            CURRENT_TIMESTAMP AS process_timestamp,
            'bronze.employees' AS source_system
        FROM bronze.employees
        WHERE employee_id IS NOT NULL
    """)

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM silver.employees").collect()[0]['cnt']
    logger.info(f"✅ Employees transformed: {row_count} rows")
    return row_count


def transform_accounts(spark):
    """Transform accounts from bronze to silver."""
    logger.info("Transforming accounts: bronze.accounts -> silver.accounts")

    spark.sql("TRUNCATE TABLE silver.accounts")

    spark.sql("""
        INSERT INTO TABLE silver.accounts
        SELECT
            account_id,
            client_id,
            UPPER(TRIM(account_number)) AS account_number,
            UPPER(TRIM(account_type)) AS account_type,
            product_id,
            branch_id,
            opening_date,
            UPPER(TRIM(account_status)) AS account_status,
            current_balance,
            UPPER(TRIM(currency)) AS currency,
            COALESCE(overdraft_limit, 0) AS overdraft_limit,
            interest_rate,
            CAST(FLOOR(DATEDIFF(CURRENT_DATE, opening_date) / 365.25) AS INT) AS account_age_years,
            CASE
                WHEN account_status = 'ACTIVE' THEN TRUE
                ELSE FALSE
            END AS is_active,
            load_timestamp,
            CURRENT_TIMESTAMP AS process_timestamp,
            'bronze.accounts' AS source_system
        FROM bronze.accounts
        WHERE account_id IS NOT NULL
    """)

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM silver.accounts").collect()[0]['cnt']
    logger.info(f"✅ Accounts transformed: {row_count} rows")
    return row_count


def transform_transactions(spark):
    """Transform transactions from bronze to silver with partitioning."""
    logger.info("Transforming transactions: bronze.transactions -> silver.transactions")

    spark.sql("TRUNCATE TABLE silver.transactions")

    spark.sql("""
        INSERT INTO TABLE silver.transactions
        PARTITION(transaction_year, transaction_month)
        SELECT
            transaction_id,
            account_id,
            transaction_date,
            UPPER(TRIM(transaction_type)) AS transaction_type,
            amount,
            ABS(amount) AS amount_abs,
            CASE
                WHEN amount > 0 THEN 'CREDIT'
                WHEN amount < 0 THEN 'DEBIT'
                ELSE 'NEUTRAL'
            END AS transaction_direction,
            UPPER(TRIM(currency)) AS currency,
            balance_after,
            TRIM(description) AS description,
            UPPER(TRIM(channel)) AS channel,
            UPPER(TRIM(transaction_status)) AS transaction_status,
            CASE
                WHEN transaction_status = 'COMPLETED' THEN TRUE
                ELSE FALSE
            END AS is_completed,
            HOUR(transaction_date) AS transaction_hour,
            DAYOFWEEK(transaction_date) AS transaction_day_of_week,
            load_timestamp,
            CURRENT_TIMESTAMP AS process_timestamp,
            'bronze.transactions' AS source_system,
            transaction_year,
            transaction_month
        FROM bronze.transactions
        WHERE transaction_id IS NOT NULL
    """)

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM silver.transactions").collect()[0]['cnt']
    logger.info(f"✅ Transactions transformed: {row_count} rows")
    return row_count


def transform_cards(spark):
    """Transform cards from bronze to silver."""
    logger.info("Transforming cards: bronze.cards -> silver.cards")

    spark.sql("TRUNCATE TABLE silver.cards")

    spark.sql("""
        INSERT INTO TABLE silver.cards
        SELECT
            card_id,
            account_id,
            card_number,
            UPPER(TRIM(card_type)) AS card_type,
            UPPER(TRIM(card_status)) AS card_status,
            issue_date,
            expiry_date,
            DATEDIFF(expiry_date, CURRENT_DATE) AS days_until_expiry,
            CASE
                WHEN DATEDIFF(expiry_date, CURRENT_DATE) <= 30 THEN TRUE
                ELSE FALSE
            END AS is_expiring_soon,
            credit_limit,
            available_credit,
            cvv,
            pin_set,
            CASE
                WHEN card_status = 'ACTIVE' THEN TRUE
                ELSE FALSE
            END AS is_active,
            load_timestamp,
            CURRENT_TIMESTAMP AS process_timestamp,
            'bronze.cards' AS source_system
        FROM bronze.cards
        WHERE card_id IS NOT NULL
    """)

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM silver.cards").collect()[0]['cnt']
    logger.info(f"✅ Cards transformed: {row_count} rows")
    return row_count


def transform_loans(spark):
    """Transform loans from bronze to silver."""
    logger.info("Transforming loans: bronze.loans -> silver.loans")

    spark.sql("TRUNCATE TABLE silver.loans")

    spark.sql("""
        INSERT INTO TABLE silver.loans
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
            UPPER(TRIM(loan_status)) AS loan_status,
            outstanding_balance,
            monthly_payment,
            UPPER(TRIM(currency)) AS currency,
            DATEDIFF(end_date, start_date) AS loan_duration_days,
            CAST(FLOOR(DATEDIFF(CURRENT_DATE, start_date) / 30.44) AS INT) AS months_since_start,
            DATEDIFF(end_date, CURRENT_DATE) AS days_until_maturity,
            ROUND((outstanding_balance / loan_amount) * 100, 2) AS outstanding_percentage,
            CASE
                WHEN loan_status = 'ACTIVE' THEN TRUE
                ELSE FALSE
            END AS is_active,
            load_timestamp,
            CURRENT_TIMESTAMP AS process_timestamp,
            'bronze.loans' AS source_system
        FROM bronze.loans
        WHERE loan_id IS NOT NULL
    """)

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM silver.loans").collect()[0]['cnt']
    logger.info(f"✅ Loans transformed: {row_count} rows")
    return row_count


def transform_contracts(spark):
    """Transform contracts from bronze to silver."""
    logger.info("Transforming contracts: bronze.contracts -> silver.contracts")

    spark.sql("TRUNCATE TABLE silver.contracts")

    spark.sql("""
        INSERT INTO TABLE silver.contracts
        SELECT
            contract_id,
            client_id,
            product_id,
            UPPER(TRIM(contract_number)) AS contract_number,
            UPPER(TRIM(contract_type)) AS contract_type,
            start_date,
            end_date,
            contract_amount,
            UPPER(TRIM(contract_status)) AS contract_status,
            signed_date,
            branch_id,
            DATEDIFF(end_date, start_date) AS contract_duration_days,
            DATEDIFF(end_date, CURRENT_DATE) AS days_until_expiry,
            CASE
                WHEN contract_status = 'ACTIVE' THEN TRUE
                ELSE FALSE
            END AS is_active,
            load_timestamp,
            CURRENT_TIMESTAMP AS process_timestamp,
            'bronze.contracts' AS source_system
        FROM bronze.contracts
        WHERE contract_id IS NOT NULL
    """)

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM silver.contracts").collect()[0]['cnt']
    logger.info(f"✅ Contracts transformed: {row_count} rows")
    return row_count


def transform_client_products(spark):
    """Transform client_products from bronze to silver."""
    logger.info("Transforming client_products: bronze.client_products -> silver.client_products")

    spark.sql("TRUNCATE TABLE silver.client_products")

    spark.sql("""
        INSERT INTO TABLE silver.client_products
        SELECT
            client_product_id,
            client_id,
            product_id,
            subscription_date,
            UPPER(TRIM(subscription_status)) AS subscription_status,
            last_usage_date,
            DATEDIFF(CURRENT_DATE, last_usage_date) AS days_since_last_usage,
            CASE
                WHEN subscription_status = 'ACTIVE' THEN TRUE
                ELSE FALSE
            END AS is_active,
            load_timestamp,
            CURRENT_TIMESTAMP AS process_timestamp,
            'bronze.client_products' AS source_system
        FROM bronze.client_products
        WHERE client_product_id IS NOT NULL
    """)

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM silver.client_products").collect()[0]['cnt']
    logger.info(f"✅ Client Products transformed: {row_count} rows")
    return row_count


def transform_credit_applications(spark):
    """Transform credit_applications from bronze to silver."""
    logger.info("Transforming credit_applications: bronze.credit_applications -> silver.credit_applications")

    spark.sql("TRUNCATE TABLE silver.credit_applications")

    spark.sql("""
        INSERT INTO TABLE silver.credit_applications
        SELECT
            application_id,
            client_id,
            product_id,
            application_date,
            requested_amount,
            UPPER(TRIM(application_status)) AS application_status,
            decision_date,
            approved_amount,
            interest_rate,
            TRIM(rejection_reason) AS rejection_reason,
            DATEDIFF(decision_date, application_date) AS days_to_decision,
            CASE
                WHEN approved_amount IS NOT NULL THEN
                    ROUND((approved_amount / requested_amount) * 100, 2)
                ELSE NULL
            END AS approval_percentage,
            CASE
                WHEN application_status = 'APPROVED' THEN TRUE
                ELSE FALSE
            END AS is_approved,
            load_timestamp,
            CURRENT_TIMESTAMP AS process_timestamp,
            'bronze.credit_applications' AS source_system
        FROM bronze.credit_applications
        WHERE application_id IS NOT NULL
    """)

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM silver.credit_applications").collect()[0]['cnt']
    logger.info(f"✅ Credit Applications transformed: {row_count} rows")
    return row_count


def transform_account_balances(spark):
    """Transform account_balances from bronze to silver with partitioning."""
    logger.info("Transforming account_balances: bronze.account_balances -> silver.account_balances")

    spark.sql("TRUNCATE TABLE silver.account_balances")

    spark.sql("""
        INSERT INTO TABLE silver.account_balances
        PARTITION(balance_year, balance_month)
        SELECT
            balance_id,
            account_id,
            balance_date,
            balance_amount,
            available_balance,
            UPPER(TRIM(currency)) AS currency,
            ROUND((available_balance / NULLIF(balance_amount, 0)) * 100, 2) AS available_percentage,
            load_timestamp,
            CURRENT_TIMESTAMP AS process_timestamp,
            'bronze.account_balances' AS source_system,
            balance_year,
            balance_month
        FROM bronze.account_balances
        WHERE balance_id IS NOT NULL
    """)

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM silver.account_balances").collect()[0]['cnt']
    logger.info(f"✅ Account Balances transformed: {row_count} rows")
    return row_count


# ============================================================================
# Main ETL Pipeline
# ============================================================================

def main():
    """Main ETL pipeline execution."""
    start_time = datetime.now()
    logger.info("="*80)
    logger.info("Starting ETL Job: Bronze -> Silver Layer")
    logger.info("="*80)

    spark = None
    exit_code = 0

    try:
        # Create Spark session
        spark = create_spark_session()

        # Transform all tables
        tables_transformed = {}

        tables_transformed['clients'] = transform_clients(spark)
        tables_transformed['products'] = transform_products(spark)
        tables_transformed['branches'] = transform_branches(spark)
        tables_transformed['employees'] = transform_employees(spark)
        tables_transformed['accounts'] = transform_accounts(spark)
        tables_transformed['transactions'] = transform_transactions(spark)
        tables_transformed['cards'] = transform_cards(spark)
        tables_transformed['loans'] = transform_loans(spark)
        tables_transformed['contracts'] = transform_contracts(spark)
        tables_transformed['client_products'] = transform_client_products(spark)
        tables_transformed['credit_applications'] = transform_credit_applications(spark)
        tables_transformed['account_balances'] = transform_account_balances(spark)

        # Summary
        logger.info("="*80)
        logger.info("ETL Job Summary:")
        logger.info("-"*80)
        total_rows = 0
        for table, count in tables_transformed.items():
            logger.info(f"  {table:30s}: {count:>10,} rows")
            total_rows += count
        logger.info("-"*80)
        logger.info(f"  {'TOTAL':30s}: {total_rows:>10,} rows")
        logger.info("="*80)

        # Check data quality
        avg_dq_score = spark.sql("SELECT ROUND(AVG(dq_score), 2) as avg_score FROM silver.clients").collect()[0]['avg_score']
        logger.info(f"Average Client Data Quality Score: {avg_dq_score}")

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
