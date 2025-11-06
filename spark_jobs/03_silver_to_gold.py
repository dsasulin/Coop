#!/usr/bin/env python3
"""
ETL Job: Silver -> Gold Layer
Description: Build dimensions, facts, and analytical data marts from silver layer
Author: Data Engineering Team
Date: 2025-01-06
Version: 1.0

This script creates:
- Dimension tables (dim_client, dim_product, dim_branch, dim_date, dim_account)
- Fact tables (fact_transaction, fact_account_balance)
- Aggregate tables (client_summary, product_performance, branch_performance)
"""

import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession

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
def create_spark_session(app_name="Banking_ETL_Silver_to_Gold"):
    """Create Spark session with Hive support and S3 configuration."""
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
    return spark


# ============================================================================
# Dimension Tables
# ============================================================================

def build_dim_client(spark):
    """Build client dimension table."""
    logger.info("Building dimension: gold.dim_client")

    spark.sql("TRUNCATE TABLE gold.dim_client")

    spark.sql("""
        INSERT INTO TABLE gold.dim_client
        SELECT
            -- Surrogate key
            c.client_id as client_key,
            c.client_id,

            -- Demographics
            c.first_name,
            c.last_name,
            c.full_name,
            c.email,
            c.email_domain,
            c.phone,
            c.birth_date,
            c.age,

            -- Age group
            CASE
                WHEN c.age < 25 THEN '18-24'
                WHEN c.age < 35 THEN '25-34'
                WHEN c.age < 45 THEN '35-44'
                WHEN c.age < 55 THEN '45-54'
                WHEN c.age < 65 THEN '55-64'
                ELSE '65+'
            END as age_group,

            c.registration_date,
            DATEDIFF(CURRENT_DATE, c.registration_date) as customer_tenure_days,
            ROUND(DATEDIFF(CURRENT_DATE, c.registration_date) / 365.25, 2) as customer_tenure_years,

            -- Location
            c.address,
            c.city,
            c.country,

            -- Region
            CASE
                WHEN c.country = 'US' THEN 'NORTH_AMERICA'
                WHEN c.country IN ('UK', 'DE', 'FR', 'IT', 'ES') THEN 'EUROPE'
                ELSE 'OTHER'
            END as region,

            -- Financial profile
            c.risk_category,
            c.credit_score,
            c.credit_score_category,
            c.employment_status,
            c.annual_income,
            c.income_category,

            -- Client segment
            CASE
                WHEN c.annual_income > 150000 AND c.credit_score > 750 THEN 'VIP'
                WHEN c.annual_income > 100000 AND c.credit_score > 700 THEN 'PREMIUM'
                WHEN c.annual_income > 50000 AND c.credit_score > 650 THEN 'REGULAR'
                ELSE 'BASIC'
            END as client_segment,

            -- Client lifetime value (placeholder)
            0.0 as client_lifetime_value,

            -- Account summary
            COALESCE(acct.total_accounts, 0) as total_accounts,
            COALESCE(acct.total_active_accounts, 0) as total_active_accounts,

            -- Product summary
            COALESCE(prod.total_products, 0) as total_products,

            -- Contracts summary
            COALESCE(cont.total_contracts, 0) as total_contracts,

            -- Cards summary
            COALESCE(cards.total_cards, 0) as total_cards,

            -- Loans summary
            COALESCE(loans.total_loans, 0) as total_loans,

            -- SCD Type 2 fields
            c.registration_date as effective_date,
            CAST(NULL AS DATE) as end_date,
            TRUE as is_current,

            -- Technical fields
            CURRENT_TIMESTAMP as created_timestamp,
            CURRENT_TIMESTAMP as updated_timestamp

        FROM silver.clients c

        LEFT JOIN (
            SELECT client_id,
                   COUNT(*) as total_accounts,
                   SUM(CASE WHEN is_active THEN 1 ELSE 0 END) as total_active_accounts
            FROM silver.accounts
            GROUP BY client_id
        ) acct ON c.client_id = acct.client_id

        LEFT JOIN (
            SELECT client_id, COUNT(DISTINCT product_id) as total_products
            FROM silver.client_products
            GROUP BY client_id
        ) prod ON c.client_id = prod.client_id

        LEFT JOIN (
            SELECT client_id, COUNT(*) as total_contracts
            FROM silver.contracts
            GROUP BY client_id
        ) cont ON c.client_id = cont.client_id

        LEFT JOIN (
            SELECT a.client_id, COUNT(c.card_id) as total_cards
            FROM silver.cards c
            INNER JOIN silver.accounts a ON c.account_id = a.account_id
            GROUP BY a.client_id
        ) cards ON c.client_id = cards.client_id

        LEFT JOIN (
            SELECT client_id, COUNT(*) as total_loans
            FROM silver.loans
            GROUP BY client_id
        ) loans ON c.client_id = loans.client_id
    """)

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM gold.dim_client").collect()[0]['cnt']
    logger.info(f"✅ dim_client built: {row_count} rows")
    return row_count


def build_dim_product(spark):
    """Build product dimension table."""
    logger.info("Building dimension: gold.dim_product")

    spark.sql("TRUNCATE TABLE gold.dim_product")

    spark.sql("""
        INSERT INTO TABLE gold.dim_product
        SELECT
            p.product_id as product_key,
            p.product_id,
            p.product_name,
            p.product_type,
            p.product_category,
            p.currency,
            p.active,
            COALESCE(cp.total_clients, 0) as total_clients,
            COALESCE(cont.total_contracts, 0) as total_contracts,
            COALESCE(cont.total_revenue, 0) as total_revenue,
            CURRENT_DATE as effective_date,
            CAST(NULL AS DATE) as end_date,
            p.active as is_current,
            CURRENT_TIMESTAMP as created_timestamp,
            CURRENT_TIMESTAMP as updated_timestamp
        FROM silver.products p
        LEFT JOIN (
            SELECT product_id, COUNT(DISTINCT client_id) as total_clients
            FROM silver.client_products
            GROUP BY product_id
        ) cp ON p.product_id = cp.product_id
        LEFT JOIN (
            SELECT product_id, COUNT(*) as total_contracts,
                   SUM(contract_amount) as total_revenue
            FROM silver.contracts
            GROUP BY product_id
        ) cont ON p.product_id = cont.product_id
    """)

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM gold.dim_product").collect()[0]['cnt']
    logger.info(f"✅ dim_product built: {row_count} rows")
    return row_count


def build_dim_branch(spark):
    """Build branch dimension table."""
    logger.info("Building dimension: gold.dim_branch")

    spark.sql("TRUNCATE TABLE gold.dim_branch")

    spark.sql("""
        INSERT INTO TABLE gold.dim_branch
        SELECT
            b.branch_id as branch_key,
            b.branch_id,
            b.branch_name,
            b.branch_code,
            b.branch_type,
            b.address,
            b.city,
            b.region,
            b.country,
            b.phone,
            b.email,
            b.manager_id,
            b.opening_date,
            COALESCE(emp.total_employees, 0) as total_employees,
            COALESCE(acct.total_accounts, 0) as total_accounts,
            COALESCE(ln.total_loans, 0) as total_loans,
            COALESCE(cont.total_contracts, 0) as total_contracts,
            CURRENT_DATE as effective_date,
            CAST(NULL AS DATE) as end_date,
            TRUE as is_current,
            CURRENT_TIMESTAMP as created_timestamp,
            CURRENT_TIMESTAMP as updated_timestamp
        FROM silver.branches b
        LEFT JOIN (
            SELECT branch_id, COUNT(*) as total_employees
            FROM silver.employees
            GROUP BY branch_id
        ) emp ON b.branch_id = emp.branch_id
        LEFT JOIN (
            SELECT branch_id, COUNT(*) as total_accounts
            FROM silver.accounts
            GROUP BY branch_id
        ) acct ON b.branch_id = acct.branch_id
        LEFT JOIN (
            SELECT branch_id, COUNT(*) as total_loans
            FROM silver.loans
            GROUP BY branch_id
        ) ln ON b.branch_id = ln.branch_id
        LEFT JOIN (
            SELECT branch_id, COUNT(*) as total_contracts
            FROM silver.contracts
            GROUP BY branch_id
        ) cont ON b.branch_id = cont.branch_id
    """)

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM gold.dim_branch").collect()[0]['cnt']
    logger.info(f"✅ dim_branch built: {row_count} rows")
    return row_count


def build_dim_date(spark):
    """Build date dimension table."""
    logger.info("Building dimension: gold.dim_date")

    spark.sql("TRUNCATE TABLE gold.dim_date")

    # This is a simplified version - full dim_date would have more fields
    spark.sql("""
        INSERT INTO TABLE gold.dim_date
        SELECT DISTINCT
            CAST(DATE_FORMAT(transaction_date, 'yyyyMMdd') AS INT) as date_key,
            transaction_date as full_date,
            YEAR(transaction_date) as year,
            MONTH(transaction_date) as month,
            DAY(transaction_date) as day,
            QUARTER(transaction_date) as quarter,
            DAYOFWEEK(transaction_date) as day_of_week,
            CASE DAYOFWEEK(transaction_date)
                WHEN 1 THEN 'Sunday'
                WHEN 2 THEN 'Monday'
                WHEN 3 THEN 'Tuesday'
                WHEN 4 THEN 'Wednesday'
                WHEN 5 THEN 'Thursday'
                WHEN 6 THEN 'Friday'
                WHEN 7 THEN 'Saturday'
            END as day_name,
            DATE_FORMAT(transaction_date, 'MMMM') as month_name,
            CASE
                WHEN DAYOFWEEK(transaction_date) IN (1, 7) THEN TRUE
                ELSE FALSE
            END as is_weekend,
            FALSE as is_holiday,
            '' as holiday_name
        FROM silver.transactions
    """)

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM gold.dim_date").collect()[0]['cnt']
    logger.info(f"✅ dim_date built: {row_count} rows")
    return row_count


def build_dim_account(spark):
    """Build account dimension table."""
    logger.info("Building dimension: gold.dim_account")

    spark.sql("TRUNCATE TABLE gold.dim_account")

    spark.sql("""
        INSERT INTO TABLE gold.dim_account
        SELECT
            a.account_id as account_key,
            a.account_id,
            a.account_number,
            a.account_type,
            a.client_id,
            a.product_id,
            a.branch_id,
            a.opening_date,
            a.account_status,
            a.current_balance,
            a.currency,
            a.overdraft_limit,
            a.interest_rate,
            a.account_age_years,
            COALESCE(trans.total_transactions, 0) as total_transactions,
            COALESCE(cards.total_cards, 0) as total_cards,
            CURRENT_DATE as effective_date,
            CAST(NULL AS DATE) as end_date,
            a.is_active as is_current,
            CURRENT_TIMESTAMP as created_timestamp,
            CURRENT_TIMESTAMP as updated_timestamp
        FROM silver.accounts a
        LEFT JOIN (
            SELECT account_id, COUNT(*) as total_transactions
            FROM silver.transactions
            GROUP BY account_id
        ) trans ON a.account_id = trans.account_id
        LEFT JOIN (
            SELECT account_id, COUNT(*) as total_cards
            FROM silver.cards
            GROUP BY account_id
        ) cards ON a.account_id = cards.account_id
    """)

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM gold.dim_account").collect()[0]['cnt']
    logger.info(f"✅ dim_account built: {row_count} rows")
    return row_count


# ============================================================================
# Fact Tables
# ============================================================================

def build_fact_transaction(spark):
    """Build transaction fact table with partitioning."""
    logger.info("Building fact: gold.fact_transaction")

    spark.sql("TRUNCATE TABLE gold.fact_transaction")

    spark.sql("""
        INSERT INTO TABLE gold.fact_transaction
        PARTITION(transaction_year, transaction_month)
        SELECT
            t.transaction_id,
            CAST(DATE_FORMAT(t.transaction_date, 'yyyyMMdd') AS INT) as date_key,
            t.account_id as account_key,
            a.client_id as client_key,
            a.product_id as product_key,
            a.branch_id as branch_key,
            t.transaction_date,
            t.transaction_type,
            t.amount,
            t.amount_abs,
            t.transaction_direction,
            t.currency,
            t.balance_after,
            t.channel,
            t.transaction_status,
            t.transaction_hour,
            t.transaction_day_of_week,
            t.is_completed,
            t.process_timestamp,
            t.transaction_year,
            t.transaction_month
        FROM silver.transactions t
        INNER JOIN silver.accounts a ON t.account_id = a.account_id
    """)

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM gold.fact_transaction").collect()[0]['cnt']
    logger.info(f"✅ fact_transaction built: {row_count} rows")
    return row_count


def build_fact_account_balance(spark):
    """Build account balance fact table with partitioning."""
    logger.info("Building fact: gold.fact_account_balance")

    spark.sql("TRUNCATE TABLE gold.fact_account_balance")

    spark.sql("""
        INSERT INTO TABLE gold.fact_account_balance
        PARTITION(balance_year, balance_month)
        SELECT
            ab.balance_id,
            CAST(DATE_FORMAT(ab.balance_date, 'yyyyMMdd') AS INT) as date_key,
            ab.account_id as account_key,
            a.client_id as client_key,
            a.product_id as product_key,
            a.branch_id as branch_key,
            ab.balance_date,
            ab.balance_amount,
            ab.available_balance,
            ab.currency,
            ab.available_percentage,
            ab.process_timestamp,
            ab.balance_year,
            ab.balance_month
        FROM silver.account_balances ab
        INNER JOIN silver.accounts a ON ab.account_id = a.account_id
    """)

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM gold.fact_account_balance").collect()[0]['cnt']
    logger.info(f"✅ fact_account_balance built: {row_count} rows")
    return row_count


# ============================================================================
# Aggregate Tables
# ============================================================================

def build_client_summary(spark):
    """Build client summary aggregate table."""
    logger.info("Building aggregate: gold.client_summary")

    spark.sql("TRUNCATE TABLE gold.client_summary")

    spark.sql("""
        INSERT INTO TABLE gold.client_summary
        SELECT
            c.client_id,
            c.full_name,
            c.client_segment,
            COALESCE(acct.total_accounts, 0) as total_accounts,
            COALESCE(acct.total_balance, 0) as total_balance,
            COALESCE(trans.total_transactions, 0) as total_transactions,
            COALESCE(trans.total_transaction_volume, 0) as total_transaction_volume,
            COALESCE(cards.total_cards, 0) as total_cards,
            COALESCE(loans.total_loans, 0) as total_loans,
            COALESCE(loans.total_loan_amount, 0) as total_loan_amount,
            c.risk_category,
            c.credit_score,
            CURRENT_DATE as summary_date,
            CURRENT_TIMESTAMP as created_timestamp
        FROM silver.clients c
        LEFT JOIN (
            SELECT client_id, COUNT(*) as total_accounts,
                   SUM(current_balance) as total_balance
            FROM silver.accounts
            GROUP BY client_id
        ) acct ON c.client_id = acct.client_id
        LEFT JOIN (
            SELECT a.client_id, COUNT(*) as total_transactions,
                   SUM(ABS(t.amount)) as total_transaction_volume
            FROM silver.transactions t
            INNER JOIN silver.accounts a ON t.account_id = a.account_id
            GROUP BY a.client_id
        ) trans ON c.client_id = trans.client_id
        LEFT JOIN (
            SELECT a.client_id, COUNT(*) as total_cards
            FROM silver.cards c
            INNER JOIN silver.accounts a ON c.account_id = a.account_id
            GROUP BY a.client_id
        ) cards ON c.client_id = cards.client_id
        LEFT JOIN (
            SELECT client_id, COUNT(*) as total_loans,
                   SUM(loan_amount) as total_loan_amount
            FROM silver.loans
            GROUP BY client_id
        ) loans ON c.client_id = loans.client_id
    """)

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM gold.client_summary").collect()[0]['cnt']
    logger.info(f"✅ client_summary built: {row_count} rows")
    return row_count


def build_product_performance(spark):
    """Build product performance aggregate table."""
    logger.info("Building aggregate: gold.product_performance")

    spark.sql("TRUNCATE TABLE gold.product_performance")

    spark.sql("""
        INSERT INTO TABLE gold.product_performance
        SELECT
            p.product_id,
            p.product_name,
            p.product_type,
            p.product_category,
            COALESCE(cp.total_clients, 0) as total_clients,
            COALESCE(acct.total_accounts, 0) as total_accounts,
            COALESCE(acct.total_balance, 0) as total_balance,
            COALESCE(cont.total_contracts, 0) as total_contracts,
            COALESCE(cont.total_contract_value, 0) as total_contract_value,
            COALESCE(loans.total_loans, 0) as total_loans,
            COALESCE(loans.total_loan_value, 0) as total_loan_value,
            p.active as is_active,
            CURRENT_DATE as performance_date,
            CURRENT_TIMESTAMP as created_timestamp
        FROM silver.products p
        LEFT JOIN (
            SELECT product_id, COUNT(DISTINCT client_id) as total_clients
            FROM silver.client_products
            GROUP BY product_id
        ) cp ON p.product_id = cp.product_id
        LEFT JOIN (
            SELECT product_id, COUNT(*) as total_accounts,
                   SUM(current_balance) as total_balance
            FROM silver.accounts
            GROUP BY product_id
        ) acct ON p.product_id = acct.product_id
        LEFT JOIN (
            SELECT product_id, COUNT(*) as total_contracts,
                   SUM(contract_amount) as total_contract_value
            FROM silver.contracts
            GROUP BY product_id
        ) cont ON p.product_id = cont.product_id
        LEFT JOIN (
            SELECT product_id, COUNT(*) as total_loans,
                   SUM(loan_amount) as total_loan_value
            FROM silver.loans
            GROUP BY product_id
        ) loans ON p.product_id = loans.product_id
    """)

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM gold.product_performance").collect()[0]['cnt']
    logger.info(f"✅ product_performance built: {row_count} rows")
    return row_count


def build_branch_performance(spark):
    """Build branch performance aggregate table."""
    logger.info("Building aggregate: gold.branch_performance")

    spark.sql("TRUNCATE TABLE gold.branch_performance")

    spark.sql("""
        INSERT INTO TABLE gold.branch_performance
        SELECT
            b.branch_id,
            b.branch_name,
            b.branch_type,
            b.city,
            b.region,
            COALESCE(emp.total_employees, 0) as total_employees,
            COALESCE(emp.total_active_employees, 0) as total_active_employees,
            COALESCE(acct.total_accounts, 0) as total_accounts,
            COALESCE(acct.total_balance, 0) as total_balance,
            COALESCE(loans.total_loans, 0) as total_loans,
            COALESCE(loans.total_loan_value, 0) as total_loan_value,
            COALESCE(cont.total_contracts, 0) as total_contracts,
            COALESCE(cont.total_contract_value, 0) as total_contract_value,
            CURRENT_DATE as performance_date,
            CURRENT_TIMESTAMP as created_timestamp
        FROM silver.branches b
        LEFT JOIN (
            SELECT branch_id, COUNT(*) as total_employees,
                   SUM(CASE WHEN is_active THEN 1 ELSE 0 END) as total_active_employees
            FROM silver.employees
            GROUP BY branch_id
        ) emp ON b.branch_id = emp.branch_id
        LEFT JOIN (
            SELECT branch_id, COUNT(*) as total_accounts,
                   SUM(current_balance) as total_balance
            FROM silver.accounts
            GROUP BY branch_id
        ) acct ON b.branch_id = acct.branch_id
        LEFT JOIN (
            SELECT branch_id, COUNT(*) as total_loans,
                   SUM(loan_amount) as total_loan_value
            FROM silver.loans
            GROUP BY branch_id
        ) loans ON b.branch_id = loans.branch_id
        LEFT JOIN (
            SELECT branch_id, COUNT(*) as total_contracts,
                   SUM(contract_amount) as total_contract_value
            FROM silver.contracts
            GROUP BY branch_id
        ) cont ON b.branch_id = cont.branch_id
    """)

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM gold.branch_performance").collect()[0]['cnt']
    logger.info(f"✅ branch_performance built: {row_count} rows")
    return row_count


# ============================================================================
# Main ETL Pipeline
# ============================================================================

def main():
    """Main ETL pipeline execution."""
    start_time = datetime.now()
    logger.info("="*80)
    logger.info("Starting ETL Job: Silver -> Gold Layer")
    logger.info("="*80)

    spark = None
    exit_code = 0

    try:
        # Create Spark session
        spark = create_spark_session()

        # Build all tables
        tables_built = {}

        # Dimensions
        logger.info("\n" + "="*80)
        logger.info("Building Dimension Tables")
        logger.info("="*80)
        tables_built['dim_client'] = build_dim_client(spark)
        tables_built['dim_product'] = build_dim_product(spark)
        tables_built['dim_branch'] = build_dim_branch(spark)
        tables_built['dim_date'] = build_dim_date(spark)
        tables_built['dim_account'] = build_dim_account(spark)

        # Facts
        logger.info("\n" + "="*80)
        logger.info("Building Fact Tables")
        logger.info("="*80)
        tables_built['fact_transaction'] = build_fact_transaction(spark)
        tables_built['fact_account_balance'] = build_fact_account_balance(spark)

        # Aggregates
        logger.info("\n" + "="*80)
        logger.info("Building Aggregate Tables")
        logger.info("="*80)
        tables_built['client_summary'] = build_client_summary(spark)
        tables_built['product_performance'] = build_product_performance(spark)
        tables_built['branch_performance'] = build_branch_performance(spark)

        # Summary
        logger.info("\n" + "="*80)
        logger.info("ETL Job Summary:")
        logger.info("="*80)
        logger.info("Dimensions:")
        for table in ['dim_client', 'dim_product', 'dim_branch', 'dim_date', 'dim_account']:
            logger.info(f"  {table:30s}: {tables_built[table]:>10,} rows")
        logger.info("\nFacts:")
        for table in ['fact_transaction', 'fact_account_balance']:
            logger.info(f"  {table:30s}: {tables_built[table]:>10,} rows")
        logger.info("\nAggregates:")
        for table in ['client_summary', 'product_performance', 'branch_performance']:
            logger.info(f"  {table:30s}: {tables_built[table]:>10,} rows")
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
