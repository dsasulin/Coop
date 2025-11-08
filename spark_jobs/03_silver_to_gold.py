#!/usr/bin/env python3
"""
ETL Job: Silver -> Gold Layer
Description: Build dimensions, facts, and analytical data marts from silver layer
Author: Data Engineering Team
Date: 2025-01-06
Version: 2.0 (Updated to match working SQL script)

This script creates:
- Dimension tables (dim_client, dim_product, dim_branch, dim_date)
- Fact tables (fact_transactions_daily, fact_account_balance_daily, fact_loan_performance)
- Aggregate tables (client_360_view, product_performance_summary, branch_performance_dashboard)
"""

import sys
import logging
from datetime import datetime, date, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, current_date, year, month, date_format,
    count, sum as _sum, avg, min as _min, max as _max, round as spark_round,
    when, case, coalesce, datediff, concat, weekofyear, dayofmonth, dayofweek, quarter
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
    """Build client dimension table with 360-degree view."""
    logger.info("Building dimension: gold.dim_client")

    spark.sql("TRUNCATE TABLE gold.dim_client")

    # Use SQL for complex aggregations - matches working SQL script
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
            SELECT
                client_id,
                COUNT(*) as total_accounts,
                SUM(CASE WHEN is_active THEN 1 ELSE 0 END) as total_active_accounts
            FROM silver.accounts
            GROUP BY client_id
        ) acct ON c.client_id = acct.client_id

        LEFT JOIN (
            SELECT
                client_id,
                COUNT(DISTINCT product_id) as total_products
            FROM silver.client_products
            WHERE is_active = TRUE
            GROUP BY client_id
        ) prod ON c.client_id = prod.client_id

        LEFT JOIN (
            SELECT
                client_id,
                COUNT(*) as total_contracts
            FROM silver.contracts
            GROUP BY client_id
        ) cont ON c.client_id = cont.client_id

        LEFT JOIN (
            SELECT
                client_id,
                COUNT(*) as total_cards
            FROM silver.cards
            GROUP BY client_id
        ) cards ON c.client_id = cards.client_id

        LEFT JOIN (
            SELECT
                cnt.client_id,
                COUNT(*) as total_loans
            FROM silver.loans l
            INNER JOIN silver.contracts cnt ON l.contract_id = cnt.contract_id
            GROUP BY cnt.client_id
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

            -- Aggregated metrics
            COALESCE(cp.total_clients, 0) as total_clients,
            COALESCE(cont.total_contracts, 0) as total_contracts,
            COALESCE(cont.total_revenue, 0) as total_revenue,

            -- SCD fields
            CURRENT_DATE as effective_date,
            CAST(NULL AS DATE) as end_date,
            p.active as is_current,

            CURRENT_TIMESTAMP as created_timestamp,
            CURRENT_TIMESTAMP as updated_timestamp

        FROM silver.products p

        LEFT JOIN (
            SELECT
                product_id,
                COUNT(DISTINCT client_id) as total_clients
            FROM silver.client_products
            GROUP BY product_id
        ) cp ON p.product_id = cp.product_id

        LEFT JOIN (
            SELECT
                product_id,
                COUNT(*) as total_contracts,
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
            b.branch_code,
            b.branch_name,
            b.address,
            b.city,
            b.state,
            b.state_code,
            b.zip_code,

            -- Region
            CASE
                WHEN b.state_code IN ('CA', 'WA', 'OR', 'NV', 'AZ') THEN 'WEST'
                WHEN b.state_code IN ('TX', 'OK', 'LA', 'AR') THEN 'SOUTH'
                WHEN b.state_code IN ('NY', 'NJ', 'PA', 'MA', 'CT') THEN 'NORTHEAST'
                WHEN b.state_code IN ('IL', 'MI', 'OH', 'IN', 'WI') THEN 'MIDWEST'
                ELSE 'OTHER'
            END as region,

            b.phone,
            b.manager_name,
            b.opening_date,
            b.branch_age_days,

            -- Aggregated metrics
            COALESCE(emp.total_employees, 0) as total_employees,
            COALESCE(acct.total_accounts, 0) as total_accounts,
            COALESCE(acct.total_clients, 0) as total_clients,

            -- SCD fields
            b.opening_date as effective_date,
            CAST(NULL AS DATE) as end_date,
            TRUE as is_current,

            CURRENT_TIMESTAMP as created_timestamp,
            CURRENT_TIMESTAMP as updated_timestamp

        FROM silver.branches b

        LEFT JOIN (
            SELECT
                branch_id,
                COUNT(*) as total_employees
            FROM silver.employees
            WHERE is_active = TRUE
            GROUP BY branch_id
        ) emp ON b.branch_id = emp.branch_id

        LEFT JOIN (
            SELECT
                branch_code,
                COUNT(*) as total_accounts,
                COUNT(DISTINCT client_id) as total_clients
            FROM silver.accounts
            GROUP BY branch_code
        ) acct ON b.branch_code = acct.branch_code
    """)

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM gold.dim_branch").collect()[0]['cnt']
    logger.info(f"✅ dim_branch built: {row_count} rows")
    return row_count


def build_dim_date(spark):
    """Build date dimension table with calendar attributes."""
    logger.info("Building dimension: gold.dim_date")

    spark.sql("TRUNCATE TABLE gold.dim_date")

    # Generate date range using SQL (2020-01-01 to 2027-12-31)
    spark.sql("""
        INSERT INTO TABLE gold.dim_date
        SELECT
            CAST(DATE_FORMAT(dt, 'yyyyMMdd') AS INT) as date_key,
            dt as date_value,
            YEAR(dt) as year,
            QUARTER(dt) as quarter,
            CONCAT('Q', QUARTER(dt), ' ', YEAR(dt)) as quarter_name,
            MONTH(dt) as month,

            CASE MONTH(dt)
                WHEN 1 THEN 'January' WHEN 2 THEN 'February' WHEN 3 THEN 'March'
                WHEN 4 THEN 'April' WHEN 5 THEN 'May' WHEN 6 THEN 'June'
                WHEN 7 THEN 'July' WHEN 8 THEN 'August' WHEN 9 THEN 'September'
                WHEN 10 THEN 'October' WHEN 11 THEN 'November' WHEN 12 THEN 'December'
            END as month_name,

            CASE MONTH(dt)
                WHEN 1 THEN 'Jan' WHEN 2 THEN 'Feb' WHEN 3 THEN 'Mar' WHEN 4 THEN 'Apr'
                WHEN 5 THEN 'May' WHEN 6 THEN 'Jun' WHEN 7 THEN 'Jul' WHEN 8 THEN 'Aug'
                WHEN 9 THEN 'Sep' WHEN 10 THEN 'Oct' WHEN 11 THEN 'Nov' WHEN 12 THEN 'Dec'
            END as month_short,

            WEEKOFYEAR(dt) as week_of_year,
            DAYOFMONTH(dt) as day_of_month,
            DAYOFWEEK(dt) as day_of_week,

            CASE DAYOFWEEK(dt)
                WHEN 1 THEN 'Sunday' WHEN 2 THEN 'Monday' WHEN 3 THEN 'Tuesday'
                WHEN 4 THEN 'Wednesday' WHEN 5 THEN 'Thursday' WHEN 6 THEN 'Friday'
                WHEN 7 THEN 'Saturday'
            END as day_name,

            CASE DAYOFWEEK(dt)
                WHEN 1 THEN 'Sun' WHEN 2 THEN 'Mon' WHEN 3 THEN 'Tue' WHEN 4 THEN 'Wed'
                WHEN 5 THEN 'Thu' WHEN 6 THEN 'Fri' WHEN 7 THEN 'Sat'
            END as day_short,

            CASE WHEN DAYOFWEEK(dt) IN (1, 7) THEN TRUE ELSE FALSE END as is_weekend,

            CASE
                WHEN MONTH(dt) = 1 AND DAYOFMONTH(dt) = 1 THEN TRUE
                WHEN MONTH(dt) = 7 AND DAYOFMONTH(dt) = 4 THEN TRUE
                WHEN MONTH(dt) = 12 AND DAYOFMONTH(dt) = 25 THEN TRUE
                ELSE FALSE
            END as is_holiday,

            CASE
                WHEN MONTH(dt) = 1 AND DAYOFMONTH(dt) = 1 THEN 'New Year Day'
                WHEN MONTH(dt) = 7 AND DAYOFMONTH(dt) = 4 THEN 'Independence Day'
                WHEN MONTH(dt) = 12 AND DAYOFMONTH(dt) = 25 THEN 'Christmas'
                ELSE NULL
            END as holiday_name,

            CASE
                WHEN DAYOFWEEK(dt) IN (1, 7) THEN FALSE
                WHEN (MONTH(dt) = 1 AND DAYOFMONTH(dt) = 1) OR
                     (MONTH(dt) = 7 AND DAYOFMONTH(dt) = 4) OR
                     (MONTH(dt) = 12 AND DAYOFMONTH(dt) = 25) THEN FALSE
                ELSE TRUE
            END as is_business_day,

            YEAR(dt) as fiscal_year,
            QUARTER(dt) as fiscal_quarter,
            MONTH(dt) as fiscal_month

        FROM (
            SELECT DATE_ADD('2020-01-01', pos) as dt
            FROM (
                SELECT posexplode(split(space(2921), ' ')) as (pos, val)
            ) x
        ) dates
    """)

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM gold.dim_date").collect()[0]['cnt']
    logger.info(f"✅ dim_date built: {row_count} rows")
    return row_count


# ============================================================================
# Fact Tables (Daily Aggregates)
# ============================================================================

def build_fact_transactions_daily(spark):
    """Build daily transaction aggregates fact table."""
    logger.info("Building fact: gold.fact_transactions_daily")

    spark.sql("""
        INSERT OVERWRITE TABLE gold.fact_transactions_daily
        PARTITION (year, month)
        SELECT
            t.transaction_date_only as transaction_date,
            COALESCE(a.client_id, -1) as client_id,
            t.from_account_id as account_id,
            COALESCE(a.branch_code, 'UNKNOWN') as branch_code,
            t.transaction_type_normalized as transaction_type,
            t.category_normalized as category,
            t.currency,

            -- Aggregated metrics
            COUNT(*) as transaction_count,
            ROUND(SUM(t.amount), 2) as total_amount,
            ROUND(AVG(t.amount), 2) as avg_amount,
            ROUND(MIN(t.amount), 2) as min_amount,
            ROUND(MAX(t.amount), 2) as max_amount,

            -- Status breakdown
            SUM(CASE WHEN t.status_normalized = 'COMPLETED' THEN 1 ELSE 0 END) as successful_count,
            SUM(CASE WHEN t.status_normalized = 'FAILED' THEN 1 ELSE 0 END) as failed_count,
            SUM(CASE WHEN t.status_normalized = 'PENDING' THEN 1 ELSE 0 END) as pending_count,
            SUM(CASE WHEN t.status_normalized = 'CANCELLED' THEN 1 ELSE 0 END) as cancelled_count,

            -- Success rate
            ROUND((SUM(CASE WHEN t.status_normalized = 'COMPLETED' THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) as success_rate,

            -- Suspicious flags
            CASE WHEN SUM(CASE WHEN t.is_suspicious THEN 1 ELSE 0 END) > 0 THEN TRUE ELSE FALSE END as has_suspicious_transactions,
            SUM(CASE WHEN t.is_suspicious THEN 1 ELSE 0 END) as suspicious_transaction_count,

            -- Technical fields
            CURRENT_TIMESTAMP as created_timestamp,
            CURRENT_TIMESTAMP as updated_timestamp,

            -- Partitions
            t.transaction_year as year,
            t.transaction_month as month

        FROM silver.transactions t
        LEFT JOIN silver.accounts a ON t.from_account_id = a.account_id

        WHERE t.transaction_date_only IS NOT NULL

        GROUP BY
            t.transaction_date_only,
            a.client_id,
            t.from_account_id,
            a.branch_code,
            t.transaction_type_normalized,
            t.category_normalized,
            t.currency,
            t.transaction_year,
            t.transaction_month
    """)

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM gold.fact_transactions_daily").collect()[0]['cnt']
    logger.info(f"✅ fact_transactions_daily built: {row_count} rows")
    return row_count


def build_fact_account_balance_daily(spark):
    """Build daily account balance snapshots fact table."""
    logger.info("Building fact: gold.fact_account_balance_daily")

    spark.sql("""
        INSERT OVERWRITE TABLE gold.fact_account_balance_daily
        PARTITION (year, month)
        SELECT
            CAST(DATE_FORMAT(ab.last_updated, 'yyyy-MM-dd') AS DATE) as snapshot_date,
            ab.account_id,
            a.client_id,
            a.account_type_normalized as account_type,
            a.branch_code,
            ab.currency,

            -- Balance metrics
            ROUND(ab.current_balance, 2) as current_balance,
            ROUND(ab.available_balance, 2) as available_balance,
            ROUND(ab.reserved_amount, 2) as reserved_amount,
            ROUND(ab.credit_limit, 2) as credit_limit,
            ab.credit_utilization as credit_utilization,
            ab.balance_category,

            -- Change metrics (placeholders)
            0.0 as balance_change_daily,
            0.0 as balance_change_weekly,
            0.0 as balance_change_monthly,

            -- Technical fields
            CURRENT_TIMESTAMP as created_timestamp,

            -- Partitions
            YEAR(ab.last_updated) as year,
            MONTH(ab.last_updated) as month

        FROM silver.account_balances ab
        INNER JOIN silver.accounts a ON ab.account_id = a.account_id

        WHERE ab.last_updated IS NOT NULL
    """)

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM gold.fact_account_balance_daily").collect()[0]['cnt']
    logger.info(f"✅ fact_account_balance_daily built: {row_count} rows")
    return row_count


def build_fact_loan_performance(spark):
    """Build loan performance fact table."""
    logger.info("Building fact: gold.fact_loan_performance")

    spark.sql("""
        INSERT OVERWRITE TABLE gold.fact_loan_performance
        PARTITION (year, month)
        SELECT
            l.loan_id,
            l.contract_id,
            cnt.client_id,
            CURRENT_DATE as snapshot_date,

            -- Loan details
            ROUND(l.loan_amount, 2) as loan_amount,
            ROUND(l.outstanding_balance, 2) as outstanding_balance,
            ROUND(l.paid_amount, 2) as paid_amount,
            l.payment_progress,
            l.interest_rate,
            l.term_months,
            l.remaining_months,
            l.elapsed_months,

            -- Payment metrics
            l.next_payment_date,
            ROUND(l.next_payment_amount, 2) as next_payment_amount,
            DATEDIFF(l.next_payment_date, CURRENT_DATE) as days_to_next_payment,
            0 as total_payments_made,
            0 as missed_payments,
            0 as late_payments,
            0.0 as on_time_payment_rate,

            -- Risk metrics
            l.delinquency_status_normalized as delinquency_status,
            l.is_delinquent,
            0 as days_past_due,
            ROUND(l.collateral_value, 2) as collateral_value,
            l.loan_to_value_ratio,
            l.ltv_category,
            0.0 as default_probability,

            -- Technical fields
            CURRENT_TIMESTAMP as created_timestamp,
            CURRENT_TIMESTAMP as updated_timestamp,

            -- Partitions
            YEAR(CURRENT_DATE) as year,
            MONTH(CURRENT_DATE) as month

        FROM silver.loans l
        INNER JOIN silver.contracts cnt ON l.contract_id = cnt.contract_id
    """)

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM gold.fact_loan_performance").collect()[0]['cnt']
    logger.info(f"✅ fact_loan_performance built: {row_count} rows")
    return row_count


# ============================================================================
# Aggregate / Data Mart Tables
# ============================================================================

def build_client_360_view(spark):
    """Build comprehensive client 360-degree view."""
    logger.info("Building aggregate: gold.client_360_view")

    # This is a complex query - use SQL directly (matches working SQL script)
    spark.sql("""
        INSERT OVERWRITE TABLE gold.client_360_view
        PARTITION (snapshot_year, snapshot_month)
        SELECT
            c.client_id,
            CURRENT_DATE as snapshot_date,

            -- Demographics
            c.full_name,
            c.email,
            c.phone,
            c.age,
            CASE
                WHEN c.age < 25 THEN '18-24'
                WHEN c.age < 35 THEN '25-34'
                WHEN c.age < 45 THEN '35-44'
                WHEN c.age < 55 THEN '45-54'
                WHEN c.age < 65 THEN '55-64'
                ELSE '65+'
            END as age_group,
            c.city,
            c.country,

            -- Financial profile
            c.risk_category,
            c.credit_score,
            c.credit_score_category,
            c.annual_income,
            c.income_category,
            CASE
                WHEN c.annual_income > 150000 AND c.credit_score > 750 THEN 'VIP'
                WHEN c.annual_income > 100000 AND c.credit_score > 700 THEN 'PREMIUM'
                WHEN c.annual_income > 50000 AND c.credit_score > 650 THEN 'REGULAR'
                ELSE 'BASIC'
            END as client_segment,

            -- Account summary
            COALESCE(acct.total_accounts, 0) as total_accounts,
            COALESCE(acct.active_accounts, 0) as active_accounts,
            COALESCE(acct.checking_accounts, 0) as checking_accounts,
            COALESCE(acct.savings_accounts, 0) as savings_accounts,
            COALESCE(acct.loan_accounts, 0) as loan_accounts,
            COALESCE(bal.total_balance, 0) as total_balance,
            COALESCE(bal.avg_balance, 0) as avg_balance,

            -- Product holdings
            COALESCE(prod.total_products, 0) as total_products,
            COALESCE(cont.total_contracts, 0) as total_contracts,
            COALESCE(cards.total_cards, 0) as total_cards,
            COALESCE(cards.active_cards, 0) as active_cards,

            -- Loan summary
            COALESCE(loans.total_loans, 0) as total_loans,
            COALESCE(loans.active_loans, 0) as active_loans,
            COALESCE(loans.total_loan_amount, 0) as total_loan_amount,
            COALESCE(loans.total_outstanding_balance, 0) as total_outstanding_balance,
            COALESCE(loans.total_loan_payment, 0) as total_loan_payment,
            COALESCE(loans.delinquent_loans, 0) as delinquent_loans,

            -- Transaction behavior (last 30 days)
            COALESCE(txn.transactions_30d, 0) as transactions_30d,
            COALESCE(txn.transaction_volume_30d, 0) as transaction_volume_30d,
            COALESCE(txn.avg_transaction_30d, 0) as avg_transaction_30d,
            COALESCE(txn.deposits_30d, 0) as deposits_30d,
            COALESCE(txn.withdrawals_30d, 0) as withdrawals_30d,
            COALESCE(txn.transfers_30d, 0) as transfers_30d,

            -- Engagement metrics
            txn.last_transaction_date,
            COALESCE(DATEDIFF(CURRENT_DATE, txn.last_transaction_date), 999) as days_since_last_transaction,
            CASE
                WHEN COALESCE(txn.transactions_30d, 0) >= 20 THEN 'DAILY'
                WHEN COALESCE(txn.transactions_30d, 0) >= 4 THEN 'WEEKLY'
                WHEN COALESCE(txn.transactions_30d, 0) >= 1 THEN 'MONTHLY'
                ELSE 'INACTIVE'
            END as transaction_frequency,
            'MOBILE' as channel_preference,

            -- Lifetime value (placeholders)
            0.0 as customer_lifetime_value,
            0.0 as total_revenue,
            0.0 as total_fees_paid,
            0.0 as profitability_score,

            -- Risk indicators (placeholders)
            0.0 as risk_score,
            0 as fraud_alerts,
            COALESCE(txn.suspicious_transactions, 0) as suspicious_activities,

            -- Technical fields
            CURRENT_TIMESTAMP as created_timestamp,
            CURRENT_TIMESTAMP as updated_timestamp,

            -- Partitions
            YEAR(CURRENT_DATE) as snapshot_year,
            MONTH(CURRENT_DATE) as snapshot_month

        FROM silver.clients c

        LEFT JOIN (
            SELECT
                client_id,
                COUNT(*) as total_accounts,
                SUM(CASE WHEN is_active THEN 1 ELSE 0 END) as active_accounts,
                SUM(CASE WHEN account_type_normalized = 'CHECKING' THEN 1 ELSE 0 END) as checking_accounts,
                SUM(CASE WHEN account_type_normalized = 'SAVINGS' THEN 1 ELSE 0 END) as savings_accounts,
                SUM(CASE WHEN account_type_normalized = 'LOAN' THEN 1 ELSE 0 END) as loan_accounts
            FROM silver.accounts
            GROUP BY client_id
        ) acct ON c.client_id = acct.client_id

        LEFT JOIN (
            SELECT
                a.client_id,
                SUM(ab.current_balance) as total_balance,
                AVG(ab.current_balance) as avg_balance
            FROM silver.account_balances ab
            INNER JOIN silver.accounts a ON ab.account_id = a.account_id
            GROUP BY a.client_id
        ) bal ON c.client_id = bal.client_id

        LEFT JOIN (
            SELECT
                client_id,
                COUNT(DISTINCT product_id) as total_products
            FROM silver.client_products
            WHERE is_active = TRUE
            GROUP BY client_id
        ) prod ON c.client_id = prod.client_id

        LEFT JOIN (
            SELECT client_id, COUNT(*) as total_contracts
            FROM silver.contracts
            GROUP BY client_id
        ) cont ON c.client_id = cont.client_id

        LEFT JOIN (
            SELECT
                client_id,
                COUNT(*) as total_cards,
                SUM(CASE WHEN NOT is_expired THEN 1 ELSE 0 END) as active_cards
            FROM silver.cards
            GROUP BY client_id
        ) cards ON c.client_id = cards.client_id

        LEFT JOIN (
            SELECT
                cnt.client_id,
                COUNT(*) as total_loans,
                SUM(CASE WHEN NOT l.is_delinquent THEN 1 ELSE 0 END) as active_loans,
                SUM(l.loan_amount) as total_loan_amount,
                SUM(l.outstanding_balance) as total_outstanding_balance,
                SUM(l.next_payment_amount) as total_loan_payment,
                SUM(CASE WHEN l.is_delinquent THEN 1 ELSE 0 END) as delinquent_loans
            FROM silver.loans l
            INNER JOIN silver.contracts cnt ON l.contract_id = cnt.contract_id
            GROUP BY cnt.client_id
        ) loans ON c.client_id = loans.client_id

        LEFT JOIN (
            SELECT
                a.client_id,
                COUNT(*) as transactions_30d,
                SUM(t.amount) as transaction_volume_30d,
                AVG(t.amount) as avg_transaction_30d,
                SUM(CASE WHEN t.transaction_type_normalized = 'DEPOSIT' THEN 1 ELSE 0 END) as deposits_30d,
                SUM(CASE WHEN t.transaction_type_normalized = 'WITHDRAWAL' THEN 1 ELSE 0 END) as withdrawals_30d,
                SUM(CASE WHEN t.transaction_type_normalized = 'TRANSFER' THEN 1 ELSE 0 END) as transfers_30d,
                SUM(CASE WHEN t.is_suspicious THEN 1 ELSE 0 END) as suspicious_transactions,
                MAX(t.transaction_date) as last_transaction_date
            FROM silver.transactions t
            INNER JOIN silver.accounts a ON t.from_account_id = a.account_id
            WHERE t.transaction_date >= DATE_SUB(CURRENT_DATE, 30)
              AND t.status_normalized = 'COMPLETED'
            GROUP BY a.client_id
        ) txn ON c.client_id = txn.client_id
    """)

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM gold.client_360_view").collect()[0]['cnt']
    logger.info(f"✅ client_360_view built: {row_count} rows")
    return row_count


def build_product_performance_summary(spark):
    """Build product performance summary table."""
    logger.info("Building aggregate: gold.product_performance_summary")

    spark.sql("""
        INSERT OVERWRITE TABLE gold.product_performance_summary
        PARTITION (year, month)
        SELECT
            p.product_id,
            CURRENT_DATE as report_date,
            p.product_name,
            p.product_type,
            p.product_category,

            -- Customer metrics
            COALESCE(cp.total_clients, 0) as total_clients,
            COALESCE(growth.new_clients_mtd, 0) as new_clients_mtd,
            COALESCE(growth.churned_clients_mtd, 0) as churned_clients_mtd,
            COALESCE(cp.active_clients, 0) as active_clients,
            CASE
                WHEN COALESCE(cp.total_clients, 0) > 0
                THEN ROUND((COALESCE(cp.active_clients, 0) / cp.total_clients) * 100, 2)
                ELSE 0.0
            END as retention_rate,

            -- Financial metrics
            COALESCE(cont.total_contracts, 0) as total_contracts,
            COALESCE(cont.active_contracts, 0) as active_contracts,
            COALESCE(cont.total_contract_value, 0) as total_contract_value,
            COALESCE(cont.avg_contract_value, 0) as avg_contract_value,
            COALESCE(cont.total_revenue_mtd, 0) as total_revenue_mtd,
            0.0 as total_revenue_ytd,

            -- Growth metrics (placeholders)
            0.0 as client_growth_rate,
            0.0 as revenue_growth_rate,
            0.0 as market_share,

            -- Technical fields
            CURRENT_TIMESTAMP as created_timestamp,
            CURRENT_TIMESTAMP as updated_timestamp,

            -- Partitions
            YEAR(CURRENT_DATE) as year,
            MONTH(CURRENT_DATE) as month

        FROM silver.products p

        LEFT JOIN (
            SELECT
                product_id,
                COUNT(DISTINCT client_id) as total_clients,
                COUNT(DISTINCT CASE WHEN is_active THEN client_id END) as active_clients
            FROM silver.client_products
            GROUP BY product_id
        ) cp ON p.product_id = cp.product_id

        LEFT JOIN (
            SELECT
                product_id,
                COUNT(*) as total_contracts,
                SUM(CASE WHEN status_normalized = 'ACTIVE' THEN 1 ELSE 0 END) as active_contracts,
                SUM(contract_amount) as total_contract_value,
                AVG(contract_amount) as avg_contract_value,
                SUM(CASE WHEN created_date >= DATE_SUB(CURRENT_DATE, 30)
                    THEN contract_amount * interest_rate / 100 ELSE 0 END) as total_revenue_mtd
            FROM silver.contracts
            GROUP BY product_id
        ) cont ON p.product_id = cont.product_id

        LEFT JOIN (
            SELECT
                product_id,
                COUNT(DISTINCT CASE WHEN start_date >= DATE_SUB(CURRENT_DATE, 30) THEN client_id END) as new_clients_mtd,
                COUNT(DISTINCT CASE WHEN end_date >= DATE_SUB(CURRENT_DATE, 30) AND end_date < CURRENT_DATE THEN client_id END) as churned_clients_mtd
            FROM silver.client_products
            GROUP BY product_id
        ) growth ON p.product_id = growth.product_id
    """)

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM gold.product_performance_summary").collect()[0]['cnt']
    logger.info(f"✅ product_performance_summary built: {row_count} rows")
    return row_count


def build_branch_performance_dashboard(spark):
    """Build branch performance dashboard table."""
    logger.info("Building aggregate: gold.branch_performance_dashboard")

    spark.sql("""
        INSERT OVERWRITE TABLE gold.branch_performance_dashboard
        PARTITION (year, month)
        SELECT
            b.branch_id,
            b.branch_code,
            CURRENT_DATE as report_date,
            b.branch_name,
            b.city,
            b.state,
            CASE
                WHEN b.state_code IN ('CA', 'WA', 'OR', 'NV', 'AZ') THEN 'WEST'
                WHEN b.state_code IN ('TX', 'OK', 'LA', 'AR') THEN 'SOUTH'
                WHEN b.state_code IN ('NY', 'NJ', 'PA', 'MA', 'CT') THEN 'NORTHEAST'
                WHEN b.state_code IN ('IL', 'MI', 'OH', 'IN', 'WI') THEN 'MIDWEST'
                ELSE 'OTHER'
            END as region,
            b.manager_name,

            -- Staff metrics
            COALESCE(emp.total_employees, 0) as total_employees,
            COALESCE(emp.active_employees, 0) as active_employees,
            COALESCE(emp.avg_employee_tenure_years, 0) as avg_employee_tenure_years,

            -- Customer metrics
            COALESCE(acct.total_clients, 0) as total_clients,
            COALESCE(acct.new_clients_mtd, 0) as new_clients_mtd,
            COALESCE(acct.active_clients, 0) as active_clients,
            COALESCE(acct.total_accounts, 0) as total_accounts,
            COALESCE(acct.active_accounts, 0) as active_accounts,

            -- Financial metrics
            COALESCE(bal.total_deposits, 0) as total_deposits,
            COALESCE(loans.total_loans_issued, 0) as total_loans_issued,
            COALESCE(txn.total_transaction_volume, 0) as total_transaction_volume,
            COALESCE(txn.total_revenue_mtd, 0) as total_revenue_mtd,
            0.0 as total_fees_collected,

            -- Performance KPIs
            COALESCE(bal.avg_account_balance, 0) as avg_account_balance,
            0.0 as loan_approval_rate,
            0.0 as customer_satisfaction_score,
            0.0 as nps_score,

            -- Rankings (placeholders)
            0 as revenue_rank,
            0 as customer_rank,
            0 as efficiency_rank,

            -- Technical fields
            CURRENT_TIMESTAMP as created_timestamp,
            CURRENT_TIMESTAMP as updated_timestamp,

            -- Partitions
            YEAR(CURRENT_DATE) as year,
            MONTH(CURRENT_DATE) as month

        FROM silver.branches b

        LEFT JOIN (
            SELECT
                branch_id,
                COUNT(*) as total_employees,
                SUM(CASE WHEN is_active THEN 1 ELSE 0 END) as active_employees,
                ROUND(AVG(tenure_years), 1) as avg_employee_tenure_years
            FROM silver.employees
            GROUP BY branch_id
        ) emp ON b.branch_id = emp.branch_id

        LEFT JOIN (
            SELECT
                branch_code,
                COUNT(*) as total_accounts,
                SUM(CASE WHEN is_active THEN 1 ELSE 0 END) as active_accounts,
                COUNT(CASE WHEN open_date >= DATE_SUB(CURRENT_DATE, 30) THEN 1 END) as new_clients_mtd,
                COUNT(DISTINCT client_id) as total_clients,
                COUNT(DISTINCT CASE WHEN is_active THEN client_id END) as active_clients
            FROM silver.accounts
            GROUP BY branch_code
        ) acct ON b.branch_code = acct.branch_code

        LEFT JOIN (
            SELECT
                a.branch_code,
                SUM(ab.current_balance) as total_deposits,
                AVG(ab.current_balance) as avg_account_balance
            FROM silver.account_balances ab
            INNER JOIN silver.accounts a ON ab.account_id = a.account_id
            GROUP BY a.branch_code
        ) bal ON b.branch_code = bal.branch_code

        LEFT JOIN (
            SELECT
                a.branch_code,
                SUM(l.loan_amount) as total_loans_issued
            FROM silver.loans l
            INNER JOIN silver.contracts cnt ON l.contract_id = cnt.contract_id
            INNER JOIN silver.accounts a ON cnt.client_id = a.client_id
            GROUP BY a.branch_code
        ) loans ON b.branch_code = loans.branch_code

        LEFT JOIN (
            SELECT
                a.branch_code,
                SUM(t.amount) as total_transaction_volume,
                SUM(CASE WHEN t.transaction_date >= DATE_SUB(CURRENT_DATE, 30)
                    THEN t.amount * 0.001 ELSE 0 END) as total_revenue_mtd
            FROM silver.transactions t
            INNER JOIN silver.accounts a ON t.from_account_id = a.account_id
            WHERE t.status_normalized = 'COMPLETED'
            GROUP BY a.branch_code
        ) txn ON b.branch_code = txn.branch_code
    """)

    row_count = spark.sql("SELECT COUNT(*) as cnt FROM gold.branch_performance_dashboard").collect()[0]['cnt']
    logger.info(f"✅ branch_performance_dashboard built: {row_count} rows")
    return row_count


# ============================================================================
# Main ETL Function
# ============================================================================

def main():
    """Main ETL execution function."""
    logger.info("=" * 80)
    logger.info("Starting ETL: Silver -> Gold Layer")
    logger.info("=" * 80)

    start_time = datetime.now()

    try:
        # Create Spark session
        spark = create_spark_session()

        # Track results
        results = {}

        # Build dimension tables
        logger.info("\n" + "=" * 80)
        logger.info("PHASE 1: Building Dimension Tables")
        logger.info("=" * 80)
        results['dim_client'] = build_dim_client(spark)
        results['dim_product'] = build_dim_product(spark)
        results['dim_branch'] = build_dim_branch(spark)
        results['dim_date'] = build_dim_date(spark)

        # Build fact tables
        logger.info("\n" + "=" * 80)
        logger.info("PHASE 2: Building Fact Tables")
        logger.info("=" * 80)
        results['fact_transactions_daily'] = build_fact_transactions_daily(spark)
        results['fact_account_balance_daily'] = build_fact_account_balance_daily(spark)
        results['fact_loan_performance'] = build_fact_loan_performance(spark)

        # Build aggregate tables
        logger.info("\n" + "=" * 80)
        logger.info("PHASE 3: Building Aggregate/Data Mart Tables")
        logger.info("=" * 80)
        results['client_360_view'] = build_client_360_view(spark)
        results['product_performance_summary'] = build_product_performance_summary(spark)
        results['branch_performance_dashboard'] = build_branch_performance_dashboard(spark)

        # Print summary
        logger.info("\n" + "=" * 80)
        logger.info("ETL SUMMARY")
        logger.info("=" * 80)
        for table_name, row_count in results.items():
            logger.info(f"{table_name:40} : {row_count:,} rows")

        # Calculate duration
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info("=" * 80)
        logger.info(f"✅ ETL Job completed successfully")
        logger.info(f"Duration: {duration}")
        logger.info("=" * 80)

        spark.stop()
        return 0

    except Exception as e:
        logger.error("=" * 80)
        logger.error(f"❌ ETL Job failed with error: {str(e)}")
        logger.error("=" * 80)
        import traceback
        logger.error(traceback.format_exc())

        if 'spark' in locals():
            spark.stop()

        return 1


if __name__ == "__main__":
    sys.exit(main())
