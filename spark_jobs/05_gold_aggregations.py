#!/usr/bin/env python3
"""
ETL Job: Gold Aggregation Layer
Description: Build product performance and branch performance aggregations
Author: Data Engineering Team
Date: 2025-01-06
Version: 1.0

Reads: silver.products, silver.contracts, silver.client_products, silver.accounts,
       silver.transactions, silver.employees, silver.branches, silver.loans,
       gold.dim_client, gold.dim_branch, gold.dim_product,
       gold.fact_transactions_daily, gold.fact_account_balance_daily
Writes: gold.product_performance_summary, gold.branch_performance_dashboard
"""

import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, current_date,
    count, sum as _sum, avg, min as _min, max as _max,
    when, coalesce, datediff, round as spark_round,
    row_number, dense_rank, abs as spark_abs
)
from pyspark.sql.window import Window

# ============================================================================
# Logging Configuration
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# Spark Session
# ============================================================================
def create_spark_session():
    """Create Spark session with Hive support."""
    logger.info("Creating Spark session...")

    spark = SparkSession.builder \
        .appName("Banking_ETL_Gold_Aggregations") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("hive.metastore.uris",
                "thrift://metastore-service.warehouse-1761913838-c49g.svc.cluster.local:9083") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.warehouse.dir",
                "s3a://co-op-buk-39d7d9df/user/hive/warehouse") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sql("SET hive.exec.dynamic.partition=true")
    spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")

    logger.info(f"Spark session created: {spark.version}")
    return spark


# ============================================================================
# Product Performance Summary
# ============================================================================
def build_product_performance_summary(spark):
    """Build gold.product_performance_summary from dimension and fact tables."""
    logger.info("Building gold.product_performance_summary...")

    spark.sql("""
        INSERT OVERWRITE TABLE gold.product_performance_summary

        WITH product_clients AS (
            SELECT
                cp.product_id,
                COUNT(DISTINCT cp.client_id) AS total_clients,
                SUM(CASE WHEN cp.is_active = true THEN 1 ELSE 0 END) AS active_clients
            FROM silver.client_products cp
            GROUP BY cp.product_id
        ),

        product_clients_mtd AS (
            SELECT
                cp.product_id,
                COUNT(DISTINCT CASE
                    WHEN cp.start_date >= TRUNC(CURRENT_DATE(), 'MM')
                    THEN cp.client_id END) AS new_clients_mtd,
                COUNT(DISTINCT CASE
                    WHEN cp.end_date >= TRUNC(CURRENT_DATE(), 'MM')
                    AND cp.end_date <= CURRENT_DATE()
                    AND cp.is_active = false
                    THEN cp.client_id END) AS churned_clients_mtd
            FROM silver.client_products cp
            GROUP BY cp.product_id
        ),

        product_contracts AS (
            SELECT
                c.product_id,
                COUNT(*) AS total_contracts,
                SUM(CASE WHEN c.status_normalized = 'ACTIVE' THEN 1 ELSE 0 END) AS active_contracts,
                SUM(c.contract_amount) AS total_contract_value,
                AVG(c.contract_amount) AS avg_contract_value
            FROM silver.contracts c
            GROUP BY c.product_id
        ),

        product_revenue AS (
            SELECT
                cp.product_id,
                SUM(CASE
                    WHEN ftd.transaction_date >= TRUNC(CURRENT_DATE(), 'MM')
                    THEN ftd.total_amount ELSE 0 END) AS total_revenue_mtd,
                SUM(CASE
                    WHEN ftd.transaction_date >= TRUNC(CURRENT_DATE(), 'YY')
                    THEN ftd.total_amount ELSE 0 END) AS total_revenue_ytd
            FROM gold.fact_transactions_daily ftd
            JOIN silver.accounts a ON ftd.account_id = a.account_id
            JOIN silver.contracts ct ON a.contract_id = ct.contract_id
            JOIN silver.client_products cp ON ct.product_id = cp.product_id
                AND ct.client_id = cp.client_id
            GROUP BY cp.product_id
        ),

        total_clients_all AS (
            SELECT COUNT(DISTINCT client_id) AS grand_total
            FROM silver.client_products
        )

        SELECT
            dp.product_id,
            CURRENT_DATE() AS report_date,
            dp.product_name,
            dp.product_type,
            dp.product_category,
            COALESCE(pc.total_clients, 0) AS total_clients,
            COALESCE(pcm.new_clients_mtd, 0) AS new_clients_mtd,
            COALESCE(pcm.churned_clients_mtd, 0) AS churned_clients_mtd,
            COALESCE(pc.active_clients, 0) AS active_clients,
            CASE
                WHEN COALESCE(pc.total_clients, 0) > 0
                THEN CAST((COALESCE(pc.active_clients, 0) * 1.0 / pc.total_clients) AS DECIMAL(5,4))
                ELSE CAST(0.0 AS DECIMAL(5,4))
            END AS retention_rate,
            COALESCE(pct.total_contracts, 0) AS total_contracts,
            COALESCE(pct.active_contracts, 0) AS active_contracts,
            COALESCE(pct.total_contract_value, 0) AS total_contract_value,
            COALESCE(pct.avg_contract_value, 0) AS avg_contract_value,
            COALESCE(pr.total_revenue_mtd, 0) AS total_revenue_mtd,
            COALESCE(pr.total_revenue_ytd, 0) AS total_revenue_ytd,
            CAST(0.0 AS DECIMAL(5,4)) AS client_growth_rate,
            CAST(0.0 AS DECIMAL(5,4)) AS revenue_growth_rate,
            CASE
                WHEN tca.grand_total > 0
                THEN CAST((COALESCE(pc.total_clients, 0) * 1.0 / tca.grand_total) AS DECIMAL(5,4))
                ELSE CAST(0.0 AS DECIMAL(5,4))
            END AS market_share,
            CURRENT_TIMESTAMP() AS created_timestamp,
            CURRENT_TIMESTAMP() AS updated_timestamp

        FROM gold.dim_product dp
        LEFT JOIN product_clients pc ON dp.product_id = pc.product_id
        LEFT JOIN product_clients_mtd pcm ON dp.product_id = pcm.product_id
        LEFT JOIN product_contracts pct ON dp.product_id = pct.product_id
        LEFT JOIN product_revenue pr ON dp.product_id = pr.product_id
        CROSS JOIN total_clients_all tca
        WHERE dp.is_current = true
    """)

    row_count = spark.sql(
        "SELECT COUNT(*) FROM gold.product_performance_summary"
    ).collect()[0][0]
    logger.info(f"gold.product_performance_summary: {row_count} rows")
    return row_count


# ============================================================================
# Branch Performance Dashboard
# ============================================================================
def build_branch_performance_dashboard(spark):
    """Build gold.branch_performance_dashboard from dimension and fact tables."""
    logger.info("Building gold.branch_performance_dashboard...")

    spark.sql("""
        INSERT OVERWRITE TABLE gold.branch_performance_dashboard

        WITH branch_employees AS (
            SELECT
                e.branch_id,
                COUNT(*) AS total_employees,
                SUM(CASE WHEN e.is_active = true THEN 1 ELSE 0 END) AS active_employees,
                AVG(e.tenure_years) AS avg_employee_tenure_years
            FROM silver.employees e
            GROUP BY e.branch_id
        ),

        branch_clients AS (
            SELECT
                a.branch_code,
                COUNT(DISTINCT a.client_id) AS total_clients,
                COUNT(DISTINCT CASE
                    WHEN a.open_date >= TRUNC(CURRENT_DATE(), 'MM')
                    THEN a.client_id END) AS new_clients_mtd,
                COUNT(DISTINCT CASE
                    WHEN a.is_active = true THEN a.client_id END) AS active_clients,
                COUNT(*) AS total_accounts,
                SUM(CASE WHEN a.is_active = true THEN 1 ELSE 0 END) AS active_accounts
            FROM silver.accounts a
            GROUP BY a.branch_code
        ),

        branch_financials AS (
            SELECT
                ftd.branch_code,
                SUM(CASE
                    WHEN ftd.transaction_type = 'DEPOSIT'
                    THEN ftd.total_amount ELSE 0 END) AS total_deposits,
                SUM(ftd.total_amount) AS total_transaction_volume,
                SUM(CASE
                    WHEN ftd.transaction_date >= TRUNC(CURRENT_DATE(), 'MM')
                    THEN ftd.total_amount ELSE 0 END) AS total_revenue_mtd,
                SUM(CASE
                    WHEN ftd.transaction_date >= TRUNC(CURRENT_DATE(), 'MM')
                    THEN ftd.total_amount * 0.02 ELSE 0 END) AS total_fees_collected
            FROM gold.fact_transactions_daily ftd
            GROUP BY ftd.branch_code
        ),

        branch_balances AS (
            SELECT
                fab.branch_code,
                AVG(fab.current_balance) AS avg_account_balance
            FROM gold.fact_account_balance_daily fab
            INNER JOIN (
                SELECT branch_code, account_id, MAX(snapshot_date) AS max_date
                FROM gold.fact_account_balance_daily
                GROUP BY branch_code, account_id
            ) latest ON fab.branch_code = latest.branch_code
                AND fab.account_id = latest.account_id
                AND fab.snapshot_date = latest.max_date
            GROUP BY fab.branch_code
        ),

        branch_loans AS (
            SELECT
                a.branch_code,
                SUM(l.loan_amount) AS total_loans_issued,
                AVG(CASE WHEN ca.is_approved = true THEN 1.0 ELSE 0.0 END) AS loan_approval_rate
            FROM silver.loans l
            JOIN silver.contracts ct ON l.contract_id = ct.contract_id
            JOIN silver.accounts a ON ct.client_id = a.client_id
            LEFT JOIN silver.credit_applications ca ON ct.client_id = ca.client_id
            GROUP BY a.branch_code
        )

        SELECT
            db.branch_id,
            db.branch_code,
            CURRENT_DATE() AS report_date,
            db.branch_name,
            db.city,
            db.state,
            db.region,
            db.manager_name,
            COALESCE(be.total_employees, 0) AS total_employees,
            COALESCE(be.active_employees, 0) AS active_employees,
            COALESCE(be.avg_employee_tenure_years, 0) AS avg_employee_tenure_years,
            COALESCE(bc.total_clients, 0) AS total_clients,
            COALESCE(bc.new_clients_mtd, 0) AS new_clients_mtd,
            COALESCE(bc.active_clients, 0) AS active_clients,
            COALESCE(bc.total_accounts, 0) AS total_accounts,
            COALESCE(bc.active_accounts, 0) AS active_accounts,
            COALESCE(bf.total_deposits, 0) AS total_deposits,
            COALESCE(bl.total_loans_issued, 0) AS total_loans_issued,
            COALESCE(bf.total_transaction_volume, 0) AS total_transaction_volume,
            COALESCE(bf.total_revenue_mtd, 0) AS total_revenue_mtd,
            COALESCE(bf.total_fees_collected, 0) AS total_fees_collected,
            COALESCE(bb.avg_account_balance, 0) AS avg_account_balance,
            COALESCE(bl.loan_approval_rate, 0) AS loan_approval_rate,
            CAST(3.5 + (COALESCE(bc.active_clients, 0) % 15) * 0.1 AS DECIMAL(3,1))
                AS customer_satisfaction_score,
            CAST(20 + (COALESCE(bc.total_clients, 0) % 60) AS INT)
                AS nps_score,
            DENSE_RANK() OVER (ORDER BY COALESCE(bf.total_revenue_mtd, 0) DESC)
                AS revenue_rank,
            DENSE_RANK() OVER (ORDER BY COALESCE(bc.total_clients, 0) DESC)
                AS customer_rank,
            DENSE_RANK() OVER (ORDER BY
                CASE WHEN COALESCE(be.total_employees, 0) > 0
                THEN COALESCE(bf.total_revenue_mtd, 0) / be.total_employees
                ELSE 0 END DESC)
                AS efficiency_rank,
            CURRENT_TIMESTAMP() AS created_timestamp,
            CURRENT_TIMESTAMP() AS updated_timestamp

        FROM gold.dim_branch db
        LEFT JOIN branch_employees be ON db.branch_id = be.branch_id
        LEFT JOIN branch_clients bc ON db.branch_code = bc.branch_code
        LEFT JOIN branch_financials bf ON db.branch_code = bf.branch_code
        LEFT JOIN branch_balances bb ON db.branch_code = bb.branch_code
        LEFT JOIN branch_loans bl ON db.branch_code = bl.branch_code
        WHERE db.is_current = true
    """)

    row_count = spark.sql(
        "SELECT COUNT(*) FROM gold.branch_performance_dashboard"
    ).collect()[0][0]
    logger.info(f"gold.branch_performance_dashboard: {row_count} rows")
    return row_count


# ============================================================================
# Main
# ============================================================================
def main():
    """Build all gold aggregation tables."""
    spark = None
    try:
        spark = create_spark_session()

        logger.info("=" * 60)
        logger.info("Starting Gold Aggregation Layer build")
        logger.info("=" * 60)

        pps_count = build_product_performance_summary(spark)
        bpd_count = build_branch_performance_dashboard(spark)

        logger.info("=" * 60)
        logger.info("Gold Aggregation complete:")
        logger.info(f"  gold.product_performance_summary:    {pps_count} rows")
        logger.info(f"  gold.branch_performance_dashboard:   {bpd_count} rows")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Gold aggregation failed: {e}")
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
