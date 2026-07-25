#!/usr/bin/env python3
"""
ETL Job: Stage (test) -> Bronze Layer
Description: Load raw data from stage tables to bronze layer
Author: Data Engineering Team
Date: 2025-01-21
Version: 2.0 (Refactored)

Improvements:
- Removed hardcoded credentials
- Added comprehensive error handling
- Added retry logic
- Eliminated code duplication
- Added structured logging
- Added metrics and monitoring
"""
import sys
from datetime import datetime
from typing import Dict

# Add parent directory to path
sys.path.insert(0, '/Users/dsasulin/Developer/GitHub/Coop')

from utils import (
    create_spark_session,
    load_table_generic,
    get_logger,
    ETLErrorContext,
    get_table_count
)

# ============================================================================
# Configuration
# ============================================================================

logger = get_logger(__name__)

# Table mappings: {table_name: partition_columns}
TABLE_MAPPINGS = {
    'clients': None,
    'products': None,
    'branches': None,
    'employees': None,
    'accounts': None,
    'transactions': ['transaction_year', 'transaction_month'],
    'cards': None,
    'loans': None,
    'contracts': None,
    'client_products': None,
    'credit_applications': None,
    'account_balances': ['balance_year', 'balance_month']
}


# ============================================================================
# Table-Specific Transformations
# ============================================================================

def transform_transactions(df):
    """Add partition columns for transactions."""
    from pyspark.sql.functions import year, month

    return df \
        .withColumn("transaction_year", year("transaction_date")) \
        .withColumn("transaction_month", month("transaction_date"))


def transform_account_balances(df):
    """Add partition columns for account balances."""
    from pyspark.sql.functions import year, month

    return df \
        .withColumn("balance_year", year("balance_date")) \
        .withColumn("balance_month", month("balance_date"))


# Table-specific transformation functions
TRANSFORMATIONS = {
    'transactions': transform_transactions,
    'account_balances': transform_account_balances
}


# ============================================================================
# Main ETL Functions
# ============================================================================

def load_all_tables(spark) -> Dict[str, int]:
    """
    Load all tables from stage to bronze.

    Args:
        spark: Spark session

    Returns:
        Dict[str, int]: Dictionary mapping table names to row counts
    """
    results = {}

    for table_name, partition_cols in TABLE_MAPPINGS.items():
        try:
            logger.info(f"Loading table: {table_name}")

            # Get transformation function if exists
            transform_func = TRANSFORMATIONS.get(table_name)

            # Load table
            row_count = load_table_generic(
                spark=spark,
                source_db='test',
                source_table=table_name,
                target_db='bronze',
                target_table=table_name,
                partition_cols=partition_cols,
                transformation_func=transform_func,
                add_metadata=True
            )

            results[table_name] = row_count

            logger.info(
                f"✅ Table {table_name} loaded successfully",
                table=table_name,
                row_count=row_count
            )

        except Exception as e:
            logger.error(
                f"Failed to load table: {table_name}",
                table=table_name,
                error=str(e)
            )
            # Continue with other tables
            results[table_name] = 0

    return results


def print_summary(results: Dict[str, int], duration: float):
    """
    Print job summary.

    Args:
        results: Dictionary of table names and row counts
        duration: Job duration in seconds
    """
    logger.info("="*80)
    logger.info("ETL Job Summary:")
    logger.info("-"*80)

    total_rows = 0
    successful_tables = 0
    failed_tables = 0

    for table, count in results.items():
        status = "✅" if count > 0 else "❌"
        logger.info(f"  {status} {table:30s}: {count:>10,} rows")

        total_rows += count
        if count > 0:
            successful_tables += 1
        else:
            failed_tables += 1

    logger.info("-"*80)
    logger.info(f"  {'TOTAL':30s}: {total_rows:>10,} rows")
    logger.info(f"  Successful tables: {successful_tables}/{len(results)}")
    logger.info(f"  Failed tables: {failed_tables}/{len(results)}")
    logger.info(f"  Duration: {duration:.2f} seconds")
    logger.info("="*80)


# ============================================================================
# Main Entry Point
# ============================================================================

def main() -> int:
    """
    Main ETL pipeline execution.

    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    start_time = datetime.now()

    logger.info("="*80)
    logger.info("Starting ETL Job: Stage (test) -> Bronze Layer")
    logger.info("="*80)

    # Use error handler context manager
    with ETLErrorContext("stage_to_bronze", enable_alerts=True, reraise=False) as error_handler:
        try:
            # Create Spark session (config from environment)
            spark = create_spark_session("Banking_ETL_Stage_to_Bronze")

            # Load all tables
            results = load_all_tables(spark)

            # Calculate duration
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            # Print summary
            print_summary(results, duration)

            # Check if all tables loaded successfully
            failed_count = sum(1 for count in results.values() if count == 0)

            if failed_count > 0:
                logger.warning(
                    f"Job completed with {failed_count} failed tables",
                    failed_tables=failed_count,
                    total_tables=len(results)
                )
                return 1

            logger.info(
                f"✅ ETL Job completed successfully in {duration:.2f} seconds",
                duration_seconds=duration,
                total_rows=sum(results.values())
            )

            return 0

        except Exception as e:
            logger.critical(f"ETL Job failed: {str(e)}", exc_info=True)
            return 1

        finally:
            if 'spark' in locals():
                spark.stop()
                logger.info("Spark session stopped")


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
