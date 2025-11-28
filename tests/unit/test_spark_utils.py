#!/usr/bin/env python3
"""
Unit tests for spark_utils module.
"""
import pytest
from pyspark.sql.functions import col
from utils.spark_utils import (
    get_table_count,
    validate_schema,
    optimize_dataframe
)
from utils.error_handler import SchemaValidationError


@pytest.mark.spark
class TestSparkUtils:
    """Test Spark utility functions."""

    def test_get_table_count_from_dataframe(self, spark, sample_clients_df):
        """Test getting count from DataFrame."""
        # Create temp view
        sample_clients_df.createOrReplaceTempView("test_clients")

        count = get_table_count(spark, "", "test_clients")

        assert count == 3

    def test_validate_schema_success(self, sample_clients_df):
        """Test schema validation with valid schema."""
        expected_columns = [
            "client_id", "first_name", "last_name", "email", "phone"
        ]

        result = validate_schema(sample_clients_df, expected_columns)

        assert result is True

    def test_validate_schema_missing_columns(self, sample_clients_df):
        """Test schema validation with missing columns."""
        expected_columns = [
            "client_id", "first_name", "missing_column"
        ]

        with pytest.raises(SchemaValidationError, match="Missing columns"):
            validate_schema(sample_clients_df, expected_columns)

    def test_validate_schema_required_columns_with_nulls(self, bad_quality_clients_df):
        """Test required columns validation with null values."""
        expected_columns = [
            "client_id", "first_name", "last_name", "email"
        ]
        required_columns = ["email"]  # email has nulls in bad data

        with pytest.raises(SchemaValidationError, match="null values"):
            validate_schema(
                bad_quality_clients_df,
                expected_columns,
                required_columns
            )

    def test_optimize_dataframe_repartition(self, sample_clients_df):
        """Test DataFrame repartitioning."""
        original_partitions = sample_clients_df.rdd.getNumPartitions()

        optimized_df = optimize_dataframe(sample_clients_df, repartition=10)

        assert optimized_df.rdd.getNumPartitions() == 10
        assert optimized_df.count() == sample_clients_df.count()

    def test_optimize_dataframe_coalesce(self, sample_clients_df):
        """Test DataFrame coalescing."""
        # First repartition to have more partitions
        df = sample_clients_df.repartition(10)

        optimized_df = optimize_dataframe(df, coalesce=2)

        assert optimized_df.rdd.getNumPartitions() == 2
        assert optimized_df.count() == sample_clients_df.count()

    def test_optimize_dataframe_cache(self, sample_clients_df):
        """Test DataFrame caching."""
        optimized_df = optimize_dataframe(sample_clients_df, cache=True)

        # Check if cached
        assert optimized_df.is_cached
        assert optimized_df.count() == sample_clients_df.count()


@pytest.mark.integration
class TestSparkUtilsIntegration:
    """Integration tests for Spark utilities."""

    def test_load_table_workflow(self, spark, sample_clients_df, create_test_databases):
        """Test complete table load workflow."""
        # Create source table
        sample_clients_df.write.mode("overwrite").saveAsTable("test.clients_source")

        # Create target table (empty)
        spark.sql("""
            CREATE TABLE IF NOT EXISTS test.clients_target
            LIKE test.clients_source
        """)

        # Verify source has data
        source_count = get_table_count(spark, "test", "clients_source")
        assert source_count == 3

        # Load from source to target (would use load_table_generic in real code)
        df = spark.table("test.clients_source")
        df.write.mode("overwrite").saveAsTable("test.clients_target")

        # Verify target has data
        target_count = get_table_count(spark, "test", "clients_target")
        assert target_count == 3

        # Cleanup
        spark.sql("DROP TABLE IF EXISTS test.clients_source")
        spark.sql("DROP TABLE IF EXISTS test.clients_target")
