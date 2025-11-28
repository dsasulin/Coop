#!/usr/bin/env python3
"""
Pytest configuration and fixtures.
"""
import pytest
import os
import sys
from pathlib import Path
from datetime import datetime, date

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


# ============================================================================
# Environment Setup
# ============================================================================

@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Setup test environment variables."""
    os.environ["ENV"] = "test"
    os.environ["LOG_LEVEL"] = "WARNING"
    os.environ["LOG_OUTPUT"] = "stdout"
    os.environ["SPARK_MASTER"] = "local[2]"

    # Mock required environment variables for testing
    os.environ["HIVE_METASTORE_URI"] = "thrift://localhost:9083"
    os.environ["HIVE_WAREHOUSE_DIR"] = "file:///tmp/warehouse"
    os.environ["S3_BUCKET"] = "test-bucket"


# ============================================================================
# Spark Fixtures
# ============================================================================

@pytest.fixture(scope="session")
def spark_session():
    """
    Create a Spark session for testing.

    Scope: session (reused across all tests)
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("banking_etl_test") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .config("spark.sql.catalogImplementation", "in-memory") \
        .getOrCreate()

    # Set log level to ERROR to reduce noise
    spark.sparkContext.setLogLevel("ERROR")

    yield spark

    # Cleanup
    spark.stop()


@pytest.fixture
def spark(spark_session):
    """
    Provide Spark session for individual tests.

    Scope: function (new for each test)
    """
    return spark_session


# ============================================================================
# Sample Data Fixtures
# ============================================================================

@pytest.fixture
def sample_clients_data():
    """Sample client data for testing."""
    return [
        (1, "John", "Doe", "john.doe@example.com", "+1234567890",
         date(1985, 5, 15), date(2020, 1, 1), "123 Main St", "New York", "USA",
         "LOW", 720, "EMPLOYED", 75000.00),
        (2, "Jane", "Smith", "jane.smith@example.com", "+1987654321",
         date(1990, 8, 20), date(2019, 6, 15), "456 Oak Ave", "Los Angeles", "USA",
         "MEDIUM", 680, "EMPLOYED", 65000.00),
        (3, "Bob", "Johnson", "bob.johnson@example.com", "+1122334455",
         date(1978, 12, 10), date(2018, 3, 20), "789 Pine Rd", "Chicago", "USA",
         "HIGH", 580, "SELF_EMPLOYED", 45000.00),
    ]


@pytest.fixture
def sample_clients_schema():
    """Schema for client data."""
    return [
        "client_id", "first_name", "last_name", "email", "phone",
        "birth_date", "registration_date", "address", "city", "country",
        "risk_category", "credit_score", "employment_status", "annual_income"
    ]


@pytest.fixture
def sample_clients_df(spark, sample_clients_data, sample_clients_schema):
    """Create sample clients DataFrame."""
    return spark.createDataFrame(sample_clients_data, sample_clients_schema)


@pytest.fixture
def sample_transactions_data():
    """Sample transaction data for testing."""
    return [
        (1, 101, datetime(2024, 1, 15, 10, 30), "DEPOSIT", 1000.00, "USD",
         5000.00, "Salary deposit", "ONLINE", "COMPLETED"),
        (2, 101, datetime(2024, 1, 16, 14, 20), "WITHDRAWAL", -200.00, "USD",
         4800.00, "ATM withdrawal", "ATM", "COMPLETED"),
        (3, 102, datetime(2024, 1, 17, 9, 15), "TRANSFER", -500.00, "USD",
         3500.00, "Bill payment", "MOBILE", "PENDING"),
    ]


@pytest.fixture
def sample_transactions_schema():
    """Schema for transaction data."""
    return [
        "transaction_id", "account_id", "transaction_date", "transaction_type",
        "amount", "currency", "balance_after", "description", "channel", "transaction_status"
    ]


@pytest.fixture
def sample_transactions_df(spark, sample_transactions_data, sample_transactions_schema):
    """Create sample transactions DataFrame."""
    return spark.createDataFrame(sample_transactions_data, sample_transactions_schema)


# ============================================================================
# Data Quality Fixtures
# ============================================================================

@pytest.fixture
def bad_quality_clients_data():
    """Sample client data with quality issues."""
    return [
        (1, "John", "Doe", None, "+1234567890",  # Missing email
         date(1985, 5, 15), date(2020, 1, 1), "123 Main St", "New York", "USA",
         "LOW", 720, "EMPLOYED", 75000.00),
        (2, "Jane", None, "jane@example.com", "+1987654321",  # Missing last name
         date(1990, 8, 20), date(2019, 6, 15), "456 Oak Ave", "Los Angeles", "USA",
         "MEDIUM", None, "EMPLOYED", 65000.00),  # Missing credit score
        (3, "Bob", "Johnson", "invalid-email", "+1122334455",  # Invalid email
         date(1978, 12, 10), date(2018, 3, 20), "789 Pine Rd", "Chicago", "USA",
         "HIGH", 950, "SELF_EMPLOYED", -45000.00),  # Invalid credit score & negative income
    ]


@pytest.fixture
def bad_quality_clients_df(spark, bad_quality_clients_data, sample_clients_schema):
    """Create bad quality clients DataFrame."""
    return spark.createDataFrame(bad_quality_clients_data, sample_clients_schema)


# ============================================================================
# Mock Fixtures
# ============================================================================

@pytest.fixture
def mock_config(monkeypatch):
    """Mock configuration for testing."""
    def mock_get_config():
        class MockConfig:
            env = "test"
            spark_master = "local[2]"
            hive_metastore_uri = "thrift://localhost:9083"
            hive_warehouse_dir = "file:///tmp/warehouse"
            s3_bucket = "test-bucket"
            log_level = "WARNING"
            log_format = "text"
            log_output = "stdout"
            dq_min_score_threshold = 0.85
            etl_max_retries = 3
            slack_enabled = False
            pagerduty_enabled = False

        return MockConfig()

    monkeypatch.setattr("utils.config.get_config", mock_get_config)
    return mock_get_config()


# ============================================================================
# Temporary Directory Fixtures
# ============================================================================

@pytest.fixture
def temp_warehouse(tmp_path):
    """Create temporary warehouse directory."""
    warehouse_dir = tmp_path / "warehouse"
    warehouse_dir.mkdir()
    return str(warehouse_dir)


@pytest.fixture
def temp_checkpoint_dir(tmp_path):
    """Create temporary checkpoint directory."""
    checkpoint_dir = tmp_path / "checkpoints"
    checkpoint_dir.mkdir()
    return str(checkpoint_dir)


# ============================================================================
# Database Fixtures (for integration tests)
# ============================================================================

@pytest.fixture
def create_test_databases(spark):
    """Create test databases in Spark."""
    spark.sql("CREATE DATABASE IF NOT EXISTS test")
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")

    yield

    # Cleanup
    spark.sql("DROP DATABASE IF EXISTS test CASCADE")
    spark.sql("DROP DATABASE IF NOT EXISTS bronze CASCADE")
    spark.sql("DROP DATABASE IF NOT EXISTS silver CASCADE")
    spark.sql("DROP DATABASE IF NOT EXISTS gold CASCADE")


# ============================================================================
# Cleanup Fixtures
# ============================================================================

@pytest.fixture(autouse=True)
def cleanup_after_test():
    """Cleanup after each test."""
    yield
    # Add any cleanup logic here
    pass
