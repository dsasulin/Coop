#!/usr/bin/env python3
"""
Unit tests for config module.
"""
import pytest
import os
from utils.config import Config, get_config


class TestConfig:
    """Test Config class."""

    def test_config_loads_from_env(self, monkeypatch):
        """Test that config loads from environment variables."""
        # Set test environment variables
        monkeypatch.setenv("HIVE_METASTORE_URI", "thrift://test:9083")
        monkeypatch.setenv("HIVE_WAREHOUSE_DIR", "s3a://test-bucket/warehouse")
        monkeypatch.setenv("S3_BUCKET", "test-bucket")
        monkeypatch.setenv("SPARK_MASTER", "local[4]")
        monkeypatch.setenv("ENV", "test")

        config = Config()

        assert config.hive_metastore_uri == "thrift://test:9083"
        assert config.hive_warehouse_dir == "s3a://test-bucket/warehouse"
        assert config.s3_bucket == "test-bucket"
        assert config.spark_master == "local[4]"
        assert config.env == "test"

    def test_config_defaults(self, monkeypatch):
        """Test default configuration values."""
        # Set required env vars
        monkeypatch.setenv("HIVE_METASTORE_URI", "thrift://test:9083")
        monkeypatch.setenv("HIVE_WAREHOUSE_DIR", "s3a://test/warehouse")
        monkeypatch.setenv("S3_BUCKET", "test")

        config = Config()

        # Test defaults
        assert config.env == "dev"
        assert config.spark_master == "local[*]"
        assert config.log_level == "INFO"
        assert config.etl_max_retries == 3

    def test_config_missing_required_vars(self, monkeypatch):
        """Test that Config raises error if required vars are missing."""
        # Clear required env vars
        monkeypatch.delenv("HIVE_METASTORE_URI", raising=False)
        monkeypatch.delenv("HIVE_WAREHOUSE_DIR", raising=False)
        monkeypatch.delenv("S3_BUCKET", raising=False)

        with pytest.raises(ValueError, match="Missing required environment variables"):
            Config()

    def test_is_production(self, monkeypatch):
        """Test is_production property."""
        monkeypatch.setenv("HIVE_METASTORE_URI", "thrift://test:9083")
        monkeypatch.setenv("HIVE_WAREHOUSE_DIR", "s3a://test/warehouse")
        monkeypatch.setenv("S3_BUCKET", "test")

        # Test dev environment
        monkeypatch.setenv("ENV", "dev")
        config = Config()
        assert config.is_production is False

        # Test production environment
        monkeypatch.setenv("ENV", "prod")
        config = Config()
        assert config.is_production is True

    def test_data_quality_thresholds(self, monkeypatch):
        """Test data quality threshold configuration."""
        monkeypatch.setenv("HIVE_METASTORE_URI", "thrift://test:9083")
        monkeypatch.setenv("HIVE_WAREHOUSE_DIR", "s3a://test/warehouse")
        monkeypatch.setenv("S3_BUCKET", "test")
        monkeypatch.setenv("DQ_MIN_SCORE_THRESHOLD", "0.90")
        monkeypatch.setenv("DQ_BAD_RECORDS_PCT_THRESHOLD", "0.02")

        config = Config()

        assert config.dq_min_score_threshold == 0.90
        assert config.dq_bad_records_pct_threshold == 0.02

    def test_alert_email_recipients(self, monkeypatch):
        """Test parsing of email recipients."""
        monkeypatch.setenv("HIVE_METASTORE_URI", "thrift://test:9083")
        monkeypatch.setenv("HIVE_WAREHOUSE_DIR", "s3a://test/warehouse")
        monkeypatch.setenv("S3_BUCKET", "test")
        monkeypatch.setenv("ALERT_EMAIL_RECIPIENTS", "user1@test.com,user2@test.com, user3@test.com")

        config = Config()

        assert config.alert_email_recipients == ["user1@test.com", "user2@test.com", "user3@test.com"]

    def test_monitoring_flags(self, monkeypatch):
        """Test monitoring enabled flags."""
        monkeypatch.setenv("HIVE_METASTORE_URI", "thrift://test:9083")
        monkeypatch.setenv("HIVE_WAREHOUSE_DIR", "s3a://test/warehouse")
        monkeypatch.setenv("S3_BUCKET", "test")
        monkeypatch.setenv("DATADOG_ENABLED", "true")
        monkeypatch.setenv("PAGERDUTY_ENABLED", "false")
        monkeypatch.setenv("SLACK_ENABLED", "TRUE")

        config = Config()

        assert config.datadog_enabled is True
        assert config.pagerduty_enabled is False
        assert config.slack_enabled is True

    def test_get_config_singleton(self, monkeypatch):
        """Test that get_config returns singleton instance."""
        monkeypatch.setenv("HIVE_METASTORE_URI", "thrift://test:9083")
        monkeypatch.setenv("HIVE_WAREHOUSE_DIR", "s3a://test/warehouse")
        monkeypatch.setenv("S3_BUCKET", "test")

        config1 = get_config()
        config2 = get_config()

        assert config1 is config2

    def test_config_repr(self, monkeypatch):
        """Test config string representation (should not expose secrets)."""
        monkeypatch.setenv("HIVE_METASTORE_URI", "thrift://test:9083")
        monkeypatch.setenv("HIVE_WAREHOUSE_DIR", "s3a://test/warehouse")
        monkeypatch.setenv("S3_BUCKET", "test-bucket")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "super-secret-key")

        config = Config()
        repr_str = repr(config)

        # Should contain safe info
        assert "test-bucket" in repr_str
        assert "env=" in repr_str

        # Should NOT contain secrets
        assert "super-secret-key" not in repr_str
