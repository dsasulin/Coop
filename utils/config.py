#!/usr/bin/env python3
"""
Configuration management module.
Loads configuration from environment variables with validation.
"""
import os
from typing import Optional, List
from pathlib import Path
from dotenv import load_dotenv


class Config:
    """
    Configuration manager for ETL jobs.

    Loads configuration from environment variables with fallback to defaults.
    Validates required configuration on initialization.
    """

    def __init__(self, env_file: Optional[str] = None):
        """
        Initialize configuration.

        Args:
            env_file: Path to .env file. If None, searches for .env in project root.
        """
        # Load .env file
        if env_file is None:
            # Try to find .env in project root
            project_root = Path(__file__).parent.parent
            env_file = project_root / ".env"

        if os.path.exists(env_file):
            load_dotenv(env_file)

        # Validate required configuration
        self._validate()

    # ========================================================================
    # Environment
    # ========================================================================

    @property
    def env(self) -> str:
        """Get environment name (dev, staging, prod)."""
        return os.getenv("ENV", "dev")

    @property
    def is_production(self) -> bool:
        """Check if running in production."""
        return self.env == "prod"

    # ========================================================================
    # Spark Configuration
    # ========================================================================

    @property
    def spark_master(self) -> str:
        """Get Spark master URL."""
        return os.getenv("SPARK_MASTER", "local[*]")

    @property
    def spark_app_name(self) -> str:
        """Get Spark application name."""
        return os.getenv("SPARK_APP_NAME", "banking_etl")

    @property
    def spark_driver_memory(self) -> str:
        """Get Spark driver memory."""
        return os.getenv("SPARK_DRIVER_MEMORY", "4g")

    @property
    def spark_driver_cores(self) -> str:
        """Get Spark driver cores."""
        return os.getenv("SPARK_DRIVER_CORES", "2")

    @property
    def spark_executor_memory(self) -> str:
        """Get Spark executor memory."""
        return os.getenv("SPARK_EXECUTOR_MEMORY", "8g")

    @property
    def spark_executor_cores(self) -> str:
        """Get Spark executor cores."""
        return os.getenv("SPARK_EXECUTOR_CORES", "4")

    @property
    def spark_executor_instances(self) -> str:
        """Get Spark executor instances."""
        return os.getenv("SPARK_EXECUTOR_INSTANCES", "5")

    # ========================================================================
    # Hive/Metastore Configuration
    # ========================================================================

    @property
    def hive_metastore_uri(self) -> str:
        """Get Hive metastore URI."""
        uri = os.getenv("HIVE_METASTORE_URI")
        if not uri:
            raise ValueError("HIVE_METASTORE_URI environment variable is required")
        return uri

    @property
    def hive_warehouse_dir(self) -> str:
        """Get Hive warehouse directory."""
        warehouse = os.getenv("HIVE_WAREHOUSE_DIR")
        if not warehouse:
            raise ValueError("HIVE_WAREHOUSE_DIR environment variable is required")
        return warehouse

    # ========================================================================
    # AWS/S3 Configuration
    # ========================================================================

    @property
    def aws_region(self) -> str:
        """Get AWS region."""
        return os.getenv("AWS_REGION", "us-east-1")

    @property
    def s3_bucket(self) -> str:
        """Get S3 bucket name."""
        bucket = os.getenv("S3_BUCKET")
        if not bucket:
            raise ValueError("S3_BUCKET environment variable is required")
        return bucket

    @property
    def aws_access_key_id(self) -> Optional[str]:
        """Get AWS access key ID."""
        return os.getenv("AWS_ACCESS_KEY_ID")

    @property
    def aws_secret_access_key(self) -> Optional[str]:
        """Get AWS secret access key."""
        return os.getenv("AWS_SECRET_ACCESS_KEY")

    # ========================================================================
    # Database Configuration
    # ========================================================================

    @property
    def db_host(self) -> str:
        """Get database host."""
        return os.getenv("DB_HOST", "localhost")

    @property
    def db_port(self) -> int:
        """Get database port."""
        return int(os.getenv("DB_PORT", "5432"))

    @property
    def db_name(self) -> str:
        """Get database name."""
        return os.getenv("DB_NAME", "banking_metadata")

    @property
    def db_user(self) -> str:
        """Get database user."""
        return os.getenv("DB_USER", "etl_user")

    @property
    def db_password(self) -> str:
        """Get database password."""
        return os.getenv("DB_PASSWORD", "")

    # ========================================================================
    # Monitoring & Alerting
    # ========================================================================

    @property
    def alert_email_recipients(self) -> List[str]:
        """Get alert email recipients."""
        emails = os.getenv("ALERT_EMAIL_RECIPIENTS", "")
        return [e.strip() for e in emails.split(",") if e.strip()]

    @property
    def datadog_enabled(self) -> bool:
        """Check if Datadog monitoring is enabled."""
        return os.getenv("DATADOG_ENABLED", "false").lower() == "true"

    @property
    def datadog_api_key(self) -> Optional[str]:
        """Get Datadog API key."""
        return os.getenv("DATADOG_API_KEY")

    @property
    def pagerduty_enabled(self) -> bool:
        """Check if PagerDuty alerting is enabled."""
        return os.getenv("PAGERDUTY_ENABLED", "false").lower() == "true"

    @property
    def pagerduty_api_key(self) -> Optional[str]:
        """Get PagerDuty API key."""
        return os.getenv("PAGERDUTY_API_KEY")

    @property
    def slack_enabled(self) -> bool:
        """Check if Slack notifications are enabled."""
        return os.getenv("SLACK_ENABLED", "false").lower() == "true"

    @property
    def slack_webhook_url(self) -> Optional[str]:
        """Get Slack webhook URL."""
        return os.getenv("SLACK_WEBHOOK_URL")

    # ========================================================================
    # Data Quality
    # ========================================================================

    @property
    def dq_min_score_threshold(self) -> float:
        """Get minimum data quality score threshold."""
        return float(os.getenv("DQ_MIN_SCORE_THRESHOLD", "0.85"))

    @property
    def dq_bad_records_pct_threshold(self) -> float:
        """Get bad records percentage threshold."""
        return float(os.getenv("DQ_BAD_RECORDS_PCT_THRESHOLD", "0.05"))

    # ========================================================================
    # ETL Job Configuration
    # ========================================================================

    @property
    def etl_batch_size(self) -> int:
        """Get ETL batch size."""
        return int(os.getenv("ETL_BATCH_SIZE", "10000"))

    @property
    def etl_max_retries(self) -> int:
        """Get maximum number of retries for ETL operations."""
        return int(os.getenv("ETL_MAX_RETRIES", "3"))

    @property
    def etl_retry_delay_seconds(self) -> int:
        """Get retry delay in seconds."""
        return int(os.getenv("ETL_RETRY_DELAY_SECONDS", "60"))

    @property
    def etl_checkpoint_dir(self) -> str:
        """Get ETL checkpoint directory."""
        return os.getenv("ETL_CHECKPOINT_DIR", f"s3a://{self.s3_bucket}/checkpoints")

    @property
    def etl_failed_records_dir(self) -> str:
        """Get directory for storing failed records."""
        return os.getenv("ETL_FAILED_RECORDS_DIR", f"s3a://{self.s3_bucket}/failed_records")

    # ========================================================================
    # Security
    # ========================================================================

    @property
    def encryption_key_id(self) -> Optional[str]:
        """Get KMS encryption key ID."""
        return os.getenv("ENCRYPTION_KEY_ID")

    @property
    def pii_encryption_enabled(self) -> bool:
        """Check if PII encryption is enabled."""
        return os.getenv("PII_ENCRYPTION_ENABLED", "true").lower() == "true"

    # ========================================================================
    # Logging
    # ========================================================================

    @property
    def log_level(self) -> str:
        """Get log level."""
        return os.getenv("LOG_LEVEL", "INFO")

    @property
    def log_format(self) -> str:
        """Get log format (json or text)."""
        return os.getenv("LOG_FORMAT", "json")

    @property
    def log_output(self) -> str:
        """Get log output (stdout, file, both)."""
        return os.getenv("LOG_OUTPUT", "stdout")

    @property
    def log_file_path(self) -> str:
        """Get log file path."""
        return os.getenv("LOG_FILE_PATH", "/var/log/banking_etl/etl.log")

    # ========================================================================
    # Validation
    # ========================================================================

    def _validate(self):
        """Validate required configuration."""
        required_vars = [
            ("HIVE_METASTORE_URI", self.hive_metastore_uri),
            ("HIVE_WAREHOUSE_DIR", self.hive_warehouse_dir),
            ("S3_BUCKET", self.s3_bucket),
        ]

        missing = []
        for var_name, var_value in required_vars:
            if not var_value:
                missing.append(var_name)

        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}. "
                f"Please set them in .env file or environment."
            )

    def __repr__(self) -> str:
        """String representation (safe, doesn't expose secrets)."""
        return (
            f"Config(env={self.env}, "
            f"spark_master={self.spark_master}, "
            f"s3_bucket={self.s3_bucket})"
        )


# Global config instance
_config: Optional[Config] = None


def get_config() -> Config:
    """
    Get global configuration instance.

    Returns:
        Config: Global configuration instance
    """
    global _config
    if _config is None:
        _config = Config()
    return _config
