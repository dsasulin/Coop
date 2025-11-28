#!/usr/bin/env python3
"""
Error handling module.
Provides custom exceptions and error handling utilities.
"""
import sys
import traceback
from typing import Optional, Callable, Any
from datetime import datetime
from .logger import get_logger

logger = get_logger(__name__)


# ============================================================================
# Custom Exceptions
# ============================================================================

class ETLError(Exception):
    """Base exception for ETL errors."""

    def __init__(self, message: str, job_name: Optional[str] = None, **context):
        """
        Initialize ETL error.

        Args:
            message: Error message
            job_name: Name of the ETL job
            **context: Additional context
        """
        super().__init__(message)
        self.message = message
        self.job_name = job_name
        self.context = context
        self.timestamp = datetime.utcnow()

    def to_dict(self):
        """Convert error to dictionary."""
        return {
            "error_type": self.__class__.__name__,
            "message": self.message,
            "job_name": self.job_name,
            "timestamp": self.timestamp.isoformat(),
            "context": self.context,
        }


class ConfigurationError(ETLError):
    """Configuration-related error."""
    pass


class DataQualityError(ETLError):
    """Data quality check failed."""
    pass


class DataLoadError(ETLError):
    """Data loading error."""
    pass


class DataTransformError(ETLError):
    """Data transformation error."""
    pass


class SchemaValidationError(ETLError):
    """Schema validation error."""
    pass


class ConnectionError(ETLError):
    """Connection error (database, S3, etc.)."""
    pass


# ============================================================================
# Error Handler
# ============================================================================

class ErrorHandler:
    """
    Centralized error handler for ETL jobs.

    Provides consistent error handling, logging, and alerting.
    """

    def __init__(self, job_name: str, enable_alerts: bool = True):
        """
        Initialize error handler.

        Args:
            job_name: Name of the ETL job
            enable_alerts: Whether to enable alerting
        """
        self.job_name = job_name
        self.enable_alerts = enable_alerts
        self.logger = get_logger(f"ErrorHandler.{job_name}")

    def handle_error(
        self,
        error: Exception,
        context: Optional[dict] = None,
        severity: str = "error"
    ):
        """
        Handle error with logging and optional alerting.

        Args:
            error: Exception that occurred
            context: Additional context
            severity: Error severity (error, critical)
        """
        context = context or {}
        error_info = {
            "job_name": self.job_name,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "traceback": traceback.format_exc(),
            **context
        }

        # Log error
        if severity == "critical":
            self.logger.critical(
                f"Critical error in {self.job_name}: {str(error)}",
                **error_info
            )
        else:
            self.logger.error(
                f"Error in {self.job_name}: {str(error)}",
                **error_info
            )

        # Send alerts if enabled
        if self.enable_alerts:
            self._send_alert(error, error_info, severity)

    def _send_alert(self, error: Exception, error_info: dict, severity: str):
        """
        Send alert to configured channels.

        Args:
            error: Exception that occurred
            error_info: Error information
            severity: Error severity
        """
        try:
            # Try to send to configured alert channels
            self._send_slack_alert(error, error_info, severity)
            self._send_pagerduty_alert(error, error_info, severity)
        except Exception as e:
            self.logger.warning(f"Failed to send alert: {e}")

    def _send_slack_alert(self, error: Exception, error_info: dict, severity: str):
        """Send Slack alert."""
        try:
            from .config import get_config
            config = get_config()

            if not config.slack_enabled or not config.slack_webhook_url:
                return

            import requests

            color = "danger" if severity == "critical" else "warning"
            message = {
                "attachments": [
                    {
                        "color": color,
                        "title": f"ETL {severity.upper()}: {self.job_name}",
                        "text": str(error),
                        "fields": [
                            {
                                "title": "Error Type",
                                "value": error_info.get("error_type", "Unknown"),
                                "short": True
                            },
                            {
                                "title": "Job Name",
                                "value": self.job_name,
                                "short": True
                            }
                        ],
                        "footer": "Banking ETL",
                        "ts": int(datetime.utcnow().timestamp())
                    }
                ]
            }

            requests.post(config.slack_webhook_url, json=message, timeout=5)
        except Exception as e:
            self.logger.warning(f"Failed to send Slack alert: {e}")

    def _send_pagerduty_alert(self, error: Exception, error_info: dict, severity: str):
        """Send PagerDuty alert."""
        try:
            from .config import get_config
            config = get_config()

            if not config.pagerduty_enabled or not config.pagerduty_api_key:
                return

            # Only send to PagerDuty for critical errors
            if severity != "critical":
                return

            # PagerDuty integration would go here
            # import pdpyras
            # session = pdpyras.APISession(config.pagerduty_api_key)
            # session.trigger_incident(...)

        except Exception as e:
            self.logger.warning(f"Failed to send PagerDuty alert: {e}")

    def save_failed_record(
        self,
        record: dict,
        error: Exception,
        table_name: str
    ):
        """
        Save failed record for later reprocessing.

        Args:
            record: Failed record
            error: Exception that occurred
            table_name: Name of the table
        """
        try:
            from .config import get_config
            import json
            from pathlib import Path

            config = get_config()
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

            # Create failed record entry
            failed_entry = {
                "timestamp": timestamp,
                "job_name": self.job_name,
                "table_name": table_name,
                "error_type": type(error).__name__,
                "error_message": str(error),
                "record": record
            }

            # Save to local file (in production, save to S3)
            failed_dir = Path("failed_records")
            failed_dir.mkdir(exist_ok=True)

            file_path = failed_dir / f"{table_name}_{timestamp}.json"
            with open(file_path, "a") as f:
                f.write(json.dumps(failed_entry) + "\n")

            self.logger.info(
                f"Saved failed record for {table_name}",
                table_name=table_name,
                file_path=str(file_path)
            )

        except Exception as e:
            self.logger.warning(f"Failed to save failed record: {e}")


# ============================================================================
# Context Manager for Error Handling
# ============================================================================

class ETLErrorContext:
    """
    Context manager for ETL error handling.

    Usage:
        with ETLErrorContext("my_job") as error_handler:
            # Your ETL code here
            pass
    """

    def __init__(
        self,
        job_name: str,
        enable_alerts: bool = True,
        reraise: bool = True
    ):
        """
        Initialize error context.

        Args:
            job_name: Name of the ETL job
            enable_alerts: Whether to enable alerting
            reraise: Whether to reraise exceptions after handling
        """
        self.job_name = job_name
        self.enable_alerts = enable_alerts
        self.reraise = reraise
        self.error_handler = ErrorHandler(job_name, enable_alerts)

    def __enter__(self):
        """Enter context."""
        return self.error_handler

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context and handle errors."""
        if exc_type is not None:
            # Determine severity
            severity = "critical" if issubclass(exc_type, ETLError) else "error"

            # Handle error
            self.error_handler.handle_error(exc_val, severity=severity)

            # Reraise if configured
            if self.reraise:
                return False

        return True


# ============================================================================
# Decorator for Error Handling
# ============================================================================

def handle_etl_errors(
    job_name: Optional[str] = None,
    enable_alerts: bool = True,
    reraise: bool = True
):
    """
    Decorator for error handling in ETL functions.

    Args:
        job_name: Name of the ETL job (defaults to function name)
        enable_alerts: Whether to enable alerting
        reraise: Whether to reraise exceptions after handling

    Usage:
        @handle_etl_errors(job_name="load_clients")
        def load_clients(spark):
            # Your code here
            pass
    """
    def decorator(func: Callable) -> Callable:
        def wrapper(*args, **kwargs) -> Any:
            _job_name = job_name or func.__name__
            with ETLErrorContext(_job_name, enable_alerts, reraise):
                return func(*args, **kwargs)
        return wrapper
    return decorator
