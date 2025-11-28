#!/usr/bin/env python3
"""
Unit tests for error_handler module.
"""
import pytest
from utils.error_handler import (
    ETLError,
    ConfigurationError,
    DataQualityError,
    DataLoadError,
    ErrorHandler,
    ETLErrorContext,
    handle_etl_errors
)


class TestETLError:
    """Test ETLError and subclasses."""

    def test_etl_error_creation(self):
        """Test creating ETL error."""
        error = ETLError("Test error", job_name="test_job", context_key="value")

        assert error.message == "Test error"
        assert error.job_name == "test_job"
        assert error.context == {"context_key": "value"}
        assert error.timestamp is not None

    def test_etl_error_to_dict(self):
        """Test converting error to dictionary."""
        error = ETLError("Test error", job_name="test_job", table="test_table")
        error_dict = error.to_dict()

        assert error_dict["error_type"] == "ETLError"
        assert error_dict["message"] == "Test error"
        assert error_dict["job_name"] == "test_job"
        assert error_dict["context"]["table"] == "test_table"
        assert "timestamp" in error_dict

    def test_configuration_error(self):
        """Test ConfigurationError."""
        error = ConfigurationError("Missing config", config_key="DATABASE_URL")

        assert isinstance(error, ETLError)
        assert error.message == "Missing config"
        assert error.context["config_key"] == "DATABASE_URL"

    def test_data_quality_error(self):
        """Test DataQualityError."""
        error = DataQualityError("Low quality score", dq_score=0.5, threshold=0.8)

        assert isinstance(error, ETLError)
        assert error.context["dq_score"] == 0.5
        assert error.context["threshold"] == 0.8

    def test_data_load_error(self):
        """Test DataLoadError."""
        error = DataLoadError(
            "Failed to load table",
            job_name="load_clients",
            table="clients",
            rows_failed=100
        )

        assert isinstance(error, ETLError)
        assert error.job_name == "load_clients"
        assert error.context["table"] == "clients"
        assert error.context["rows_failed"] == 100


class TestErrorHandler:
    """Test ErrorHandler class."""

    def test_error_handler_creation(self):
        """Test creating error handler."""
        handler = ErrorHandler("test_job", enable_alerts=False)

        assert handler.job_name == "test_job"
        assert handler.enable_alerts is False

    def test_handle_error_logs_error(self, caplog):
        """Test that handle_error logs the error."""
        handler = ErrorHandler("test_job", enable_alerts=False)

        error = ValueError("Test error")
        handler.handle_error(error, context={"key": "value"})

        # Check that error was logged
        assert "Test error" in caplog.text
        assert "test_job" in caplog.text

    def test_handle_critical_error(self, caplog):
        """Test handling critical error."""
        handler = ErrorHandler("test_job", enable_alerts=False)

        error = ETLError("Critical error")
        handler.handle_error(error, severity="critical")

        # Should log as critical
        assert "critical" in caplog.text.lower()

    def test_save_failed_record(self, tmp_path, monkeypatch):
        """Test saving failed record."""
        # Use temp directory
        monkeypatch.chdir(tmp_path)

        handler = ErrorHandler("test_job", enable_alerts=False)

        record = {"id": 1, "name": "test"}
        error = ValueError("Validation error")

        handler.save_failed_record(record, error, "clients")

        # Check that file was created
        failed_dir = tmp_path / "failed_records"
        assert failed_dir.exists()

        # Check that at least one file was created
        files = list(failed_dir.glob("clients_*.json"))
        assert len(files) > 0


class TestETLErrorContext:
    """Test ETLErrorContext context manager."""

    def test_error_context_success(self):
        """Test error context with successful execution."""
        executed = False

        with ETLErrorContext("test_job", enable_alerts=False, reraise=False):
            executed = True

        assert executed is True

    def test_error_context_handles_error(self, caplog):
        """Test error context handles errors."""
        with ETLErrorContext("test_job", enable_alerts=False, reraise=False):
            raise ValueError("Test error")

        # Error should be logged but not raised
        assert "Test error" in caplog.text

    def test_error_context_reraises_if_configured(self):
        """Test error context reraises errors if configured."""
        with pytest.raises(ValueError, match="Test error"):
            with ETLErrorContext("test_job", enable_alerts=False, reraise=True):
                raise ValueError("Test error")


class TestHandleETLErrorsDecorator:
    """Test handle_etl_errors decorator."""

    def test_decorator_success(self):
        """Test decorator with successful function."""
        @handle_etl_errors(job_name="test_job", enable_alerts=False, reraise=False)
        def successful_func():
            return "success"

        result = successful_func()
        assert result == "success"

    def test_decorator_handles_error(self, caplog):
        """Test decorator handles errors."""
        @handle_etl_errors(job_name="test_job", enable_alerts=False, reraise=False)
        def failing_func():
            raise ValueError("Test error")
            return "should not reach here"

        result = failing_func()

        # Error should be logged
        assert "Test error" in caplog.text

    def test_decorator_uses_function_name_as_job_name(self, caplog):
        """Test decorator uses function name if job_name not provided."""
        @handle_etl_errors(enable_alerts=False, reraise=False)
        def my_test_function():
            raise ValueError("Error")

        my_test_function()

        # Should use function name as job name
        assert "my_test_function" in caplog.text
