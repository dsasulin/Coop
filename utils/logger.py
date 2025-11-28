#!/usr/bin/env python3
"""
Structured logging module.
Provides JSON-formatted logging with context and metrics.
"""
import logging
import sys
import json
from datetime import datetime
from typing import Any, Dict, Optional
from pathlib import Path


class StructuredLogger:
    """
    Structured logger with JSON output support.

    Provides consistent logging format with context and metrics.
    Supports both JSON and text output formats.
    """

    def __init__(
        self,
        name: str,
        level: str = "INFO",
        format_type: str = "json",
        output: str = "stdout",
        log_file: Optional[str] = None
    ):
        """
        Initialize structured logger.

        Args:
            name: Logger name (usually module name)
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            format_type: Output format ('json' or 'text')
            output: Output destination ('stdout', 'file', 'both')
            log_file: Path to log file (required if output is 'file' or 'both')
        """
        self.name = name
        self.format_type = format_type
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, level.upper()))
        self.logger.handlers = []  # Clear existing handlers

        # Setup handlers
        if output in ("stdout", "both"):
            self._add_console_handler()

        if output in ("file", "both"):
            if not log_file:
                raise ValueError("log_file is required when output is 'file' or 'both'")
            self._add_file_handler(log_file)

    def _add_console_handler(self):
        """Add console handler."""
        handler = logging.StreamHandler(sys.stdout)
        if self.format_type == "json":
            handler.setFormatter(JSONFormatter())
        else:
            handler.setFormatter(logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            ))
        self.logger.addHandler(handler)

    def _add_file_handler(self, log_file: str):
        """Add file handler."""
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        handler = logging.FileHandler(log_file)
        if self.format_type == "json":
            handler.setFormatter(JSONFormatter())
        else:
            handler.setFormatter(logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            ))
        self.logger.addHandler(handler)

    def _log(self, level: str, message: str, **kwargs):
        """
        Internal logging method.

        Args:
            level: Log level
            message: Log message
            **kwargs: Additional context fields
        """
        extra = {"context": kwargs} if kwargs else {}
        getattr(self.logger, level.lower())(message, extra=extra)

    def debug(self, message: str, **kwargs):
        """Log debug message."""
        self._log("DEBUG", message, **kwargs)

    def info(self, message: str, **kwargs):
        """Log info message."""
        self._log("INFO", message, **kwargs)

    def warning(self, message: str, **kwargs):
        """Log warning message."""
        self._log("WARNING", message, **kwargs)

    def error(self, message: str, **kwargs):
        """Log error message."""
        self._log("ERROR", message, **kwargs)

    def critical(self, message: str, **kwargs):
        """Log critical message."""
        self._log("CRITICAL", message, **kwargs)

    def exception(self, message: str, **kwargs):
        """Log exception with traceback."""
        extra = {"context": kwargs} if kwargs else {}
        self.logger.exception(message, extra=extra)

    def log_metric(self, metric_name: str, value: Any, **tags):
        """
        Log a metric.

        Args:
            metric_name: Name of the metric
            value: Metric value
            **tags: Additional tags for the metric
        """
        self.info(
            f"Metric: {metric_name}",
            metric_name=metric_name,
            metric_value=value,
            **tags
        )

    def log_duration(self, operation: str, duration_seconds: float, **context):
        """
        Log operation duration.

        Args:
            operation: Name of the operation
            duration_seconds: Duration in seconds
            **context: Additional context
        """
        self.info(
            f"Operation completed: {operation}",
            operation=operation,
            duration_seconds=round(duration_seconds, 2),
            **context
        )


class JSONFormatter(logging.Formatter):
    """
    JSON log formatter.

    Formats log records as JSON for structured logging.
    """

    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record as JSON.

        Args:
            record: Log record

        Returns:
            str: JSON-formatted log message
        """
        log_data: Dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # Add context if present
        if hasattr(record, "context"):
            log_data.update(record.context)

        # Add standard fields
        log_data.update({
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        })

        return json.dumps(log_data)


def get_logger(
    name: str,
    level: Optional[str] = None,
    format_type: Optional[str] = None,
    output: Optional[str] = None,
    log_file: Optional[str] = None
) -> StructuredLogger:
    """
    Get a structured logger instance.

    Args:
        name: Logger name
        level: Log level (defaults from config)
        format_type: Output format (defaults from config)
        output: Output destination (defaults from config)
        log_file: Log file path (defaults from config)

    Returns:
        StructuredLogger: Logger instance
    """
    # Try to import config, but don't fail if not available
    try:
        from .config import get_config
        config = get_config()
        level = level or config.log_level
        format_type = format_type or config.log_format
        output = output or config.log_output
        log_file = log_file or config.log_file_path
    except (ImportError, ValueError):
        # Use defaults if config not available
        level = level or "INFO"
        format_type = format_type or "text"
        output = output or "stdout"

    return StructuredLogger(
        name=name,
        level=level,
        format_type=format_type,
        output=output,
        log_file=log_file if output in ("file", "both") else None
    )
