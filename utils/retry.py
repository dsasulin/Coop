#!/usr/bin/env python3
"""
Retry logic module.
Provides retry decorators and utilities for transient failures.
"""
import time
import random
from typing import Callable, Tuple, Type, Any, Optional
from functools import wraps
from .logger import get_logger
from .error_handler import ETLError

logger = get_logger(__name__)


def retry_with_backoff(
    max_retries: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    on_retry: Optional[Callable] = None
):
    """
    Retry decorator with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay in seconds
        max_delay: Maximum delay in seconds
        exponential_base: Base for exponential backoff
        jitter: Whether to add random jitter to delay
        exceptions: Tuple of exceptions to catch and retry
        on_retry: Optional callback function called on each retry

    Usage:
        @retry_with_backoff(max_retries=3, initial_delay=2.0)
        def my_function():
            # Code that might fail transiently
            pass

    Returns:
        Decorated function
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            delay = initial_delay
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    # Try to execute function
                    result = func(*args, **kwargs)

                    # Log success if this was a retry
                    if attempt > 0:
                        logger.info(
                            f"Function {func.__name__} succeeded after {attempt} retries",
                            function=func.__name__,
                            attempt=attempt,
                            total_retries=max_retries
                        )

                    return result

                except exceptions as e:
                    last_exception = e

                    # Check if we should retry
                    if attempt >= max_retries:
                        logger.error(
                            f"Function {func.__name__} failed after {max_retries} retries",
                            function=func.__name__,
                            max_retries=max_retries,
                            error=str(e)
                        )
                        raise

                    # Calculate delay with exponential backoff
                    current_delay = min(delay * (exponential_base ** attempt), max_delay)

                    # Add jitter if enabled
                    if jitter:
                        current_delay = current_delay * (0.5 + random.random() * 0.5)

                    # Log retry attempt
                    logger.warning(
                        f"Function {func.__name__} failed, retrying in {current_delay:.2f}s",
                        function=func.__name__,
                        attempt=attempt + 1,
                        max_retries=max_retries,
                        delay_seconds=round(current_delay, 2),
                        error=str(e)
                    )

                    # Call on_retry callback if provided
                    if on_retry:
                        try:
                            on_retry(attempt, e, current_delay)
                        except Exception as callback_error:
                            logger.warning(f"on_retry callback failed: {callback_error}")

                    # Wait before retrying
                    time.sleep(current_delay)

            # This should never be reached, but just in case
            if last_exception:
                raise last_exception

        return wrapper
    return decorator


def retry_on_transient_errors(
    max_retries: int = 3,
    initial_delay: float = 2.0,
    on_retry: Optional[Callable] = None
):
    """
    Retry decorator specifically for transient errors.

    Retries on common transient errors like network issues, timeouts, etc.

    Args:
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay in seconds
        on_retry: Optional callback function called on each retry

    Usage:
        @retry_on_transient_errors(max_retries=5)
        def fetch_data_from_s3():
            # Code that might have transient failures
            pass
    """
    # Common transient errors
    transient_errors = (
        ConnectionError,
        TimeoutError,
        OSError,  # Can include network errors
    )

    # Try to import Spark-specific exceptions
    try:
        from pyspark.sql.utils import AnalysisException
        transient_errors = transient_errors + (AnalysisException,)
    except ImportError:
        pass

    # Try to import boto3 exceptions
    try:
        from botocore.exceptions import BotoCoreError, ClientError
        transient_errors = transient_errors + (BotoCoreError, ClientError)
    except ImportError:
        pass

    return retry_with_backoff(
        max_retries=max_retries,
        initial_delay=initial_delay,
        exceptions=transient_errors,
        on_retry=on_retry
    )


class RetryContext:
    """
    Context manager for retry logic.

    Usage:
        retry_ctx = RetryContext(max_retries=3)
        with retry_ctx:
            # Your code here
            pass
    """

    def __init__(
        self,
        max_retries: int = 3,
        initial_delay: float = 1.0,
        exceptions: Tuple[Type[Exception], ...] = (Exception,)
    ):
        """
        Initialize retry context.

        Args:
            max_retries: Maximum number of retry attempts
            initial_delay: Initial delay in seconds
            exceptions: Tuple of exceptions to catch and retry
        """
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.exceptions = exceptions
        self.attempt = 0

    def __enter__(self):
        """Enter context."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context and handle retries."""
        if exc_type is not None and issubclass(exc_type, self.exceptions):
            self.attempt += 1

            if self.attempt <= self.max_retries:
                delay = self.initial_delay * (2 ** (self.attempt - 1))
                logger.warning(
                    f"Retry attempt {self.attempt}/{self.max_retries} after {delay}s",
                    attempt=self.attempt,
                    max_retries=self.max_retries,
                    delay_seconds=delay
                )
                time.sleep(delay)
                return True  # Suppress exception and retry

        return False  # Don't suppress exception


def safe_execute(
    func: Callable,
    *args,
    max_retries: int = 3,
    default_value: Any = None,
    log_errors: bool = True,
    **kwargs
) -> Any:
    """
    Safely execute a function with retry logic and error handling.

    Args:
        func: Function to execute
        *args: Positional arguments for the function
        max_retries: Maximum number of retry attempts
        default_value: Default value to return on failure
        log_errors: Whether to log errors
        **kwargs: Keyword arguments for the function

    Returns:
        Function result or default_value on failure

    Usage:
        result = safe_execute(risky_function, arg1, arg2, max_retries=5)
    """
    for attempt in range(max_retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if log_errors:
                if attempt >= max_retries:
                    logger.error(
                        f"Function {func.__name__} failed after {max_retries} retries",
                        function=func.__name__,
                        error=str(e)
                    )
                else:
                    logger.warning(
                        f"Function {func.__name__} failed, retry {attempt + 1}/{max_retries}",
                        function=func.__name__,
                        attempt=attempt + 1,
                        error=str(e)
                    )

            if attempt >= max_retries:
                return default_value

            time.sleep(2 ** attempt)  # Exponential backoff

    return default_value


# ============================================================================
# Spark-specific Retry Utilities
# ============================================================================

def retry_spark_operation(
    max_retries: int = 3,
    checkpoint_enabled: bool = True
):
    """
    Retry decorator for Spark operations.

    Handles Spark-specific transient failures and checkpoint management.

    Args:
        max_retries: Maximum number of retry attempts
        checkpoint_enabled: Whether to enable checkpointing

    Usage:
        @retry_spark_operation(max_retries=5)
        def process_large_dataset(spark, df):
            # Spark transformations
            pass
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            from datetime import datetime

            for attempt in range(max_retries + 1):
                try:
                    # Set checkpoint if enabled
                    if checkpoint_enabled and len(args) > 0:
                        spark = args[0]
                        if hasattr(spark, 'sparkContext'):
                            checkpoint_dir = f"/tmp/checkpoints/{func.__name__}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                            spark.sparkContext.setCheckpointDir(checkpoint_dir)

                    # Execute function
                    result = func(*args, **kwargs)

                    if attempt > 0:
                        logger.info(
                            f"Spark operation {func.__name__} succeeded after {attempt} retries"
                        )

                    return result

                except Exception as e:
                    if attempt >= max_retries:
                        logger.error(
                            f"Spark operation {func.__name__} failed after {max_retries} retries",
                            error=str(e)
                        )
                        raise

                    delay = 5 * (2 ** attempt)  # 5, 10, 20, 40 seconds
                    logger.warning(
                        f"Spark operation {func.__name__} failed, retrying in {delay}s",
                        attempt=attempt + 1,
                        max_retries=max_retries,
                        delay_seconds=delay,
                        error=str(e)
                    )

                    time.sleep(delay)

        return wrapper
    return decorator
