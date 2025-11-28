#!/usr/bin/env python3
"""
Unit tests for retry module.
"""
import pytest
import time
from utils.retry import (
    retry_with_backoff,
    retry_on_transient_errors,
    safe_execute,
    RetryContext
)


class TestRetryWithBackoff:
    """Test retry_with_backoff decorator."""

    def test_successful_execution_no_retry(self):
        """Test that successful function doesn't retry."""
        call_count = []

        @retry_with_backoff(max_retries=3)
        def successful_func():
            call_count.append(1)
            return "success"

        result = successful_func()

        assert result == "success"
        assert len(call_count) == 1  # Called only once

    def test_retry_on_failure(self):
        """Test that function retries on failure."""
        call_count = []

        @retry_with_backoff(max_retries=3, initial_delay=0.01)
        def failing_func():
            call_count.append(1)
            if len(call_count) < 3:
                raise ValueError("Transient error")
            return "success after retries"

        result = failing_func()

        assert result == "success after retries"
        assert len(call_count) == 3  # Retried 2 times, succeeded on 3rd

    def test_max_retries_exceeded(self):
        """Test that function raises after max retries."""
        call_count = []

        @retry_with_backoff(max_retries=2, initial_delay=0.01)
        def always_failing():
            call_count.append(1)
            raise ValueError("Always fails")

        with pytest.raises(ValueError, match="Always fails"):
            always_failing()

        assert len(call_count) == 3  # Initial + 2 retries

    def test_exponential_backoff(self):
        """Test exponential backoff delay."""
        timestamps = []

        @retry_with_backoff(max_retries=3, initial_delay=0.1, exponential_base=2, jitter=False)
        def timing_func():
            timestamps.append(time.time())
            if len(timestamps) < 3:
                raise ValueError("Retry")
            return "done"

        timing_func()

        # Check delays between attempts (approximately)
        # Delays should be: ~0.1s, ~0.2s
        delay1 = timestamps[1] - timestamps[0]
        delay2 = timestamps[2] - timestamps[1]

        assert 0.08 < delay1 < 0.15  # ~0.1s with some tolerance
        assert 0.18 < delay2 < 0.25  # ~0.2s with some tolerance

    def test_specific_exceptions_only(self):
        """Test retry only on specific exceptions."""
        call_count = []

        @retry_with_backoff(max_retries=3, initial_delay=0.01, exceptions=(ValueError,))
        def specific_error_func(error_type):
            call_count.append(1)
            if len(call_count) < 2:
                raise error_type("Error")
            return "success"

        # Should retry on ValueError
        call_count.clear()
        result = specific_error_func(ValueError)
        assert result == "success"
        assert len(call_count) == 2

        # Should NOT retry on TypeError (not in exceptions tuple)
        call_count.clear()
        with pytest.raises(TypeError):
            specific_error_func(TypeError)
        assert len(call_count) == 1  # No retry

    def test_on_retry_callback(self):
        """Test on_retry callback is called."""
        callback_calls = []

        def on_retry_callback(attempt, error, delay):
            callback_calls.append({
                'attempt': attempt,
                'error': str(error),
                'delay': delay
            })

        @retry_with_backoff(max_retries=2, initial_delay=0.01, on_retry=on_retry_callback, jitter=False)
        def failing_func():
            if len(callback_calls) < 2:
                raise ValueError(f"Attempt {len(callback_calls)}")
            return "success"

        result = failing_func()

        assert result == "success"
        assert len(callback_calls) == 2
        assert callback_calls[0]['attempt'] == 0
        assert callback_calls[1]['attempt'] == 1


class TestRetryOnTransientErrors:
    """Test retry_on_transient_errors decorator."""

    def test_retries_on_connection_error(self):
        """Test retry on ConnectionError."""
        call_count = []

        @retry_on_transient_errors(max_retries=3, initial_delay=0.01)
        def connection_func():
            call_count.append(1)
            if len(call_count) < 2:
                raise ConnectionError("Network error")
            return "connected"

        result = connection_func()

        assert result == "connected"
        assert len(call_count) == 2

    def test_retries_on_timeout_error(self):
        """Test retry on TimeoutError."""
        call_count = []

        @retry_on_transient_errors(max_retries=3, initial_delay=0.01)
        def timeout_func():
            call_count.append(1)
            if len(call_count) < 2:
                raise TimeoutError("Request timeout")
            return "success"

        result = timeout_func()

        assert result == "success"
        assert len(call_count) == 2


class TestSafeExecute:
    """Test safe_execute function."""

    def test_safe_execute_success(self):
        """Test safe_execute with successful function."""
        def successful_func(x, y):
            return x + y

        result = safe_execute(successful_func, 2, 3)
        assert result == 5

    def test_safe_execute_with_failure_returns_default(self):
        """Test safe_execute returns default on failure."""
        def failing_func():
            raise ValueError("Error")

        result = safe_execute(failing_func, max_retries=2, default_value="default")
        assert result == "default"

    def test_safe_execute_retries(self):
        """Test safe_execute retries before returning default."""
        call_count = []

        def sometimes_failing():
            call_count.append(1)
            if len(call_count) < 3:
                raise ValueError("Error")
            return "success"

        result = safe_execute(sometimes_failing, max_retries=3, default_value="default")
        assert result == "success"
        assert len(call_count) == 3


class TestRetryContext:
    """Test RetryContext context manager."""

    def test_retry_context_success(self):
        """Test retry context with successful execution."""
        with RetryContext(max_retries=3) as ctx:
            result = "success"

        assert result == "success"
        assert ctx.attempt == 0

    def test_retry_context_with_retry(self):
        """Test retry context retries on exception."""
        attempts = []

        for _ in range(5):  # Try multiple times
            try:
                with RetryContext(max_retries=3, initial_delay=0.01) as ctx:
                    attempts.append(ctx.attempt)
                    if ctx.attempt < 2:
                        raise ValueError("Retry needed")
                    break  # Success
            except ValueError:
                continue

        assert max(attempts) >= 2  # Should have retried at least twice
