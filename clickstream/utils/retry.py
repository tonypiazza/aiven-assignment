# ==============================================================================
# Retry Configuration
# ==============================================================================
"""
Shared retry configuration for network resilience.

Provides reusable retry decorators with exponential backoff for handling
transient network failures across Kafka, PostgreSQL, Valkey, and Aiven API.

Standard retry: 10 attempts over ~60 seconds (for streaming pipelines)
Light retry: 3 attempts over ~7 seconds (for status checks)
"""

import logging
from typing import Tuple, Type

from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

# ==============================================================================
# Retry Constants
# ==============================================================================

# Standard retry configuration: 10 retries over ~60 seconds
# Exponential backoff: 1s, 2s, 4s, 8s, 16s, 32s, 32s, 32s, 32s, 32s = ~63s total
RETRY_ATTEMPTS = 10
RETRY_WAIT_MIN = 1  # seconds
RETRY_WAIT_MAX = 32  # seconds (cap for exponential backoff)

# Light retry configuration: 3 retries over ~7 seconds (for status checks)
# Exponential backoff: 1s, 2s, 4s = ~7s total
RETRY_ATTEMPTS_LIGHT = 3

# Valkey retry configuration (used by redis-py client)
VALKEY_RETRIES = 10


# ==============================================================================
# Logging Callbacks
# ==============================================================================


def log_retry_attempt(logger: logging.Logger):
    """
    Create a callback that logs retry attempts.

    Args:
        logger: Logger instance to use for logging

    Returns:
        Callback function for tenacity's before_sleep parameter
    """

    def _log_retry(retry_state: RetryCallState) -> None:
        exception = retry_state.outcome.exception() if retry_state.outcome else None
        logger.warning(
            "Retry attempt %d/%d after error: %s",
            retry_state.attempt_number,
            RETRY_ATTEMPTS,
            exception,
        )

    return _log_retry


def log_retry_attempt_light(logger: logging.Logger):
    """
    Create a callback that logs retry attempts for light retry.

    Args:
        logger: Logger instance to use for logging

    Returns:
        Callback function for tenacity's before_sleep parameter
    """

    def _log_retry(retry_state: RetryCallState) -> None:
        exception = retry_state.outcome.exception() if retry_state.outcome else None
        logger.warning(
            "Retry attempt %d/%d after error: %s",
            retry_state.attempt_number,
            RETRY_ATTEMPTS_LIGHT,
            exception,
        )

    return _log_retry


# ==============================================================================
# Retry Decorators
# ==============================================================================


def retry_standard(exception_types: Tuple[Type[Exception], ...], logger: logging.Logger):
    """
    Create a standard retry decorator (10 attempts, ~60 seconds).

    Use this for streaming pipeline operations that need high resilience.

    Args:
        exception_types: Tuple of exception types to retry on
        logger: Logger instance for retry logging

    Returns:
        Tenacity retry decorator

    Example:
        @retry_standard((OperationalError, InterfaceError), logger)
        def batch_write(self, messages):
            ...
    """
    return retry(
        stop=stop_after_attempt(RETRY_ATTEMPTS),
        wait=wait_exponential(multiplier=1, min=RETRY_WAIT_MIN, max=RETRY_WAIT_MAX),
        retry=retry_if_exception_type(exception_types),
        before_sleep=log_retry_attempt(logger),
        reraise=True,
    )


def retry_light(exception_types: Tuple[Type[Exception], ...], logger: logging.Logger):
    """
    Create a light retry decorator (3 attempts, ~7 seconds).

    Use this for status checks and non-critical operations.

    Args:
        exception_types: Tuple of exception types to retry on
        logger: Logger instance for retry logging

    Returns:
        Tenacity retry decorator

    Example:
        @retry_light((ConnectionError, TimeoutError), logger)
        def check_service_status():
            ...
    """
    return retry(
        stop=stop_after_attempt(RETRY_ATTEMPTS_LIGHT),
        wait=wait_exponential(multiplier=1, min=RETRY_WAIT_MIN, max=RETRY_WAIT_MAX),
        retry=retry_if_exception_type(exception_types),
        before_sleep=log_retry_attempt_light(logger),
        reraise=True,
    )


# ==============================================================================
# Pre-configured Decorators for Common Use Cases
# ==============================================================================

# Import common exception types for convenience
try:
    import psycopg2

    POSTGRES_RETRY_EXCEPTIONS = (
        psycopg2.OperationalError,
        psycopg2.InterfaceError,
    )
except ImportError:
    POSTGRES_RETRY_EXCEPTIONS = ()

try:
    import requests

    HTTP_RETRY_EXCEPTIONS = (
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
    )
except ImportError:
    HTTP_RETRY_EXCEPTIONS = ()

try:
    from redis.exceptions import ConnectionError as RedisConnectionError
    from redis.exceptions import TimeoutError as RedisTimeoutError

    REDIS_RETRY_EXCEPTIONS = (
        RedisConnectionError,
        RedisTimeoutError,
    )
except ImportError:
    REDIS_RETRY_EXCEPTIONS = ()
