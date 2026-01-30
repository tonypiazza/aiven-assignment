# ==============================================================================
# Aiven API Client
# ==============================================================================
"""
Aiven REST API client for fast service status checks.

This module provides functions to check Aiven service status via the REST API,
which is significantly faster than making direct connections (especially for
Kafka with mTLS authentication).

Includes light retry logic (3 attempts, ~7 seconds) for network resilience.

API Documentation: https://api.aiven.io/doc/
"""

from typing import Any, Optional

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from clickstream.utils.config import get_settings
from clickstream.utils.retry import (
    RETRY_ATTEMPTS_LIGHT,
    RETRY_WAIT_MIN,
    RETRY_WAIT_MAX,
    log_retry_attempt_light,
)

AIVEN_API_BASE = "https://api.aiven.io/v1"
DEFAULT_TIMEOUT = 10  # seconds


# ==============================================================================
# Generic Service Status
# ==============================================================================


def _make_service_request(
    service_name: str, settings: Any, timeout: int
) -> Optional[dict[str, Any]]:
    """
    Make a request to the Aiven API for service status.

    This is a helper function that handles the actual HTTP request,
    allowing for retry logic to be applied.

    Args:
        service_name: Name of the Aiven service
        settings: Settings object with Aiven configuration
        timeout: Request timeout in seconds

    Returns:
        Dictionary with service status information, or None if request fails.

    Raises:
        requests.exceptions.ConnectionError: On connection errors (retryable)
        requests.exceptions.Timeout: On timeout (retryable)
    """
    url = f"{AIVEN_API_BASE}/project/{settings.aiven.project_name}/service/{service_name}"
    headers = {"Authorization": f"Bearer {settings.aiven.api_token}"}

    response = requests.get(url, headers=headers, timeout=timeout)
    response.raise_for_status()
    data = response.json()
    service = data.get("service", {})

    return {
        "state": service.get("state"),
        "plan": service.get("plan"),
        "node_states": service.get("node_states", []),
        "service_uri": service.get("service_uri"),
        "service_type": service.get("service_type"),
    }


@retry(
    stop=stop_after_attempt(RETRY_ATTEMPTS_LIGHT),
    wait=wait_exponential(multiplier=1, min=RETRY_WAIT_MIN, max=RETRY_WAIT_MAX),
    retry=retry_if_exception_type(
        (requests.exceptions.ConnectionError, requests.exceptions.Timeout)
    ),
    before_sleep=log_retry_attempt_light,
    reraise=True,
)
def _make_service_request_with_retry(
    service_name: str, settings: Any, timeout: int
) -> Optional[dict[str, Any]]:
    """
    Make a request to the Aiven API with retry logic.

    Retries on connection errors and timeouts (3 attempts, ~7 seconds).
    """
    return _make_service_request(service_name, settings, timeout)


def get_service_status(
    service_name: str, timeout: int = DEFAULT_TIMEOUT
) -> Optional[dict[str, Any]]:
    """
    Get service status from Aiven API.

    Uses light retry logic (3 attempts, ~7 seconds) for connection errors.

    Args:
        service_name: Name of the Aiven service
        timeout: Request timeout in seconds

    Returns:
        Dictionary with service status information:
            - state: "RUNNING", "POWEROFF", "REBUILDING", etc.
            - plan: Service plan (e.g., "startup-2")
            - node_states: List of node health states
            - service_uri: Service connection URI

        Returns None if API is not configured or request fails.
    """
    settings = get_settings()
    if not settings.aiven.is_configured:
        return None

    try:
        return _make_service_request_with_retry(service_name, settings, timeout)
    except requests.exceptions.Timeout:
        return None
    except requests.exceptions.RequestException:
        return None
    except (KeyError, ValueError):
        return None


def is_service_running(service_name: str, timeout: int = DEFAULT_TIMEOUT) -> Optional[bool]:
    """
    Check if an Aiven service is running.

    Args:
        service_name: Name of the Aiven service
        timeout: Request timeout in seconds

    Returns:
        True if service is running, False if not running,
        None if unable to determine (API not configured or request failed).
    """
    status = get_service_status(service_name, timeout)
    if status is None:
        return None
    return status.get("state") == "RUNNING"


# ==============================================================================
# Service-Specific Status Functions
# ==============================================================================


def get_kafka_status(timeout: int = DEFAULT_TIMEOUT) -> Optional[dict[str, Any]]:
    """
    Get Kafka service status via Aiven API.

    Returns:
        Service status dict or None if unavailable.
    """
    settings = get_settings()
    service_name = settings.aiven.get_service_name("kafka")
    if not service_name:
        return None
    return get_service_status(service_name, timeout)


def get_postgres_status(timeout: int = DEFAULT_TIMEOUT) -> Optional[dict[str, Any]]:
    """
    Get PostgreSQL service status via Aiven API.

    Returns:
        Service status dict or None if unavailable.
    """
    settings = get_settings()
    service_name = settings.aiven.get_service_name("pg")
    if not service_name:
        return None
    return get_service_status(service_name, timeout)


def get_opensearch_status(timeout: int = DEFAULT_TIMEOUT) -> Optional[dict[str, Any]]:
    """
    Get OpenSearch service status via Aiven API.

    Returns:
        Service status dict or None if unavailable.
    """
    settings = get_settings()
    service_name = settings.aiven.get_service_name("opensearch")
    if not service_name:
        return None
    return get_service_status(service_name, timeout)


def get_valkey_status(timeout: int = DEFAULT_TIMEOUT) -> Optional[dict[str, Any]]:
    """
    Get Valkey service status via Aiven API.

    Returns:
        Service status dict or None if unavailable.
    """
    settings = get_settings()
    service_name = settings.aiven.get_service_name("valkey")
    if not service_name:
        return None
    return get_service_status(service_name, timeout)


# ==============================================================================
# Convenience Functions
# ==============================================================================


def check_all_services(timeout: int = DEFAULT_TIMEOUT) -> dict[str, Optional[dict[str, Any]]]:
    """
    Check status of all configured Aiven services.

    Returns:
        Dictionary mapping service type to status:
        {
            "kafka": {...} or None,
            "postgres": {...} or None,
            "opensearch": {...} or None,
            "valkey": {...} or None,
        }
    """
    return {
        "kafka": get_kafka_status(timeout),
        "postgres": get_postgres_status(timeout),
        "opensearch": get_opensearch_status(timeout),
        "valkey": get_valkey_status(timeout),
    }


def is_aiven_configured() -> bool:
    """
    Check if Aiven API is configured.

    Returns:
        True if API token and project name are set, False otherwise.
    """
    settings = get_settings()
    return settings.aiven.is_configured
