"""
Retry handler with exponential backoff for CS2 Predictor.

Provides decorator for automatic retry of failed requests with configurable
backoff strategies and retryable error types.
"""

import time
import random
from functools import wraps
from typing import Callable, Tuple, Type, Optional
import requests

from .logger import get_utils_logger
from .exceptions import NetworkException, APIException

logger = get_utils_logger()

# Default retryable HTTP status codes
DEFAULT_RETRYABLE_STATUS_CODES = (429, 500, 502, 503, 504)

# Non-retryable status codes (client errors)
NON_RETRYABLE_STATUS_CODES = (400, 401, 403, 404)


def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    backoff_factor: float = 2.0,
    retryable_status_codes: Tuple[int, ...] = DEFAULT_RETRYABLE_STATUS_CODES,
    retryable_exceptions: Tuple[Type[Exception], ...] = (
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
        requests.exceptions.ChunkedEncodingError,
    )
):
    """
    Decorator to retry a function with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Initial delay in seconds before first retry
        backoff_factor: Multiplier for delay after each retry
        retryable_status_codes: HTTP status codes that should trigger retry
        retryable_exceptions: Exception types that should trigger retry

    Returns:
        Decorated function with retry logic

    Example:
        @retry_with_backoff(max_retries=3, base_delay=1.0)
        def fetch_data(url):
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception: Optional[Exception] = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)

                except requests.exceptions.HTTPError as e:
                    # Check if status code is retryable
                    if e.response is not None:
                        status_code = e.response.status_code

                        # Don't retry client errors
                        if status_code in NON_RETRYABLE_STATUS_CODES:
                            logger.error(
                                f"{func.__name__} failed with non-retryable status {status_code}: {e}"
                            )
                            raise APIException(
                                f"HTTP {status_code} error",
                                url=e.response.url,
                                status_code=status_code,
                                response_body=e.response.text[:500] if e.response.text else None
                            ) from e

                        # Check if status code is in retryable list
                        if status_code not in retryable_status_codes:
                            logger.error(
                                f"{func.__name__} failed with non-retryable status {status_code}: {e}"
                            )
                            raise APIException(
                                f"HTTP {status_code} error",
                                url=e.response.url,
                                status_code=status_code,
                                response_body=e.response.text[:500] if e.response.text else None
                            ) from e

                    last_exception = e

                    if attempt < max_retries:
                        # Calculate delay with exponential backoff and jitter
                        delay = base_delay * (backoff_factor ** attempt) + random.uniform(0, 1)
                        logger.warning(
                            f"{func.__name__} attempt {attempt + 1}/{max_retries + 1} failed "
                            f"with HTTP {e.response.status_code if e.response else 'unknown'}: {e}. "
                            f"Retrying in {delay:.2f}s..."
                        )
                        time.sleep(delay)
                    else:
                        logger.error(
                            f"{func.__name__} failed after {max_retries + 1} attempts: {e}"
                        )

                except retryable_exceptions as e:
                    last_exception = e

                    if attempt < max_retries:
                        # Calculate delay with exponential backoff and jitter
                        delay = base_delay * (backoff_factor ** attempt) + random.uniform(0, 1)
                        logger.warning(
                            f"{func.__name__} attempt {attempt + 1}/{max_retries + 1} failed "
                            f"with {type(e).__name__}: {e}. Retrying in {delay:.2f}s..."
                        )
                        time.sleep(delay)
                    else:
                        logger.error(
                            f"{func.__name__} failed after {max_retries + 1} attempts: {e}"
                        )

                except Exception as e:
                    # Non-retryable exception, raise immediately
                    logger.error(f"{func.__name__} failed with non-retryable exception: {e}")
                    raise

            # If we've exhausted all retries, raise NetworkException
            if last_exception:
                raise NetworkException(
                    f"Failed after {max_retries + 1} attempts",
                ) from last_exception

        return wrapper

    return decorator
