"""
Custom exception hierarchy for CS2 Predictor data fetching.

Provides domain-specific exceptions with request context for better error handling
and debugging.
"""

from typing import Optional, Dict, Any


class DataFetchException(Exception):
    """Base exception for all data fetching errors."""

    def __init__(
        self,
        message: str,
        url: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        status_code: Optional[int] = None,
        response_body: Optional[str] = None
    ):
        super().__init__(message)
        self.message = message
        self.url = url
        self.params = params
        self.status_code = status_code
        self.response_body = response_body

    def __str__(self):
        parts = [self.message]
        if self.url:
            parts.append(f"URL: {self.url}")
        if self.params:
            parts.append(f"Params: {self.params}")
        if self.status_code:
            parts.append(f"Status: {self.status_code}")
        if self.response_body:
            # Truncate response body to avoid excessive output
            body_preview = self.response_body[:200] + "..." if len(self.response_body) > 200 else self.response_body
            parts.append(f"Response: {body_preview}")
        return " | ".join(parts)


class NetworkException(DataFetchException):
    """Exception for network-related errors (timeouts, connection failures)."""
    pass


class APIException(DataFetchException):
    """Exception for API-specific errors (invalid responses, HTTP errors)."""
    pass


class RateLimitException(DataFetchException):
    """Exception for rate limit errors."""
    pass


class DataValidationException(DataFetchException):
    """Exception for data validation failures."""
    pass


class PaginationException(DataFetchException):
    """Exception for pagination-related errors (infinite loops, invalid offsets)."""
    pass
