"""
Rate limiter with configurable strategies for CS2 Predictor.

Provides thread-safe rate limiting with distributed and burst strategies.
Uses sliding window approach for accurate rate limiting.
"""

import time
import threading
from collections import deque
from typing import Literal, Dict, Any
from enum import Enum

from .logger import get_utils_logger

logger = get_utils_logger()


class RateLimitStrategy(Enum):
    """Rate limiting strategies."""
    DISTRIBUTED = "distributed"  # Spread requests evenly across window
    BURST = "burst"  # Allow rapid requests until limit


class RateLimiter:
    """
    Thread-safe rate limiter with configurable strategies.

    Uses a sliding window approach to track requests and enforce limits.
    """

    def __init__(
        self,
        max_requests: int = 60,
        window_seconds: int = 3600,
        strategy: Literal["distributed", "burst"] = "distributed"
    ):
        """
        Initialize rate limiter.

        Args:
            max_requests: Maximum number of requests allowed per window
            window_seconds: Time window in seconds
            strategy: Rate limiting strategy ("distributed" or "burst")
        """
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.strategy = RateLimitStrategy(strategy)

        # Thread-safe request tracking
        self._lock = threading.Lock()
        self._request_times: deque = deque()

        # Distributed strategy: minimum interval between requests
        self._min_interval = window_seconds / max_requests if strategy == "distributed" else 0
        self._last_request_time = 0.0

        logger.info(
            f"RateLimiter initialized: {max_requests} requests per {window_seconds}s "
            f"using {strategy} strategy"
        )

    def wait_if_needed(self) -> None:
        """
        Wait if necessary to comply with rate limit.

        This method blocks until it's safe to proceed with the next request.
        """
        with self._lock:
            now = time.monotonic()

            # Remove requests outside the current window
            cutoff_time = now - self.window_seconds
            while self._request_times and self._request_times[0] < cutoff_time:
                self._request_times.popleft()

            # Distributed strategy: ensure minimum interval between requests
            if self.strategy == RateLimitStrategy.DISTRIBUTED:
                time_since_last = now - self._last_request_time
                if time_since_last < self._min_interval:
                    wait_time = self._min_interval - time_since_last
                    logger.debug(f"Distributed strategy: waiting {wait_time:.2f}s for minimum interval")
                    time.sleep(wait_time)
                    now = time.monotonic()

            # Check if we've hit the rate limit
            if len(self._request_times) >= self.max_requests:
                # Calculate how long to wait until the oldest request falls out of the window
                oldest_request = self._request_times[0]
                wait_time = (oldest_request + self.window_seconds) - now

                if wait_time > 0:
                    logger.info(
                        f"Rate limit reached ({len(self._request_times)}/{self.max_requests} requests). "
                        f"Waiting {wait_time:.2f}s..."
                    )
                    time.sleep(wait_time)
                    now = time.monotonic()

                    # Clean up old requests after waiting
                    cutoff_time = now - self.window_seconds
                    while self._request_times and self._request_times[0] < cutoff_time:
                        self._request_times.popleft()

            # Record this request
            self._request_times.append(now)
            self._last_request_time = now

    def get_stats(self) -> Dict[str, Any]:
        """
        Get current rate limiter statistics.

        Returns:
            Dictionary with statistics:
                - requests_in_window: Number of requests in current window
                - max_requests: Maximum allowed requests
                - window_seconds: Window duration in seconds
                - strategy: Current strategy
                - time_until_reset: Seconds until oldest request expires (if at limit)
        """
        with self._lock:
            now = time.monotonic()
            cutoff_time = now - self.window_seconds

            # Remove old requests
            while self._request_times and self._request_times[0] < cutoff_time:
                self._request_times.popleft()

            time_until_reset = None
            if self._request_times:
                oldest_request = self._request_times[0]
                time_until_reset = max(0, (oldest_request + self.window_seconds) - now)

            return {
                "requests_in_window": len(self._request_times),
                "max_requests": self.max_requests,
                "window_seconds": self.window_seconds,
                "strategy": self.strategy.value,
                "time_until_reset": time_until_reset
            }

    def reset(self) -> None:
        """Reset the rate limiter state (clear all tracked requests)."""
        with self._lock:
            self._request_times.clear()
            self._last_request_time = 0.0
            logger.info("RateLimiter reset")
