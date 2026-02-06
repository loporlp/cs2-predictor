import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("LIQUIPEDIA_API_KEY")

BASE_URLS = {
    "tournament": "https://api.liquipedia.net/api/v3/tournament",
    "match": "https://api.liquipedia.net/api/v3/match"
}

HEADERS = {
    "accept": "application/json",
    "authorization": f"Apikey {API_KEY}",
}

# Request timeout settings (in seconds)
CONNECT_TIMEOUT = 10  # Time to wait for connection to be established
READ_TIMEOUT = 30  # Time to wait for server response

# Retry configuration
MAX_RETRIES = 3  # Maximum number of retry attempts
RETRY_BASE_DELAY = 1  # Initial delay in seconds before first retry
RETRY_BACKOFF_FACTOR = 2  # Multiplier for delay after each retry

# Rate limiting configuration
RATE_LIMIT_MAX_REQUESTS = 60  # Maximum requests per window
RATE_LIMIT_WINDOW = 3600  # Time window in seconds (1 hour)
RATE_LIMIT_STRATEGY = "distributed"  # "distributed" or "burst"

# Pagination safety
MAX_PAGINATION_ITERATIONS = 100  # Maximum pagination loops to prevent infinite loops

# Validation configuration
STRICT_DATE_VALIDATION = False  # Whether to fail on invalid dates or coerce them

# Initialize global rate limiter
from .utils.rate_limiter_v2 import RateLimiter

rate_limiter = RateLimiter(
    max_requests=RATE_LIMIT_MAX_REQUESTS,
    window_seconds=RATE_LIMIT_WINDOW,
    strategy=RATE_LIMIT_STRATEGY
)
