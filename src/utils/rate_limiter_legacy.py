import time

# Rate limit parameters (60 requests per 1 hour)
MAX_REQUESTS_PER_WINDOW = 60
WINDOW_SECONDS = 3600 

# Global state for the rate limiter
request_count = 0
window_start_time = time.time() 

def wait_for_api_call():
    """
    Implements a burst rate limit: allows 60 requests as fast as possible,
    then waits for the remainder of the 1-hour window.
    """
    global request_count, window_start_time
    current_time = time.time()

    # 1. Check if the current window has expired
    # If the elapsed time is greater than the window size, the window has reset.
    if (current_time - window_start_time) >= WINDOW_SECONDS:
        print("RATE LIMITER: Hour window expired. Resetting request count.")
        request_count = 0
        window_start_time = current_time

    # 2. Check if the request limit has been hit
    if request_count >= MAX_REQUESTS_PER_WINDOW:
        # Limit hit. Calculate time remaining until the window resets.
        elapsed_time = current_time - window_start_time
        time_to_wait = WINDOW_SECONDS - elapsed_time
        
        print(f"RATE LIMITING: Limit of {MAX_REQUESTS_PER_WINDOW} requests hit for the current hour.")
        print(f"RATE LIMITING: Waiting for {time_to_wait:.2f} seconds until the window resets.")
        
        # Wait for the remaining time
        time.sleep(time_to_wait)

        # After the wait, reset the counter and start a new window
        request_count = 0
        window_start_time = time.time()
        print("RATE LIMITING: Window reset. Continuing requests.")
    
    # 3. Allow the request and increment the counter
    request_count += 1
    print(f"RATE LIMITER: Request {request_count}/{MAX_REQUESTS_PER_WINDOW} allowed.")