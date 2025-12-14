import requests
from src.config import BASE_URLS, HEADERS
from src.utils.rate_limiter import wait_for_api_call

def fetch_matches(tournament_pagename, offset=0, limit=1000):
    """Fetches a single page of matches for a given tournament pagename."""
    
    # Enforce rate limit before calling the API
    wait_for_api_call()
    
    conditions = f"[[parent::{tournament_pagename}]]"

    params = {
        "wiki": "counterstrike",
        "limit": limit,
        "offset": offset,
        "conditions": conditions,
        "order": "date ASC",
        "includehidden": "true",
    }

    try:
        r = requests.get(BASE_URLS["match"], headers=HEADERS, params=params)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.HTTPError as e:
        print(f"Error fetching matches for {tournament_pagename}: {e}")
        return {"result": []}


def fetch_all_matches_for_tournament(tournament_pagename):
    """Paginates through all matches for a single tournament."""
    all_matches = []
    offset = 0
    limit = 1000
    
    print(f"  -> Starting fetch for: {tournament_pagename}")

    while True:
        data = fetch_matches(tournament_pagename, offset=offset, limit=limit)
        matches = data.get("result", [])

        if not matches:
            break

        all_matches.extend(matches)
        
        if len(matches) < limit:
            break

        offset += limit

    print(f"  -> Fetched {len(all_matches)} matches.")
    return all_matches