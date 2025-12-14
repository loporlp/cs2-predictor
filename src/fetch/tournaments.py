import requests
from src.config import BASE_URLS, HEADERS

def fetch_tournaments(start_date, end_date, limit=1000):
    conditions = (
        f"[[startdate::>{start_date}]] AND [[startdate::<{end_date}]]"
    )

    params = {
        "wiki": "counterstrike",
        "limit": limit,
        "conditions": conditions,
        "order": "startdate ASC",
    }

    r = requests.get(BASE_URLS["tournament"], headers=HEADERS, params=params)
    r.raise_for_status()
    return r.json()


def fetch_all_cs2_tournaments(
    start_date="2023-10-15",
    end_date="2025-12-01"
):
    all_tournaments = []
    current_start = start_date

    while True:
        data = fetch_tournaments(current_start, end_date)
        tournaments = data.get("result", [])

        if not tournaments:
            break

        all_tournaments.extend(tournaments)

        last_date = tournaments[-1]["startdate"]
        if last_date == current_start:
            break

        current_start = last_date
        print(f"Fetched {len(tournaments)} tournaments, next start: {current_start}")

        if len(tournaments) < 1000:
            break

    return all_tournaments
