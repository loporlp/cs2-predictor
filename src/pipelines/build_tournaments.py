import pandas as pd
from src.fetch.tournaments import fetch_all_cs2_tournaments
from src.parse.tournaments import normalize_tournament

OUTPUT_PATH = "data/processed/cs2_tournaments.csv"

def build_tournament_table():
    print("Starting tournament pipeline")

    raw = fetch_all_cs2_tournaments()
    parsed = [normalize_tournament(t) for t in raw]

    df = pd.DataFrame(parsed)
    df["startdate"] = pd.to_datetime(df["startdate"])
    df["enddate"] = pd.to_datetime(df["enddate"])

    df.to_csv(OUTPUT_PATH, index=False)
    print(f"Saved {len(df)} tournaments → {OUTPUT_PATH}")


if __name__ == "__main__":
    build_tournament_table()
