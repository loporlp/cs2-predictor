import pandas as pd
from src.fetch.matches import fetch_all_matches_for_tournament
from src.parse.matches import normalize_match

# Paths for input (tournaments) and output (matches)
INPUT_PATH = "data/processed/cs2_tournaments.csv"
OUTPUT_PATH = "data/processed/cs2_matches.csv"

def build_match_table():
    print("Starting match pipeline")

    # Load the tournament data built in the previous step
    try:
        df_tournaments = pd.read_csv(INPUT_PATH)
    except FileNotFoundError:
        print(f"Error: Tournament file not found at {INPUT_PATH}. Run build_tournaments.py first.")
        return

    # Filter out any tournaments without a pagename, as we can't query matches for them
    tournaments_to_process = df_tournaments[df_tournaments["pagename"].notna()]["pagename"].tolist()
    
    print(f"Loaded {len(df_tournaments)} tournaments. Processing {len(tournaments_to_process)} with pagenames.")

    all_raw_matches = []
    
    # Iterate over all unique tournament pagenames
    for i, pagename in enumerate(tournaments_to_process):
        print(f"[{i+1}/{len(tournaments_to_process)}] Processing {pagename}...")
        raw_matches = fetch_all_matches_for_tournament(pagename)
        all_raw_matches.extend(raw_matches)

    print("\nStarting data normalization...")
    
    # Parse the raw match data
    parsed_matches = [normalize_match(m) for m in all_raw_matches]
    
    # Create the final DataFrame
    df_matches = pd.DataFrame(parsed_matches)
    
    # Convert date to datetime and handle invalid entries
    df_matches["date"] = pd.to_datetime(df_matches["date"], errors='coerce')
    
    # Drop duplicates (in case the API returned duplicates across pages/queries)
    df_matches = df_matches.drop_duplicates(subset=["match_id"]).reset_index(drop=True)

    # Save the final table
    df_matches.to_csv(OUTPUT_PATH, index=False)
    print(f"\nSaved {len(df_matches)} unique matches → {OUTPUT_PATH}")


if __name__ == "__main__":
    build_match_table()