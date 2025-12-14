def normalize_match(m):
    # Extract opponent and outcome data cleanly
    team1 = m.get("match2opponents", [{}])[0]
    team2 = m.get("match2opponents", [{}, {}])[1]
    winner_id = m.get("winner")

    return {
        "match_id": m.get("id"),
        "tournament_pagename": m.get("parent"),
        "date": m.get("date"),
        "bestof": m.get("extradata", {}).get("bestof"),
        "team1_id": team1.get("id"),
        "team1_name": team1.get("name"),
        "team1_score": team1.get("score"),
        "team2_id": team2.get("id"),
        "team2_name": team2.get("name"),
        "team2_score": team2.get("score"),
        "winner_id": winner_id,
        "team1_win": 1 if str(team1.get("id")) == str(winner_id) else 0,
        "team2_win": 1 if str(team2.get("id")) == str(winner_id) else 0,
    }