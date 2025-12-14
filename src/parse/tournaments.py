def normalize_tournament(t):
    return {
        "tournament_id": t.get("id"),
        "name": t.get("name"),
        "pagename": t.get("pagename"),
        "startdate": t.get("startdate"),
        "enddate": t.get("enddate"),
        "tier": t.get("liquipediatier"),
        "prizepool": t.get("prizepool"),
        "location": t.get("location"),
        "type": t.get("type"),
        "game": t.get("game"),
    }
