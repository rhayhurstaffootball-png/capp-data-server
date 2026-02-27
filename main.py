from fastapi import FastAPI, Query, Header, HTTPException, Depends
from typing import Optional
import os
from espn_fetcher import get_live_games, get_game_plays, get_game_version, start_poller


app = FastAPI(title="CAPP Data Server")

# --- API Key Auth ---
def _valid_keys() -> set:
    raw = os.environ.get("CAPP_API_KEYS", "")
    return {k.strip() for k in raw.split(",") if k.strip()}

def verify_api_key(x_api_key: str = Header(..., description="CAPP API key")):
    if x_api_key not in _valid_keys():
        raise HTTPException(status_code=401, detail="Invalid or missing API key")

@app.on_event("startup")
def startup():
    start_poller()

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/games", dependencies=[Depends(verify_api_key)])
def games(
    league: str = Query("all", description="all, cfb, or nfl"),
    year: Optional[int] = Query(None, description="Season year e.g. 2025"),
    week: Optional[int] = Query(None, description="Week number"),
    seasontype: int = Query(2, description="2=regular, 3=postseason"),
):
    return get_live_games(league=league, year=year, week=week, seasontype=seasontype)

@app.get("/game/{game_id}/plays", dependencies=[Depends(verify_api_key)])
def plays(
    game_id: str,
    league: str = Query("cfb", description="cfb or nfl"),
    force_refresh: bool = Query(False, description="Bypass cache and re-fetch from API"),
):
    return get_game_plays(game_id, league=league, force_refresh=force_refresh)

@app.get("/game/{game_id}/version", dependencies=[Depends(verify_api_key)])
def game_version(game_id: str):
    """Lightweight endpoint — returns only the fetched_at timestamp for the
    cached entry.  Clients poll this every 60 s to detect retroactive data
    corrections without re-downloading the full play list each time."""
    return {"game_id": game_id, "fetched_at": get_game_version(game_id)}
