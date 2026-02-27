from fastapi import FastAPI, Query
from typing import Optional
from espn_fetcher import get_live_games, get_game_plays, get_game_version, start_poller


app = FastAPI(title="CAPP Data Server")

@app.on_event("startup")
def startup():
    start_poller()

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/games")
def games(
    league: str = Query("all", description="all, cfb, or nfl"),
    year: Optional[int] = Query(None, description="Season year e.g. 2025"),
    week: Optional[int] = Query(None, description="Week number"),
    seasontype: int = Query(2, description="2=regular, 3=postseason"),
):
    return get_live_games(league=league, year=year, week=week, seasontype=seasontype)

@app.get("/game/{game_id}/plays")
def plays(
    game_id: str,
    league: str = Query("cfb", description="cfb or nfl"),
    force_refresh: bool = Query(False, description="Bypass cache and re-fetch from API"),
):
    return get_game_plays(game_id, league=league, force_refresh=force_refresh)

@app.get("/game/{game_id}/version")
def game_version(game_id: str):
    """Lightweight endpoint — returns only the fetched_at timestamp for the
    cached entry.  Clients poll this every 60 s to detect retroactive data
    corrections without re-downloading the full play list each time."""
    return {"game_id": game_id, "fetched_at": get_game_version(game_id)}
