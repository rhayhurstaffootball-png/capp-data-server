from fastapi import FastAPI, Query
from typing import Optional
from espn_fetcher import get_live_games, get_game_plays, start_poller

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
):
    return get_game_plays(game_id, league=league)
