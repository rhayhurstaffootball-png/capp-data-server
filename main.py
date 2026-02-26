from fastapi import FastAPI
from espn_fetcher import get_live_games, get_game_plays, start_poller

app = FastAPI(title="CAPP Data Server")

@app.on_event("startup")
def startup():
    start_poller()

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/games")
def games():
    return get_live_games()

@app.get("/game/{game_id}/plays")
def plays(game_id: str):
    return get_game_plays(game_id)
