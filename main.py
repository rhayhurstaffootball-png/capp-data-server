from fastapi import FastAPI, Query, Header, HTTPException, Depends, Body
from fastapi.responses import JSONResponse
from typing import Optional
import os
import json
import hashlib
import httpx

from espn_fetcher import get_live_games, get_game_plays, get_game_version, start_poller


app = FastAPI(title="CAPP Data Server")

# --- Supabase config ---
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
SUPABASE_BUCKET = "capp-workflow"

def _supabase_headers():
    return {
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "apikey": SUPABASE_KEY,
    }

def _storage_path(client_id: str, filename: str) -> str:
    return f"{client_id}/{filename}"

# --- API Key Auth ---
def verify_api_key(x_api_key: str = Header(..., description="CAPP API key")):
    url = f"{SUPABASE_URL}/rest/v1/capp_clients"
    params = {"api_key": f"eq.{x_api_key}", "select": "client_id,active"}
    with httpx.Client() as client:
        r = client.get(url, params=params, headers=_supabase_headers())
    if r.status_code != 200 or not r.json():
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
    if not r.json()[0].get("active"):
        raise HTTPException(status_code=401, detail="Account is not active")

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


# ── Auth Endpoints ─────────────────────────────────────────────────────────────

@app.post("/auth/login")
def auth_login(
    username: str = Body(..., embed=True),
    password: str = Body(..., embed=True),
):
    """Validate username/password and return client_id + api_key. No API key required."""
    url = f"{SUPABASE_URL}/rest/v1/capp_clients"
    params = {"username": f"eq.{username}", "select": "client_id,password_hash,salt,api_key,active"}
    with httpx.Client() as client:
        r = client.get(url, params=params, headers=_supabase_headers())
    if r.status_code != 200 or not r.json():
        raise HTTPException(status_code=401, detail="Invalid username or password")
    user = r.json()[0]
    if not user.get("active"):
        raise HTTPException(status_code=401, detail="Account is not active")
    expected = hashlib.sha256((password + user["salt"]).encode()).hexdigest()
    if expected != user["password_hash"]:
        raise HTTPException(status_code=401, detail="Invalid username or password")
    return {"client_id": user["client_id"], "api_key": user["api_key"]}


# ── Storage Endpoints ──────────────────────────────────────────────────────────

@app.post("/storage/save", dependencies=[Depends(verify_api_key)])
def storage_save(
    client_id: str = Query(...),
    filename: str = Query(...),
    payload: dict = Body(...),
):
    """Save a JSON file to Supabase storage for this client."""
    path = _storage_path(client_id, filename)
    url = f"{SUPABASE_URL}/storage/v1/object/{SUPABASE_BUCKET}/{path}"
    data = json.dumps(payload).encode()
    with httpx.Client() as client:
        # Try update first, then insert
        r = client.put(url, content=data, headers={
            **_supabase_headers(),
            "Content-Type": "application/json",
            "x-upsert": "true",
        })
    if r.status_code not in (200, 201):
        raise HTTPException(status_code=500, detail=f"Supabase save failed: {r.text}")
    return {"status": "saved", "path": path}


@app.get("/storage/load", dependencies=[Depends(verify_api_key)])
def storage_load(
    client_id: str = Query(...),
    filename: str = Query(...),
):
    """Load a JSON file from Supabase storage for this client."""
    path = _storage_path(client_id, filename)
    url = f"{SUPABASE_URL}/storage/v1/object/{SUPABASE_BUCKET}/{path}"
    with httpx.Client() as client:
        r = client.get(url, headers=_supabase_headers())
    if r.status_code == 404:
        raise HTTPException(status_code=404, detail="File not found")
    if r.status_code != 200:
        raise HTTPException(status_code=500, detail=f"Supabase load failed: {r.text}")
    return r.json()


@app.get("/storage/list", dependencies=[Depends(verify_api_key)])
def storage_list(client_id: str = Query(...)):
    """List all stored files for this client."""
    url = f"{SUPABASE_URL}/storage/v1/object/list/{SUPABASE_BUCKET}"
    with httpx.Client() as client:
        r = client.post(url, json={"prefix": f"{client_id}/"},
                        headers={**_supabase_headers(), "Content-Type": "application/json"})
    if r.status_code != 200:
        raise HTTPException(status_code=500, detail=f"Supabase list failed: {r.text}")
    return r.json()
