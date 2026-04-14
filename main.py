from fastapi import FastAPI, Query, Header, HTTPException, Depends, Body, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response, HTMLResponse
from typing import Optional, Dict, List
import os
import json
import hashlib
import httpx
import sqlite3
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from contextlib import asynccontextmanager

from espn_fetcher import get_live_games, get_game_plays, get_game_version, start_poller
from db_updater import run_update


SERVER_DB_PATH = os.path.join(os.path.dirname(__file__), "workflow_server.db")

def get_db_meta():
    """Read current version info from workflow_server.db."""
    try:
        conn = sqlite3.connect(SERVER_DB_PATH)
        cur  = conn.cursor()
        cur.execute("SELECT version, updated_at, season FROM db_meta WHERE id=1")
        row = cur.fetchone()
        conn.close()
        if row:
            return {"version": row[0], "updated_at": row[1], "season": row[2]}
    except Exception:
        pass
    return None

def _bootstrap_db():
    """
    On startup, download workflow_server.db from Supabase if not present locally.
    Render's filesystem is ephemeral — the DB gets wiped on every deploy.
    """
    if os.path.exists(SERVER_DB_PATH):
        return  # already there (local dev)
    supabase_url = os.environ.get("SUPABASE_URL", "")
    supabase_key = os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not supabase_url or not supabase_key:
        print("WARNING: No Supabase credentials — cannot bootstrap DB")
        return
    url = f"{supabase_url}/storage/v1/object/capp-workflow/shared/workflow.db"
    headers = {"Authorization": f"Bearer {supabase_key}", "apikey": supabase_key}
    print("Bootstrapping workflow_server.db from Supabase...")
    r = httpx.get(url, headers=headers, timeout=120, follow_redirects=True)
    if r.status_code == 200:
        with open(SERVER_DB_PATH, "wb") as f:
            f.write(r.content)
        print(f"DB bootstrapped ({len(r.content)//1024} KB)")
    else:
        print(f"WARNING: DB bootstrap failed ({r.status_code})")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Download DB from Supabase if not present (Render ephemeral disk)
    _bootstrap_db()
    # Start scheduler
    scheduler = AsyncIOScheduler()
    # Run at 6:00 AM and 6:00 PM UTC daily
    scheduler.add_job(lambda: run_update(), "cron", hour="6,18", minute=0,
                      id="db_update", replace_existing=True)
    scheduler.start()
    yield
    scheduler.shutdown()

app = FastAPI(title="CAPP Data Server", lifespan=lifespan)

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

def get_client_id(x_api_key: str = Header(..., description="CAPP API key")) -> str:
    """Like verify_api_key but returns the client_id for use in endpoints."""
    url = f"{SUPABASE_URL}/rest/v1/capp_clients"
    params = {"api_key": f"eq.{x_api_key}", "select": "client_id,active"}
    with httpx.Client() as client:
        r = client.get(url, params=params, headers=_supabase_headers())
    if r.status_code != 200 or not r.json():
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
    row = r.json()[0]
    if not row.get("active"):
        raise HTTPException(status_code=401, detail="Account is not active")
    return row["client_id"]

@app.on_event("startup")
def startup():
    start_poller()

@app.get("/health")
def health():
    return {"status": "ok", "version": "2"}

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
    try:
        return get_game_plays(game_id, league=league, force_refresh=force_refresh)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}")

@app.get("/game/{game_id}/version", dependencies=[Depends(verify_api_key)])
def game_version(game_id: str):
    """Lightweight endpoint — returns only the fetched_at timestamp for the
    cached entry.  Clients poll this every 60 s to detect retroactive data
    corrections without re-downloading the full play list each time."""
    return {"game_id": game_id, "fetched_at": get_game_version(game_id)}


# ── Auth Endpoints ─────────────────────────────────────────────────────────────

@app.post("/nodes/login")
def nodes_login(
    username: str = Body(..., embed=True),
    password: str = Body(..., embed=True),
):
    """Authenticate a CAPP Node agent by username/password.
    Returns the api_key for that account — no seat or machine binding."""
    url = f"{SUPABASE_URL}/rest/v1/capp_clients"
    params = {
        "username": f"eq.{username}",
        "select": "client_id,password_hash,salt,api_key,active"
    }
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
    return {"api_key": user["api_key"], "client_id": user["client_id"]}


@app.post("/auth/login")
def auth_login(
    username: str = Body(..., embed=True),
    password: str = Body(..., embed=True),
    seat: str = Body(..., embed=True),       # "seat_1" or "seat_2"
    machine_id: str = Body(..., embed=True), # hashed machine fingerprint
):
    """Validate credentials, enforce machine binding, return client_id + api_key."""
    url = f"{SUPABASE_URL}/rest/v1/capp_clients"
    params = {
        "username": f"eq.{username}",
        "select": "client_id,password_hash,salt,api_key,active,is_admin,seat_1_machine,seat_2_machine"
    }
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

    # --- Machine binding (skipped for admin accounts) ---
    if not user.get("is_admin"):
        seat_col = "seat_1_machine" if seat == "seat_1" else "seat_2_machine"
        bound_machine = user.get(seat_col)
        if bound_machine is None:
            # First activation on this seat — bind this machine
            with httpx.Client() as client:
                client.patch(
                    f"{SUPABASE_URL}/rest/v1/capp_clients",
                    params={"username": f"eq.{username}"},
                    json={seat_col: machine_id},
                    headers={**_supabase_headers(), "Prefer": "return=minimal"},
                )
        elif bound_machine != machine_id:
            raise HTTPException(
                status_code=403,
                detail="This seat is already activated on a different machine. Contact your administrator."
            )

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


# ── DB Update Endpoints ───────────────────────────────────────────────────────

@app.get("/db/version")
def db_version():
    """
    Public endpoint — returns current DB version and timestamp.
    Clients call this first to decide whether to download a new DB.
    """
    meta = get_db_meta()
    if not meta:
        raise HTTPException(status_code=503, detail="DB metadata unavailable")
    return meta

@app.get("/db/download", dependencies=[Depends(verify_api_key)])
def db_download():
    """
    Returns a signed Supabase URL for the client to download workflow.db directly.
    Auth required — client must have a valid API key.
    File transfer goes Supabase -> Client (not through Render).
    """
    # Generate a signed URL (1 hour expiry)
    url = f"{SUPABASE_URL}/storage/v1/object/sign/{SUPABASE_BUCKET}/shared/workflow.db"
    headers = {**_supabase_headers(), "Content-Type": "application/json"}
    import httpx as _httpx
    r = _httpx.post(url, json={"expiresIn": 3600}, headers=headers)
    if r.status_code == 200:
        signed = r.json().get("signedURL") or r.json().get("signedUrl", "")
        if signed:
            # Supabase returns a relative path like /object/sign/...
            # The full URL needs the /storage/v1 prefix
            if signed.startswith("/"):
                signed = f"{SUPABASE_URL}/storage/v1{signed}"
            return {"download_url": signed}
    raise HTTPException(status_code=502, detail="Could not generate download URL")

@app.get("/contacts/version")
def contacts_version():
    """
    Public endpoint — returns current contacts.xlsx version.
    Increment CONTACTS_VERSION env var when uploading a new contacts.xlsx to Supabase.
    """
    version = int(os.environ.get("CONTACTS_VERSION", "1"))
    return {"version": version}

@app.get("/contacts/download", dependencies=[Depends(verify_api_key)])
def contacts_download():
    """
    Returns a signed Supabase URL for downloading contacts.xlsx.
    Auth required — client must have a valid API key.
    """
    url = f"{SUPABASE_URL}/storage/v1/object/sign/{SUPABASE_BUCKET}/shared/contacts.xlsx"
    headers = {**_supabase_headers(), "Content-Type": "application/json"}
    import httpx as _httpx
    r = _httpx.post(url, json={"expiresIn": 3600}, headers=headers)
    if r.status_code == 200:
        signed = r.json().get("signedURL") or r.json().get("signedUrl", "")
        if signed:
            if signed.startswith("/"):
                signed = f"{SUPABASE_URL}/storage/v1{signed}"
            return {"download_url": signed}
    raise HTTPException(status_code=502, detail="Could not generate download URL")

@app.get("/agent/download")
def agent_download():
    """
    Public endpoint — returns a signed Supabase URL for downloading CAPPNodes_Agent.exe.
    No auth required so new clients can download before they have an API key.
    File transfer goes Supabase -> Client (not through Render).
    """
    url = f"{SUPABASE_URL}/storage/v1/object/sign/{SUPABASE_BUCKET}/shared/CAPPNodes_Agent.exe"
    headers = {**_supabase_headers(), "Content-Type": "application/json"}
    import httpx as _httpx
    r = _httpx.post(url, json={"expiresIn": 3600}, headers=headers)
    if r.status_code == 200:
        signed = r.json().get("signedURL") or r.json().get("signedUrl", "")
        if signed:
            if signed.startswith("/"):
                signed = f"{SUPABASE_URL}/storage/v1{signed}"
            return {"download_url": signed}
    raise HTTPException(status_code=502, detail="Could not generate download URL")

@app.post("/db/update", dependencies=[Depends(verify_api_key)])
def db_force_update():
    """
    Admin endpoint — manually trigger a DB update cycle immediately.
    Useful for pushing a fix without waiting for the scheduled run.
    """
    import threading
    threading.Thread(target=run_update, daemon=True).start()
    return {"status": "update started"}


# ── App Update Endpoints ──────────────────────────────────────────────────────

@app.get("/app/version")
def app_version():
    """
    Public endpoint — returns current app version.
    Clients compare this to their local APP_VERSION to decide whether to update.
    Bump APP_VERSION env var on Render when a new installer is uploaded to Supabase.
    """
    version = os.environ.get("APP_VERSION", "2.0.0")
    return {"version": version}

@app.get("/app/download", dependencies=[Depends(verify_api_key)])
def app_download():
    """
    Returns a signed Supabase URL for downloading the latest CAPP installer.
    Auth required. File transfer goes Supabase -> Client (not through Render).
    Upload new installer to Supabase at: capp-workflow/shared/CAPP_Setup.exe
    """
    url = f"{SUPABASE_URL}/storage/v1/object/sign/{SUPABASE_BUCKET}/shared/CAPP_Setup.exe"
    headers = {**_supabase_headers(), "Content-Type": "application/json"}
    import httpx as _httpx
    r = _httpx.post(url, json={"expiresIn": 3600}, headers=headers)
    if r.status_code == 200:
        signed = r.json().get("signedURL") or r.json().get("signedUrl", "")
        if signed:
            if signed.startswith("/"):
                signed = f"{SUPABASE_URL}/storage/v1{signed}"
            return {"download_url": signed}
    raise HTTPException(status_code=502, detail="Could not generate download URL")


# ── Season Data Endpoints ─────────────────────────────────────────────────────

@app.get("/season/version")
def season_version():
    """
    Public endpoint — returns current season week data version.
    Bump SEASON_VERSION env var on Render when season_weeks.json is updated.
    """
    version = int(os.environ.get("SEASON_VERSION", "1"))
    return {"version": version}

@app.get("/season/data")
def season_data():
    """
    Public endpoint — returns season week date ranges for the game selector.
    Small JSON payload, returned directly (no signed URL needed).
    To update: edit SEASON_WEEKS env var on Render (JSON string) or update
    the default below, then bump SEASON_VERSION.
    Format: {"seasons": {"2025": {"0": ["20250823","20250825"], ...}, "2026": {...}}}
    """
    env_data = os.environ.get("SEASON_WEEKS", "")
    if env_data:
        try:
            return json.loads(env_data)
        except Exception:
            pass
    # Default — hardcoded fallback matching espn_live.py _SEASON_WEEK_DATES
    return {
        "seasons": {
            "2025": {
                "0":  ["20250823", "20250825"],
                "1":  ["20250826", "20250901"],
                "2":  ["20250902", "20250908"],
                "3":  ["20250909", "20250915"],
                "4":  ["20250916", "20250922"],
                "5":  ["20250923", "20250929"],
                "6":  ["20250930", "20251006"],
                "7":  ["20251007", "20251013"],
                "8":  ["20251014", "20251020"],
                "9":  ["20251021", "20251027"],
                "10": ["20251028", "20251103"],
                "11": ["20251104", "20251110"],
                "12": ["20251111", "20251117"],
                "13": ["20251118", "20251124"],
                "14": ["20251125", "20251201"],
                "15": ["20251202", "20251208"],
            }
        }
    }

# ── Storage Endpoints ─────────────────────────────────────────────────────────

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


# ── CAPP Nodes Endpoints ────────────────────────────────────────────────────────

NODES_FILE = "capp_nodes.json"

def _load_nodes(client_id: str) -> list:
    path = _storage_path(client_id, NODES_FILE)
    url = f"{SUPABASE_URL}/storage/v1/object/{SUPABASE_BUCKET}/{path}"
    with httpx.Client() as client:
        r = client.get(url, headers=_supabase_headers())
    if r.status_code == 404:
        return []
    if r.status_code != 200:
        return []
    return r.json().get("nodes", [])

def _save_nodes(client_id: str, nodes: list):
    path = _storage_path(client_id, NODES_FILE)
    url = f"{SUPABASE_URL}/storage/v1/object/{SUPABASE_BUCKET}/{path}"
    data = json.dumps({"nodes": nodes}).encode()
    with httpx.Client() as client:
        client.put(url, content=data, headers={
            **_supabase_headers(),
            "Content-Type": "application/json",
            "x-upsert": "true",
        })


@app.post("/nodes/register")
def nodes_register(
    client_id: str = Depends(get_client_id),
    machine_name: str = Body(..., embed=True),
    rustdesk_id: str = Body(..., embed=True),
    password: str = Body("", embed=True),
    notes: str = Body("", embed=True),
):
    """Register or update a node for this client. Identified by rustdesk_id."""
    from datetime import datetime, timezone
    import uuid

    nodes = _load_nodes(client_id)
    now = datetime.now(timezone.utc).isoformat()

    # Update if rustdesk_id already exists, otherwise add
    existing = next((n for n in nodes if n.get("rustdesk_id") == rustdesk_id), None)
    if existing:
        existing["machine_name"] = machine_name   # raw hostname, always updated by agent
        existing["last_seen"] = now
        existing["status"] = "online"
        if password:
            existing["password"] = password
        if notes:
            existing["notes"] = notes
        # display_name is user-set nickname — never touched by agent re-registration
    else:
        nodes.append({
            "id": str(uuid.uuid4()),
            "machine_name": machine_name,
            "rustdesk_id": rustdesk_id,
            "password": password,
            "notes": notes,
            "added": now,
            "last_seen": now,
            "status": "online",
        })

    _save_nodes(client_id, nodes)
    return {"status": "registered", "machine_name": machine_name, "rustdesk_id": rustdesk_id}


@app.get("/nodes")
def nodes_list(client_id: str = Depends(get_client_id)):
    """List all registered nodes for this client."""
    return {"nodes": _load_nodes(client_id)}


@app.patch("/nodes/{node_id}")
def nodes_rename(node_id: str, body: dict, client_id: str = Depends(get_client_id)):
    """Set a user-facing nickname (display_name) for a node. Never overwritten by agent."""
    new_name = body.get("machine_name", "").strip()
    if not new_name:
        raise HTTPException(status_code=400, detail="machine_name is required")
    nodes = _load_nodes(client_id)
    for n in nodes:
        if n.get("id") == node_id:
            n["display_name"] = new_name   # stored separately from machine_name (hostname)
            _save_nodes(client_id, nodes)
            return {"status": "ok"}
    raise HTTPException(status_code=404, detail="Node not found")


@app.delete("/nodes/{node_id}")
def nodes_delete(node_id: str, client_id: str = Depends(get_client_id)):
    """Remove a node by its id."""
    nodes = _load_nodes(client_id)
    updated = [n for n in nodes if n.get("id") != node_id]
    if len(updated) == len(nodes):
        raise HTTPException(status_code=404, detail="Node not found")
    _save_nodes(client_id, updated)
    return {"status": "deleted"}


# ── VNC Relay ───────────────────────────────────────────────────────────────
# Bridges screen-share sessions between client agent (host) and CAPP Launcher (viewer).
# Session key = "{client_id}:{machine_id}" — scoped per account for security.

_vnc_sessions: Dict[str, Dict[str, object]] = {}
_active_relay:  Dict[str, str]              = {}   # session_key → relay URL agent is on

SELF_URL = os.environ.get("SERVER_SELF_URL", "https://capp-data-server.onrender.com")


async def _verify_api_key_async(x_api_key: str) -> Optional[str]:
    """Async API key validation — returns client_id or None."""
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(
                f"{SUPABASE_URL}/rest/v1/capp_clients",
                params={"api_key": f"eq.{x_api_key}", "select": "client_id,active"},
                headers=_supabase_headers(),
                timeout=8,
            )
        if r.status_code != 200 or not r.json():
            return None
        row = r.json()[0]
        return row["client_id"] if row.get("active") else None
    except Exception:
        return None


@app.websocket("/vnc/{machine_id}/{role}")
async def vnc_relay(
    websocket: WebSocket,
    machine_id: str,
    role: str,
    x_api_key: str = Query(None),
):
    """
    WebSocket relay that bridges host (client machine) and viewer (CAPP Launcher).
    role must be 'host' or 'viewer'.
    Both sides authenticate with their CAPP API key.
    """
    if role not in ("host", "viewer"):
        await websocket.close(code=4002)
        return

    if not x_api_key:
        await websocket.close(code=4001)
        return

    client_id = await _verify_api_key_async(x_api_key)
    if not client_id:
        await websocket.close(code=4001)
        return

    session_key = f"{client_id}:{machine_id}"
    other_role = "viewer" if role == "host" else "host"

    await websocket.accept()

    if session_key not in _vnc_sessions:
        _vnc_sessions[session_key] = {"host": None, "viewer": None}
    _vnc_sessions[session_key][role] = websocket

    # Track which relay the agent is on so the viewer can find it
    if role == "host":
        _active_relay[session_key] = SELF_URL

    try:
        while True:
            data = await websocket.receive()
            msg_type = data.get("type", "")
            if msg_type == "websocket.disconnect":
                break

            other_ws = _vnc_sessions.get(session_key, {}).get(other_role)
            if other_ws is not None:
                try:
                    if data.get("text") is not None:
                        await other_ws.send_text(data["text"])
                    elif data.get("bytes") is not None:
                        await other_ws.send_bytes(data["bytes"])
                except Exception:
                    pass

            # If viewer sends input and host is NOT on WebSocket (HTTP polling
            # fallback), store in _poll_inputs so the agent can pick it up
            if role == "viewer" and data.get("text") is not None:
                host_ws = _vnc_sessions.get(session_key, {}).get("host")
                if host_ws is None:
                    try:
                        import json as _json
                        ev = _json.loads(data["text"])
                        _poll_inputs.setdefault(session_key, []).append(ev)
                        _poll_inputs[session_key] = _poll_inputs[session_key][-50:]
                    except Exception:
                        pass
    except Exception:
        pass
    finally:
        if session_key in _vnc_sessions:
            _vnc_sessions[session_key][role] = None
            if all(v is None for v in _vnc_sessions[session_key].values()):
                del _vnc_sessions[session_key]
        if role == "host":
            _active_relay.pop(session_key, None)


@app.post("/nodes/active_relay")
async def set_active_relay(body: dict = Body(...), x_api_key: str = Header(None)):
    """Agent calls this after connecting to a relay so the viewer knows where to find it."""
    client_id = await _verify_api_key_async(x_api_key)
    if not client_id:
        raise HTTPException(status_code=401, detail="Unauthorized")
    machine_id = body.get("machine_id", "")
    relay_url  = body.get("relay_url", "")
    if machine_id and relay_url:
        # Store in memory (fast) AND persist to node data (survives Render restarts)
        _active_relay[f"{client_id}:{machine_id}"] = relay_url
        nodes = _load_nodes(client_id)
        for n in nodes:
            if n.get("rustdesk_id") == machine_id:
                n["active_relay"] = relay_url
                break
        _save_nodes(client_id, nodes)
    return {"ok": True}


@app.get("/nodes/{machine_id}/relay")
async def get_agent_relay(machine_id: str, x_api_key: str = Header(None)):
    """Viewer calls this before connecting — finds out which relay the agent is on."""
    client_id = await _verify_api_key_async(x_api_key)
    if not client_id:
        raise HTTPException(status_code=401, detail="Unauthorized")
    # In-memory first (fast), then persistent node data (survives Render restarts)
    relay = _active_relay.get(f"{client_id}:{machine_id}")
    if not relay:
        nodes = _load_nodes(client_id)
        node = next((n for n in nodes if n.get("rustdesk_id") == machine_id), None)
        relay = node.get("active_relay", SELF_URL) if node else SELF_URL
    return {"relay_url": relay}


# ── HTTPS Polling Relay (fallback for networks that block WebSocket) ─────────
# Agent pushes frames via PUT, viewer polls via GET.
# Input events go viewer→server→agent via POST/GET.
# Scoped per client_id:machine_id — same auth as WS relay.

_poll_frames: Dict[str, bytes] = {}   # "{client_id}:{machine_id}" → latest JPEG
_poll_inputs: Dict[str, List]  = {}   # "{client_id}:{machine_id}" → pending events


@app.put("/poll/{machine_id}/frame")
async def poll_push_frame(
    machine_id: str,
    request: Request,
    client_id: str = Depends(get_client_id),
):
    """Agent pushes latest frame. Stored for HTTP viewers and forwarded to any
    WebSocket viewer that is already connected via the WS relay."""
    key = f"{client_id}:{machine_id}"
    frame = await request.body()
    _poll_frames[key] = frame

    # Bridge to WebSocket viewer if one is connected
    ws_viewer = _vnc_sessions.get(key, {}).get("viewer")
    if ws_viewer is not None:
        try:
            await ws_viewer.send_bytes(frame)
        except Exception:
            pass

    return {"ok": True}


@app.get("/poll/{machine_id}/frame")
async def poll_get_frame(
    machine_id: str,
    client_id: str = Depends(get_client_id),
):
    """Viewer polls for latest JPEG frame."""
    frame = _poll_frames.get(f"{client_id}:{machine_id}")
    if not frame:
        raise HTTPException(status_code=404, detail="No frame available")
    return Response(content=frame, media_type="image/jpeg")


@app.post("/poll/{machine_id}/input")
async def poll_push_input(
    machine_id: str,
    request: Request,
    client_id: str = Depends(get_client_id),
):
    """Viewer pushes an input event (mouse/keyboard)."""
    ev = await request.json()
    key = f"{client_id}:{machine_id}"
    _poll_inputs.setdefault(key, []).append(ev)
    _poll_inputs[key] = _poll_inputs[key][-50:]   # cap queue
    return {"ok": True}


@app.get("/poll/{machine_id}/inputs")
async def poll_get_inputs(
    machine_id: str,
    client_id: str = Depends(get_client_id),
):
    """Agent polls for pending input events, clears queue on read."""
    key = f"{client_id}:{machine_id}"
    return {"events": _poll_inputs.pop(key, [])}


# ── Admin Panel ───────────────────────────────────────────────────────────────

ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "")

def _require_admin(x_admin_token: str = Header(...)):
    if not ADMIN_PASSWORD or x_admin_token != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Unauthorized")

def _supa_headers_json():
    return {
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "apikey": SUPABASE_KEY,
        "Content-Type": "application/json",
    }

import secrets as _secrets
import string as _string
import re as _re

def _gen_password(length=16) -> str:
    alpha = _string.ascii_letters + _string.digits + "!@#$^&*"
    return "".join(_secrets.choice(alpha) for _ in range(length))

def _gen_api_key() -> str:
    return _secrets.token_hex(32)

def _hash_pw(password: str, salt: str) -> str:
    return hashlib.sha256((password + salt).encode()).hexdigest()


def _school_slug(name: str) -> str:
    """'Air Force' -> 'air_force'"""
    return _re.sub(r'[^a-z0-9]+', '_', name.lower().strip()).strip('_') or "school"


@app.post("/register")
def self_register(
    school:   str = Body(..., embed=True),   # school/team name
    email:    str = Body(..., embed=True),   # used as username
    password: str = Body(..., embed=True),
):
    """
    Self-service registration — no admin token required.
    Creates an active account and returns client_id + api_key immediately.
    The customer can then activate from inside the app.
    """
    username = email.strip().lower()
    if not username or not password or not school.strip():
        raise HTTPException(status_code=400, detail="school, email, and password are required.")

    # Reject duplicate email/username
    r = httpx.get(
        f"{SUPABASE_URL}/rest/v1/capp_clients",
        params={"username": f"eq.{username}", "select": "username"},
        headers=_supa_headers_json(),
    )
    if r.status_code == 200 and r.json():
        raise HTTPException(status_code=409, detail="An account with that email already exists.")

    # Generate a unique client_id from school name
    base_slug = _school_slug(school.strip())
    client_id = base_slug
    for n in range(2, 20):
        rc = httpx.get(
            f"{SUPABASE_URL}/rest/v1/capp_clients",
            params={"client_id": f"eq.{client_id}", "select": "client_id"},
            headers=_supa_headers_json(),
        )
        if rc.status_code == 200 and not rc.json():
            break
        client_id = f"{base_slug}_{n}"

    salt    = _secrets.token_hex(16)
    pw_hash = _hash_pw(password, salt)
    api_key = _gen_api_key()

    row = {
        "client_id":      client_id,
        "username":       username,
        "password_hash":  pw_hash,
        "salt":           salt,
        "api_key":        api_key,
        "active":         True,
        "is_admin":       False,
        "seat_1_machine": None,
        "seat_2_machine": None,
    }
    r2 = httpx.post(
        f"{SUPABASE_URL}/rest/v1/capp_clients",
        json=row,
        headers={**_supa_headers_json(), "Prefer": "return=minimal"},
    )
    if r2.status_code not in (200, 201):
        raise HTTPException(status_code=500, detail=f"Account creation failed: {r2.text}")

    return {"client_id": client_id, "api_key": api_key}


@app.get("/admin", response_class=HTMLResponse, include_in_schema=False)
def admin_page():
    return HTMLResponse(_ADMIN_HTML)


@app.get("/admin/api/teams", dependencies=[Depends(_require_admin)])
def admin_teams():
    """Return all teams grouped by division → conference for the admin panel.
    Uses team_conferences table with the latest season for accurate realignment data."""
    try:
        conn = sqlite3.connect(SERVER_DB_PATH)
        cur  = conn.cursor()
        # Find the latest season in team_conferences
        cur.execute("SELECT MAX(season) FROM team_conferences")
        latest = cur.fetchone()[0] or 2026
        # FBS and FCS from season-aware table
        cur.execute("""
            SELECT team, conference, UPPER(classification) as division
            FROM team_conferences
            WHERE season = ?
              AND conference IS NOT NULL
              AND team NOT LIKE 'ZZZZZZ%%'
            ORDER BY classification, conference, team
        """, (latest,))
        rows = cur.fetchall()
        # NFL from teams table (not in team_conferences)
        cur.execute("""
            SELECT team, conference, division
            FROM teams
            WHERE division = 'NFL'
              AND conference IS NOT NULL
            ORDER BY conference, team
        """)
        nfl_rows = cur.fetchall()
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    grouped: dict = {}
    for team, conference, division in rows:
        div_label = "FBS" if division == "FBS" else "FCS"
        grouped.setdefault(div_label, {}).setdefault(conference, []).append(team)
    for team, conference, _ in nfl_rows:
        grouped.setdefault("NFL", {}).setdefault(conference, []).append(team)
    return grouped


@app.post("/admin/api/clients", dependencies=[Depends(_require_admin)])
def admin_create_client(
    username:  str = Body(...),
    client_id: str = Body(...),
    school:    str = Body(...),
    password:  str = Body(""),
    is_admin:  bool = Body(False),
):
    # Duplicate check
    r = httpx.get(
        f"{SUPABASE_URL}/rest/v1/capp_clients",
        params={"username": f"eq.{username}", "select": "username"},
        headers=_supa_headers_json(),
    )
    if r.status_code == 200 and r.json():
        raise HTTPException(status_code=409, detail=f"Username '{username}' already exists.")

    pw       = password.strip() or _gen_password()
    salt     = _secrets.token_hex(16)
    pw_hash  = _hash_pw(pw, salt)
    api_key  = _gen_api_key()

    row = {
        "client_id":      client_id.strip().lower(),
        "username":       username.strip().lower(),
        "password_hash":  pw_hash,
        "salt":           salt,
        "api_key":        api_key,
        "active":         True,
        "is_admin":       is_admin,
        "seat_1_machine": None,
        "seat_2_machine": None,
    }
    r2 = httpx.post(
        f"{SUPABASE_URL}/rest/v1/capp_clients",
        json=row,
        headers={**_supa_headers_json(), "Prefer": "return=minimal"},
    )
    if r2.status_code not in (200, 201):
        raise HTTPException(status_code=500, detail=f"Supabase error: {r2.text}")

    return {"ok": True, "username": username, "password": pw, "client_id": client_id, "school": school}


@app.get("/admin/api/clients", dependencies=[Depends(_require_admin)])
def admin_list_clients():
    r = httpx.get(
        f"{SUPABASE_URL}/rest/v1/capp_clients",
        params={"select": "username,client_id,active,is_admin,seat_1_machine,seat_2_machine,notes,next_invoice_date",
                "order": "username.asc"},
        headers=_supa_headers_json(),
    )
    if r.status_code != 200:
        raise HTTPException(status_code=500, detail=r.text)
    return r.json()


@app.patch("/admin/api/clients/{username}", dependencies=[Depends(_require_admin)])
def admin_update_client(username: str, payload: dict = Body(...)):
    """Update editable fields: notes, next_invoice_date."""
    allowed = {k: v for k, v in payload.items() if k in ("notes", "next_invoice_date")}
    if not allowed:
        raise HTTPException(status_code=400, detail="No editable fields provided.")
    r = httpx.patch(
        f"{SUPABASE_URL}/rest/v1/capp_clients",
        params={"username": f"eq.{username}"},
        json=allowed,
        headers={**_supa_headers_json(), "Prefer": "return=minimal"},
    )
    if r.status_code not in (200, 204):
        raise HTTPException(status_code=500, detail=r.text)
    return {"ok": True}


@app.patch("/admin/api/clients/{username}/reset-seat", dependencies=[Depends(_require_admin)])
def admin_reset_single_seat(username: str, seat: int = Body(..., embed=True)):
    """Reset a single seat (1 or 2)."""
    col = "seat_1_machine" if seat == 1 else "seat_2_machine"
    r = httpx.patch(
        f"{SUPABASE_URL}/rest/v1/capp_clients",
        params={"username": f"eq.{username}"},
        json={col: None},
        headers={**_supa_headers_json(), "Prefer": "return=minimal"},
    )
    if r.status_code not in (200, 204):
        raise HTTPException(status_code=500, detail=r.text)
    return {"ok": True}


@app.patch("/admin/api/clients/{username}/reset-seats", dependencies=[Depends(_require_admin)])
def admin_reset_seats(username: str):
    r = httpx.patch(
        f"{SUPABASE_URL}/rest/v1/capp_clients",
        params={"username": f"eq.{username}"},
        json={"seat_1_machine": None, "seat_2_machine": None},
        headers={**_supa_headers_json(), "Prefer": "return=minimal"},
    )
    if r.status_code not in (200, 204):
        raise HTTPException(status_code=500, detail=r.text)
    return {"ok": True}


@app.patch("/admin/api/clients/{username}/deactivate", dependencies=[Depends(_require_admin)])
def admin_deactivate(username: str, active: bool = Body(..., embed=True)):
    r = httpx.patch(
        f"{SUPABASE_URL}/rest/v1/capp_clients",
        params={"username": f"eq.{username}"},
        json={"active": active},
        headers={**_supa_headers_json(), "Prefer": "return=minimal"},
    )
    if r.status_code not in (200, 204):
        raise HTTPException(status_code=500, detail=r.text)
    return {"ok": True}


@app.delete("/admin/api/clients/{username}", dependencies=[Depends(_require_admin)])
def admin_delete_client(username: str):
    """Permanently delete a client account from Supabase."""
    r = httpx.delete(
        f"{SUPABASE_URL}/rest/v1/capp_clients",
        params={"username": f"eq.{username}"},
        headers={**_supa_headers_json(), "Prefer": "return=minimal"},
    )
    if r.status_code not in (200, 204):
        raise HTTPException(status_code=500, detail=r.text)
    return {"ok": True}


_ADMIN_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>CAPP Admin</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: #070a0f; color: #e2e8f0; font-family: 'Segoe UI', sans-serif; min-height: 100vh; overflow-x: hidden; }
  #login { display: flex; flex-direction: column; align-items: center; justify-content: center; height: 100vh; gap: 16px; }
  #login h1 { font-size: 22px; font-weight: 700; letter-spacing: 2px; }
  #login input { background: #1a2230; border: 1px solid #2c3b55; border-radius: 8px; color: white; font-size: 14px; padding: 10px 14px; width: 280px; outline: none; }
  #login input:focus { border-color: #3a7ebf; }
  #login button { background: #3a7ebf; border: none; border-radius: 8px; color: white; cursor: pointer; font-size: 14px; font-weight: 700; padding: 10px 0; width: 280px; }
  #login button:hover { background: #4a8ecf; }
  #login .err { color: #ef4444; font-size: 13px; }
  #app { display: none; height: 100vh; flex-direction: column; }
  .header { background: #0d1117; border-bottom: 1px solid #1e2a3a; padding: 14px 28px; display: flex; align-items: center; justify-content: space-between; flex-shrink: 0; }
  .header h1 { font-size: 16px; font-weight: 700; letter-spacing: 2px; color: #3a7ebf; }
  .header button { background: none; border: 1px solid #2c3b55; border-radius: 6px; color: #8b95a1; cursor: pointer; font-size: 12px; padding: 5px 12px; }
  .tabs { display: flex; gap: 2px; background: #0d1117; padding: 0 28px; border-bottom: 1px solid #1e2a3a; flex-shrink: 0; }
  .tab { background: none; border: none; border-bottom: 2px solid transparent; color: #8b95a1; cursor: pointer; font-size: 13px; font-weight: 600; padding: 12px 18px; }
  .tab.active { border-bottom-color: #3a7ebf; color: white; }
  .main-area { display: flex; flex: 1; overflow: hidden; position: relative; }
  .panel { display: none; padding: 28px; overflow-y: auto; flex: 1; }
  .panel.active { display: block; }
  .card { background: #1a2230; border: 1px solid #2c3b55; border-radius: 12px; padding: 22px; margin-bottom: 18px; }
  .card h2 { font-size: 15px; font-weight: 700; margin-bottom: 16px; color: #94b4d4; }
  .form-row { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-bottom: 12px; }
  .form-group { display: flex; flex-direction: column; gap: 5px; }
  .form-group label { color: #8b95a1; font-size: 11px; font-weight: 700; letter-spacing: 1px; text-transform: uppercase; }
  .form-group input, .form-group textarea { background: #0d1117; border: 1px solid #2c3b55; border-radius: 7px; color: white; font-size: 13px; padding: 8px 12px; outline: none; font-family: inherit; }
  .form-group input:focus, .form-group textarea:focus { border-color: #3a7ebf; }
  .form-group input:read-only { opacity: 0.5; cursor: default; }
  .form-group textarea { resize: vertical; min-height: 70px; }
  .form-group input::placeholder, .form-group textarea::placeholder { color: #3a4a5a; }
  .btn { border: none; border-radius: 8px; cursor: pointer; font-size: 13px; font-weight: 700; padding: 9px 22px; transition: background 0.15s; }
  .btn-primary { background: #3a7ebf; color: white; }
  .btn-primary:hover { background: #4a8ecf; }
  .btn-danger { background: #7f1d1d; color: #fca5a5; }
  .btn-danger:hover { background: #991b1b; }
  .btn-warning { background: #78350f; color: #fcd34d; }
  .btn-warning:hover { background: #92400e; }
  .btn-success { background: #14532d; color: #86efac; }
  .btn-success:hover { background: #166534; }
  .btn-sm { font-size: 11px; padding: 5px 12px; border-radius: 6px; }
  .result { background: #0d1117; border: 1px solid #2c3b55; border-radius: 8px; color: #86efac; font-family: monospace; font-size: 13px; margin-top: 14px; padding: 14px; white-space: pre-wrap; display: none; }
  .result.err { color: #fca5a5; }
  table { border-collapse: collapse; width: 100%; font-size: 13px; }
  th { background: #0d1117; color: #8b95a1; font-size: 11px; font-weight: 700; letter-spacing: 1px; padding: 10px 12px; text-align: left; text-transform: uppercase; }
  td { border-top: 1px solid #1e2a3a; color: #e2e8f0; padding: 10px 12px; vertical-align: middle; }
  tr.clickable { cursor: pointer; }
  tr.clickable:hover td { background: #1e2d42; }
  .badge { border-radius: 4px; font-size: 11px; font-weight: 700; padding: 2px 8px; }
  .badge-green { background: #14532d; color: #86efac; }
  .badge-red   { background: #7f1d1d; color: #fca5a5; }
  .badge-blue  { background: #1e3a5f; color: #93c5fd; }
  .badge-gray  { background: #1e2a3a; color: #8b95a1; }
  .loading { color: #8b95a1; font-size: 13px; padding: 20px 0; text-align: center; }
  select { background: #0d1117; border: 1px solid #2c3b55; border-radius: 7px; color: white; font-size: 13px; padding: 8px 12px; outline: none; width: 100%; cursor: pointer; }
  select:focus { border-color: #3a7ebf; }
  select:disabled { opacity: 0.4; cursor: not-allowed; }

  /* Slide-out panel */
  #slideout { position: fixed; top: 0; right: -420px; width: 420px; height: 100vh; background: #0d1117;
    border-left: 1px solid #2c3b55; z-index: 100; transition: right 0.28s ease; overflow-y: auto;
    display: flex; flex-direction: column; }
  #slideout.open { right: 0; }
  .so-header { background: #111827; border-bottom: 1px solid #2c3b55; padding: 18px 22px;
    display: flex; align-items: center; justify-content: space-between; flex-shrink: 0; }
  .so-header h2 { font-size: 15px; font-weight: 700; color: #94b4d4; }
  .so-close { background: none; border: none; color: #8b95a1; cursor: pointer; font-size: 20px; line-height: 1; padding: 0 4px; }
  .so-close:hover { color: white; }
  .so-body { padding: 22px; flex: 1; }
  .so-section { margin-bottom: 22px; }
  .so-section h3 { color: #8b95a1; font-size: 10px; font-weight: 700; letter-spacing: 1px; text-transform: uppercase; margin-bottom: 12px; }
  .so-field { margin-bottom: 10px; }
  .so-field label { color: #8b95a1; font-size: 11px; font-weight: 700; letter-spacing: 1px; text-transform: uppercase; display: block; margin-bottom: 4px; }
  .so-val { color: #e2e8f0; font-size: 13px; background: #1a2230; border-radius: 6px; padding: 7px 10px; font-family: monospace; word-break: break-all; }
  .so-val.muted { color: #8b95a1; font-family: inherit; }
  .seat-row { display: flex; align-items: center; gap: 10px; margin-bottom: 8px; }
  .seat-row .so-val { flex: 1; font-size: 11px; }
  .so-footer { padding: 18px 22px; border-top: 1px solid #2c3b55; display: flex; gap: 8px; flex-wrap: wrap; flex-shrink: 0; }
  .overlay { display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.4); z-index: 99; }
  .overlay.on { display: block; }
  .so-save-msg { color: #86efac; font-size: 12px; margin-top: 6px; display: none; }
</style>
</head>
<body>

<!-- Login -->
<div id="login">
  <h1>CAPP ADMIN</h1>
  <input type="password" id="pw-input" placeholder="Admin password" autocomplete="current-password">
  <button onclick="doLogin()">Sign In</button>
  <div class="err" id="login-err"></div>
</div>

<!-- App -->
<div id="app">
  <div class="header">
    <h1>CAPP ADMIN PANEL</h1>
    <button onclick="logout()">Sign Out</button>
  </div>
  <div class="tabs">
    <button class="tab active" onclick="showTab('clients-tab', this)">Clients</button>
    <button class="tab" onclick="showTab('create-tab', this)">Create Account</button>
  </div>
  <div class="main-area">

    <!-- Clients list -->
    <div class="panel active" id="clients-tab">
      <div class="card">
        <h2>All Accounts
          <button class="btn btn-primary" onclick="loadClients()" style="float:right;font-size:12px;padding:5px 14px;">Refresh</button>
        </h2>
        <p style="color:#8b95a1;font-size:12px;margin-bottom:14px;">Click any row to view details.</p>
        <div id="clients-table"><div class="loading">Loading...</div></div>
      </div>
    </div>

    <!-- Create account -->
    <div class="panel" id="create-tab">
      <div class="card">
        <h2>Create New Account</h2>
        <div class="form-row">
          <div class="form-group">
            <label>League</label>
            <select id="c-division" onchange="onDivisionChange()"><option value="">— Select League —</option></select>
          </div>
          <div class="form-group">
            <label>Conference</label>
            <select id="c-conference" onchange="onConferenceChange()" disabled><option value="">— Select Conference —</option></select>
          </div>
        </div>
        <div class="form-row">
          <div class="form-group">
            <label>Team</label>
            <select id="c-team" onchange="onTeamChange()" disabled><option value="">— Select Team —</option></select>
          </div>
          <div class="form-group">
            <label>Password (blank = auto-generate)</label>
            <input type="text" id="c-password" placeholder="Leave blank to generate">
          </div>
        </div>
        <div class="form-row">
          <div class="form-group">
            <label>Username <span style="color:#8b95a1;font-weight:400">(auto-filled, editable)</span></label>
            <input type="text" id="c-username" placeholder="auto-filled from team">
          </div>
          <div class="form-group">
            <label>Client ID <span style="color:#8b95a1;font-weight:400">(auto-filled, editable)</span></label>
            <input type="text" id="c-clientid" placeholder="auto-filled from team">
          </div>
        </div>
        <button class="btn btn-primary" onclick="createClient()">Create Account</button>
        <div class="result" id="create-result"></div>
      </div>
    </div>

  </div><!-- end main-area -->
</div><!-- end app -->

<!-- Slide-out overlay -->
<div class="overlay" id="overlay" onclick="closeSlideout()"></div>

<!-- Slide-out panel -->
<div id="slideout">
  <div class="so-header">
    <h2 id="so-title">Account Details</h2>
    <button class="so-close" onclick="closeSlideout()">&#x2715;</button>
  </div>
  <div class="so-body">

    <div class="so-section">
      <h3>Account Info</h3>
      <div class="so-field"><label>Username</label><div class="so-val" id="so-username">—</div></div>
      <div class="so-field"><label>Client ID</label><div class="so-val" id="so-clientid">—</div></div>
      <div class="so-field"><label>Status</label><div id="so-status">—</div></div>
    </div>

    <div class="so-section">
      <h3>Machine Seats</h3>
      <div class="so-field">
        <label>Seat 1</label>
        <div class="seat-row">
          <div class="so-val muted" id="so-seat1">—</div>
          <button class="btn btn-warning btn-sm" onclick="resetSeat(1)">Reset</button>
        </div>
      </div>
      <div class="so-field">
        <label>Seat 2</label>
        <div class="seat-row">
          <div class="so-val muted" id="so-seat2">—</div>
          <button class="btn btn-warning btn-sm" onclick="resetSeat(2)">Reset</button>
        </div>
      </div>
    </div>

    <div class="so-section">
      <h3>Billing</h3>
      <div class="so-field">
        <label>Next Invoice Date</label>
        <div class="form-group"><input type="date" id="so-invoice-date"></div>
      </div>
    </div>

    <div class="so-section">
      <h3>Notes</h3>
      <div class="form-group">
        <textarea id="so-notes" placeholder="Contact name, phone, PO number, anything useful..."></textarea>
      </div>
      <button class="btn btn-primary btn-sm" onclick="saveDetails()" style="margin-top:8px;">Save Notes & Date</button>
      <div class="so-save-msg" id="so-save-msg">Saved.</div>
    </div>

  </div>
  <div class="so-footer">
    <button class="btn btn-success btn-sm" id="so-toggle-btn" onclick="toggleActiveFromSlideout()">—</button>
    <button class="btn btn-danger btn-sm" onclick="deleteFromSlideout()">Delete Account</button>
  </div>
</div>

<script>
let _token = "";
let _currentUser = null;
let _allClients = [];

function doLogin() {
  const pw = document.getElementById("pw-input").value.trim();
  if (!pw) return;
  fetch("/admin/api/clients", { headers: { "x-admin-token": pw } })
    .then(r => {
      if (r.ok) {
        _token = pw;
        document.getElementById("login").style.display = "none";
        const app = document.getElementById("app");
        app.style.display = "flex";
        loadClients();
      } else {
        document.getElementById("login-err").textContent = "Incorrect password.";
      }
    })
    .catch(() => document.getElementById("login-err").textContent = "Cannot reach server.");
}

document.getElementById("pw-input").addEventListener("keydown", e => { if (e.key === "Enter") doLogin(); });

function logout() {
  _token = ""; _currentUser = null; _allClients = [];
  document.getElementById("app").style.display = "none";
  document.getElementById("login").style.display = "flex";
  document.getElementById("pw-input").value = "";
  closeSlideout();
}

function showTab(id, btn) {
  document.querySelectorAll(".panel").forEach(p => p.classList.remove("active"));
  document.querySelectorAll(".tab").forEach(t => t.classList.remove("active"));
  document.getElementById(id).classList.add("active");
  btn.classList.add("active");
  if (id === "clients-tab") loadClients();
  if (id === "create-tab") loadTeams();
  closeSlideout();
}

function api(method, path, body) {
  return fetch("/admin/api" + path, {
    method,
    headers: { "x-admin-token": _token, "Content-Type": "application/json" },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  }).then(r => r.json());
}

// ── Clients table ─────────────────────────────────────────────────────────────

function loadClients() {
  document.getElementById("clients-table").innerHTML = '<div class="loading">Loading...</div>';
  api("GET", "/clients").then(data => {
    if (!Array.isArray(data)) {
      document.getElementById("clients-table").innerHTML = '<div class="loading">Error loading clients.</div>';
      return;
    }
    _allClients = data;
    if (data.length === 0) {
      document.getElementById("clients-table").innerHTML = '<div class="loading">No accounts yet.</div>';
      return;
    }
    const rows = data.map(c => {
      const active = c.active ? '<span class="badge badge-green">Active</span>' : '<span class="badge badge-red">Inactive</span>';
      const admin  = c.is_admin ? ' <span class="badge badge-blue">Admin</span>' : '';
      const s1     = c.seat_1_machine ? '<span class="badge badge-gray">Bound</span>' : '<span class="badge badge-green">Open</span>';
      const s2     = c.seat_2_machine ? '<span class="badge badge-gray">Bound</span>' : '<span class="badge badge-green">Open</span>';
      const inv    = c.next_invoice_date ? c.next_invoice_date : '<span style="color:#8b95a1">—</span>';
      return `<tr class="clickable" onclick="openSlideout('${c.username}')">
        <td>${c.username}</td>
        <td>${c.client_id}</td>
        <td>${active}${admin}</td>
        <td>${s1}</td><td>${s2}</td>
        <td>${inv}</td>
      </tr>`;
    }).join("");
    document.getElementById("clients-table").innerHTML = `
      <table>
        <thead><tr><th>Username</th><th>Client ID</th><th>Status</th><th>Seat 1</th><th>Seat 2</th><th>Next Invoice</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>`;
  });
}

// ── Slide-out ─────────────────────────────────────────────────────────────────

function openSlideout(username) {
  const c = _allClients.find(x => x.username === username);
  if (!c) return;
  _currentUser = c;

  document.getElementById("so-title").textContent = c.username;
  document.getElementById("so-username").textContent = c.username;
  document.getElementById("so-clientid").textContent = c.client_id;

  const statusEl = document.getElementById("so-status");
  statusEl.innerHTML = c.active
    ? '<span class="badge badge-green">Active</span>'
    : '<span class="badge badge-red">Inactive</span>';
  if (c.is_admin) statusEl.innerHTML += ' <span class="badge badge-blue">Admin</span>';

  document.getElementById("so-seat1").textContent = c.seat_1_machine
    ? c.seat_1_machine.substring(0, 24) + "…" : "Open";
  document.getElementById("so-seat2").textContent = c.seat_2_machine
    ? c.seat_2_machine.substring(0, 24) + "…" : "Open";

  document.getElementById("so-invoice-date").value = c.next_invoice_date || "";
  document.getElementById("so-notes").value = c.notes || "";
  document.getElementById("so-save-msg").style.display = "none";

  const toggleBtn = document.getElementById("so-toggle-btn");
  if (c.active) {
    toggleBtn.textContent = "Deactivate";
    toggleBtn.className = "btn btn-danger btn-sm";
  } else {
    toggleBtn.textContent = "Reactivate";
    toggleBtn.className = "btn btn-success btn-sm";
  }

  document.getElementById("slideout").classList.add("open");
  document.getElementById("overlay").classList.add("on");
}

function closeSlideout() {
  document.getElementById("slideout").classList.remove("open");
  document.getElementById("overlay").classList.remove("on");
  _currentUser = null;
}

function resetSeat(seat) {
  if (!_currentUser) return;
  if (!confirm("Reset Seat " + seat + " for " + _currentUser.username + "?\\nThey can activate on a new machine for that seat.")) return;
  api("PATCH", "/clients/" + _currentUser.username + "/reset-seat", { seat })
    .then(d => {
      if (d.ok) { loadClients(); closeSlideout(); }
      else alert("Error: " + JSON.stringify(d));
    });
}

function saveDetails() {
  if (!_currentUser) return;
  const notes   = document.getElementById("so-notes").value;
  const invDate = document.getElementById("so-invoice-date").value || null;
  api("PATCH", "/clients/" + _currentUser.username, { notes, next_invoice_date: invDate })
    .then(d => {
      if (d.ok) {
        const msg = document.getElementById("so-save-msg");
        msg.style.display = "block";
        setTimeout(() => msg.style.display = "none", 2000);
        loadClients();
      } else {
        alert("Error: " + JSON.stringify(d));
      }
    });
}

function toggleActiveFromSlideout() {
  if (!_currentUser) return;
  const newState = !_currentUser.active;
  const msg = newState
    ? "Reactivate " + _currentUser.username + "?"
    : "Deactivate " + _currentUser.username + "? This will block all their logins.";
  if (!confirm(msg)) return;
  api("PATCH", "/clients/" + _currentUser.username + "/deactivate", { active: newState })
    .then(d => { if (d.ok) { loadClients(); closeSlideout(); } else alert("Error: " + JSON.stringify(d)); });
}

function deleteFromSlideout() {
  if (!_currentUser) return;
  if (!confirm(
    "DELETE account: " + _currentUser.username + "\\n\\n" +
    "This permanently removes their account from the database.\\n" +
    "Their CAPP app will stop working immediately.\\n\\n" +
    "This cannot be undone. Are you sure?"
  )) return;
  api("DELETE", "/clients/" + _currentUser.username)
    .then(d => { if (d.ok) { loadClients(); closeSlideout(); } else alert("Error: " + JSON.stringify(d)); });
}

// ── Create account ────────────────────────────────────────────────────────────

let _teamsData = {};

function loadTeams() {
  api("GET", "/teams").then(data => {
    _teamsData = data;
    const divSel = document.getElementById("c-division");
    divSel.innerHTML = '<option value="">— Select League —</option>';
    const order = ["NFL", "FBS", "FCS"];
    const divs = order.filter(d => data[d]).concat(Object.keys(data).filter(d => !order.includes(d)));
    divs.forEach(div => {
      const opt = document.createElement("option");
      opt.value = div;
      opt.textContent = div === "FBS" ? "College — FBS" : div === "FCS" ? "College — FCS" : div;
      divSel.appendChild(opt);
    });
  });
}

function onDivisionChange() {
  const div  = document.getElementById("c-division").value;
  const conf = document.getElementById("c-conference");
  const team = document.getElementById("c-team");
  conf.innerHTML = '<option value="">— Select Conference —</option>';
  team.innerHTML = '<option value="">— Select Team —</option>';
  team.disabled = true;
  if (!div) { conf.disabled = true; return; }
  conf.disabled = false;
  Object.keys(_teamsData[div] || {}).sort().forEach(c => {
    const opt = document.createElement("option");
    opt.value = c; opt.textContent = c;
    conf.appendChild(opt);
  });
}

function onConferenceChange() {
  const div  = document.getElementById("c-division").value;
  const conf = document.getElementById("c-conference").value;
  const team = document.getElementById("c-team");
  team.innerHTML = '<option value="">— Select Team —</option>';
  if (!conf) { team.disabled = true; return; }
  team.disabled = false;
  ((_teamsData[div] || {})[conf] || []).forEach(t => {
    const opt = document.createElement("option");
    opt.value = t; opt.textContent = toTitle(t);
    team.appendChild(opt);
  });
}

function onTeamChange() {
  const team = document.getElementById("c-team").value;
  if (!team) return;
  const slug = team.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "");
  document.getElementById("c-username").value = slug;
  document.getElementById("c-clientid").value  = slug;
}

function toTitle(str) {
  return str.toLowerCase().replace(/\\b\\w/g, c => c.toUpperCase());
}

function createClient() {
  const school   = toTitle(document.getElementById("c-team").value.trim());
  const username = document.getElementById("c-username").value.trim().toLowerCase();
  const clientId = document.getElementById("c-clientid").value.trim().toLowerCase();
  const password = document.getElementById("c-password").value.trim();
  const result   = document.getElementById("create-result");

  if (!school || !username || !clientId) {
    result.className = "result err"; result.style.display = "block";
    result.textContent = "Please select a team and confirm username / client ID.";
    return;
  }
  result.className = "result"; result.style.display = "block";
  result.textContent = "Creating account...";

  api("POST", "/clients", { username, client_id: clientId, school, password, is_admin: false })
    .then(d => {
      if (d.ok) {
        result.textContent = [
          "ACCOUNT CREATED", "",
          "School:         " + d.school,
          "Username:       " + d.username,
          "Activation Key: " + d.password,
          "Client ID:      " + d.client_id, "",
          "--- EMAIL TEMPLATE ---", "",
          "Your CAPP Video Coordinator Suite account is ready.", "",
          "Open CAPP and enter the following when prompted:",
          "  Username:       " + d.username,
          "  Activation Key: " + d.password, "",
          "Two seats are included. The first two machines you",
          "activate will be bound to your account automatically.", "",
          "Questions? Contact roger@cappvcs.com",
        ].join("\\n");
        ["c-username","c-clientid","c-password"].forEach(id => document.getElementById(id).value = "");
        document.getElementById("c-division").value = "";
        onDivisionChange();
      } else {
        result.className = "result err";
        result.textContent = "Error: " + (d.detail || JSON.stringify(d));
      }
    })
    .catch(e => { result.className = "result err"; result.textContent = "Network error: " + e; });
}
</script>
</body>
</html>"""
