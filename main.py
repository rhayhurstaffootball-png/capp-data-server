from fastapi import FastAPI, Query, Header, HTTPException, Depends, Body, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response
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
    return get_game_plays(game_id, league=league, force_refresh=force_refresh)

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
    except Exception:
        pass
    finally:
        if session_key in _vnc_sessions:
            _vnc_sessions[session_key][role] = None
            if all(v is None for v in _vnc_sessions[session_key].values()):
                del _vnc_sessions[session_key]


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
    """Agent pushes latest JPEG frame."""
    _poll_frames[f"{client_id}:{machine_id}"] = await request.body()
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
