from fastapi import FastAPI, Query, Header, HTTPException, Depends, Body, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response, HTMLResponse
from typing import Optional, Dict, List
import os
import json
import hashlib
import asyncio
import httpx
import sqlite3
import time
from collections import deque, defaultdict
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from contextlib import asynccontextmanager

from espn_fetcher import (
    get_live_games,
    get_game_plays,
    get_game_version,
    start_poller,
    get_team_list,
    get_team_schedule,
    get_fetcher_metrics,
    get_game_monitor_rows,
)
from db_updater import run_update


SERVER_DB_PATH = os.path.join(os.path.dirname(__file__), "workflow_server.db")
APP_STARTED_AT = time.time()
BOOTSTRAP_FINISHED_AT = 0.0
SCHEDULER_STARTED_AT = 0.0
REQUEST_TOTAL = 0
REQUEST_ERRORS = 0
REQUEST_PATH_COUNTS: dict = defaultdict(int)
RECENT_LATENCY_MS = deque(maxlen=500)
GAME_REQUEST_STATS: dict = defaultdict(
    lambda: {
        "plays_requests": 0,
        "version_requests": 0,
        "errors": 0,
        "bytes_sent": 0,
        "last_request_at": 0.0,
        "last_success_at": 0.0,
        "last_error": "",
        "last_status_code": 0,
        "latency_ms": deque(maxlen=100),
        "request_times": deque(maxlen=300),
        "slow_over_2000": 0,
        "slow_over_5000": 0,
        "slow_over_10000": 0,
    }
)


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(len(ordered) * pct))
    return ordered[index]

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


def _process_memory_bytes() -> int:
    try:
        import resource  # Linux / macOS

        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if os.name == "posix" and rss < 1024 * 1024 * 1024:
            return int(rss * 1024)
        return int(rss)
    except Exception:
        pass

    try:
        import ctypes
        import ctypes.wintypes as wintypes

        class PROCESS_MEMORY_COUNTERS(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        counters = PROCESS_MEMORY_COUNTERS()
        counters.cb = ctypes.sizeof(counters)
        handle = ctypes.windll.kernel32.GetCurrentProcess()
        ok = ctypes.windll.psapi.GetProcessMemoryInfo(handle, ctypes.byref(counters), counters.cb)
        if ok:
            return int(counters.WorkingSetSize)
    except Exception:
        pass

    return 0


def _latency_summary() -> dict:
    values = list(RECENT_LATENCY_MS)
    if not values:
        return {"count": 0, "avg_ms": 0.0, "p95_ms": 0.0, "max_ms": 0.0}
    values.sort()
    p95_index = min(len(values) - 1, int(len(values) * 0.95))
    return {
        "count": len(values),
        "avg_ms": round(sum(values) / len(values), 1),
        "p95_ms": round(values[p95_index], 1),
        "max_ms": round(values[-1], 1),
    }


def _record_game_request(game_id: str, endpoint: str, latency_ms: float, status_code: int, payload_bytes: int = 0, error: str = ""):
    if not game_id:
        return
    now = time.time()
    stats = GAME_REQUEST_STATS[str(game_id)]
    stats["last_request_at"] = now
    stats["last_status_code"] = int(status_code or 0)
    stats["request_times"].append(now)
    if endpoint == "plays":
        stats["plays_requests"] += 1
    elif endpoint == "version":
        stats["version_requests"] += 1
    if latency_ms > 0:
        stats["latency_ms"].append(float(latency_ms))
        if latency_ms > 2000:
            stats["slow_over_2000"] += 1
        if latency_ms > 5000:
            stats["slow_over_5000"] += 1
        if latency_ms > 10000:
            stats["slow_over_10000"] += 1
    if payload_bytes > 0:
        stats["bytes_sent"] += int(payload_bytes)
    if 200 <= int(status_code or 0) < 400:
        stats["last_success_at"] = now
    else:
        stats["errors"] += 1
        stats["last_error"] = error or f"HTTP {status_code}"


def _window_count(timestamps: deque, seconds: int) -> int:
    if not timestamps:
        return 0
    cutoff = time.time() - seconds
    return sum(1 for ts in timestamps if ts >= cutoff)


def _game_request_snapshot() -> dict:
    snapshot = {}
    for game_id, stats in GAME_REQUEST_STATS.items():
        latencies = list(stats["latency_ms"])
        snapshot[game_id] = {
            "plays_requests": stats["plays_requests"],
            "version_requests": stats["version_requests"],
            "errors": stats["errors"],
            "bytes_sent": stats["bytes_sent"],
            "last_request_at": stats["last_request_at"] or None,
            "last_success_at": stats["last_success_at"] or None,
            "last_error": stats["last_error"],
            "last_status_code": stats["last_status_code"],
            "requests_last_60s": _window_count(stats["request_times"], 60),
            "requests_last_300s": _window_count(stats["request_times"], 300),
            "avg_latency_ms": round(sum(latencies) / len(latencies), 1) if latencies else 0.0,
            "p95_latency_ms": round(_percentile(latencies, 0.95), 1) if latencies else 0.0,
            "max_latency_ms": round(max(latencies), 1) if latencies else 0.0,
            "slow_over_2000": stats["slow_over_2000"],
            "slow_over_5000": stats["slow_over_5000"],
            "slow_over_10000": stats["slow_over_10000"],
        }
    return snapshot


def _build_gameday_payload() -> dict:
    health = _build_health_payload()
    game_rows = get_game_monitor_rows()
    request_rows = _game_request_snapshot()

    merged_games = []
    for row in game_rows:
        request_stats = request_rows.get(row["game_id"], {})
        merged = {**row, **request_stats}
        merged["alert_level"] = "green"
        reasons = []
        if merged.get("p95_latency_ms", 0) > 5000:
            merged["alert_level"] = "red"
            reasons.append("p95 latency > 5s")
        elif merged.get("p95_latency_ms", 0) > 2000:
            if merged["alert_level"] != "red":
                merged["alert_level"] = "yellow"
            reasons.append("p95 latency > 2s")
        if merged.get("errors", 0) > 0:
            merged["alert_level"] = "red"
            reasons.append("request errors")
        if merged.get("qc_issue_count", 0) > 0:
            if merged["alert_level"] != "red":
                merged["alert_level"] = "yellow"
            reasons.append(f"{merged['qc_issue_count']} QC issue(s)")
        if merged.get("auto_fixed_count", 0) > 0:
            reasons.append(f"{merged['auto_fixed_count']} auto-fixed")
        if merged.get("status") == "in" and merged.get("age_seconds") is not None and merged["age_seconds"] > 45:
            merged["alert_level"] = "red"
            reasons.append("live game cache stale")
        merged["alert_reasons"] = reasons
        merged_games.append(merged)

    alerts = []
    recent_latency = health.get("requests", {}).get("recent_latency", {})
    fetcher = health.get("fetcher", {})
    if not fetcher.get("poller_alive"):
        alerts.append({"level": "red", "kind": "poller", "message": "Poller is not alive."})
    if fetcher.get("last_poll_error"):
        alerts.append({"level": "red", "kind": "poller", "message": f"Last poll error: {fetcher['last_poll_error']}"})
    if recent_latency.get("p95_ms", 0) > 5000:
        alerts.append({"level": "red", "kind": "latency", "message": f"Server p95 latency is {recent_latency['p95_ms']} ms."})
    elif recent_latency.get("p95_ms", 0) > 2000:
        alerts.append({"level": "yellow", "kind": "latency", "message": f"Server p95 latency is {recent_latency['p95_ms']} ms."})
    if health.get("memory", {}).get("rss_bytes", 0) > 1_500_000_000:
        alerts.append({"level": "red", "kind": "memory", "message": "Process memory is above 1.5 GB."})
    elif health.get("memory", {}).get("rss_bytes", 0) > 1_000_000_000:
        alerts.append({"level": "yellow", "kind": "memory", "message": "Process memory is above 1.0 GB."})

    for row in merged_games:
        if row["alert_level"] != "green":
            alerts.append({
                "level": row["alert_level"],
                "kind": "game",
                "game_id": row["game_id"],
                "message": f"{row.get('away_name','')} at {row.get('home_name','')}: {', '.join(row['alert_reasons'])}",
            })

    alerts.sort(key=lambda item: (item["level"] != "red", item["kind"], item["message"]))
    return {
        "generated_at": time.time(),
        "platform": health,
        "summary": {
            "tracked_games": len(merged_games),
            "live_games": sum(1 for row in merged_games if row.get("status") == "in"),
            "games_with_auto_fixes": sum(1 for row in merged_games if row.get("auto_fixed_count", 0) > 0),
            "auto_fix_total": sum(int(row.get("auto_fixed_count", 0) or 0) for row in merged_games),
            "games_with_qc_issues": sum(1 for row in merged_games if row.get("qc_issue_count", 0) > 0),
            "games_with_errors": sum(1 for row in merged_games if row.get("errors", 0) > 0),
            "games_slow_p95": sum(1 for row in merged_games if row.get("p95_latency_ms", 0) > 2000),
            "alerts_red": sum(1 for item in alerts if item["level"] == "red"),
            "alerts_yellow": sum(1 for item in alerts if item["level"] == "yellow"),
        },
        "alerts": alerts[:50],
        "games": merged_games,
    }


def _build_health_payload() -> dict:
    fetcher = get_fetcher_metrics()
    db_meta = get_db_meta()
    uptime = max(0.0, time.time() - APP_STARTED_AT)
    ready = bool(BOOTSTRAP_FINISHED_AT and fetcher.get("poller_alive") and fetcher.get("initial_poll_complete"))
    return {
        "status": "ok" if ready else "warming",
        "ready": ready,
        "version": "2",
        "started_at": APP_STARTED_AT,
        "uptime_seconds": round(uptime, 1),
        "bootstrap_finished_at": BOOTSTRAP_FINISHED_AT,
        "scheduler_started_at": SCHEDULER_STARTED_AT,
        "db": {
            "present": os.path.exists(SERVER_DB_PATH),
            "path": SERVER_DB_PATH,
            "meta": db_meta,
        },
        "memory": {
            "rss_bytes": _process_memory_bytes(),
        },
        "requests": {
            "total": REQUEST_TOTAL,
            "errors": REQUEST_ERRORS,
            "by_path": dict(REQUEST_PATH_COUNTS),
            "recent_latency": _latency_summary(),
        },
        "fetcher": fetcher,
    }

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
    with httpx.stream("GET", url, headers=headers, timeout=120, follow_redirects=True) as r:
        if r.status_code == 200:
            size = 0
            with open(SERVER_DB_PATH, "wb") as f:
                for chunk in r.iter_bytes(chunk_size=65536):
                    f.write(chunk)
                    size += len(chunk)
            print(f"DB bootstrapped ({size//1024} KB)")
        else:
            print(f"WARNING: DB bootstrap failed ({r.status_code})")

def _evict_game_request_stats():
    """Remove GAME_REQUEST_STATS entries not accessed in the last 48 hours."""
    cutoff = time.time() - 48 * 3600
    stale = [gid for gid, stats in GAME_REQUEST_STATS.items()
             if stats.get("last_request_at", 0) < cutoff]
    for gid in stale:
        del GAME_REQUEST_STATS[gid]
    if stale:
        import gc
        gc.collect()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Download DB from Supabase if not present (Render ephemeral disk)
    _bootstrap_db()
    global BOOTSTRAP_FINISHED_AT, SCHEDULER_STARTED_AT
    BOOTSTRAP_FINISHED_AT = time.time()
    # Start scheduler
    scheduler = AsyncIOScheduler()
    # Run at 6:00 AM and 6:00 PM UTC daily
    scheduler.add_job(lambda: run_update(), "cron", hour="6,18", minute=0,
                      id="db_update", replace_existing=True)
    # Evict stale game request stats every hour
    scheduler.add_job(_evict_game_request_stats, "interval", hours=1,
                      id="evict_game_stats", replace_existing=True)
    scheduler.start()
    SCHEDULER_STARTED_AT = time.time()
    yield
    scheduler.shutdown()

app = FastAPI(title="CAPP Data Server", lifespan=lifespan)


@app.middleware("http")
async def _metrics_middleware(request: Request, call_next):
    global REQUEST_TOTAL, REQUEST_ERRORS

    started = time.perf_counter()
    REQUEST_TOTAL += 1
    REQUEST_PATH_COUNTS[request.url.path] += 1
    try:
        response = await call_next(request)
    except Exception:
        REQUEST_ERRORS += 1
        raise

    elapsed_ms = (time.perf_counter() - started) * 1000
    RECENT_LATENCY_MS.append(elapsed_ms)
    if response.status_code >= 400:
        REQUEST_ERRORS += 1
    return response

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

# --- API Key Cache (avoids hitting Supabase on every request) ---
import time as _time
_key_cache: dict = {}          # api_key -> {"client_id": str, "active": bool, "ts": float}
_KEY_CACHE_TTL = 300           # seconds — re-validate against Supabase every 5 minutes

async def _lookup_api_key(api_key: str) -> dict:
    """Return cached {client_id, active} for an API key, re-fetching after TTL."""
    now = _time.time()
    cached = _key_cache.get(api_key)
    if cached and now - cached["ts"] < _KEY_CACHE_TTL:
        return cached

    url    = f"{SUPABASE_URL}/rest/v1/capp_clients"
    params = {"api_key": f"eq.{api_key}", "select": "client_id,active"}
    async with httpx.AsyncClient() as client:
        r = await client.get(url, params=params, headers=_supabase_headers(), timeout=8)

    if r.status_code != 200 or not r.json():
        return {}

    row = r.json()[0]
    entry = {"client_id": row.get("client_id", ""), "active": row.get("active", False), "ts": now}
    _key_cache[api_key] = entry
    return entry

# --- API Key Auth ---
async def verify_api_key(x_api_key: str = Header(..., description="CAPP API key")):
    row = await _lookup_api_key(x_api_key)
    if not row:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
    if not row["active"]:
        raise HTTPException(status_code=401, detail="Account is not active")

async def get_client_id(x_api_key: str = Header(..., description="CAPP API key")) -> str:
    """Like verify_api_key but returns the client_id for use in endpoints."""
    row = await _lookup_api_key(x_api_key)
    if not row:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
    if not row["active"]:
        raise HTTPException(status_code=401, detail="Account is not active")
    return row["client_id"]

@app.on_event("startup")
def startup():
    start_poller()

@app.get("/health")
def health():
    return _build_health_payload()


@app.get("/metrics/status")
def metrics_status():
    return _build_health_payload()

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
    started = time.perf_counter()
    try:
        payload = get_game_plays(game_id, league=league, force_refresh=force_refresh)
        latency_ms = (time.perf_counter() - started) * 1000
        try:
            payload_bytes = len(json.dumps(payload).encode("utf-8"))
        except Exception:
            payload_bytes = 0
        _record_game_request(game_id, "plays", latency_ms, 200, payload_bytes=payload_bytes)
        return payload
    except Exception as e:
        latency_ms = (time.perf_counter() - started) * 1000
        _record_game_request(game_id, "plays", latency_ms, 500, error=f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}")

@app.get("/game/{game_id}/version", dependencies=[Depends(verify_api_key)])
def game_version(game_id: str):
    """Lightweight endpoint — returns only the fetched_at timestamp for the
    cached entry.  Clients poll this every 60 s to detect retroactive data
    corrections without re-downloading the full play list each time."""
    started = time.perf_counter()
    payload = {"game_id": game_id, "fetched_at": get_game_version(game_id)}
    latency_ms = (time.perf_counter() - started) * 1000
    _record_game_request(game_id, "version", latency_ms, 200, payload_bytes=len(json.dumps(payload).encode("utf-8")))
    return payload

@app.get("/teams", dependencies=[Depends(verify_api_key)])
def teams(league: str = Query("cfb", description="cfb or nfl")):
    """Raw ESPN team list — [{display_name, id}]. Client handles name resolution."""
    try:
        return {"teams": get_team_list(league)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}")

@app.get("/team/{team_id}/schedule", dependencies=[Depends(verify_api_key)])
def team_schedule(
    team_id: str,
    league: str = Query("cfb", description="cfb or nfl"),
    season: Optional[int] = Query(None, description="Season year e.g. 2026"),
):
    """Proxy ESPN team schedule. Returns [{game_id, home_team, away_team, status, week, …}]."""
    try:
        return {"games": get_team_schedule(team_id, season=season, league=league)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}")


# ── Trial / License Status ────────────────────────────────────────────────────

TRIAL_DAYS = 7

@app.get("/trial/status")
async def trial_status(x_api_key: str = Header(None, alias="x-api-key")):
    """
    Returns trial/license status for an activated account.
    Both seats of a school share the same clock (tied to account created_at).
    """
    if not x_api_key:
        raise HTTPException(status_code=401, detail="API key required.")

    async with httpx.AsyncClient() as client:
        r = await client.get(
            f"{SUPABASE_URL}/rest/v1/capp_clients",
            params={"api_key": f"eq.{x_api_key}",
                    "select": "client_id,active,licensed,created_at"},
            headers=_supabase_headers(),
        )
    if r.status_code != 200 or not r.json():
        raise HTTPException(status_code=401, detail="Invalid API key.")

    user = r.json()[0]
    if not user.get("active"):
        raise HTTPException(status_code=403, detail="Account is deactivated.")

    # Fully licensed — no expiry
    if user.get("licensed"):
        return {"active": True, "licensed": True, "days_remaining": 9999,
                "trial_days": TRIAL_DAYS}

    # Trial — clock starts at account created_at
    from datetime import datetime, timezone
    try:
        created_str = user.get("created_at", "")
        created_at  = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
        elapsed     = (datetime.now(timezone.utc) - created_at).days
        remaining   = max(0, TRIAL_DAYS - elapsed)
    except Exception:
        remaining = TRIAL_DAYS   # malformed date — be generous

    return {
        "active":        remaining > 0,
        "licensed":      False,
        "days_remaining": remaining,
        "trial_days":    TRIAL_DAYS,
    }


# ── Auth Endpoints ─────────────────────────────────────────────────────────────

@app.post("/nodes/login")
async def nodes_login(
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
    async with httpx.AsyncClient() as client:
        r = await client.get(url, params=params, headers=_supabase_headers())
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
async def auth_login(
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
    async with httpx.AsyncClient() as client:
        r = await client.get(url, params=params, headers=_supabase_headers())
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
            async with httpx.AsyncClient() as client:
                await client.patch(
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
async def storage_save(
    client_id: str = Query(...),
    filename: str = Query(...),
    payload: dict = Body(...),
):
    """Save a JSON file to Supabase storage for this client."""
    path = _storage_path(client_id, filename)
    url = f"{SUPABASE_URL}/storage/v1/object/{SUPABASE_BUCKET}/{path}"
    data = json.dumps(payload).encode()
    async with httpx.AsyncClient() as client:
        r = await client.put(url, content=data, headers={
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

async def _signed_url(storage_path: str) -> str:
    """Generate a 1-hour Supabase signed download URL for any object in the bucket."""
    url = f"{SUPABASE_URL}/storage/v1/object/sign/{SUPABASE_BUCKET}/{storage_path}"
    async with httpx.AsyncClient() as client:
        r = await client.post(url, json={"expiresIn": 3600},
                              headers={**_supabase_headers(), "Content-Type": "application/json"})
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail="Could not generate download URL")
    signed = r.json().get("signedURL") or r.json().get("signedUrl", "")
    if not signed:
        raise HTTPException(status_code=502, detail="Could not generate download URL")
    if signed.startswith("/"):
        signed = f"{SUPABASE_URL}/storage/v1{signed}"
    return signed


@app.get("/db/download", dependencies=[Depends(verify_api_key)])
async def db_download():
    """Signed Supabase URL for workflow.db. File transfer goes Supabase → Client."""
    return {"download_url": await _signed_url("shared/workflow.db")}

@app.get("/contacts/version")
def contacts_version():
    """
    Public endpoint — returns current contacts.xlsx version.
    Increment CONTACTS_VERSION env var when uploading a new contacts.xlsx to Supabase.
    """
    version = int(os.environ.get("CONTACTS_VERSION", "1"))
    return {"version": version}

@app.get("/contacts/download", dependencies=[Depends(verify_api_key)])
async def contacts_download():
    """Signed Supabase URL for contacts.xlsx. File transfer goes Supabase → Client."""
    return {"download_url": await _signed_url("shared/contacts.xlsx")}

@app.get("/agent/download")
async def agent_download():
    """Public signed URL for CAPPNodes_Agent.exe — no auth required."""
    return {"download_url": await _signed_url("shared/CAPPNodes_Agent.exe")}

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
async def app_download():
    """Signed Supabase URL for CAPP_Setup.exe. File transfer goes Supabase → Client."""
    return {"download_url": await _signed_url("shared/CAPP_Setup.exe")}


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
async def storage_load(
    client_id: str = Query(...),
    filename: str = Query(...),
):
    """Load a JSON file from Supabase storage for this client."""
    path = _storage_path(client_id, filename)
    url = f"{SUPABASE_URL}/storage/v1/object/{SUPABASE_BUCKET}/{path}"
    async with httpx.AsyncClient() as client:
        r = await client.get(url, headers=_supabase_headers())
    if r.status_code == 404:
        raise HTTPException(status_code=404, detail="File not found")
    if r.status_code != 200:
        raise HTTPException(status_code=500, detail=f"Supabase load failed: {r.text}")
    return r.json()


@app.get("/storage/list", dependencies=[Depends(verify_api_key)])
async def storage_list(client_id: str = Query(...)):
    """List all stored files for this client."""
    url = f"{SUPABASE_URL}/storage/v1/object/list/{SUPABASE_BUCKET}"
    async with httpx.AsyncClient() as client:
        r = await client.post(url, json={"prefix": f"{client_id}/"},
                              headers={**_supabase_headers(), "Content-Type": "application/json"})
    if r.status_code != 200:
        raise HTTPException(status_code=500, detail=f"Supabase list failed: {r.text}")
    return r.json()


# ── CAPP Nodes Endpoints ────────────────────────────────────────────────────────

NODES_FILE = "capp_nodes.json"

async def _load_nodes(client_id: str) -> list:
    path = _storage_path(client_id, NODES_FILE)
    url = f"{SUPABASE_URL}/storage/v1/object/{SUPABASE_BUCKET}/{path}"
    async with httpx.AsyncClient() as client:
        r = await client.get(url, headers=_supabase_headers())
    if r.status_code == 200:
        return r.json().get("nodes", [])
    return []

async def _save_nodes(client_id: str, nodes: list):
    path = _storage_path(client_id, NODES_FILE)
    url = f"{SUPABASE_URL}/storage/v1/object/{SUPABASE_BUCKET}/{path}"
    data = json.dumps({"nodes": nodes}).encode()
    async with httpx.AsyncClient() as client:
        await client.put(url, content=data, headers={
            **_supabase_headers(),
            "Content-Type": "application/json",
            "x-upsert": "true",
        })


@app.post("/nodes/register")
async def nodes_register(
    client_id: str = Depends(get_client_id),
    machine_name: str = Body(..., embed=True),
    rustdesk_id: str = Body(..., embed=True),
    password: str = Body("", embed=True),
    notes: str = Body("", embed=True),
):
    """Register or update a node for this client. Identified by rustdesk_id."""
    from datetime import datetime, timezone
    import uuid

    nodes = await _load_nodes(client_id)
    now = datetime.now(timezone.utc).isoformat()

    existing = next((n for n in nodes if n.get("rustdesk_id") == rustdesk_id), None)
    if existing:
        existing["machine_name"] = machine_name
        existing["last_seen"] = now
        existing["status"] = "online"
        if password:
            existing["password"] = password
        if notes:
            existing["notes"] = notes
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

    await _save_nodes(client_id, nodes)
    return {"status": "registered", "machine_name": machine_name, "rustdesk_id": rustdesk_id}


@app.get("/nodes")
async def nodes_list(client_id: str = Depends(get_client_id)):
    """List all registered nodes for this client."""
    return {"nodes": await _load_nodes(client_id)}


@app.patch("/nodes/{node_id}")
async def nodes_rename(node_id: str, body: dict, client_id: str = Depends(get_client_id)):
    """Set a user-facing nickname (display_name) for a node. Never overwritten by agent."""
    new_name = body.get("machine_name", "").strip()
    if not new_name:
        raise HTTPException(status_code=400, detail="machine_name is required")
    nodes = await _load_nodes(client_id)
    for n in nodes:
        if n.get("id") == node_id:
            n["display_name"] = new_name
            await _save_nodes(client_id, nodes)
            return {"status": "ok"}
    raise HTTPException(status_code=404, detail="Node not found")


@app.delete("/nodes/{node_id}")
async def nodes_delete(node_id: str, client_id: str = Depends(get_client_id)):
    """Remove a node by its id."""
    nodes = await _load_nodes(client_id)
    updated = [n for n in nodes if n.get("id") != node_id]
    if len(updated) == len(nodes):
        raise HTTPException(status_code=404, detail="Node not found")
    await _save_nodes(client_id, updated)
    return {"status": "deleted"}


# ── VNC Relay ───────────────────────────────────────────────────────────────
# Bridges screen-share sessions between client agent (host) and CAPP Launcher (viewer).
# Session key = "{client_id}:{machine_id}" — scoped per account for security.

_vnc_sessions: Dict[str, Dict[str, object]] = {}
_vnc_locks:    Dict[str, Dict[str, asyncio.Lock]] = {}
_active_relay:  Dict[str, str]              = {}   # session_key → relay URL agent is on

SELF_URL = os.environ.get("SERVER_SELF_URL", "https://capp-data-server.onrender.com")


async def _vnc_keepalive_task(websocket, lock):
    while True:
        await asyncio.sleep(10)
        async with lock:
            try:
                await websocket.send_text('{"type":"keepalive"}')
            except Exception:
                try:
                    await websocket.close()
                except Exception:
                    pass
                break


async def _verify_api_key_async(x_api_key: str) -> Optional[str]:
    """Async API key validation — returns client_id or None. Uses shared cache."""
    try:
        row = await _lookup_api_key(x_api_key)
        return row["client_id"] if row and row.get("active") else None
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
        _vnc_locks[session_key]    = {"host": asyncio.Lock(), "viewer": asyncio.Lock()}
    _vnc_sessions[session_key][role] = websocket
    _vnc_locks[session_key][role]    = asyncio.Lock()   # fresh lock for this connection

    my_lock = _vnc_locks[session_key][role]

    # Track which relay the agent is on so the viewer can find it
    if role == "host":
        _active_relay[session_key] = SELF_URL

    ka = asyncio.create_task(_vnc_keepalive_task(websocket, my_lock))

    try:
        while True:
            data = await websocket.receive()
            msg_type = data.get("type", "")
            if msg_type == "websocket.disconnect":
                break

            other_ws   = _vnc_sessions.get(session_key, {}).get(other_role)
            other_lock = _vnc_locks.get(session_key, {}).get(other_role)

            if other_ws is not None and other_lock is not None:
                async with other_lock:
                    try:
                        if data.get("bytes") is not None:
                            await other_ws.send_bytes(data["bytes"])
                        elif data.get("text") is not None:
                            await other_ws.send_text(data["text"])
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
        ka.cancel()
        if session_key in _vnc_sessions:
            if _vnc_sessions[session_key][role] is websocket:
                _vnc_sessions[session_key][role] = None
                if all(v is None for v in _vnc_sessions[session_key].values()):
                    del _vnc_sessions[session_key]
                    del _vnc_locks[session_key]
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
        nodes = await _load_nodes(client_id)
        for n in nodes:
            if n.get("rustdesk_id") == machine_id:
                n["active_relay"] = relay_url
                break
        await _save_nodes(client_id, nodes)
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
        nodes = await _load_nodes(client_id)
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
async def self_register(
    school:   str = Body(..., embed=True),
    email:    str = Body(..., embed=True),
    password: str = Body(..., embed=True),
):
    """
    Self-service registration — no admin token required.
    Creates an active account and returns client_id + api_key immediately.
    """
    username = email.strip().lower()
    if not username or not password or not school.strip():
        raise HTTPException(status_code=400, detail="school, email, and password are required.")

    async with httpx.AsyncClient() as client:
        r = await client.get(
            f"{SUPABASE_URL}/rest/v1/capp_clients",
            params={"username": f"eq.{username}", "select": "username"},
            headers=_supa_headers_json(),
        )
    if r.status_code == 200 and r.json():
        raise HTTPException(status_code=409, detail="An account with that email already exists.")

    base_slug = _school_slug(school.strip())
    client_id = base_slug
    async with httpx.AsyncClient() as c:
        for n in range(2, 20):
            rc = await c.get(
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
    async with httpx.AsyncClient() as client:
        r2 = await client.post(
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
async def admin_create_client(
    username:  str = Body(...),
    client_id: str = Body(...),
    school:    str = Body(...),
    password:  str = Body(""),
    is_admin:  bool = Body(False),
):
    async with httpx.AsyncClient() as c:
        r = await c.get(
            f"{SUPABASE_URL}/rest/v1/capp_clients",
            params={"username": f"eq.{username}", "select": "username"},
            headers=_supa_headers_json(),
        )
    if r.status_code == 200 and r.json():
        raise HTTPException(status_code=409, detail=f"Username '{username}' already exists.")

    pw      = password.strip() or _gen_password()
    salt    = _secrets.token_hex(16)
    pw_hash = _hash_pw(pw, salt)
    api_key = _gen_api_key()

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
    async with httpx.AsyncClient() as c:
        r2 = await c.post(
            f"{SUPABASE_URL}/rest/v1/capp_clients",
            json=row,
            headers={**_supa_headers_json(), "Prefer": "return=minimal"},
        )
    if r2.status_code not in (200, 201):
        raise HTTPException(status_code=500, detail=f"Supabase error: {r2.text}")

    return {"ok": True, "username": username, "password": pw, "client_id": client_id, "school": school}


@app.get("/admin/api/clients", dependencies=[Depends(_require_admin)])
async def admin_list_clients():
    async with httpx.AsyncClient() as c:
        r = await c.get(
            f"{SUPABASE_URL}/rest/v1/capp_clients",
            params={"select": "username,client_id,active,is_admin,seat_1_machine,seat_2_machine,notes,next_invoice_date",
                    "order": "username.asc"},
            headers=_supa_headers_json(),
        )
    if r.status_code != 200:
        raise HTTPException(status_code=500, detail=r.text)
    return r.json()


@app.get("/admin/api/gameday/status", dependencies=[Depends(_require_admin)])
def admin_gameday_status():
    """Game-day operator status: platform health, alerts, and per-game telemetry."""
    return _build_gameday_payload()


@app.patch("/admin/api/clients/{username}", dependencies=[Depends(_require_admin)])
async def admin_update_client(username: str, payload: dict = Body(...)):
    """Update editable fields: notes, next_invoice_date."""
    allowed = {k: v for k, v in payload.items() if k in ("notes", "next_invoice_date")}
    if not allowed:
        raise HTTPException(status_code=400, detail="No editable fields provided.")
    async with httpx.AsyncClient() as c:
        r = await c.patch(
            f"{SUPABASE_URL}/rest/v1/capp_clients",
            params={"username": f"eq.{username}"},
            json=allowed,
            headers={**_supa_headers_json(), "Prefer": "return=minimal"},
        )
    if r.status_code not in (200, 204):
        raise HTTPException(status_code=500, detail=r.text)
    return {"ok": True}


@app.patch("/admin/api/clients/{username}/reset-seat", dependencies=[Depends(_require_admin)])
async def admin_reset_single_seat(username: str, seat: int = Body(..., embed=True)):
    """Reset a single seat (1 or 2)."""
    col = "seat_1_machine" if seat == 1 else "seat_2_machine"
    async with httpx.AsyncClient() as c:
        r = await c.patch(
            f"{SUPABASE_URL}/rest/v1/capp_clients",
            params={"username": f"eq.{username}"},
            json={col: None},
            headers={**_supa_headers_json(), "Prefer": "return=minimal"},
        )
    if r.status_code not in (200, 204):
        raise HTTPException(status_code=500, detail=r.text)
    return {"ok": True}


@app.patch("/admin/api/clients/{username}/reset-seats", dependencies=[Depends(_require_admin)])
async def admin_reset_seats(username: str):
    async with httpx.AsyncClient() as c:
        r = await c.patch(
            f"{SUPABASE_URL}/rest/v1/capp_clients",
            params={"username": f"eq.{username}"},
            json={"seat_1_machine": None, "seat_2_machine": None},
            headers={**_supa_headers_json(), "Prefer": "return=minimal"},
        )
    if r.status_code not in (200, 204):
        raise HTTPException(status_code=500, detail=r.text)
    return {"ok": True}


@app.patch("/admin/api/clients/{username}/deactivate", dependencies=[Depends(_require_admin)])
async def admin_deactivate(username: str, active: bool = Body(..., embed=True)):
    async with httpx.AsyncClient() as c:
        r = await c.patch(
            f"{SUPABASE_URL}/rest/v1/capp_clients",
            params={"username": f"eq.{username}"},
            json={"active": active},
            headers={**_supa_headers_json(), "Prefer": "return=minimal"},
        )
    if r.status_code not in (200, 204):
        raise HTTPException(status_code=500, detail=r.text)
    return {"ok": True}


@app.delete("/admin/api/clients/{username}", dependencies=[Depends(_require_admin)])
async def admin_delete_client(username: str):
    """Permanently delete a client account from Supabase."""
    async with httpx.AsyncClient() as c:
        r = await c.delete(
            f"{SUPABASE_URL}/rest/v1/capp_clients",
            params={"username": f"eq.{username}"},
            headers={**_supa_headers_json(), "Prefer": "return=minimal"},
        )
    if r.status_code not in (200, 204):
        raise HTTPException(status_code=500, detail=r.text)
    return {"ok": True}


# ─────────────────────────────────────────────────────────────────────────────
# CAPP Friends
# ─────────────────────────────────────────────────────────────────────────────
from datetime import datetime as _dt, timezone as _tz


@app.get("/friends/directory", dependencies=[Depends(verify_api_key)])
async def friends_directory():
    """Return all school profiles. Every authenticated client can read all profiles."""
    async with httpx.AsyncClient() as c:
        r = await c.get(
            f"{SUPABASE_URL}/rest/v1/school_profiles",
            params={"select": "*", "order": "team.asc.nullslast"},
            headers=_supa_headers_json(),
            timeout=10,
        )
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail="Directory unavailable")
    return r.json()


@app.put("/friends/profile")
async def friends_save_profile(
    profile: dict = Body(...),
    client_id: str = Depends(get_client_id),
):
    """Upsert own school profile. client_id is always derived from the API key — never trusted from body."""
    profile.pop("client_id", None)
    profile["client_id"]   = client_id
    profile["updated_at"]  = _dt.now(_tz.utc).isoformat()
    async with httpx.AsyncClient() as c:
        r = await c.post(
            f"{SUPABASE_URL}/rest/v1/school_profiles",
            json=profile,
            headers={**_supa_headers_json(), "Prefer": "resolution=merge-duplicates,return=minimal"},
            timeout=10,
        )
    if r.status_code not in (200, 201, 204):
        raise HTTPException(status_code=502, detail=f"Save failed: {r.text[:200]}")
    return {"ok": True}


@app.get("/friends/conversations")
async def friends_conversations(client_id: str = Depends(get_client_id)):
    """Inbox: latest message per conversation partner + unread count."""
    async with httpx.AsyncClient() as c:
        r = await c.get(
            f"{SUPABASE_URL}/rest/v1/messages",
            params={
                "or":    f"(sender_id.eq.{client_id},recipient_id.eq.{client_id})",
                "order": "sent_at.desc",
                "limit": "500",
            },
            headers=_supa_headers_json(),
            timeout=10,
        )
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail="Conversations unavailable")
    msgs   = r.json()
    seen: dict = {}
    for msg in msgs:
        other = msg["recipient_id"] if msg["sender_id"] == client_id else msg["sender_id"]
        if other not in seen:
            seen[other] = {
                "client_id": other,
                "last_msg":  msg["body"],
                "last_ts":   msg["sent_at"],
                "unread":    0,
            }
        if msg["recipient_id"] == client_id and not msg.get("read_at"):
            seen[other]["unread"] += 1
    return list(seen.values())


@app.get("/friends/messages")
async def friends_get_messages(
    with_id: str = Query(...),
    client_id: str = Depends(get_client_id),
):
    """Full message thread between this client and another."""
    async with httpx.AsyncClient() as c:
        r = await c.get(
            f"{SUPABASE_URL}/rest/v1/messages",
            params={
                "or": (
                    f"(and(sender_id.eq.{client_id},recipient_id.eq.{with_id}),"
                    f"and(sender_id.eq.{with_id},recipient_id.eq.{client_id}))"
                ),
                "order": "sent_at.asc",
                "limit": "500",
            },
            headers=_supa_headers_json(),
            timeout=10,
        )
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail="Messages unavailable")
    return r.json()


@app.post("/friends/messages")
async def friends_send_message(
    body: dict = Body(...),
    client_id: str = Depends(get_client_id),
):
    """Send a message to another client."""
    to   = str(body.get("to", "")).strip()
    text = str(body.get("body", "")).strip()
    if not to or not text:
        raise HTTPException(status_code=400, detail="to and body required")
    async with httpx.AsyncClient() as c:
        r = await c.post(
            f"{SUPABASE_URL}/rest/v1/messages",
            json={"sender_id": client_id, "recipient_id": to, "body": text},
            headers={**_supa_headers_json(), "Prefer": "return=minimal"},
            timeout=10,
        )
    if r.status_code not in (200, 201, 204):
        raise HTTPException(status_code=502, detail=f"Send failed: {r.text[:200]}")
    return {"ok": True}


@app.post("/friends/messages/read")
async def friends_mark_read(
    body: dict = Body(...),
    client_id: str = Depends(get_client_id),
):
    """Mark all unread messages from a sender as read."""
    from_id = str(body.get("with_id", "")).strip()
    if not from_id:
        raise HTTPException(status_code=400, detail="with_id required")
    now_iso = _dt.now(_tz.utc).isoformat()
    async with httpx.AsyncClient() as c:
        r = await c.patch(
            f"{SUPABASE_URL}/rest/v1/messages",
            params={
                "sender_id":    f"eq.{from_id}",
                "recipient_id": f"eq.{client_id}",
                "read_at":      "is.null",
            },
            json={"read_at": now_iso},
            headers={**_supa_headers_json(), "Prefer": "return=minimal"},
            timeout=10,
        )
    if r.status_code not in (200, 204):
        raise HTTPException(status_code=502, detail="Mark read failed")
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
  .kpi-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin-bottom: 18px; }
  .kpi { background: #0d1117; border: 1px solid #2c3b55; border-radius: 10px; padding: 14px; }
  .kpi .label { color: #8b95a1; font-size: 11px; font-weight: 700; letter-spacing: 1px; text-transform: uppercase; }
  .kpi .value { color: #e2e8f0; font-size: 24px; font-weight: 700; margin-top: 8px; }
  .kpi .sub { color: #8b95a1; font-size: 12px; margin-top: 4px; }
  .alert-list { display: flex; flex-direction: column; gap: 10px; }
  .alert-item { border-radius: 10px; padding: 12px 14px; border: 1px solid #2c3b55; }
  .alert-red { background: #2a1111; border-color: #7f1d1d; color: #fecaca; }
  .alert-yellow { background: #2a210f; border-color: #92400e; color: #fde68a; }
  .alert-green { background: #102318; border-color: #166534; color: #bbf7d0; }
  .mono { font-family: Consolas, monospace; }
  .small { font-size: 12px; color: #8b95a1; }
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
    <button class="tab" onclick="showTab('gameday-tab', this)">Game Day</button>
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

    <div class="panel" id="gameday-tab">
      <div class="card">
        <h2>Game-Day Monitoring
          <button class="btn btn-primary" onclick="loadGameDayStatus()" style="float:right;font-size:12px;padding:5px 14px;">Refresh</button>
        </h2>
        <p class="small" style="margin-bottom:14px;">Platform health, alerting, and per-game delivery/QC telemetry for live operations.</p>
        <div id="gameday-summary"><div class="loading">Loading...</div></div>
        <div id="gameday-alerts" style="margin-top:18px;"></div>
        <div id="gameday-games" style="margin-top:18px;"></div>
        <div id="gameday-detail" style="margin-top:18px;"></div>
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
let _gameDayTimer = null;
let _gameDayGames = [];

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
  stopGameDayRefresh();
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
   stopGameDayRefresh();
  if (id === "clients-tab") loadClients();
  if (id === "create-tab") loadTeams();
  if (id === "gameday-tab") { loadGameDayStatus(); startGameDayRefresh(); }
  closeSlideout();
}

function api(method, path, body) {
  return fetch("/admin/api" + path, {
    method,
    headers: { "x-admin-token": _token, "Content-Type": "application/json" },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  }).then(r => r.json());
}

function fmtBytes(bytes) {
  if (!bytes) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  let value = bytes;
  let idx = 0;
  while (value >= 1024 && idx < units.length - 1) {
    value /= 1024;
    idx += 1;
  }
  return value.toFixed(1) + " " + units[idx];
}

function fmtAge(seconds) {
  if (seconds === null || seconds === undefined) return "—";
  if (seconds < 60) return seconds.toFixed(0) + "s";
  if (seconds < 3600) return (seconds / 60).toFixed(1) + "m";
  return (seconds / 3600).toFixed(1) + "h";
}

function badgeForLevel(level, text) {
  const cls = level === "red" ? "badge-red" : level === "yellow" ? "badge-blue" : "badge-green";
  return '<span class="badge ' + cls + '">' + text + '</span>';
}

function startGameDayRefresh() {
  stopGameDayRefresh();
  _gameDayTimer = setInterval(() => {
    const panel = document.getElementById("gameday-tab");
    if (panel && panel.classList.contains("active")) loadGameDayStatus();
  }, 15000);
}

function stopGameDayRefresh() {
  if (_gameDayTimer) {
    clearInterval(_gameDayTimer);
    _gameDayTimer = null;
  }
}

function loadGameDayStatus() {
  document.getElementById("gameday-summary").innerHTML = '<div class="loading">Loading summary...</div>';
  document.getElementById("gameday-alerts").innerHTML = '<div class="loading">Loading alerts...</div>';
  document.getElementById("gameday-games").innerHTML = '<div class="loading">Loading game telemetry...</div>';
  document.getElementById("gameday-detail").innerHTML = '<div class="loading">Select a game row to inspect fixes and remaining QC issues.</div>';
  api("GET", "/gameday/status").then(data => {
    if (!data || !data.summary) {
      document.getElementById("gameday-summary").innerHTML = '<div class="loading">Error loading game-day status.</div>';
      return;
    }
    const platform = data.platform || {};
    const requests = (platform.requests || {});
    const recent = (requests.recent_latency || {});
    const fetcher = (platform.fetcher || {});
    const memory = ((platform.memory || {}).rss_bytes || 0);
    document.getElementById("gameday-summary").innerHTML = `
      <div class="kpi-grid">
        <div class="kpi"><div class="label">Server Status</div><div class="value">${platform.status || "unknown"}</div><div class="sub">ready=${platform.ready}</div></div>
        <div class="kpi"><div class="label">Memory RSS</div><div class="value">${fmtBytes(memory)}</div><div class="sub">process memory</div></div>
        <div class="kpi"><div class="label">Req P95</div><div class="value">${recent.p95_ms || 0} ms</div><div class="sub">recent server latency</div></div>
        <div class="kpi"><div class="label">Tracked Games</div><div class="value">${data.summary.tracked_games || 0}</div><div class="sub">live=${data.summary.live_games || 0}</div></div>
        <div class="kpi"><div class="label">Poller</div><div class="value">${fetcher.poller_alive ? "Alive" : "Down"}</div><div class="sub">last poll ${fetcher.last_poll_duration_ms || 0} ms</div></div>
        <div class="kpi"><div class="label">Auto-Fixed</div><div class="value">${data.summary.auto_fix_total || 0}</div><div class="sub">${data.summary.games_with_auto_fixes || 0} games had fixes</div></div>
        <div class="kpi"><div class="label">QC Issues</div><div class="value">${data.summary.games_with_qc_issues || 0}</div><div class="sub">games still flagged</div></div>
      </div>
      <div class="small">Generated: ${new Date((data.generated_at || 0) * 1000).toLocaleString()}</div>
    `;

    const alerts = Array.isArray(data.alerts) ? data.alerts : [];
    if (!alerts.length) {
      document.getElementById("gameday-alerts").innerHTML = '<div class="alert-item alert-green">No active alerts right now.</div>';
    } else {
      document.getElementById("gameday-alerts").innerHTML =
        '<div class="alert-list">' +
        alerts.map(a => `<div class="alert-item alert-${a.level}"><strong>${(a.kind || "alert").toUpperCase()}</strong><div style="margin-top:4px;">${a.message}</div></div>`).join("") +
        '</div>';
    }

    const games = Array.isArray(data.games) ? data.games : [];
    _gameDayGames = games;
    if (!games.length) {
      document.getElementById("gameday-games").innerHTML = '<div class="loading">No tracked games in cache yet.</div>';
      document.getElementById("gameday-detail").innerHTML = '<div class="loading">No game detail available yet.</div>';
      return;
    }
    const rows = games.map(g => `
      <tr class="clickable" onclick="showGameDayDetail('${g.game_id}')">
        <td>${badgeForLevel(g.alert_level, (g.alert_level || "green").toUpperCase())}</td>
        <td>${g.away_name || "—"} at ${g.home_name || "—"}</td>
        <td>${g.status || "—"}</td>
        <td>${g.plays_count || 0}</td>
        <td>${g.auto_fixed_count || 0}</td>
        <td>${g.qc_issue_count || 0}</td>
        <td>${g.requests_last_60s || 0}</td>
        <td>${g.p95_latency_ms || 0} ms</td>
        <td>${fmtBytes(g.payload_bytes || 0)}</td>
        <td>${fmtAge(g.age_seconds)}</td>
        <td class="mono">${(g.auto_fixed_examples || []).slice(0, 2).join(", ") || "—"}</td>
        <td class="mono">${(g.alert_reasons || []).join(", ") || "—"}</td>
      </tr>
    `).join("");
    document.getElementById("gameday-games").innerHTML = `
      <table>
        <thead>
          <tr>
            <th>Level</th><th>Game</th><th>Status</th><th>Plays</th><th>Fixed</th><th>Flagged</th>
            <th>Req/60s</th><th>P95</th><th>Payload</th><th>Age</th><th>Fixed Examples</th><th>Notes</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    `;
    document.getElementById("gameday-detail").innerHTML = '<div class="loading">Click a game row to inspect QC details.</div>';
  }).catch(() => {
    document.getElementById("gameday-summary").innerHTML = '<div class="loading">Cannot reach game-day status endpoint.</div>';
    document.getElementById("gameday-alerts").innerHTML = "";
    document.getElementById("gameday-games").innerHTML = "";
    document.getElementById("gameday-detail").innerHTML = "";
  });
}

function showGameDayDetail(gameId) {
  const game = (_gameDayGames || []).find(g => String(g.game_id) === String(gameId));
  if (!game) return;
  const fixedExamples = (game.auto_fixed_examples || []).length
    ? (game.auto_fixed_examples || []).map(x => `<li>${x}</li>`).join("")
    : "<li>No auto-fixes recorded for this game.</li>";
  const flaggedExamples = (game.qc_examples || []).length
    ? (game.qc_examples || []).map(x => `<li>${x}</li>`).join("")
    : "<li>No remaining QC flags for this game.</li>";
  const notes = (game.alert_reasons || []).length
    ? (game.alert_reasons || []).join(", ")
    : "No active alerts for this game.";
  document.getElementById("gameday-detail").innerHTML = `
    <div class="card">
      <h2>${game.away_name || "—"} at ${game.home_name || "—"} <span class="small mono">(${game.game_id})</span></h2>
      <div class="kpi-grid">
        <div class="kpi"><div class="label">Alert Level</div><div class="value">${(game.alert_level || "green").toUpperCase()}</div><div class="sub">${notes}</div></div>
        <div class="kpi"><div class="label">Auto-Fixed</div><div class="value">${game.auto_fixed_count || 0}</div><div class="sub">manual gaps: ${game.manual_gap_count || 0}</div></div>
        <div class="kpi"><div class="label">Still Flagged</div><div class="value">${game.qc_issue_count || 0}</div><div class="sub">remaining QC issues</div></div>
        <div class="kpi"><div class="label">Latency</div><div class="value">${game.p95_latency_ms || 0} ms</div><div class="sub">avg ${game.avg_latency_ms || 0} ms</div></div>
        <div class="kpi"><div class="label">Traffic</div><div class="value">${game.requests_last_60s || 0}</div><div class="sub">req/60s, ${game.requests_last_300s || 0} req/5m</div></div>
        <div class="kpi"><div class="label">Payload</div><div class="value">${fmtBytes(game.payload_bytes || 0)}</div><div class="sub">plays=${game.plays_count || 0}, age=${fmtAge(game.age_seconds)}</div></div>
      </div>
      <div class="form-row">
        <div class="card" style="margin-bottom:0;">
          <h2>Auto-Fixed Examples</h2>
          <ul>${fixedExamples}</ul>
        </div>
        <div class="card" style="margin-bottom:0;">
          <h2>Still Flagged</h2>
          <ul>${flaggedExamples}</ul>
        </div>
      </div>
    </div>
  `;
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
