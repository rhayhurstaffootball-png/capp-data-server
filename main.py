from fastapi import FastAPI, Query, Header, HTTPException, Depends, Body, WebSocket, WebSocketDisconnect, Request
# smtplib is blocking. Sending a blast on the event loop would stall every
# other request on the server for the length of the send - on game day that is
# an outage, not a slow page.
from starlette.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse, RedirectResponse, Response, HTMLResponse
from typing import Optional, Dict, List
import os
import re
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
    mark_game_active,
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


_PATH_ID_RE = re.compile(r"/\d+|/[0-9a-fA-F-]{16,}")


def _norm_path(path: str) -> str:
    """Collapse dynamic path segments (game IDs, team IDs, machine IDs) to a
    placeholder so REQUEST_PATH_COUNTS stays bounded to a handful of route
    templates instead of growing one key per unique ID forever."""
    return _PATH_ID_RE.sub("/{id}", path)


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
        # Empty list == Supabase wired up correctly. Non-empty means the storage
        # routes are down and WHY — check this first when nodes/downloads fail.
        "config_errors": supabase_config_errors(),
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


def _evict_stale_poll_buffers():
    """Drop HTTP-poll frame/input buffers for sessions with no live WS session.
    Covers pure-HTTP-poll nodes whose WS-relay teardown cleanup never runs, so a
    stale (1440p) JPEG can't sit in RAM indefinitely after a node goes away."""
    now = time.time()
    # Age-based: the agent has stopped pushing, so nothing here can be current.
    # This is the one that catches a pure-HTTP-poll node, which never appears in
    # _vnc_sessions at all and so was invisible to the check below.
    stale = [k for k in list(_poll_frames.keys())
             if now - _poll_frame_seen.get(k, 0) > POLL_FRAME_TTL_SECONDS]
    # Original rule: a WS session existed and has since torn down.
    stale += [k for k in list(_poll_frames.keys())
              if k not in _vnc_sessions and k not in stale]
    for k in stale:
        _poll_frames.pop(k, None)
        _poll_frame_seen.pop(k, None)
    for k in [k for k in list(_poll_inputs.keys()) if k not in _vnc_sessions]:
        _poll_inputs.pop(k, None)
    # Never let the seen-maps outlive the frames they describe.
    for k in [k for k in list(_poll_frame_seen.keys()) if k not in _poll_frames]:
        _poll_frame_seen.pop(k, None)
    for k in [k for k in list(_poll_get_seen.keys())
              if now - _poll_get_seen[k] > POLL_FRAME_TTL_SECONDS * 4]:
        _poll_get_seen.pop(k, None)
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
    scheduler.add_job(_evict_stale_poll_buffers, "interval", hours=1,
                      id="evict_poll_buffers", replace_existing=True)
    scheduler.start()
    SCHEDULER_STARTED_AT = time.time()
    # MUST live here, not in @app.on_event("startup") — FastAPI IGNORES on_event
    # handlers entirely when a lifespan is supplied, which silently killed the
    # poller from Mar 10 2026 (commit ef8d443, the commit that added lifespan)
    # until Jul 30 2026. Anything that needs to run at startup goes in here.
    start_poller()
    yield
    scheduler.shutdown()

app = FastAPI(title="CAPP Data Server", lifespan=lifespan)

from fastapi.middleware.cors import CORSMiddleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://cappvcs.com", "https://www.cappvcs.com"],
    allow_origin_regex=r"https://.*\.pages\.dev",   # Cloudflare Pages preview/prod URLs
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
    allow_headers=["*"],
)


@app.middleware("http")
async def _metrics_middleware(request: Request, call_next):
    global REQUEST_TOTAL, REQUEST_ERRORS

    started = time.perf_counter()
    REQUEST_TOTAL += 1
    REQUEST_PATH_COUNTS[_norm_path(request.url.path)] += 1
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


def supabase_config_errors() -> list:
    """Which Supabase settings are missing/unusable. Empty list == healthy.

    Exists because of the Aug 12 2026 outage: SUPABASE_URL went missing from the
    Render environment and every storage-backed route (the CAPP Nodes list, all
    four download endpoints) answered a BARE 500 — httpx raising UnsupportedProtocol
    on a URL with no scheme, unhandled. Nothing said "misconfigured", so it read as
    a code fault and cost an unnecessary agent rebuild before the real cause was
    found. Now it is named, loudly, everywhere it matters.
    """
    errs = []
    if not SUPABASE_URL:
        errs.append("SUPABASE_URL is not set")
    elif not SUPABASE_URL.startswith(("http://", "https://")):
        errs.append(f"SUPABASE_URL has no http(s):// scheme: {SUPABASE_URL[:40]!r}")
    if not SUPABASE_KEY:
        errs.append("SUPABASE_SERVICE_KEY is not set")
    return errs


def _require_supabase():
    """Guard every storage call. Turns a bare 500 into a 503 that says the cause.

    Deliberately NOT a hard startup crash: a missing storage variable must not take
    down live-game polling, licensing, or the play feed on a Saturday. The server
    stays up and keeps serving everything it still can; only the routes that truly
    need Supabase fail, and they fail legibly.
    """
    errs = supabase_config_errors()
    if errs:
        raise HTTPException(
            status_code=503,
            detail="Supabase is not configured on this server: "
                   + "; ".join(errs)
                   + ". Set it in the Render dashboard (Environment) — storage routes "
                     "stay down until then.",
        )


# Loud, unmissable banner in the Render logs at boot.
_SB_ERRS = supabase_config_errors()
if _SB_ERRS:
    print("=" * 72, flush=True)
    print("*** SUPABASE MISCONFIGURED - storage routes WILL fail with 503 ***", flush=True)
    for _e in _SB_ERRS:
        print(f"  - {_e}", flush=True)
    print("  Affected: /nodes, /nodes/register, /agent/download, /helper/download,", flush=True)
    print("            /db/download, /contacts/download", flush=True)
    print("  Fix: Render dashboard -> capp-data-server -> Environment", flush=True)
    print("=" * 72, flush=True)
else:
    print(f"Supabase configured OK -> {SUPABASE_URL}", flush=True)


async def _bad_supabase_url_handler(request: Request, exc: Exception):
    """Catch-all so a broken SUPABASE_URL can never again masquerade as a 500.

    SUPABASE_URL is interpolated into ~180 request URLs across this file (auth,
    CAPP Nodes, Binder, CRM, messages) — guarding each call site individually would
    be worse than the disease. But every one of them fails the same way when the
    variable is missing or malformed: httpx rejects the URL string before any
    network call. Handling it in one place turns that entire class of failure into
    a 503 that names the cause, whatever route it came from.

    ⚠ Registered for UnsupportedProtocol and InvalidURL ONLY — the two "this URL
    string is wrong" errors. Do NOT widen this to their parents: UnsupportedProtocol
    descends from TransportError, so catching that would also swallow real timeouts
    and connection failures and mislabel a Supabase outage as a config problem.
    (Verified by MRO: UnsupportedProtocol -> TransportError -> RequestError; it is
    NOT a subclass of InvalidURL, which is why registering only InvalidURL silently
    missed every auth-dependent route.)
    """
    errs = supabase_config_errors()
    print(f"InvalidURL on {request.url.path}: {exc} | config_errors={errs}", flush=True)
    return JSONResponse(
        status_code=503,
        content={"detail": "Supabase is not configured on this server: "
                           + ("; ".join(errs) if errs else str(exc))
                           + ". Check the Render dashboard (Environment)."},
    )


app.add_exception_handler(httpx.UnsupportedProtocol, _bad_supabase_url_handler)
app.add_exception_handler(httpx.InvalidURL, _bad_supabase_url_handler)


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

@app.get("/health")
def health():
    return _build_health_payload()


_BETA_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>CAPP Beta</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      background: #0d0d0d;
      color: #f0f0f0;
      font-family: 'Segoe UI', Arial, sans-serif;
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      min-height: 100vh;
    }
    .logo {
      font-size: 3rem;
      font-weight: 800;
      letter-spacing: 0.15em;
      color: #ffffff;
      margin-bottom: 0.25rem;
    }
    .logo span { color: #e8a020; }
    .tagline {
      font-size: 0.95rem;
      color: #888;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      margin-bottom: 3rem;
    }
    .card {
      background: #1a1a1a;
      border: 1px solid #2a2a2a;
      border-radius: 12px;
      padding: 2.5rem 3rem;
      text-align: center;
      max-width: 480px;
      width: 90%;
    }
    .badge {
      display: inline-block;
      background: #e8a020;
      color: #000;
      font-size: 0.7rem;
      font-weight: 700;
      letter-spacing: 0.12em;
      text-transform: uppercase;
      padding: 0.25rem 0.75rem;
      border-radius: 20px;
      margin-bottom: 1.25rem;
    }
    .card h2 {
      font-size: 1.4rem;
      font-weight: 600;
      margin-bottom: 0.75rem;
    }
    .card p {
      font-size: 0.9rem;
      color: #999;
      line-height: 1.6;
      margin-bottom: 2rem;
    }
    .download-btn {
      display: inline-block;
      background: #e8a020;
      color: #000;
      font-size: 1rem;
      font-weight: 700;
      letter-spacing: 0.05em;
      text-decoration: none;
      padding: 0.85rem 2.5rem;
      border-radius: 8px;
      transition: background 0.2s;
    }
    .download-btn:hover { background: #f0b030; }
    .meta {
      font-size: 0.75rem;
      color: #555;
      margin-top: 1.5rem;
    }
  </style>
</head>
<body>
  <div class="logo">C<span>A</span>PP</div>
  <div class="tagline">Video Coordinator Suite</div>
  <div class="card">
    <div class="badge">Beta Access</div>
    <h2>Download CAPP v2.0.0</h2>
    <p>You've been invited to try the CAPP Beta.<br>
       Download the installer below and run it to get started.<br>
       Updates will be delivered automatically inside the app.</p>
    <a class="download-btn" href="https://relay.cappvcs.com/installer/download">
      Download Installer
    </a>
    <div class="meta">Windows 10/11 &nbsp;&middot;&nbsp; ~276 MB</div>
  </div>
</body>
</html>"""


@app.get("/beta", response_class=HTMLResponse, include_in_schema=False)
def beta_page():
    return HTMLResponse(_BETA_HTML)


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
    # Heartbeat: a client asking for plays means a user has this game open, which
    # is what registers it for background polling. No client change needed.
    mark_game_active(game_id, league)
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
    mark_game_active(game_id)          # heartbeat — see /plays above
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
                    "select": "client_id,active,licensed,created_at,trial_extension_days"},
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

    # Trial — clock starts at account created_at; admin can extend via trial_extension_days
    from datetime import datetime, timezone
    try:
        created_str = user.get("created_at", "")
        created_at  = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
        elapsed     = (datetime.now(timezone.utc) - created_at).days
        extension   = int(user.get("trial_extension_days") or 0)
        remaining   = max(0, TRIAL_DAYS + extension - elapsed)
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
        "select": "client_id,password_hash,salt,api_key,active,is_admin,"
                  "seat_1_machine,seat_2_machine,seat_3_machine,seat_limit"
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
    #
    # `seat` is a HINT, not an instruction. The client loops seat_1 then seat_2
    # and accepts whichever returns 200, so the server is free to decide which
    # seat a machine actually lands on. That is what lets a school have three
    # seats WITHOUT any client change: an installed 2.6.x asking for "seat_1"
    # on a third computer is simply bound to seat 3 and never knows.
    #
    # Machine binding is fully intact - the NUMBER of seats is what varies now,
    # not whether binding applies. (is_admin still skips binding entirely; that
    # is a different thing and deliberately untouched.)
    if not user.get("is_admin"):
        limit = user.get("seat_limit") or 2
        limit = max(1, min(3, int(limit)))
        seats = [user.get(f"seat_{n}_machine") for n in range(1, limit + 1)]

        if machine_id in seats:
            pass                        # already bound here - re-activation is a no-op
        elif None in seats:
            # Bind the lowest free seat. The is.null filter on the PATCH makes
            # this safe if two machines activate at the same moment: the second
            # write matches no row and is reported as a conflict, instead of
            # silently overwriting the first machine's binding.
            n = seats.index(None) + 1
            col = f"seat_{n}_machine"
            async with httpx.AsyncClient() as client:
                w = await client.patch(
                    f"{SUPABASE_URL}/rest/v1/capp_clients",
                    params={"username": f"eq.{username}", col: "is.null"},
                    json={col: machine_id},
                    headers={**_supabase_headers(), "Prefer": "return=representation"},
                )
            if w.status_code not in (200, 204) or (
                    w.status_code == 200 and not w.json()):
                raise HTTPException(
                    status_code=409,
                    detail="That seat was just taken by another computer. Try again.",
                )
        else:
            raise HTTPException(
                status_code=403,
                detail=(f"All {limit} seats are already activated on other machines. "
                        f"Release one at cappvcs.com/seats, then activate again."),
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
    _require_supabase()
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

@app.get("/helper/download")
async def helper_download():
    """Public signed URL for CAPPNodesHelper.exe — the Ctrl+Alt+Delete helper the
    agent installs on first run. Delivered separately (not embedded in the agent)
    to avoid the exe-in-exe antivirus/onefile-extraction problems."""
    return {"download_url": await _signed_url("shared/CAPPNodesHelper.exe")}


@app.get("/converter/download")
async def converter_download():
    """CAPP_Binder_Converter.exe — the per-coach local conversion worker (see
    BINDER LOCAL PLAN.txt). Served from the DO relay, NOT Supabase — the
    signed exe is ~128MB and Supabase's project-wide Storage upload limit
    rejects it on the free plan (same reason the >300MB installer lives on
    the relay too; same pattern as /app/download). No auth required — the
    Binder's 'Complete Setup' screen downloads this alongside a one-time
    pairing token for the currently-signed-in coach."""
    return {"download_url": "https://relay.cappvcs.com/converter/download"}


@app.get("/converter/version")
def converter_version():
    """Public — current CAPP Binder Converter version. Bump CONVERTER_VERSION
    on Render when a new CAPP_Binder_Converter.exe is uploaded."""
    return {"version": os.environ.get("CONVERTER_VERSION", "1.0.0")}

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
    """Installer served from DO relay — no Supabase file size limits."""
    return {"download_url": "https://relay.cappvcs.com/installer/download"}


@app.get("/agent/version")
def agent_version():
    """
    Public endpoint — returns current CAPPNodes Agent version.
    Bump AGENT_VERSION env var on Render when a new CAPPNodes_Agent.exe is uploaded.
    """
    version = os.environ.get("AGENT_VERSION", "2.0.0")
    return {"version": version}


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
    _require_supabase()
    path = _storage_path(client_id, NODES_FILE)
    url = f"{SUPABASE_URL}/storage/v1/object/{SUPABASE_BUCKET}/{path}"
    async with httpx.AsyncClient() as client:
        r = await client.get(url, headers=_supabase_headers())
    if r.status_code == 200:
        return r.json().get("nodes", [])
    return []

async def _save_nodes(client_id: str, nodes: list):
    _require_supabase()
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
    agent_version: str = Body("", embed=True),
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
        if agent_version:
            existing["agent_version"] = agent_version
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
            "agent_version": agent_version,
        })

    await _save_nodes(client_id, nodes)
    return {"status": "registered", "machine_name": machine_name, "rustdesk_id": rustdesk_id}


# A node re-registers every POLL_INTERVAL (300s) in the agent, so anything seen
# inside 3 missed check-ins is still healthy. Generous on purpose: a node that
# blinks offline in the panel every time one POST is slow is worse than useless.
NODE_ONLINE_WINDOW_SECONDS = 15 * 60


def _derive_node_status(node: dict) -> str:
    """Compute online/offline from last_seen.

    ⚠ The stored "status" field is set to "online" at registration and NEVER
    updated, so it reported EVERY node online forever — including one whose
    last_seen was 13 days stale. That actively misled a live outage diagnosis on
    Aug 18 2026, where the panel showed a storming, unusable node as healthy.
    last_seen is the only real signal, so status is now derived from it and the
    stored value is ignored on read.
    """
    from datetime import datetime, timezone
    seen = node.get("last_seen")
    if not seen:
        return "offline"
    try:
        ts = datetime.fromisoformat(str(seen).replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return "unknown"
    age = (datetime.now(timezone.utc) - ts).total_seconds()
    return "online" if age <= NODE_ONLINE_WINDOW_SECONDS else "offline"


@app.get("/nodes")
async def nodes_list(client_id: str = Depends(get_client_id)):
    """List all registered nodes for this client."""
    nodes = await _load_nodes(client_id)
    for n in nodes:
        n["status"] = _derive_node_status(n)
        n.setdefault("agent_version", "")
    return {"nodes": nodes}


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
                    # Release the HTTP-poll fallback buffers for this session —
                    # _poll_frames holds a full (1440p) JPEG and was never freed.
                    _poll_frames.pop(session_key, None)
                    _poll_inputs.pop(session_key, None)
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
_poll_frame_seen: Dict[str, float] = {}   # key → time.time() of the last agent PUT
_poll_get_seen:   Dict[str, float] = {}   # key → first GET while NO frame existed

# How long a frame stays servable after the agent stops pushing. Past this the
# session is over as far as the server is concerned, and a still-polling viewer
# is told 410 Gone so it STOPS instead of polling into the void.
# Aug 15 2026: one forgotten viewer polled ~30x/min for 28 HOURS and accounted
# for 63% of every request this server handled. The client now stops itself on
# navigate-away and after 15 min idle — this is the backstop for clients already
# in the field that will never receive that update.
POLL_FRAME_TTL_SECONDS = 90


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
    _poll_frame_seen[key] = time.time()

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
    key = f"{client_id}:{machine_id}"
    frame = _poll_frames.get(key)
    if not frame:
        # No frame AT ALL for this session. Normally that just means the agent
        # hasn't pushed its first one yet, so 404 = "keep waiting".
        # But an ABANDONED viewer sits here forever: after a server restart the
        # buffer is empty, so the stale-frame 410 below can never fire and the
        # client polls 404 for eternity. That is exactly the session that ran
        # 28 hours. So: give a genuinely-connecting viewer POLL_FRAME_TTL_SECONDS
        # of grace, then call it over. Works on clients already in the field,
        # which is the whole point of having a server-side guard.
        first = _poll_get_seen.setdefault(key, time.time())
        if time.time() - first > POLL_FRAME_TTL_SECONDS:
            _poll_get_seen.pop(key, None)
            raise HTTPException(status_code=410, detail="Session ended (agent never connected)")
        raise HTTPException(status_code=404, detail="No frame available")
    _poll_get_seen.pop(key, None)   # a real frame arrived — reset the grace window
    # The agent has stopped pushing — the session is over. 410 (not 404) so the
    # viewer can tell "nothing yet, keep waiting" from "this is finished, stop".
    age = time.time() - _poll_frame_seen.get(key, 0)
    if age > POLL_FRAME_TTL_SECONDS:
        _poll_frames.pop(key, None)
        _poll_inputs.pop(key, None)
        _poll_frame_seen.pop(key, None)
        raise HTTPException(status_code=410, detail="Session ended (no frames from agent)")
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


# ── Binder Wall #2 — Row-Level Security enforcement ─────────────────────────
# _supa_headers_json() above uses the SERVICE-ROLE key, which BYPASSES Postgres
# RLS by design (Supabase grants service_role the BYPASSRLS attribute). That's
# correct for the Owner admin panel and the conversion worker (both are meant
# to see across teams) but WRONG for player/coach/team-admin requests — those
# must be enforced by the database itself, independent of the app's own
# team_id filters (Wall #1), so a bug in application code can never leak
# another team's data. "Total isolation... a data breach would ruin us."
# (Roger, Jul 8 2026.)
#
# Mechanism: PostgREST accepts ANY JWT signed with the project's JWT secret —
# not only ones issued by Supabase Auth. So for every team-scoped request we
# mint a short-lived JWT carrying {role: authenticated, team_id: <caller's
# team>}, and RLS policies (playbook_rls_policies.sql) check
# auth.jwt()->>'team_id' against each row's team_id. The DB then physically
# cannot return another team's rows to this request, no matter what filter
# the Python code did or didn't apply.
#
# SAFE-BY-DEFAULT ROLLOUT: if SUPABASE_JWT_SECRET (or SUPABASE_ANON_KEY) isn't
# set, _scoped_headers() silently falls back to the service-role key — i.e.
# Wall #1 only, today's behavior, zero risk of breaking anything. Wall #2
# switches ON automatically the moment both env vars are set on Render; no
# code change needed at that point. Find both values in the Supabase
# dashboard: Project Settings -> API -> "JWT Secret" and "anon public" key.
SUPABASE_JWT_SECRET = os.environ.get("SUPABASE_JWT_SECRET", "")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "")

import hmac as _rls_hmac
import base64 as _rls_b64


def _sign_hs256(payload: dict, secret: str) -> str:
    """Hand-rolled HS256 JWT (header+payload+sig, base64url, no padding) — no
    new dependency, same pattern as this file's R2 SigV4 signer."""
    header = {"alg": "HS256", "typ": "JWT"}
    def _b64u(obj) -> str:
        raw = json.dumps(obj, separators=(",", ":")).encode() if isinstance(obj, dict) else obj
        return _rls_b64.urlsafe_b64encode(raw).rstrip(b"=").decode()
    signing_input = f"{_b64u(header)}.{_b64u(payload)}".encode()
    sig = _rls_hmac.new(secret.encode(), signing_input, hashlib.sha256).digest()
    sig_b64 = _rls_b64.urlsafe_b64encode(sig).rstrip(b"=").decode()
    return f"{signing_input.decode()}.{sig_b64}"


def _scoped_headers(team_id: str) -> dict:
    """Headers for a Supabase REST call that must be confined to ONE team by
    the database itself (Wall #2), not just by the query filters we add in
    Python (Wall #1). Falls back to the service key (Wall #1 only) if RLS
    enforcement isn't configured yet — see the block comment above."""
    if not SUPABASE_JWT_SECRET or not SUPABASE_ANON_KEY:
        return _supa_headers_json()
    now = int(time.time())
    token = _sign_hs256(
        {"role": "authenticated", "team_id": str(team_id), "iat": now, "exp": now + 120},
        SUPABASE_JWT_SECRET,
    )
    return {
        "Authorization": f"Bearer {token}",
        "apikey": SUPABASE_ANON_KEY,
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


def _school_username_base(name: str) -> str:
    """'Florida State' -> 'FloridaState'  |  'Air Force' -> 'AirForce'"""
    words = _re.sub(r"[^a-zA-Z0-9 ]", "", name.strip()).split()
    return "".join(w.capitalize() for w in words) or "School"


def _send_registration_email(to_email: str, to_name: str, username: str, school: str) -> None:
    """Send the assigned-username confirmation email via SMTP (env-configured)."""
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    smtp_host = os.environ.get("SMTP_HOST", "")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER", "")
    smtp_pass = os.environ.get("SMTP_PASS", "")
    from_addr = os.environ.get("FROM_EMAIL", smtp_user)

    if not smtp_host or not smtp_user or not smtp_pass:
        return  # email not configured — skip silently

    body_html = f"""
    <div style="font-family:'Segoe UI',Arial,sans-serif;background:#070a0f;color:#e8edf5;padding:40px 0;">
      <div style="max-width:480px;margin:0 auto;background:#111825;border:1px solid rgba(255,255,255,0.07);border-radius:12px;padding:36px;">
        <div style="text-align:center;margin-bottom:24px;">
          <img src="https://cappvcs.com/capplogo.png" alt="CAPP" style="height:40px;" />
        </div>
        <h2 style="color:#e8edf5;font-size:1.25rem;font-weight:700;margin:0 0 8px;">Welcome to CAPP, {to_name.split()[0]}!</h2>
        <p style="color:#8b96a8;font-size:0.9rem;margin:0 0 24px;">
          Your account for <strong style="color:#e8edf5;">{school}</strong> has been created.
          Here are your login credentials:
        </p>
        <div style="background:rgba(58,126,191,0.08);border:1px solid rgba(58,126,191,0.25);border-radius:8px;padding:16px 20px;margin-bottom:24px;">
          <div style="font-size:0.75rem;font-weight:700;letter-spacing:0.08em;text-transform:uppercase;color:#8b96a8;margin-bottom:4px;">Username</div>
          <div style="font-size:1.1rem;font-weight:700;color:#3a7ebf;letter-spacing:0.03em;">{username}</div>
        </div>
        <p style="color:#8b96a8;font-size:0.85rem;margin:0 0 8px;">
          Open the CAPP app, click <strong style="color:#e8edf5;">Sign In</strong>, and enter the username above along with the password you chose during registration.
        </p>
        <p style="color:#8b96a8;font-size:0.85rem;margin:0;">
          Questions? Reply to this email or contact <a href="mailto:roger@cappvcs.com" style="color:#3a7ebf;">roger@cappvcs.com</a>.
        </p>
      </div>
    </div>
    """

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Your CAPP username: {username}"
    msg["From"]    = f"CAPP Video Coordinator Suite <{from_addr}>"
    msg["To"]      = to_email
    msg.attach(MIMEText(body_html, "html"))

    with smtplib.SMTP(smtp_host, smtp_port) as s:
        s.starttls()
        s.login(smtp_user, smtp_pass)
        s.sendmail(from_addr, [to_email], msg.as_string())


# ── Binder playbook-update notifications — email + Web Push ─────────────────
# Coach-triggered (never automatic): a "Send Notification" control on the
# upload page lets a coach pick position(s), or All Team, and fire one
# notification. No opt-in step — anyone signed in (web or the installed app)
# is eligible; email always fires (everyone has one on file), push fires for
# whichever of the recipient's devices have actually granted notification
# permission (a hard browser/OS requirement, not something an app can skip).
VAPID_PUBLIC_KEY = os.environ.get("VAPID_PUBLIC_KEY", "")
VAPID_PRIVATE_KEY = os.environ.get("VAPID_PRIVATE_KEY", "")
VAPID_CLAIMS_EMAIL = os.environ.get("VAPID_CLAIMS_EMAIL", "roger@cappvcs.com")


def _send_playbook_update_email(to_email: str, to_name: str, team_name: str,
                                 folder_path: str, message: str) -> None:
    """Same SMTP pattern as _send_registration_email. Silently skips if SMTP
    isn't configured — never blocks/breaks the notify request over this."""
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    smtp_host = os.environ.get("SMTP_HOST", "")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER", "")
    smtp_pass = os.environ.get("SMTP_PASS", "")
    from_addr = os.environ.get("FROM_EMAIL", smtp_user)
    if not smtp_host or not smtp_user or not smtp_pass:
        return

    first = (to_name.split() or [to_name])[0] if to_name else "there"
    folder_line = f'New content in <strong style="color:#e8edf5;">{folder_path}</strong>.' if folder_path else "The playbook has been updated."
    msg_line = f'<p style="color:#8b96a8;font-size:0.85rem;margin:16px 0 0;">{message}</p>' if message else ""
    body_html = f"""
    <div style="font-family:'Segoe UI',Arial,sans-serif;background:#070a0f;color:#e8edf5;padding:40px 0;">
      <div style="max-width:480px;margin:0 auto;background:#111825;border:1px solid rgba(255,255,255,0.07);border-radius:12px;padding:36px;">
        <div style="text-align:center;margin-bottom:24px;">
          <img src="https://cappvcs.com/capplogo.png" alt="CAPP" style="height:36px;" />
        </div>
        <h2 style="color:#e8edf5;font-size:1.15rem;font-weight:700;margin:0 0 10px;">Hey {first} — the {team_name} playbook was just updated</h2>
        <p style="color:#8b96a8;font-size:0.9rem;margin:0;">{folder_line}</p>
        {msg_line}
        <a href="https://www.cappvcs.com/binder/" style="display:inline-block;margin-top:22px;background:#0873EC;color:#fff;text-decoration:none;padding:10px 20px;border-radius:8px;font-size:0.85rem;font-weight:700;">Open the Binder</a>
      </div>
    </div>
    """
    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"{team_name} Playbook Updated"
    msg["From"]    = f"CAPP Binder <{from_addr}>"
    msg["To"]      = to_email
    msg.attach(MIMEText(body_html, "html"))
    with smtplib.SMTP(smtp_host, smtp_port) as s:
        s.starttls()
        s.login(smtp_user, smtp_pass)
        s.sendmail(from_addr, [to_email], msg.as_string())


def _send_push(sub: dict, title: str, body: str) -> str:
    """Push one notification to one subscribed device. Returns "ok",
    "expired" (dead subscription — 404/410, caller should delete the row),
    "unconfigured" (VAPID keys not set), or "failed" (any other error)."""
    if not VAPID_PUBLIC_KEY or not VAPID_PRIVATE_KEY:
        return "unconfigured"
    try:
        from pywebpush import webpush
        webpush(
            subscription_info={
                "endpoint": sub["endpoint"],
                "keys": {"p256dh": sub["p256dh"], "auth": sub["auth"]},
            },
            data=json.dumps({"title": title, "body": body, "url": "/binder/"}),
            vapid_private_key=VAPID_PRIVATE_KEY,
            vapid_claims={"sub": f"mailto:{VAPID_CLAIMS_EMAIL}"},
            ttl=86400,
        )
        return "ok"
    except Exception as e:
        status = getattr(getattr(e, "response", None), "status_code", None)
        return "expired" if status in (404, 410) else "failed"


@app.post("/register")
async def self_register(
    school:     str = Body(..., embed=True),
    email:      str = Body(..., embed=True),
    password:   str = Body(..., embed=True),
    name:       str = Body("", embed=True),
    conference: str = Body("", embed=True),
):
    """
    Self-service registration from cappvcs.com/register.
    Generates a SchoolName1/SchoolName2 username, sends it by email, and
    returns the username so the website can display it.  The app signs in
    separately via /auth/login.
    """
    email  = email.strip().lower()
    school = school.strip()
    name   = name.strip()

    if not email or "@" not in email:
        raise HTTPException(status_code=400, detail="A valid email address is required.")
    if not school:
        raise HTTPException(status_code=400, detail="School / team name is required.")
    if not password:
        raise HTTPException(status_code=400, detail="Password is required.")

    # (Email uniqueness is not enforced server-side; seat limits per school
    #  prevent duplicate registrations for the same school.)

    # Determine username: SchoolName1 or SchoolName2 (max 2 seats per school)
    base_username = _school_username_base(school)
    base_slug     = _school_slug(school)

    async with httpx.AsyncClient() as c:
        # Find the first available seat number
        assigned_username = None
        for seat in (1, 2):
            candidate = f"{base_username}{seat}"
            rc = await c.get(
                f"{SUPABASE_URL}/rest/v1/capp_clients",
                params={"username": f"eq.{candidate}", "select": "username"},
                headers=_supa_headers_json(),
            )
            if rc.status_code == 200 and not rc.json():
                assigned_username = candidate
                break

        if assigned_username is None:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Both seats for {school} are already registered. "
                    "Contact roger@cappvcs.com if you need access."
                ),
            )

        # Resolve a unique client_id slug
        client_id = base_slug
        for n in range(2, 20):
            rc2 = await c.get(
                f"{SUPABASE_URL}/rest/v1/capp_clients",
                params={"client_id": f"eq.{client_id}", "select": "client_id"},
                headers=_supa_headers_json(),
            )
            if rc2.status_code == 200 and not rc2.json():
                break
            client_id = f"{base_slug}_{n}"

    salt    = _secrets.token_hex(16)
    pw_hash = _hash_pw(password, salt)
    api_key = _gen_api_key()

    row = {
        "client_id":      client_id,
        "username":       assigned_username,
        "email":          email,
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

    # Send confirmation email (non-blocking — don't fail registration if email fails)
    try:
        _send_registration_email(email, name or school, assigned_username, school)
        print(f"[REGISTER] Email sent to {email} for {assigned_username}", flush=True)
    except Exception as _email_err:
        print(f"[REGISTER] Email FAILED for {assigned_username} -> {email}: {_email_err}", flush=True)

    return {
        "username": assigned_username,
        "message":  "Account created. Check your email for your login username.",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Self-service password reset (cappvcs.com/reset)
# Token is stored HASHED in capp_clients (reset_token_hash) with a 60-min
# expiry; the raw token only ever exists inside the emailed link. Single-use.
# ─────────────────────────────────────────────────────────────────────────────
RESET_PAGE_URL = "https://cappvcs.com/reset"
_RESET_TOKEN_TTL_MIN = 60
_reset_last_sent: dict = {}   # username -> epoch seconds (throttle repeat emails)


def _send_password_reset_email(to_email: str, username: str, reset_url: str) -> None:
    """Same SMTP pattern as _send_registration_email."""
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    smtp_host = os.environ.get("SMTP_HOST", "")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER", "")
    smtp_pass = os.environ.get("SMTP_PASS", "")
    from_addr = os.environ.get("FROM_EMAIL", smtp_user)
    if not smtp_host or not smtp_user or not smtp_pass:
        raise RuntimeError("SMTP not configured")

    body_html = f"""
    <div style="font-family:'Segoe UI',Arial,sans-serif;background:#070a0f;color:#e8edf5;padding:40px 0;">
      <div style="max-width:480px;margin:0 auto;background:#111825;border:1px solid rgba(255,255,255,0.07);border-radius:12px;padding:36px;">
        <div style="text-align:center;margin-bottom:24px;">
          <img src="https://cappvcs.com/capplogo.png" alt="CAPP" style="height:40px;" />
        </div>
        <h2 style="color:#e8edf5;font-size:1.25rem;font-weight:700;margin:0 0 8px;">Reset your CAPP password</h2>
        <p style="color:#8b96a8;font-size:0.9rem;margin:0 0 24px;">
          A password reset was requested for the account
          <strong style="color:#3a7ebf;">{username}</strong>. Click the button below to
          choose a new password. This link works once and expires in {_RESET_TOKEN_TTL_MIN} minutes.
        </p>
        <div style="text-align:center;margin-bottom:24px;">
          <a href="{reset_url}" style="display:inline-block;background:#3a7ebf;color:#ffffff;text-decoration:none;font-weight:700;font-size:0.95rem;padding:12px 28px;border-radius:8px;">Choose a New Password</a>
        </div>
        <p style="color:#8b96a8;font-size:0.8rem;margin:0;">
          Didn't request this? You can ignore this email — your password is unchanged.
          Questions? Contact <a href="mailto:roger@cappvcs.com" style="color:#3a7ebf;">roger@cappvcs.com</a>.
        </p>
      </div>
    </div>
    """

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Reset your CAPP password ({username})"
    msg["From"]    = f"CAPP Video Coordinator Suite <{from_addr}>"
    msg["To"]      = to_email
    msg.attach(MIMEText(body_html, "html"))

    with smtplib.SMTP(smtp_host, smtp_port) as s:
        s.starttls()
        s.login(smtp_user, smtp_pass)
        s.sendmail(from_addr, [to_email], msg.as_string())


async def _issue_reset_token(username: str, email: str) -> None:
    """Generate a fresh token for this account, store its hash + expiry, and
    email the reset link. Raises on SMTP/storage failure."""
    token   = _secrets.token_urlsafe(32)
    th      = hashlib.sha256(token.encode()).hexdigest()
    expires = (_dt.now(_tz.utc) + _timedelta(minutes=_RESET_TOKEN_TTL_MIN)).isoformat()
    async with httpx.AsyncClient() as c:
        r = await c.patch(
            f"{SUPABASE_URL}/rest/v1/capp_clients",
            params={"username": f"eq.{username}"},
            json={"reset_token_hash": th, "reset_token_expires": expires},
            headers={**_supa_headers_json(), "Prefer": "return=minimal"},
        )
    if r.status_code not in (200, 204):
        raise RuntimeError(f"token store failed: {r.text}")
    _send_password_reset_email(email, username, f"{RESET_PAGE_URL}?token={token}")


@app.post("/auth/reset-request")
async def auth_reset_request(account: str = Body(..., embed=True)):
    """Start a password reset. `account` is a username OR the email used at
    registration. Response is intentionally the same whether or not the
    account exists (no account probing)."""
    acct = (account or "").strip()
    generic = {"ok": True, "message": "If that account has an email on file, a reset link is on its way."}
    if not acct:
        raise HTTPException(status_code=400, detail="Enter your username or email.")

    async with httpx.AsyncClient() as c:
        r = await c.get(
            f"{SUPABASE_URL}/rest/v1/capp_clients",
            params={"username": f"eq.{acct}", "select": "username,email"},
            headers=_supa_headers_json(),
        )
        rows = r.json() if r.status_code == 200 else []
        if not rows and "@" in acct:
            # Both seats of a school can share an email — reset every match.
            r2 = await c.get(
                f"{SUPABASE_URL}/rest/v1/capp_clients",
                params={"email": f"eq.{acct.lower()}", "select": "username,email"},
                headers=_supa_headers_json(),
            )
            rows = r2.json() if r2.status_code == 200 else []

    now = _time.time()
    for row in rows[:5]:
        uname, email = row.get("username"), (row.get("email") or "").strip()
        if not email:
            print(f"[RESET] {uname}: no email on file — cannot send link", flush=True)
            continue
        if now - _reset_last_sent.get(uname, 0) < 60:
            continue   # throttle: one email per account per minute
        try:
            await _issue_reset_token(uname, email)
            _reset_last_sent[uname] = now
            print(f"[RESET] Link sent to {email} for {uname}", flush=True)
        except Exception as e:
            print(f"[RESET] FAILED for {uname}: {e}", flush=True)
    return generic


@app.post("/auth/reset-confirm")
async def auth_reset_confirm(
    token:    str = Body(..., embed=True),
    password: str = Body(..., embed=True),
):
    """Finish a password reset: validate the emailed token, set the new
    password. api_key is untouched, so CAPP Node agents keep working."""
    if len(password or "") < 6:
        raise HTTPException(status_code=400, detail="Password must be at least 6 characters.")
    th = hashlib.sha256((token or "").strip().encode()).hexdigest()
    async with httpx.AsyncClient() as c:
        r = await c.get(
            f"{SUPABASE_URL}/rest/v1/capp_clients",
            params={"reset_token_hash": f"eq.{th}", "select": "username,reset_token_expires"},
            headers=_supa_headers_json(),
        )
        rows = r.json() if r.status_code == 200 else []
        if not rows:
            raise HTTPException(status_code=400, detail="This reset link is invalid or was already used. Request a new one.")
        row     = rows[0]
        uname   = row["username"]
        expired = True
        try:
            exp = _dt.fromisoformat((row.get("reset_token_expires") or "").replace("Z", "+00:00"))
            expired = _dt.now(_tz.utc) > exp
        except ValueError:
            pass
        clear = {"reset_token_hash": None, "reset_token_expires": None}
        if expired:
            await c.patch(
                f"{SUPABASE_URL}/rest/v1/capp_clients",
                params={"username": f"eq.{uname}"},
                json=clear,
                headers={**_supa_headers_json(), "Prefer": "return=minimal"},
            )
            raise HTTPException(status_code=400, detail="This reset link has expired. Request a new one.")
        salt = _secrets.token_hex(16)
        r2 = await c.patch(
            f"{SUPABASE_URL}/rest/v1/capp_clients",
            params={"username": f"eq.{uname}"},
            json={"password_hash": _hash_pw(password, salt), "salt": salt, **clear},
            headers={**_supa_headers_json(), "Prefer": "return=minimal"},
        )
        if r2.status_code not in (200, 204):
            raise HTTPException(status_code=500, detail="Could not update the password. Try again.")
    print(f"[RESET] Password updated for {uname}", flush=True)
    return {"ok": True, "username": uname}


# ─────────────────────────────────────────────────────────────────────────────
# Strict payments (cappvcs.com/pay)
# A payment REQUIRES a valid Invoice/Quote number from capp_sales_docs (rows
# pushed by the Sales Docs tool on Generate, or added manually in the admin
# panel). Checkout is a Stripe Checkout Session for the EXACT amount on the
# document — no fixed-price buy buttons.
# ─────────────────────────────────────────────────────────────────────────────
STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY", "")
PAY_PAGE_URL = "https://cappvcs.com/pay"


def _norm_doc_number(number: str) -> str:
    return (number or "").strip().upper()


async def _fetch_sales_doc(c: httpx.AsyncClient, number: str):
    r = await c.get(
        f"{SUPABASE_URL}/rest/v1/capp_sales_docs",
        params={"number": f"eq.{number}", "select": "*"},
        headers=_supa_headers_json(),
    )
    rows = r.json() if r.status_code == 200 else []
    return rows[0] if rows else None


def _public_doc(doc: dict) -> dict:
    return {
        "number":       doc["number"],
        "doc_type":     doc.get("doc_type", "invoice"),
        "school":       doc.get("school", ""),
        "description":  doc.get("description", ""),
        "amount_cents": doc.get("amount_cents", 0),
        "status":       doc.get("status", "unpaid"),
    }


@app.post("/pay/lookup")
async def pay_lookup(number: str = Body(..., embed=True)):
    """Validate an Invoice/Quote number and return what's owed."""
    num = _norm_doc_number(number)
    if not num:
        raise HTTPException(status_code=400, detail="Enter your Invoice or Quote number.")
    async with httpx.AsyncClient() as c:
        doc = await _fetch_sales_doc(c, num)
    if not doc or doc.get("status") == "void":
        raise HTTPException(
            status_code=404,
            detail="That number wasn't found. Check it against your invoice or quote, "
                   "or email roger@cappvcs.com.")
    return _public_doc(doc)


@app.post("/pay/checkout")
async def pay_checkout(number: str = Body(..., embed=True)):
    """Create a Stripe Checkout Session for the exact amount on the document."""
    num = _norm_doc_number(number)
    async with httpx.AsyncClient() as c:
        doc = await _fetch_sales_doc(c, num)
        if not doc or doc.get("status") == "void":
            raise HTTPException(status_code=404, detail="That number wasn't found.")
        if doc.get("status") == "paid":
            raise HTTPException(status_code=409, detail="This document is already paid — thank you!")
        if not STRIPE_SECRET_KEY:
            raise HTTPException(
                status_code=503,
                detail="Online card payment is temporarily unavailable — "
                       "email roger@cappvcs.com for a payment link.")
        label = f"{doc['number']} — CAPP Video Coordinator Suite ({doc.get('school','')})"
        sr = await c.post(
            "https://api.stripe.com/v1/checkout/sessions",
            headers={"Authorization": f"Bearer {STRIPE_SECRET_KEY}"},
            data={
                "mode": "payment",
                "client_reference_id": doc["number"],
                "metadata[number]": doc["number"],
                "success_url": f"{PAY_PAGE_URL}?session_id={{CHECKOUT_SESSION_ID}}",
                "cancel_url":  f"{PAY_PAGE_URL}?canceled=1",
                "line_items[0][quantity]": "1",
                "line_items[0][price_data][currency]": "usd",
                "line_items[0][price_data][unit_amount]": str(int(doc["amount_cents"])),
                "line_items[0][price_data][product_data][name]": label,
            },
        )
        if sr.status_code != 200:
            print(f"[PAY] Stripe session create FAILED for {num}: {sr.text[:300]}", flush=True)
            raise HTTPException(status_code=502, detail="Could not start checkout. Try again in a minute.")
        session = sr.json()
        await c.patch(
            f"{SUPABASE_URL}/rest/v1/capp_sales_docs",
            params={"number": f"eq.{num}"},
            json={"stripe_session_id": session["id"]},
            headers={**_supa_headers_json(), "Prefer": "return=minimal"},
        )
    print(f"[PAY] Checkout started for {num} (${doc['amount_cents']/100:.2f})", flush=True)
    return {"url": session["url"]}


@app.post("/pay/confirm")
async def pay_confirm(session_id: str = Body(..., embed=True)):
    """Landing check after Stripe redirects back: verify the session really
    paid, then mark the document paid + advance the CRM prospect."""
    sid = (session_id or "").strip()
    if not sid:
        raise HTTPException(status_code=400, detail="Missing session id.")
    if not STRIPE_SECRET_KEY:
        raise HTTPException(status_code=503, detail="Payment verification unavailable.")
    async with httpx.AsyncClient() as c:
        sr = await c.get(
            f"https://api.stripe.com/v1/checkout/sessions/{sid}",
            headers={"Authorization": f"Bearer {STRIPE_SECRET_KEY}"},
        )
        if sr.status_code != 200:
            raise HTTPException(status_code=404, detail="Payment session not found.")
        session = sr.json()
        num = _norm_doc_number((session.get("metadata") or {}).get("number", ""))
        if session.get("payment_status") != "paid":
            raise HTTPException(status_code=402, detail="This payment hasn't completed.")
        doc = await _fetch_sales_doc(c, num) if num else None
        if not doc:
            raise HTTPException(status_code=404, detail="Paid session has no matching document — email roger@cappvcs.com.")
        if doc.get("status") != "paid":
            await c.patch(
                f"{SUPABASE_URL}/rest/v1/capp_sales_docs",
                params={"number": f"eq.{num}"},
                json={"status": "paid", "paid_at": _dt.now(_tz.utc).isoformat(),
                      "stripe_session_id": sid},
                headers={**_supa_headers_json(), "Prefer": "return=minimal"},
            )
            # Advance the CRM prospect to Paid (best-effort; never blocks the receipt)
            try:
                school = (doc.get("school") or "").strip()
                if school:
                    pr = await c.get(
                        f"{SUPABASE_URL}/rest/v1/capp_prospects",
                        params={"school": f"ilike.{school}", "select": "id,status"},
                        headers=_supa_headers_json(),
                    )
                    for p in (pr.json() if pr.status_code == 200 else []):
                        await c.patch(
                            f"{SUPABASE_URL}/rest/v1/capp_prospects",
                            params={"id": f"eq.{p['id']}"},
                            json={"status": "Paid", "updated_at": _dt.now(_tz.utc).isoformat()},
                            headers={**_supa_headers_json(), "Prefer": "return=minimal"},
                        )
            except Exception as e:
                print(f"[PAY] CRM advance failed for {num}: {e}", flush=True)
            print(f"[PAY] ✓ PAID {num} (${doc['amount_cents']/100:.2f}) — {doc.get('school','')}", flush=True)
    return {"ok": True, **_public_doc({**doc, "status": "paid"})}


# ── Admin: sales-doc records behind the pay page ─────────────────────────────
_SALES_DOC_TYPES = ("quote", "agreement", "invoice")


@app.get("/admin/api/salesdocs", dependencies=[Depends(_require_admin)])
async def admin_list_salesdocs():
    async with httpx.AsyncClient() as c:
        r = await c.get(
            f"{SUPABASE_URL}/rest/v1/capp_sales_docs",
            params={"select": "*", "order": "created_at.desc"},
            headers=_supa_headers_json(),
        )
    if r.status_code != 200:
        raise HTTPException(status_code=500, detail=r.text)
    return r.json()


@app.post("/admin/api/salesdocs", dependencies=[Depends(_require_admin)])
async def admin_upsert_salesdoc(payload: dict = Body(...)):
    """Create or update a payable document record (upsert by number).
    Used by the Sales Docs tool on Generate AND the admin panel manual form."""
    num = _norm_doc_number(payload.get("number", ""))
    if not num:
        raise HTTPException(status_code=400, detail="Document number is required.")
    school = (payload.get("school") or "").strip()
    if not school:
        raise HTTPException(status_code=400, detail="School is required.")
    try:
        cents = int(round(float(payload.get("amount_cents", 0))))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Bad amount.")
    if cents <= 0:
        raise HTTPException(status_code=400, detail="Amount must be greater than zero.")
    doc_type = (payload.get("doc_type") or "invoice").strip().lower()
    if doc_type not in _SALES_DOC_TYPES:
        raise HTTPException(status_code=400, detail="doc_type must be quote, agreement, or invoice.")
    row = {
        "number": num, "doc_type": doc_type, "school": school,
        "description": (payload.get("description") or "").strip(),
        "amount_cents": cents,
    }
    async with httpx.AsyncClient() as c:
        existing = await _fetch_sales_doc(c, num)
        if existing and existing.get("status") == "paid":
            raise HTTPException(status_code=409, detail=f"{num} is already PAID — not changing it.")
        if existing:
            r = await c.patch(
                f"{SUPABASE_URL}/rest/v1/capp_sales_docs",
                params={"number": f"eq.{num}"},
                json=row,
                headers={**_supa_headers_json(), "Prefer": "return=minimal"},
            )
        else:
            row["status"] = "unpaid"
            r = await c.post(
                f"{SUPABASE_URL}/rest/v1/capp_sales_docs",
                json=row,
                headers={**_supa_headers_json(), "Prefer": "return=minimal"},
            )
    if r.status_code not in (200, 201, 204):
        raise HTTPException(status_code=500, detail=r.text)
    return {"ok": True, "number": num, "updated": bool(existing)}


@app.delete("/admin/api/salesdocs/{number}", dependencies=[Depends(_require_admin)])
async def admin_delete_salesdoc(number: str):
    async with httpx.AsyncClient() as c:
        r = await c.delete(
            f"{SUPABASE_URL}/rest/v1/capp_sales_docs",
            params={"number": f"eq.{_norm_doc_number(number)}"},
            headers={**_supa_headers_json(), "Prefer": "return=minimal"},
        )
    if r.status_code not in (200, 204):
        raise HTTPException(status_code=500, detail=r.text)
    return {"ok": True}


@app.get("/admin", response_class=HTMLResponse, include_in_schema=False)
def admin_page():
    return HTMLResponse(_ADMIN_HTML)


@app.get("/public/schools")
def public_schools():
    """Public endpoint — teams grouped by division → conference for registration dropdowns."""
    try:
        conn = sqlite3.connect(SERVER_DB_PATH)
        cur  = conn.cursor()
        cur.execute("SELECT MAX(season) FROM team_conferences")
        latest = cur.fetchone()[0] or 2026
        cur.execute("""
            SELECT team, conference, UPPER(classification) as division
            FROM team_conferences
            WHERE season = ?
              AND conference IS NOT NULL
              AND team NOT LIKE 'ZZZZZZ%%'
            ORDER BY classification, conference, team
        """, (latest,))
        rows = cur.fetchall()
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
            params={"select": "username,client_id,email,active,licensed,is_admin,seat_1_machine,seat_2_machine,seat_3_machine,seat_limit,notes,next_invoice_date,created_at,trial_extension_days",
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
    """Update editable fields: notes, next_invoice_date, email."""
    allowed = {k: v for k, v in payload.items() if k in ("notes", "next_invoice_date", "email")}
    if not allowed:
        raise HTTPException(status_code=400, detail="No editable fields provided.")
    if "email" in allowed:
        em = (allowed["email"] or "").strip().lower()
        if em and "@" not in em:
            raise HTTPException(status_code=400, detail="That doesn't look like an email address.")
        allowed["email"] = em or None
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


@app.post("/admin/api/clients/{username}/send-reset", dependencies=[Depends(_require_admin)])
async def admin_send_reset(username: str):
    """Manually email a password-reset link to the address on file."""
    async with httpx.AsyncClient() as c:
        r = await c.get(
            f"{SUPABASE_URL}/rest/v1/capp_clients",
            params={"username": f"eq.{username}", "select": "username,email"},
            headers=_supa_headers_json(),
        )
    rows = r.json() if r.status_code == 200 else []
    if not rows:
        raise HTTPException(status_code=404, detail="No such account.")
    email = (rows[0].get("email") or "").strip()
    if not email:
        raise HTTPException(status_code=400, detail="No email on file — add one and save first.")
    try:
        await _issue_reset_token(username, email)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not send: {e}")
    return {"ok": True, "email": email}


# ── Broadcast notices (admin blasts a message to every client) ────────────────
# Roger, Aug 21 2026: "I also need a way to Blast all users with update info."
#
# The client polls /app/notice at launch. `min_version` doubles as a MANDATORY
# UPDATE floor -- below it the client should block rather than offer.
#
# ⚠ Stored in Supabase, NOT a Render env var. APP_VERSION is an env var and it
# already served a stale value until the next deploy. A broadcast you can only
# send by redeploying is not a broadcast.

_NOTICES = "capp_notices"


@app.get("/app/notice")
async def app_notice():
    """Public — the newest active notice, or nothing.

    Deliberately unauthenticated and cheap: the client calls it at launch,
    before it necessarily has a working API key, and a broadcast that only
    reaches licensed clients cannot warn anyone about a licensing problem.
    """
    async with httpx.AsyncClient() as c:
        r = await c.get(f"{SUPABASE_URL}/rest/v1/{_NOTICES}",
                        params={"select": "id,title,body,severity,min_version,created_at",
                                "active": "eq.true",
                                "order": "created_at.desc", "limit": "1"},
                        headers=_supa_headers_json())
    if r.status_code != 200:
        return {"notice": None}          # never break a launch over a broadcast
    rows = r.json() or []
    return {"notice": rows[0] if rows else None,
            "app_version": os.environ.get("APP_VERSION", "2.0.0")}


@app.get("/admin/api/notices", dependencies=[Depends(_require_admin)])
async def admin_notices_list(limit: int = 20):
    async with httpx.AsyncClient() as c:
        r = await c.get(f"{SUPABASE_URL}/rest/v1/{_NOTICES}",
                        params={"select": "*", "order": "created_at.desc",
                                "limit": str(min(limit, 100))},
                        headers=_supa_headers_json())
    if r.status_code != 200:
        raise HTTPException(status_code=500, detail=r.text)
    return r.json()


@app.post("/admin/api/notices", dependencies=[Depends(_require_admin)])
async def admin_notice_create(payload: dict = Body(...)):
    """Publish a notice. Only the newest active one is ever shown, so
    publishing supersedes rather than stacking."""
    row = {
        "title":    (payload.get("title") or "").strip(),
        "body":     (payload.get("body") or "").strip(),
        "severity": (payload.get("severity") or "info").strip().lower(),
        "min_version": (payload.get("min_version") or "").strip() or None,
        "active":   True,
    }
    if not row["title"] or not row["body"]:
        raise HTTPException(status_code=400, detail="Title and message are required.")
    if row["severity"] not in ("info", "warning", "critical"):
        raise HTTPException(status_code=400, detail="Severity must be info, warning or critical.")
    async with httpx.AsyncClient() as c:
        r = await c.post(f"{SUPABASE_URL}/rest/v1/{_NOTICES}", json=row,
                         headers={**_supa_headers_json(), "Prefer": "return=representation"})
    if r.status_code not in (200, 201):
        raise HTTPException(status_code=500, detail=r.text)
    return (r.json() or [{}])[0]


@app.patch("/admin/api/notices/{notice_id}/retract", dependencies=[Depends(_require_admin)])
async def admin_notice_retract(notice_id: str):
    """Pull a notice back. Clients stop showing it on their next launch —
    anyone already looking at it keeps it until they restart."""
    async with httpx.AsyncClient() as c:
        r = await c.patch(f"{SUPABASE_URL}/rest/v1/{_NOTICES}",
                          params={"id": f"eq.{notice_id}"}, json={"active": False},
                          headers={**_supa_headers_json(), "Prefer": "return=minimal"})
    if r.status_code not in (200, 204):
        raise HTTPException(status_code=500, detail=r.text)
    return {"ok": True}


# ── Self-service seat management (cappvcs.com/seats) ──────────────────────────
# Schools asked to move CAPP between computers without emailing Roger. The
# unbind already existed as an ADMIN action; these two endpoints let the school
# do it themselves with the credentials they already have.
#
# ⚠ These deliberately do NOT bind a machine. /auth/login binds on first use,
# so it cannot be reused to check seat status — looking at your seats would
# consume one.

SEAT_RELEASE_COOLDOWN_HOURS = 24


async def _seat_client(username: str, password: str) -> dict:
    """Verify credentials and return the client row. 401 on any failure."""
    async with httpx.AsyncClient() as c:
        r = await c.get(
            f"{SUPABASE_URL}/rest/v1/capp_clients",
            params={"username": f"eq.{username}",
                    "select": "username,client_id,active,password_hash,salt,seat_limit,"
                              "seat_1_machine,seat_2_machine,seat_3_machine,"
                              "seat_1_released_at,seat_2_released_at,"
                              "seat_3_released_at"},
            headers=_supabase_headers())
    if r.status_code != 200 or not r.json():
        raise HTTPException(status_code=401, detail="Invalid username or password.")
    u = r.json()[0]
    if hashlib.sha256((password + u["salt"]).encode()).hexdigest() != u["password_hash"]:
        raise HTTPException(status_code=401, detail="Invalid username or password.")
    if not u.get("active"):
        raise HTTPException(status_code=403, detail="This account is not active.")
    return u


def _seat_view(u: dict, n: int) -> dict:
    """What the seats page shows for one seat."""
    machine = u.get(f"seat_{n}_machine")
    rel = u.get(f"seat_{n}_released_at")
    wait = 0
    if rel:
        try:
            last = _dtmod.datetime.fromisoformat(str(rel).replace("Z", "+00:00"))
            age = (datetime.now(timezone.utc) - last).total_seconds() / 3600
            wait = max(0, SEAT_RELEASE_COOLDOWN_HOURS - age)
        except Exception:
            wait = 0
    return {
        "seat": n,
        "bound": bool(machine),
        # Fingerprint only — there is no computer name to show. Truncated
        # because the full hash is meaningless to a coach and just noise.
        "machine": (str(machine)[:12] + "…") if machine else None,
        "can_release": bool(machine) and wait <= 0,
        "hours_until_release": round(wait, 1),
    }


@app.post("/seats/status")
async def seats_status(username: str = Body(..., embed=True),
                       password: str = Body(..., embed=True)):
    """Show this account's seats. Does NOT bind a machine."""
    u = await _seat_client(username, password)
    limit = max(1, min(3, int(u.get("seat_limit") or 2)))
    return {"username": u["username"],
            "seat_limit": limit,
            "seats": [_seat_view(u, n) for n in range(1, limit + 1)]}


@app.post("/seats/release")
async def seats_release(username: str = Body(..., embed=True),
                        password: str = Body(..., embed=True),
                        seat: int = Body(..., embed=True)):
    """Release one seat so it can be activated on another computer.

    ⚠ Rate-limited to one release per seat per 24h. Without that, this becomes
    a way to rotate a single licence around a whole staff — the exact sharing
    the machine binding exists to prevent.
    """
    if seat not in (1, 2, 3):
        raise HTTPException(status_code=400, detail="Seat must be 1, 2 or 3.")
    u = await _seat_client(username, password)
    if seat > int(u.get("seat_limit") or 2):
        raise HTTPException(status_code=400,
                            detail=f"This account does not have a seat {seat}.")
    view = _seat_view(u, seat)
    if not view["bound"]:
        raise HTTPException(status_code=400, detail=f"Seat {seat} is already open.")
    if not view["can_release"]:
        raise HTTPException(
            status_code=429,
            detail=f"Seat {seat} was released recently. You can release it again in "
                   f"{view['hours_until_release']:.0f} more hour(s).")
    async with httpx.AsyncClient() as c:
        r = await c.patch(
            f"{SUPABASE_URL}/rest/v1/capp_clients",
            params={"username": f"eq.{username}"},
            json={f"seat_{seat}_machine": None,
                  f"seat_{seat}_released_at": datetime.now(timezone.utc).isoformat()},
            headers={**_supa_headers_json(), "Prefer": "return=minimal"})
    if r.status_code not in (200, 204):
        raise HTTPException(status_code=500, detail=r.text)
    return {"ok": True, "seat": seat,
            "message": f"Seat {seat} released. Activate CAPP on the new computer to use it."}


@app.patch("/admin/api/clients/{username}/reset-seat", dependencies=[Depends(_require_admin)])
async def admin_reset_single_seat(username: str, seat: int = Body(..., embed=True)):
    """Reset a single seat (1, 2 or 3)."""
    if seat not in (1, 2, 3):
        raise HTTPException(status_code=400, detail="Seat must be 1, 2 or 3.")
    col = f"seat_{seat}_machine"
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


# ── Message blasts to schools ─────────────────────────────────────────────────
#
# Sends one message to every customer school. Email today - it reaches an
# installed CAPP of ANY version, and needs nothing installed or updated. The
# same rows also carry an in-app notice for a later client build, so there is
# one system rather than two.
#
# ⚠ THE SAFETY STORY MATTERS MORE THAN THE FEATURE. This mails real customers,
# and a mistake cannot be recalled. So:
#   * /preview lists exactly who would receive it, BEFORE anything is sent
#   * /test sends only to the address given, so the real thing is never the
#     first time the message has been seen
#   * a send records every recipient, so "did Nebraska get it?" is answerable
#   * one send per row - a row that already has sent_at is refused

BROADCAST_AUDIENCES = ("licensed", "licensed_trial", "all")


async def _broadcast_recipients(audience: str) -> list:
    """
    Who a blast would go to. Also used by /preview, so what you review is
    produced by the same code that does the sending - never a separate guess.
    """
    if audience not in BROADCAST_AUDIENCES:
        raise HTTPException(status_code=400, detail=f"Unknown audience '{audience}'.")

    async with httpx.AsyncClient() as c:
        r = await c.get(
            f"{SUPABASE_URL}/rest/v1/capp_clients",
            params={"select": "username,email,school,licensed,active,is_admin",
                    "order": "username.asc"},
            headers=_supabase_headers())
    if r.status_code != 200:
        raise HTTPException(status_code=500, detail=r.text)

    out = []
    for u in r.json():
        if not u.get("active"):
            continue                     # never mail a disabled account
        if not (u.get("email") or "").strip():
            continue                     # nothing to send to
        if audience == "licensed" and not u.get("licensed"):
            continue
        if audience == "licensed_trial" and u.get("is_admin"):
            continue                     # internal/admin accounts are not customers
        out.append({
            "username": u.get("username"),
            "email": (u.get("email") or "").strip(),
            "school": u.get("school") or u.get("username"),
            "licensed": bool(u.get("licensed")),
        })
    return out


def _broadcast_email_html(school: str, subject: str, body: str) -> str:
    """
    Same visual language as the registration and reset emails, so a blast looks
    like it came from CAPP rather than from a mailing tool.

    The body is plain text from the compose box, escaped and turned into
    paragraphs - never raw HTML. A stray angle bracket in a message must not be
    able to break the email, and nothing typed in that box should be executable.
    """
    import html as _html
    paras = "".join(
        f'<p style="color:#c8d2e0;font-size:0.92rem;line-height:1.6;margin:0 0 14px;">{_html.escape(chunk).replace(chr(10), "<br/>")}</p>'
        for chunk in body.split(chr(10) + chr(10)) if chunk.strip())

    return f"""
    <div style="font-family:'Segoe UI',Arial,sans-serif;background:#070a0f;color:#e8edf5;padding:40px 0;">
      <div style="max-width:520px;margin:0 auto;background:#111825;border:1px solid rgba(255,255,255,0.07);border-radius:12px;padding:36px;">
        <div style="text-align:center;margin-bottom:24px;">
          <img src="https://cappvcs.com/capplogo.png" alt="CAPP" style="height:40px;" />
        </div>
        <h2 style="color:#e8edf5;font-size:1.2rem;font-weight:700;margin:0 0 18px;">{_html.escape(subject)}</h2>
        {paras}
        <p style="color:#6d7a8c;font-size:0.8rem;margin:26px 0 0;padding-top:16px;border-top:1px solid rgba(255,255,255,0.07);">
          Sent to {_html.escape(school)} &middot; CAPP Video Coordinator Suite<br/>
          Questions? Reply to this email or contact
          <a href="mailto:roger@cappvcs.com" style="color:#3a7ebf;">roger@cappvcs.com</a>.
        </p>
      </div>
    </div>
    """


def _send_broadcast_email(to_email: str, school: str, subject: str, body: str) -> None:
    """One message. Raises on failure so the caller can count it."""
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    smtp_host = os.environ.get("SMTP_HOST", "")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER", "")
    smtp_pass = os.environ.get("SMTP_PASS", "")
    from_addr = os.environ.get("FROM_EMAIL", smtp_user)
    if not smtp_host or not smtp_user or not smtp_pass:
        raise RuntimeError("SMTP is not configured on the server.")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"CAPP Video Coordinator Suite <{from_addr}>"
    msg["To"] = to_email
    # Sent individually rather than as one BCC blast: each school sees only its
    # own address, and one bad address cannot take down the whole send.
    msg.attach(MIMEText(_broadcast_email_html(school, subject, body), "html"))

    with smtplib.SMTP(smtp_host, smtp_port) as srv:
        srv.starttls()
        srv.login(smtp_user, smtp_pass)
        srv.sendmail(from_addr, [to_email], msg.as_string())


@app.get("/admin/api/broadcast/preview", dependencies=[Depends(_require_admin)])
async def broadcast_preview(audience: str = Query("licensed_trial")):
    """Exactly who would receive this. Always look before sending."""
    people = await _broadcast_recipients(audience)
    return {"audience": audience, "count": len(people), "recipients": people}


@app.get("/admin/api/broadcast", dependencies=[Depends(_require_admin)])
async def broadcast_list():
    """Recent blasts, newest first."""
    async with httpx.AsyncClient() as c:
        r = await c.get(
            f"{SUPABASE_URL}/rest/v1/capp_broadcasts",
            params={"select": "id,subject,body,audience,send_email,show_in_app,"
                              "active,sent_at,sent_count,failed_count,created_at",
                    "order": "created_at.desc", "limit": "25"},
            headers=_supabase_headers())
    if r.status_code != 200:
        raise HTTPException(status_code=500, detail=r.text)
    return r.json()


@app.post("/admin/api/broadcast/test", dependencies=[Depends(_require_admin)])
async def broadcast_test(payload: dict = Body(...)):
    """
    Send the message to ONE address, usually Roger's own.

    Deliberately separate from a real send and it writes nothing: the real
    blast should never be the first time a message has actually been seen.
    """
    to = str(payload.get("to", "")).strip()
    subject = str(payload.get("subject", "")).strip()
    body = str(payload.get("body", "")).strip()
    if not to or "@" not in to:
        raise HTTPException(status_code=400, detail="A test address is required.")
    if not subject or not body:
        raise HTTPException(status_code=400, detail="Subject and message are both required.")
    try:
        await run_in_threadpool(_send_broadcast_email, to, "Test", subject, body)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Could not send: {exc}")
    return {"ok": True, "sent_to": to}


@app.post("/admin/api/broadcast/send", dependencies=[Depends(_require_admin)])
async def broadcast_send(payload: dict = Body(...)):
    """
    Send a blast, and record exactly who it reached.

    `confirm_count` must equal the number of recipients the preview reported.
    That is not ceremony: it means a send cannot quietly go to a different set
    of people than the one that was reviewed - if somebody's licence changed
    between previewing and sending, this refuses rather than surprising you.
    """
    subject = str(payload.get("subject", "")).strip()
    body = str(payload.get("body", "")).strip()
    audience = str(payload.get("audience", "licensed_trial")).strip()
    show_in_app = bool(payload.get("show_in_app"))
    send_email = payload.get("send_email", True)
    confirm_count = payload.get("confirm_count")

    if not subject or not body:
        raise HTTPException(status_code=400, detail="Subject and message are both required.")

    people = await _broadcast_recipients(audience)
    if confirm_count is not None and int(confirm_count) != len(people):
        raise HTTPException(
            status_code=409,
            detail=(f"The recipient list changed since you previewed it "
                    f"({confirm_count} then, {len(people)} now). Preview again."))
    if send_email and not people:
        raise HTTPException(status_code=400, detail="No one matches that audience.")

    # In-app delivery goes through the notice system that already exists
    # (capp_notices, read by /app/notice at launch). One message, one source of
    # truth - the blast row records that it happened and points at it.
    notice_id = None
    if show_in_app:
        notice_row = {
            "title": subject, "body": body,
            "severity": str(payload.get("severity", "info")).strip().lower() or "info",
            "min_version": (str(payload.get("min_version", "")).strip() or None),
            "active": True,
        }
        if notice_row["severity"] not in ("info", "warning", "critical"):
            raise HTTPException(status_code=400,
                                detail="Severity must be info, warning or critical.")
        async with httpx.AsyncClient() as c:
            nr = await c.post(f"{SUPABASE_URL}/rest/v1/{_NOTICES}", json=notice_row,
                              headers={**_supa_headers_json(),
                                       "Prefer": "return=representation"})
        if nr.status_code not in (200, 201):
            raise HTTPException(status_code=500,
                                detail=f"Could not publish the in-app notice: {nr.text[:200]}")
        notice_id = (nr.json() or [{}])[0].get("id")

    sent, failed, delivered = 0, 0, []
    if send_email:
        for person in people:
            try:
                await run_in_threadpool(_send_broadcast_email, person["email"],
                                        person["school"], subject, body)
                sent += 1
                delivered.append({"username": person["username"],
                                  "email": person["email"], "ok": True})
            except Exception as exc:
                # Keep going. One bad address must not stop everyone else's
                # message, and the failure is recorded rather than swallowed.
                failed += 1
                delivered.append({"username": person["username"],
                                  "email": person["email"], "ok": False,
                                  "error": str(exc)[:200]})

    row = {
        "subject": subject, "body": body, "audience": audience,
        "send_email": bool(send_email), "show_in_app": show_in_app,
        "active": True,
        "sent_at": datetime.now(timezone.utc).isoformat() if send_email else None,
        "sent_count": sent, "failed_count": failed,
        "recipients": delivered,
        "notice_id": notice_id,
    }
    async with httpx.AsyncClient() as c:
        w = await c.post(f"{SUPABASE_URL}/rest/v1/capp_broadcasts",
                         json=row,
                         headers={**_supa_headers_json(), "Prefer": "return=representation"})
    if w.status_code not in (200, 201):
        # The mail is already gone; say so rather than implying nothing happened.
        raise HTTPException(
            status_code=500,
            detail=f"Sent to {sent} school(s), but the record could not be saved: {w.text[:200]}")

    return {"ok": True, "sent": sent, "failed": failed,
            "recipients": delivered, "id": (w.json() or [{}])[0].get("id")}


@app.patch("/admin/api/broadcast/{bid}/retire", dependencies=[Depends(_require_admin)])
async def broadcast_retire(bid: int):
    """
    Stop an in-app notice showing. Does NOT un-send any email - mail that has
    left cannot be recalled, and pretending otherwise would be worse than
    saying so.

    Retires BOTH rows: the blast record and the capp_notices row it published.
    Retiring only one is how you end up with a message that is "off" in one
    place and still on screen in the other.
    """
    async with httpx.AsyncClient() as c:
        cur = await c.get(f"{SUPABASE_URL}/rest/v1/capp_broadcasts",
                          params={"id": f"eq.{bid}", "select": "notice_id"},
                          headers=_supabase_headers())
        notice_id = None
        if cur.status_code == 200 and cur.json():
            notice_id = (cur.json()[0] or {}).get("notice_id")

        r = await c.patch(f"{SUPABASE_URL}/rest/v1/capp_broadcasts",
                          params={"id": f"eq.{bid}"},
                          json={"active": False},
                          headers={**_supa_headers_json(), "Prefer": "return=minimal"})
        if r.status_code not in (200, 204):
            raise HTTPException(status_code=500, detail=r.text)

        if notice_id:
            await c.patch(f"{SUPABASE_URL}/rest/v1/{_NOTICES}",
                          params={"id": f"eq.{notice_id}"},
                          json={"active": False},
                          headers={**_supa_headers_json(), "Prefer": "return=minimal"})
    return {"ok": True, "notice_retired": bool(notice_id)}


@app.patch("/admin/api/clients/{username}/seat-limit", dependencies=[Depends(_require_admin)])
async def admin_set_seat_limit(username: str, seat_limit: int = Body(..., embed=True)):
    """
    Change how many seats a school gets (1-3).

    Lowering the limit deliberately does NOT unbind anything. A machine already
    on seat 3 keeps working until someone releases it on purpose - quietly
    cutting a coach off mid-season because a number changed would be the worst
    possible behaviour here.
    """
    if seat_limit not in (1, 2, 3):
        raise HTTPException(status_code=400, detail="Seat limit must be 1, 2 or 3.")
    async with httpx.AsyncClient() as c:
        r = await c.patch(
            f"{SUPABASE_URL}/rest/v1/capp_clients",
            params={"username": f"eq.{username}"},
            json={"seat_limit": seat_limit},
            headers={**_supa_headers_json(), "Prefer": "return=minimal"},
        )
    if r.status_code not in (200, 204):
        raise HTTPException(status_code=500, detail=r.text)
    return {"ok": True, "username": username, "seat_limit": seat_limit}


@app.patch("/admin/api/clients/{username}/reset-seats", dependencies=[Depends(_require_admin)])
async def admin_reset_seats(username: str):
    async with httpx.AsyncClient() as c:
        r = await c.patch(
            f"{SUPABASE_URL}/rest/v1/capp_clients",
            params={"username": f"eq.{username}"},
            json={"seat_1_machine": None, "seat_2_machine": None,
                  "seat_3_machine": None},
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


@app.patch("/admin/api/clients/{username}/license", dependencies=[Depends(_require_admin)])
async def admin_set_licensed(username: str, licensed: bool = Body(..., embed=True)):
    async with httpx.AsyncClient() as c:
        r = await c.patch(
            f"{SUPABASE_URL}/rest/v1/capp_clients",
            params={"username": f"eq.{username}"},
            json={"licensed": licensed},
            headers={**_supa_headers_json(), "Prefer": "return=minimal"},
        )
    if r.status_code not in (200, 204):
        raise HTTPException(status_code=500, detail=r.text)
    return {"ok": True}


@app.patch("/admin/api/clients/{username}/extend-trial", dependencies=[Depends(_require_admin)])
async def admin_extend_trial(username: str, days: int = Body(..., embed=True)):
    """Add N days to a trial account's extension. Cumulative — safe to call multiple times."""
    if days < 1 or days > 365:
        raise HTTPException(status_code=400, detail="days must be between 1 and 365.")
    # Fetch current extension value
    async with httpx.AsyncClient() as c:
        r = await c.get(
            f"{SUPABASE_URL}/rest/v1/capp_clients",
            params={"username": f"eq.{username}", "select": "trial_extension_days"},
            headers=_supa_headers_json(),
        )
    if r.status_code != 200 or not r.json():
        raise HTTPException(status_code=404, detail="Account not found.")
    current = int(r.json()[0].get("trial_extension_days") or 0)
    async with httpx.AsyncClient() as c:
        r2 = await c.patch(
            f"{SUPABASE_URL}/rest/v1/capp_clients",
            params={"username": f"eq.{username}"},
            json={"trial_extension_days": current + days},
            headers={**_supa_headers_json(), "Prefer": "return=minimal"},
        )
    if r2.status_code not in (200, 204):
        raise HTTPException(status_code=500, detail=r2.text)
    return {"ok": True, "trial_extension_days": current + days}


@app.patch("/admin/api/clients/{username}/reset-trial", dependencies=[Depends(_require_admin)])
async def admin_reset_trial(username: str):
    """Reset trial to a fresh 7 days from now by computing the needed extension offset."""
    from datetime import datetime, timezone
    async with httpx.AsyncClient() as c:
        r = await c.get(
            f"{SUPABASE_URL}/rest/v1/capp_clients",
            params={"username": f"eq.{username}", "select": "created_at"},
            headers=_supa_headers_json(),
        )
    if r.status_code != 200 or not r.json():
        raise HTTPException(status_code=404, detail="Account not found.")
    created_str = r.json()[0].get("created_at", "")
    try:
        created_at = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
        elapsed    = (datetime.now(timezone.utc) - created_at).days
    except Exception:
        elapsed = 0
    # Set extension so that: TRIAL_DAYS + extension - elapsed = TRIAL_DAYS  →  extension = elapsed
    new_extension = elapsed
    async with httpx.AsyncClient() as c:
        r2 = await c.patch(
            f"{SUPABASE_URL}/rest/v1/capp_clients",
            params={"username": f"eq.{username}"},
            json={"trial_extension_days": new_extension},
            headers={**_supa_headers_json(), "Prefer": "return=minimal"},
        )
    if r2.status_code not in (200, 204):
        raise HTTPException(status_code=500, detail=r2.text)
    return {"ok": True, "trial_extension_days": new_extension}


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
# CRM — demo prospects (separate from licensed capp_clients)
# Simple pipeline tracker: schools Roger has demoed who may not be customers yet.
# ─────────────────────────────────────────────────────────────────────────────
_PROSPECT_STATUSES = ("Demo Done", "Quote/Agreement Sent", "Trial", "Paid", "Lost")
_PROSPECT_FIELDS   = ("school", "contact", "email", "phone", "status", "quote_sent_date", "notes")
# Old rows/clients may still carry the pre-Jul-2026 status wording — map it forward.
_LEGACY_STATUSES   = {"Quote/Contract Sent": "Quote/Agreement Sent"}


@app.get("/admin/api/prospects", dependencies=[Depends(_require_admin)])
async def admin_list_prospects():
    """All CRM prospects, most-recently-updated first."""
    async with httpx.AsyncClient() as c:
        r = await c.get(
            f"{SUPABASE_URL}/rest/v1/capp_prospects",
            params={"select": "*", "order": "updated_at.desc.nullslast"},
            headers=_supa_headers_json(),
        )
    if r.status_code != 200:
        raise HTTPException(status_code=500, detail=r.text)
    rows = r.json()
    for p in rows:
        p["status"] = _LEGACY_STATUSES.get(p.get("status"), p.get("status"))
    return rows


@app.post("/admin/api/prospects", dependencies=[Depends(_require_admin)])
async def admin_create_prospect(payload: dict = Body(...)):
    """Create a prospect. Only `school` is required."""
    row = {k: v for k, v in payload.items() if k in _PROSPECT_FIELDS}
    if not (row.get("school") or "").strip():
        raise HTTPException(status_code=400, detail="School is required.")
    row.setdefault("status", "Demo Done")
    row["status"] = _LEGACY_STATUSES.get(row["status"], row["status"])
    if row["status"] not in _PROSPECT_STATUSES:
        raise HTTPException(status_code=400, detail="Invalid status.")
    row["quote_sent_date"] = row.get("quote_sent_date") or None
    now = _dt.now(_tz.utc).isoformat()
    row["created_at"] = now
    row["updated_at"] = now
    async with httpx.AsyncClient() as c:
        r = await c.post(
            f"{SUPABASE_URL}/rest/v1/capp_prospects",
            json=row,
            headers={**_supa_headers_json(), "Prefer": "return=representation"},
        )
    if r.status_code not in (200, 201):
        raise HTTPException(status_code=500, detail=r.text)
    return r.json()[0] if r.json() else {"ok": True}


@app.patch("/admin/api/prospects/{prospect_id}", dependencies=[Depends(_require_admin)])
async def admin_update_prospect(prospect_id: str, payload: dict = Body(...)):
    """Update editable prospect fields. Always bumps updated_at."""
    row = {k: v for k, v in payload.items() if k in _PROSPECT_FIELDS}
    if not row:
        raise HTTPException(status_code=400, detail="No editable fields provided.")
    if "status" in row:
        row["status"] = _LEGACY_STATUSES.get(row["status"], row["status"])
        if row["status"] not in _PROSPECT_STATUSES:
            raise HTTPException(status_code=400, detail="Invalid status.")
    if "quote_sent_date" in row:
        row["quote_sent_date"] = row["quote_sent_date"] or None
    row["updated_at"] = _dt.now(_tz.utc).isoformat()
    async with httpx.AsyncClient() as c:
        r = await c.patch(
            f"{SUPABASE_URL}/rest/v1/capp_prospects",
            params={"id": f"eq.{prospect_id}"},
            json=row,
            headers={**_supa_headers_json(), "Prefer": "return=minimal"},
        )
    if r.status_code not in (200, 204):
        raise HTTPException(status_code=500, detail=r.text)
    return {"ok": True}


@app.delete("/admin/api/prospects/{prospect_id}", dependencies=[Depends(_require_admin)])
async def admin_delete_prospect(prospect_id: str):
    """Permanently delete a prospect."""
    async with httpx.AsyncClient() as c:
        r = await c.delete(
            f"{SUPABASE_URL}/rest/v1/capp_prospects",
            params={"id": f"eq.{prospect_id}"},
            headers={**_supa_headers_json(), "Prefer": "return=minimal"},
        )
    if r.status_code not in (200, 204):
        raise HTTPException(status_code=500, detail=r.text)
    return {"ok": True}


# ─────────────────────────────────────────────────────────────────────────────
# Playbook Portal — player accounts
# Admin uploads a roster (first/last/position/email). Email is the login AND the
# allowlist (no self-signup). Each player sets their own password on first login.
# Passwords are hashed (sha256 + per-user salt), same scheme as capp_clients.
# ─────────────────────────────────────────────────────────────────────────────
_PB_TABLE = "playbook_users"
_PB_TEAMS = "playbook_teams"

# Auto-populate a known program's logo at team-creation time from the SAME
# numbered logo library the desktop suite already uses (TEAM_LOGOS_NUMBERED +
# TEAM_NUMBER_MAP in capp_launcher_qt.py) — "we have the logos in several
# places in CAPP, we should be able to have it populate automatically"
# (Roger, Jul 8 2026). team_logo_numbers.json is a one-time export of that
# same map; the actual PNG files are bulk-uploaded to R2 once under
# _team_logos/{number}.png (shared, public-read brand assets — NOT
# per-team-scoped storage, so no team_id prefix). If a school isn't in this
# list (independent/non-CAPP program), the Team Admin's manual logo upload
# (/team-admin/logo) is the fallback.
_TEAM_LOGO_NUMBERS = {}
try:
    with open(os.path.join(os.path.dirname(__file__), "team_logo_numbers.json"), encoding="utf-8") as _f:
        _TEAM_LOGO_NUMBERS = json.load(_f)
except Exception:
    pass   # catalog is optional — teams just fall back to manual upload


async def _team_is_active(team_id: str) -> bool:
    """False only if the team exists and is explicitly deactivated. A missing/
    unresolvable team fails OPEN here on purpose — this is a courtesy check,
    not a security boundary (that's Wall #1 team_id filtering + Wall #2 RLS,
    which don't depend on this flag at all); we'd rather a lookup hiccup not
    lock everyone out than silently under-protect anything."""
    team = await _team_get(team_id=team_id)
    return bool(team) and team.get("active", True) is not False


async def _team_get(team_id: str = None, slug: str = None):
    """Fetch one team row by id or slug, or None. Exactly one of team_id/slug."""
    params = {"select": "*", "limit": "1"}
    if slug:
        params["slug"] = f"eq.{slug}"
    else:
        params["id"] = f"eq.{team_id}"
    async with httpx.AsyncClient() as c:
        r = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_TEAMS}", params=params,
                        headers=_supa_headers_json())
    if r.status_code != 200:
        return None
    rows = r.json()
    return rows[0] if rows else None


def _norm_email(e: str) -> str:
    return (e or "").strip().lower()


async def _pb_get(email: str):
    """Fetch one player row by email, or None."""
    async with httpx.AsyncClient() as c:
        r = await c.get(
            f"{SUPABASE_URL}/rest/v1/{_PB_TABLE}",
            params={"select": "*", "email": f"eq.{email}", "limit": "1"},
            headers=_supa_headers_json(),
        )
    if r.status_code != 200:
        return None
    rows = r.json()
    return rows[0] if rows else None


@app.post("/admin/api/playbook/upload", dependencies=[Depends(_require_admin)])
async def admin_playbook_upload(payload: dict = Body(...)):
    """Bulk add/update players from parsed CSV rows:
    payload = {"rows": [...], "team": "airforce"}  (team slug optional, defaults
    to airforce so the current admin panel keeps working unchanged.)
    New emails are inserted (no password yet) under the target team; existing
    emails get their name/position refreshed but their password is left
    untouched. An email that ALREADY belongs to a DIFFERENT team is skipped,
    never silently reassigned — email is globally unique across all teams, so
    one team's roster upload can never absorb another team's player."""
    team = await _team_get(slug=(payload.get("team") or "airforce"))
    if not team:
        raise HTTPException(status_code=400, detail="Unknown team.")
    team_id = team["id"]
    rows = payload.get("rows") or []
    processed, skipped = 0, []
    async with httpx.AsyncClient() as c:
        for raw in rows:
            email = _norm_email(raw.get("email"))
            if not email or "@" not in email:
                skipped.append({"email": raw.get("email", ""), "reason": "invalid email"})
                continue
            existing = await _pb_get(email)
            if existing and existing.get("team_id") != team_id:
                skipped.append({"email": email, "reason": "already registered to a different team"})
                continue
            row = {
                "email": email,
                "team_id": team_id,
                "first_name": (raw.get("first_name") or "").strip(),
                "last_name":  (raw.get("last_name") or "").strip(),
                "position":   (raw.get("position") or "").strip(),
            }
            # Upsert on email; merge-duplicates updates only the columns we send
            # (so an existing player's pw_hash/pw_salt are preserved). created_at
            # is left to the table default for new rows.
            r = await c.post(
                f"{SUPABASE_URL}/rest/v1/{_PB_TABLE}",
                params={"on_conflict": "email"},
                json=row,
                headers={**_supa_headers_json(),
                         "Prefer": "resolution=merge-duplicates,return=minimal"},
            )
            if r.status_code in (200, 201, 204):
                processed += 1
            else:
                skipped.append({"email": email, "reason": r.text[:120]})
    return {"processed": processed, "skipped": skipped, "total": len(rows)}


@app.get("/admin/api/playbook/users", dependencies=[Depends(_require_admin)])
async def admin_playbook_users(team: str = "airforce"):
    """Roster list for the admin panel (no password material) — one team's
    roster; defaults to airforce so the current admin panel is unchanged."""
    t = await _team_get(slug=team)
    if not t:
        raise HTTPException(status_code=400, detail="Unknown team.")
    async with httpx.AsyncClient() as c:
        r = await c.get(
            f"{SUPABASE_URL}/rest/v1/{_PB_TABLE}",
            params={"select": "id,email,first_name,last_name,position,pw_hash,is_admin,created_at",
                    "team_id": f"eq.{t['id']}",
                    "order": "last_name.asc,first_name.asc"},
            headers=_supa_headers_json(),
        )
    if r.status_code != 200:
        raise HTTPException(status_code=500, detail=r.text)
    return [{
        "id": u["id"], "email": u["email"],
        "first_name": u.get("first_name", ""), "last_name": u.get("last_name", ""),
        "position": u.get("position", ""), "is_admin": bool(u.get("is_admin")),
        "active": bool(u.get("pw_hash")),     # True once they've set a password
    } for u in r.json()]


@app.delete("/admin/api/playbook/users/{uid}", dependencies=[Depends(_require_admin)])
async def admin_playbook_delete(uid: str):
    async with httpx.AsyncClient() as c:
        r = await c.delete(
            f"{SUPABASE_URL}/rest/v1/{_PB_TABLE}",
            params={"id": f"eq.{uid}"},
            headers={**_supa_headers_json(), "Prefer": "return=minimal"},
        )
    if r.status_code not in (200, 204):
        raise HTTPException(status_code=500, detail=r.text)
    return {"ok": True}


# ── Owner (super-admin) — team creation, the top of the multi-tenancy chain ───
# Roger creates a team + seeds its FIRST Team Admin; that admin then builds
# their own roster and can promote more admins (see /team-admin/* above).

@app.get("/admin/api/playbook/teams", dependencies=[Depends(_require_admin)])
async def admin_list_teams():
    async with httpx.AsyncClient() as c:
        r = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_TEAMS}",
                        params={"select": "*", "order": "name.asc"},
                        headers=_supa_headers_json())
    if r.status_code != 200:
        raise HTTPException(status_code=500, detail=r.text)
    return r.json()


@app.post("/admin/api/playbook/teams", dependencies=[Depends(_require_admin)])
async def admin_create_team(payload: dict = Body(...)):
    """Create a team. slug must be unique (e.g. 'navy'); name is the display
    name shown post-login. Optional logo_school: an EXACT key from the
    known-programs catalog (see /admin/api/playbook/team-logo-catalog) —
    when given, the team's logo is set immediately from the shared CAPP
    logo library, no upload needed. Free-text name is NEVER auto-matched
    against the catalog (too error-prone); the picker is what drives this."""
    slug = _re.sub(r'[^a-z0-9_]+', '_', (payload.get("slug") or "").lower().strip()).strip('_')
    name = (payload.get("name") or "").strip()
    if not slug or not name:
        raise HTTPException(status_code=400, detail="slug and name are required.")
    row = {"slug": slug, "name": name}
    logo_school = (payload.get("logo_school") or "").strip()
    if logo_school and logo_school in _TEAM_LOGO_NUMBERS:
        row["logo_r2_key"] = f"_team_logos/{_TEAM_LOGO_NUMBERS[logo_school]}.png"
    async with httpx.AsyncClient() as c:
        r = await c.post(f"{SUPABASE_URL}/rest/v1/{_PB_TEAMS}",
                         json=row,
                         headers={**_supa_headers_json(), "Prefer": "return=representation"})
    if r.status_code == 409 or (r.status_code == 400 and "duplicate" in r.text.lower()):
        raise HTTPException(status_code=409, detail="That team slug already exists.")
    if r.status_code not in (200, 201):
        raise HTTPException(status_code=500, detail=r.text)
    return (r.json() or [{}])[0]


@app.post("/admin/api/playbook/teams/create-with-admin", dependencies=[Depends(_require_admin)])
async def admin_create_team_with_admin(payload: dict = Body(...)):
    """Create a team AND seed its first admin in ONE action. Replaces the old
    two-step flow (create team, then find its row in a list and click its own
    'Seed admin' button) — that flow let Roger click the WRONG row's button
    and seed a new team's admin onto an existing team by mistake (Jul 8 2026
    incident). Checking the admin email BEFORE creating the team also avoids
    ending up with an orphan team if the email turns out to be taken."""
    slug = _re.sub(r'[^a-z0-9_]+', '_', (payload.get("slug") or "").lower().strip()).strip('_')
    name = (payload.get("name") or "").strip()
    admin_email = _norm_email(payload.get("admin_email"))
    if not slug or not name:
        raise HTTPException(status_code=400, detail="slug and name are required.")
    if not admin_email or "@" not in admin_email:
        raise HTTPException(status_code=400, detail="A valid admin email is required.")
    existing = await _pb_get(admin_email)
    if existing:
        raise HTTPException(status_code=409, detail="That email already belongs to a team — pick a different admin email.")

    team_row = {"slug": slug, "name": name}
    logo_school = (payload.get("logo_school") or "").strip()
    if logo_school and logo_school in _TEAM_LOGO_NUMBERS:
        team_row["logo_r2_key"] = f"_team_logos/{_TEAM_LOGO_NUMBERS[logo_school]}.png"

    async with httpx.AsyncClient() as c:
        tr = await c.post(f"{SUPABASE_URL}/rest/v1/{_PB_TEAMS}", json=team_row,
                          headers={**_supa_headers_json(), "Prefer": "return=representation"})
        if tr.status_code == 409 or (tr.status_code == 400 and "duplicate" in tr.text.lower()):
            raise HTTPException(status_code=409, detail="That team slug already exists.")
        if tr.status_code not in (200, 201):
            raise HTTPException(status_code=500, detail=tr.text)
        team = (tr.json() or [{}])[0]

        admin_row = {
            "email": admin_email, "team_id": team["id"], "is_admin": True,
            "first_name": (payload.get("admin_first_name") or "").strip(),
            "last_name":  (payload.get("admin_last_name") or "").strip(),
            "position":   "Team Admin",
        }
        ar = await c.post(f"{SUPABASE_URL}/rest/v1/{_PB_TABLE}", json=admin_row,
                          headers={**_supa_headers_json(), "Prefer": "return=representation"})
        if ar.status_code not in (200, 201):
            # Team exists but the admin didn't get created — surface this
            # loudly rather than silently leaving a team with no admin.
            return {"team": team, "admin": None,
                    "error": "Team created, but seeding the admin failed: " + ar.text[:200]}
    return {"team": team, "admin": (ar.json() or [{}])[0]}


@app.get("/admin/api/playbook/team-logo-catalog", dependencies=[Depends(_require_admin)])
async def admin_team_logo_catalog():
    """Known-program list for the Create Team logo picker — name -> already
    has a logo on file, no upload needed."""
    return {"schools": sorted(_TEAM_LOGO_NUMBERS.keys())}


@app.patch("/admin/api/playbook/teams/{team_id}", dependencies=[Depends(_require_admin)])
async def admin_update_team(team_id: str, payload: dict = Body(...)):
    """Rename / relabel / activate-deactivate a team. slug is immutable on
    purpose (it's baked into R2 keys as a storage prefix)."""
    patch = {}
    if "name" in payload:
        patch["name"] = (payload.get("name") or "").strip()
    if "active" in payload:
        patch["active"] = bool(payload.get("active"))
    if "logo_r2_key" in payload:
        patch["logo_r2_key"] = payload.get("logo_r2_key")
    if not patch:
        raise HTTPException(status_code=400, detail="Nothing to update.")
    async with httpx.AsyncClient() as c:
        r = await c.patch(f"{SUPABASE_URL}/rest/v1/{_PB_TEAMS}",
                          params={"id": f"eq.{team_id}"}, json=patch,
                          headers={**_supa_headers_json(), "Prefer": "return=minimal"})
    if r.status_code not in (200, 204):
        raise HTTPException(status_code=500, detail=r.text)
    return {"ok": True}


@app.post("/admin/api/playbook/teams/{team_id}/logo-sign-upload", dependencies=[Depends(_require_admin)])
async def admin_team_logo_sign_upload(team_id: str):
    """Presigned PUT for a team logo image (small, no size cap enforced here)."""
    import uuid as _uuid
    key = f"{team_id}/logo/{_uuid.uuid4().hex}"
    return {"key": key, "put_url": _r2_presign("PUT", key, expires=900)}


@app.post("/admin/api/playbook/teams/{team_id}/seed-admin", dependencies=[Depends(_require_admin)])
async def admin_seed_team_admin(team_id: str, payload: dict = Body(...)):
    """Add the FIRST Team Admin for a new team. They appear on the roster with
    is_admin=true and no password yet — they set one on first login exactly
    like any player, then see the Team Admin roster tools."""
    team = await _team_get(team_id=team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Team not found.")
    email = _norm_email(payload.get("email"))
    if not email or "@" not in email:
        raise HTTPException(status_code=400, detail="A valid email is required.")
    existing = await _pb_get(email)
    if existing and existing.get("team_id") != team_id:
        raise HTTPException(status_code=409, detail="That email already belongs to a different team.")
    row = {
        "email": email, "team_id": team_id, "is_admin": True,
        "first_name": (payload.get("first_name") or "").strip(),
        "last_name":  (payload.get("last_name") or "").strip(),
        "position":   (payload.get("position") or "Team Admin").strip(),
    }
    async with httpx.AsyncClient() as c:
        r = await c.post(f"{SUPABASE_URL}/rest/v1/{_PB_TABLE}",
                         params={"on_conflict": "email"}, json=row,
                         headers={**_supa_headers_json(),
                                  "Prefer": "resolution=merge-duplicates,return=representation"})
    if r.status_code not in (200, 201):
        raise HTTPException(status_code=500, detail=r.text)
    return (r.json() or [{}])[0]


@app.post("/playbook/check")
async def playbook_check(email: str = Body(..., embed=True)):
    """First step of login: is this email on the roster, and has it set a password?"""
    u = await _pb_get(_norm_email(email))
    if not u:
        return {"status": "unknown"}
    return {"status": "set" if u.get("pw_hash") else "needs_setup",
            "first_name": u.get("first_name", "")}


@app.get("/vapid-public-key")
async def vapid_public_key():
    """Public by design — the VAPID public key identifies this app to push
    services, same as a website's own domain; it's meant to be handed to any
    browser subscribing to push, not a secret."""
    return {"key": VAPID_PUBLIC_KEY}


@app.post("/playbook/setup")
async def playbook_setup(email: str = Body(..., embed=True),
                         password: str = Body(..., embed=True)):
    """First-time password creation. Allowed only for a rostered email with no
    password yet."""
    email = _norm_email(email)
    if len(password or "") < 6:
        raise HTTPException(status_code=400, detail="Password must be at least 6 characters.")
    u = await _pb_get(email)
    if not u:
        raise HTTPException(status_code=403, detail="This email isn't on the roster. Ask your coach to add you.")
    if not await _team_is_active(u["team_id"]):
        raise HTTPException(status_code=403, detail="This team's account is currently deactivated.")
    if u.get("pw_hash"):
        raise HTTPException(status_code=409, detail="Password already set — just sign in.")
    salt = _secrets.token_hex(8)
    row = {"pw_salt": salt, "pw_hash": _hash_pw(password, salt),
           "password_set_at": _dt.now(_tz.utc).isoformat()}
    async with httpx.AsyncClient() as c:
        r = await c.patch(
            f"{SUPABASE_URL}/rest/v1/{_PB_TABLE}",
            params={"id": f"eq.{u['id']}"}, json=row,
            headers={**_supa_headers_json(), "Prefer": "return=minimal"},
        )
    if r.status_code not in (200, 204):
        raise HTTPException(status_code=500, detail=r.text)
    return {"status": "ok", "token": _pb_make_token(email),
            "first_name": u.get("first_name", ""),
            "last_name": u.get("last_name", ""), "position": u.get("position", "")}


@app.post("/playbook/login")
async def playbook_login(email: str = Body(..., embed=True),
                         password: str = Body(..., embed=True)):
    """Returning login. If the rostered email has no password yet, tells the
    client to run first-time setup instead."""
    email = _norm_email(email)
    u = await _pb_get(email)
    if not u:
        raise HTTPException(status_code=403, detail="This email isn't on the roster. Ask your coach to add you.")
    if not await _team_is_active(u["team_id"]):
        raise HTTPException(status_code=403, detail="This team's account is currently deactivated.")
    if not u.get("pw_hash"):
        return {"status": "needs_setup", "first_name": u.get("first_name", "")}
    if _hash_pw(password, u.get("pw_salt", "")) != u["pw_hash"]:
        raise HTTPException(status_code=401, detail="Wrong password.")
    return {"status": "ok", "token": _pb_make_token(email),
            "first_name": u.get("first_name", ""),
            "last_name": u.get("last_name", ""), "position": u.get("position", "")}


# ── R2 (Cloudflare) + player session tokens for the Playbook Portal ───────────
import hmac as _hmac, base64 as _b64, time as _time, datetime as _dtmod, urllib.parse as _uq

R2_BUCKET = os.environ.get("R2_BUCKET", "capp-playbook")
_PB_TOKEN_SECRET = (ADMIN_PASSWORD or "changeme") + "|playbook-session"
_PB_TOKEN_TTL = 60 * 60 * 24 * 30   # 30 days


def _pb_make_token(email: str) -> str:
    exp = int(_time.time()) + _PB_TOKEN_TTL
    msg = f"{email}|{exp}"
    sig = _hmac.new(_PB_TOKEN_SECRET.encode(), msg.encode(), hashlib.sha256).hexdigest()
    return _b64.urlsafe_b64encode(f"{msg}|{sig}".encode()).decode()


def _pb_read_token(token: str):
    try:
        raw = _b64.urlsafe_b64decode(token.encode()).decode()
        email, exp, sig = raw.rsplit("|", 2)
        if int(exp) < _time.time():
            return None
        good = _hmac.new(_PB_TOKEN_SECRET.encode(), f"{email}|{exp}".encode(), hashlib.sha256).hexdigest()
        return email if _hmac.compare_digest(good, sig) else None
    except Exception:
        return None


async def _require_player(x_pb_token: str = Header("")):
    """Validates the session and returns the FULL user row (dict), not just the
    email. team_id is always resolved fresh from the DB on every request — never
    trusted from the token — so a stale/forged team can't leak cross-team data,
    and no existing session (token format unchanged) is invalidated by adding
    multi-tenancy. Deleted players are revoked automatically (row lookup fails)."""
    email = _pb_read_token(x_pb_token)
    if not email:
        raise HTTPException(status_code=401, detail="Please sign in again.")
    u = await _pb_get(email)
    if not u:
        raise HTTPException(status_code=401, detail="Account not found.")
    if not await _team_is_active(u["team_id"]):
        raise HTTPException(status_code=401, detail="This team's account is currently deactivated.")
    return u


# ── Binder password reset ────────────────────────────────────────────────────
# Stateless, single-use, 60-min links — no reset_token columns on
# playbook_users and no migration to run. The signing key mixes in the row's
# CURRENT pw_hash/pw_salt, so the moment the password changes the key changes
# and every previously-issued link stops verifying. That buys single-use for
# free, and it also means a second "send reset" silently invalidates the first
# only after one of them is USED (both remain valid until then — deliberate:
# an admin re-sending because the player "didn't get it" must not kill the
# copy that was actually delivered).
#
# The player's existing password keeps working until the link is used, so a
# reset can never lock anyone out. That matters here: roster emails at a
# service academy are formulaic (c30first.last@...), so any flow that blanks
# the password would leave a guessable, claimable account sitting open.
_PB_RESET_TTL_MIN = 60
BINDER_URL = os.environ.get("BINDER_URL", "https://www.cappvcs.com/binder/")
_pb_reset_last_sent: dict = {}      # email -> epoch seconds (throttle)


def _pb_reset_secret(u: dict) -> str:
    """Per-user signing key. Binding the current password material in is what
    makes an issued link die the instant the password is changed."""
    return f"{_PB_TOKEN_SECRET}|pbreset|{u.get('pw_hash') or 'nopw'}|{u.get('pw_salt') or ''}"


def _pb_make_reset_token(u: dict) -> str:
    exp = int(_time.time()) + _PB_RESET_TTL_MIN * 60
    msg = f"{u['email']}|{exp}"
    sig = _hmac.new(_pb_reset_secret(u).encode(), msg.encode(), hashlib.sha256).hexdigest()
    return _b64.urlsafe_b64encode(f"{msg}|{sig}".encode()).decode()


async def _pb_read_reset_token(token: str):
    """Returns the full user row for a valid, unexpired, unused reset token —
    otherwise None. The row is re-read from the DB every time, so a deleted
    player's link stops working immediately."""
    try:
        raw = _b64.urlsafe_b64decode((token or "").encode()).decode()
        email, exp, sig = raw.rsplit("|", 2)
        if int(exp) < _time.time():
            return None
    except Exception:
        return None
    u = await _pb_get(_norm_email(email))
    if not u:
        return None
    good = _hmac.new(_pb_reset_secret(u).encode(), f"{email}|{exp}".encode(),
                     hashlib.sha256).hexdigest()
    return u if _hmac.compare_digest(good, sig) else None


def _send_pb_reset_email(to_email: str, first_name: str, team_name: str, reset_url: str) -> None:
    """Binder-branded reset email. RAISES if SMTP isn't configured or the send
    fails — unlike the notify email, an admin pressing this button needs to be
    told it didn't go out rather than getting a silent success."""
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    smtp_host = os.environ.get("SMTP_HOST", "")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER", "")
    smtp_pass = os.environ.get("SMTP_PASS", "")
    from_addr = os.environ.get("FROM_EMAIL", smtp_user)
    if not smtp_host or not smtp_user or not smtp_pass:
        raise RuntimeError("SMTP not configured")

    first = (first_name or "").strip() or "there"
    body_html = f"""
    <div style="font-family:'Segoe UI',Arial,sans-serif;background:#070a0f;color:#e8edf5;padding:40px 0;">
      <div style="max-width:480px;margin:0 auto;background:#111825;border:1px solid rgba(255,255,255,0.07);border-radius:12px;padding:36px;">
        <div style="text-align:center;margin-bottom:24px;">
          <img src="https://cappvcs.com/capplogo.png" alt="CAPP" style="height:36px;" />
        </div>
        <h2 style="color:#e8edf5;font-size:1.15rem;font-weight:700;margin:0 0 10px;">Reset your {team_name} playbook password</h2>
        <p style="color:#8b96a8;font-size:0.9rem;margin:0 0 22px;">
          Hey {first} — use the button below to choose a new password. This link
          works once and expires in {_PB_RESET_TTL_MIN} minutes. Until you use it,
          your current password still works.
        </p>
        <div style="text-align:center;margin-bottom:22px;">
          <a href="{reset_url}" style="display:inline-block;background:#0873EC;color:#fff;text-decoration:none;padding:12px 28px;border-radius:8px;font-size:0.9rem;font-weight:700;">Choose a New Password</a>
        </div>
        <p style="color:#8b96a8;font-size:0.78rem;margin:0;">
          Didn't ask for this? Ignore this email — nothing has changed.
        </p>
      </div>
    </div>
    """
    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Reset your {team_name} playbook password"
    msg["From"]    = f"CAPP Binder <{from_addr}>"
    msg["To"]      = to_email
    msg.attach(MIMEText(body_html, "html"))
    with smtplib.SMTP(smtp_host, smtp_port) as s:
        s.starttls()
        s.login(smtp_user, smtp_pass)
        s.sendmail(from_addr, [to_email], msg.as_string())


async def _pb_send_reset(u: dict) -> str:
    """Mint a link for this roster row and email it. Returns the address used.
    Raises on SMTP failure so callers can surface it."""
    team = await _team_get(team_id=u["team_id"])
    team_name = (team or {}).get("name", "your team")
    sep = "&" if "?" in BINDER_URL else "?"
    url = f"{BINDER_URL}{sep}rt={_pb_make_reset_token(u)}"
    _send_pb_reset_email(u["email"], u.get("first_name", ""), team_name, url)
    return u["email"]


@app.post("/playbook/reset-request")
async def playbook_reset_request(email: str = Body(..., embed=True)):
    """Self-service 'Forgot password?' from the Binder login. The response is
    identical whether or not the email is on a roster — otherwise this endpoint
    would confirm who is on a team to anyone who asked."""
    generic = {"ok": True, "message": "If that email is on a roster, a reset link is on its way."}
    email = _norm_email(email)
    if not email or "@" not in email:
        return generic
    u = await _pb_get(email)
    if not u or not await _team_is_active(u["team_id"]):
        return generic
    now = _time.time()
    if now - _pb_reset_last_sent.get(email, 0) < 60:
        return generic                      # throttle repeats, still generic
    try:
        await _pb_send_reset(u)
        _pb_reset_last_sent[email] = now
        print(f"[PBRESET] link sent to {email}", flush=True)
    except Exception as e:
        print(f"[PBRESET] FAILED for {email}: {e}", flush=True)
    return generic


@app.post("/playbook/reset-confirm")
async def playbook_reset_confirm(token: str = Body(..., embed=True),
                                 password: str = Body(..., embed=True)):
    """Finish a reset: validate the emailed token and set the new password.
    Signs them straight in, same as first-time setup, so there's no bounce back
    to a login screen right after choosing a password."""
    if len(password or "") < 6:
        raise HTTPException(status_code=400, detail="Password must be at least 6 characters.")
    u = await _pb_read_reset_token(token)
    if not u:
        raise HTTPException(status_code=400,
                            detail="This reset link is invalid, expired, or was already used. Request a new one.")
    if not await _team_is_active(u["team_id"]):
        raise HTTPException(status_code=403, detail="This team's account is currently deactivated.")
    salt = _secrets.token_hex(8)
    async with httpx.AsyncClient() as c:
        r = await c.patch(
            f"{SUPABASE_URL}/rest/v1/{_PB_TABLE}",
            params={"id": f"eq.{u['id']}"},
            json={"pw_salt": salt, "pw_hash": _hash_pw(password, salt),
                  "password_set_at": _dt.now(_tz.utc).isoformat()},
            headers={**_supa_headers_json(), "Prefer": "return=minimal"},
        )
    if r.status_code not in (200, 204):
        raise HTTPException(status_code=500, detail=r.text)
    print(f"[PBRESET] password updated for {u['email']}", flush=True)
    return {"status": "ok", "token": _pb_make_token(u["email"]),
            "email": u["email"],
            "first_name": u.get("first_name", ""),
            "last_name": u.get("last_name", ""), "position": u.get("position", "")}


@app.post("/admin/api/playbook/users/{uid}/send-reset", dependencies=[Depends(_require_admin)])
async def admin_playbook_send_reset(uid: str):
    """Owner admin panel — email a reset link to one roster member."""
    async with httpx.AsyncClient() as c:
        g = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_TABLE}",
                        params={"select": "*", "id": f"eq.{uid}", "limit": "1"},
                        headers=_supa_headers_json())
    rows = g.json() if g.status_code == 200 else []
    if not rows:
        raise HTTPException(status_code=404, detail="No such roster member.")
    try:
        email = await _pb_send_reset(rows[0])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not send: {e}")
    return {"ok": True, "email": email, "minutes": _PB_RESET_TTL_MIN}


@app.post("/playbook/push/subscribe")
async def playbook_push_subscribe(payload: dict = Body(...), _u: dict = Depends(_require_player)):
    """Register (or refresh) this browser/device's push subscription. Upserts
    on endpoint — a device re-subscribing (e.g. after clearing storage) just
    updates its keys rather than creating a duplicate row."""
    endpoint = (payload.get("endpoint") or "").strip()
    keys = payload.get("keys") or {}
    p256dh, auth = (keys.get("p256dh") or "").strip(), (keys.get("auth") or "").strip()
    if not endpoint or not p256dh or not auth:
        raise HTTPException(status_code=400, detail="endpoint and keys are required.")
    row = {"team_id": _u["team_id"], "email": _u["email"],
           "endpoint": endpoint, "p256dh": p256dh, "auth": auth}
    async with httpx.AsyncClient() as c:
        r = await c.post(f"{SUPABASE_URL}/rest/v1/playbook_push_subscriptions",
                         params={"on_conflict": "endpoint"}, json=row,
                         headers={**_supa_headers_json(),
                                  "Prefer": "resolution=merge-duplicates,return=minimal"})
    if r.status_code not in (200, 201, 204):
        raise HTTPException(status_code=500, detail=r.text)
    return {"ok": True}


@app.post("/playbook/push/unsubscribe")
async def playbook_push_unsubscribe(payload: dict = Body(...), _u: dict = Depends(_require_player)):
    """Remove this device's subscription (e.g. player turned notifications
    off). Scoped to the caller's own email — can't unsubscribe someone else's
    device even by guessing an endpoint string."""
    endpoint = (payload.get("endpoint") or "").strip()
    async with httpx.AsyncClient() as c:
        r = await c.delete(f"{SUPABASE_URL}/rest/v1/playbook_push_subscriptions",
                           params={"endpoint": f"eq.{endpoint}", "email": f"eq.{_u['email']}"},
                           headers={**_supa_headers_json(), "Prefer": "return=minimal"})
    if r.status_code not in (200, 204):
        raise HTTPException(status_code=500, detail=r.text)
    return {"ok": True}


def _r2_presign(method: str, key: str, expires: int = 600) -> str:
    """SigV4 presigned URL for a direct browser<->R2 transfer (no bytes flow
    through this server). method = GET | PUT | DELETE."""
    access  = os.environ["R2_ACCESS_KEY_ID"]
    secret  = os.environ["R2_SECRET_KEY"]
    account = os.environ["R2_ACCOUNT_ID"]
    host = f"{account}.r2.cloudflarestorage.com"
    region, service = "auto", "s3"
    now = _dtmod.datetime.utcnow()
    amzdate, datestamp = now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")
    scope = f"{datestamp}/{region}/{service}/aws4_request"
    canon_uri = "/" + R2_BUCKET + "/" + _uq.quote(key, safe="/~")
    q = {
        "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
        "X-Amz-Credential": f"{access}/{scope}",
        "X-Amz-Date": amzdate,
        "X-Amz-Expires": str(expires),
        "X-Amz-SignedHeaders": "host",
    }
    canon_qs = "&".join(f"{_uq.quote(k, safe='~')}={_uq.quote(v, safe='~')}"
                        for k, v in sorted(q.items()))
    canon_req = "\n".join([method, canon_uri, canon_qs, f"host:{host}\n", "host", "UNSIGNED-PAYLOAD"])
    sts = "\n".join(["AWS4-HMAC-SHA256", amzdate, scope,
                     hashlib.sha256(canon_req.encode()).hexdigest()])
    def _s(k, m): return _hmac.new(k, m.encode(), hashlib.sha256).digest()
    kdate = _s(("AWS4" + secret).encode(), datestamp)
    ksig  = _s(_s(_s(kdate, region), service), "aws4_request")
    sig = _hmac.new(ksig, sts.encode(), hashlib.sha256).hexdigest()
    return f"https://{host}{canon_uri}?{canon_qs}&X-Amz-Signature={sig}"


# ── Playbook content — admin manages folders/PDFs; players read via signed URLs ─
_PB_DOCS = "playbook_docs"


@app.post("/admin/api/playbook/docs/sign-upload", dependencies=[Depends(_require_admin)])
async def admin_pb_sign_upload(payload: dict = Body(...)):
    """Presigned PUT so the admin browser uploads the PDF straight to R2.
    team (slug, defaults airforce) picks the storage prefix."""
    import uuid as _uuid
    team = await _team_get(slug=(payload.get("team") or "airforce"))
    if not team:
        raise HTTPException(status_code=400, detail="Unknown team.")
    key = f"{team['id']}/pdfs/{_uuid.uuid4().hex}.pdf"
    return {"key": key, "put_url": _r2_presign("PUT", key, expires=900)}


@app.post("/admin/api/playbook/docs", dependencies=[Depends(_require_admin)])
async def admin_pb_create_doc(payload: dict = Body(...)):
    """Record a doc after its bytes were uploaded to R2. team (slug) defaults
    to airforce so the current admin panel keeps working unchanged."""
    team = await _team_get(slug=(payload.get("team") or "airforce"))
    if not team:
        raise HTTPException(status_code=400, detail="Unknown team.")
    row = {
        "team_id":     team["id"],
        "folder_path": (payload.get("folder") or "").strip().strip("/"),
        "title":       (payload.get("title") or "").strip(),
        "r2_key":      (payload.get("key") or "").strip(),
        "pages":       payload.get("pages"),
        "size_bytes":  payload.get("size"),
        "sort_order":  payload.get("sort_order") or 0,
    }
    if not row["title"] or not row["r2_key"]:
        raise HTTPException(status_code=400, detail="title and key are required.")
    async with httpx.AsyncClient() as c:
        r = await c.post(f"{SUPABASE_URL}/rest/v1/{_PB_DOCS}", json=row,
                         headers={**_supa_headers_json(), "Prefer": "return=representation"})
    if r.status_code not in (200, 201):
        raise HTTPException(status_code=500, detail=r.text)
    return (r.json() or [{}])[0]


@app.post("/admin/api/playbook/docs/{doc_id}/replace", dependencies=[Depends(_require_admin)])
async def admin_pb_replace_doc(doc_id: str, payload: dict = Body(...)):
    """Swap a doc's PDF for new bytes already uploaded to R2. The row keeps its
    id, folder, title, and sort order — so player Touch Notes stay attached."""
    new_key = (payload.get("key") or "").strip()
    if not new_key:
        raise HTTPException(status_code=400, detail="key is required.")
    async with httpx.AsyncClient() as c:
        g = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_DOCS}",
                        params={"select": "r2_key", "id": f"eq.{doc_id}", "limit": "1"},
                        headers=_supa_headers_json())
        if g.status_code != 200 or not g.json():
            raise HTTPException(status_code=404, detail="Doc not found.")
        old_key = g.json()[0].get("r2_key")
        patch = {"r2_key": new_key,
                 "size_bytes": payload.get("size"),
                 "pages": payload.get("pages")}
        r = await c.patch(f"{SUPABASE_URL}/rest/v1/{_PB_DOCS}",
                          params={"id": f"eq.{doc_id}"}, json=patch,
                          headers={**_supa_headers_json(), "Prefer": "return=minimal"})
        if r.status_code not in (200, 204):
            raise HTTPException(status_code=500, detail=r.text)
        if old_key and old_key != new_key:
            try:
                await c.delete(_r2_presign("DELETE", old_key, expires=300))
            except Exception:
                pass    # row already points at the new PDF; orphaned object is harmless
    return {"ok": True}


@app.get("/admin/api/playbook/docs", dependencies=[Depends(_require_admin)])
async def admin_pb_list_docs(team: str = "airforce"):
    t = await _team_get(slug=team)
    if not t:
        raise HTTPException(status_code=400, detail="Unknown team.")
    async with httpx.AsyncClient() as c:
        r = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_DOCS}",
                        params={"select": "*", "team_id": f"eq.{t['id']}",
                                "order": "folder_path.asc,sort_order.asc,title.asc"},
                        headers=_supa_headers_json())
    if r.status_code != 200:
        raise HTTPException(status_code=500, detail=r.text)
    return r.json()


@app.get("/admin/api/playbook/access-log", dependencies=[Depends(_require_admin)])
async def admin_pb_access_log(team: str = "airforce", limit: int = 200):
    """Who viewed which doc and when — metadata only, NEVER file content. This
    is the accountability record for the fact that the Owner has no code path
    to view a team's PDFs directly; every legitimate in-app view is logged
    here instead. (Roger, Jul 8 2026.)"""
    t = await _team_get(slug=team)
    if not t:
        raise HTTPException(status_code=400, detail="Unknown team.")
    async with httpx.AsyncClient() as c:
        r = await c.get(f"{SUPABASE_URL}/rest/v1/playbook_access_log",
                        params={"select": "email,doc_id,created_at", "team_id": f"eq.{t['id']}",
                                "order": "created_at.desc", "limit": str(min(limit, 1000))},
                        headers=_supa_headers_json())
    if r.status_code != 200:
        raise HTTPException(status_code=500, detail=r.text)
    return r.json()


@app.delete("/admin/api/playbook/docs/{doc_id}", dependencies=[Depends(_require_admin)])
async def admin_pb_delete_doc(doc_id: str):
    async with httpx.AsyncClient() as c:
        g = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_DOCS}",
                        params={"select": "r2_key", "id": f"eq.{doc_id}", "limit": "1"},
                        headers=_supa_headers_json())
        key = (g.json()[0]["r2_key"] if g.status_code == 200 and g.json() else None)
        r = await c.delete(f"{SUPABASE_URL}/rest/v1/{_PB_DOCS}",
                           params={"id": f"eq.{doc_id}"},
                           headers={**_supa_headers_json(), "Prefer": "return=minimal"})
        if r.status_code not in (200, 204):
            raise HTTPException(status_code=500, detail=r.text)
        if key:
            try:
                await c.delete(_r2_presign("DELETE", key, expires=300))
            except Exception:
                pass    # metadata is gone; an orphaned R2 object is harmless
    return {"ok": True}


# ── Playbook folders — rows that make EMPTY folders visible in the portal tree ─
_PB_FOLDERS = "playbook_folders"


@app.get("/admin/api/playbook/folders", dependencies=[Depends(_require_admin)])
async def admin_pb_list_folders(team: str = "airforce"):
    t = await _team_get(slug=team)
    if not t:
        raise HTTPException(status_code=400, detail="Unknown team.")
    async with httpx.AsyncClient() as c:
        r = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_FOLDERS}",
                        params={"select": "*", "team_id": f"eq.{t['id']}",
                                "order": "folder_path.asc"},
                        headers=_supa_headers_json())
    if r.status_code != 200:
        raise HTTPException(status_code=500, detail=r.text)
    return r.json()


@app.post("/admin/api/playbook/folders", dependencies=[Depends(_require_admin)])
async def admin_pb_create_folder(payload: dict = Body(...)):
    path = (payload.get("path") or "").strip().strip("/")
    if not path:
        raise HTTPException(status_code=400, detail="path is required.")
    team = await _team_get(slug=(payload.get("team") or "airforce"))
    if not team:
        raise HTTPException(status_code=400, detail="Unknown team.")
    async with httpx.AsyncClient() as c:
        r = await c.post(f"{SUPABASE_URL}/rest/v1/{_PB_FOLDERS}",
                         params={"on_conflict": "team_id,folder_path"},
                         json={"folder_path": path, "team_id": team["id"]},
                         headers={**_supa_headers_json(),
                                  "Prefer": "resolution=merge-duplicates,return=representation"})
    if r.status_code not in (200, 201):
        raise HTTPException(status_code=500, detail=r.text)
    return (r.json() or [{}])[0]


@app.delete("/admin/api/playbook/folders/{folder_id}", dependencies=[Depends(_require_admin)])
async def admin_pb_delete_folder(folder_id: str):
    async with httpx.AsyncClient() as c:
        r = await c.delete(f"{SUPABASE_URL}/rest/v1/{_PB_FOLDERS}",
                           params={"id": f"eq.{folder_id}"},
                           headers={**_supa_headers_json(), "Prefer": "return=minimal"})
    if r.status_code not in (200, 204):
        raise HTTPException(status_code=500, detail=r.text)
    return {"ok": True}


@app.get("/playbook/manifest")
async def playbook_manifest(_u: dict = Depends(_require_player)):
    """Folder tree for a signed-in player — scoped to their own team only."""
    team_id = _u["team_id"]
    team = await _team_get(team_id=team_id)
    async with httpx.AsyncClient() as c:
        r = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_DOCS}",
                        params={"select": "id,folder_path,title,pages,sort_order,r2_key",
                                "team_id": f"eq.{team_id}",
                                "order": "folder_path.asc,sort_order.asc,title.asc"},
                        headers=_scoped_headers(team_id))
        f = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_FOLDERS}",
                        params={"select": "folder_path", "team_id": f"eq.{team_id}",
                                "order": "folder_path.asc"},
                        headers=_scoped_headers(team_id))
    if r.status_code != 200:
        raise HTTPException(status_code=500, detail=r.text)
    # Folders are additive; if the table doesn't exist yet, just omit them.
    folders = ([x["folder_path"] for x in f.json()] if f.status_code == 200 else [])
    # "v" = the doc's r2_key: replacing a PDF always writes a new key, so the
    # portal's offline cache uses it as a version stamp to spot stale copies.
    return {"sections": [{"id": d["id"], "folder": d.get("folder_path", ""),
                          "title": d.get("title", ""), "pages": d.get("pages"),
                          "v": d.get("r2_key", "")}
                         for d in r.json()],
            "folders": folders,
            "team": {"name": (team or {}).get("name", "CAPP Binder"),
                     "logo_r2_key": (team or {}).get("logo_r2_key"),
                     # Presigned so the header <img> can just use it directly —
                     # same pattern as doc URLs, no separate endpoint needed.
                     "logo_url": (_r2_presign("GET", (team or {}).get("logo_r2_key"), expires=3600)
                                  if (team or {}).get("logo_r2_key") else None)}}


@app.get("/playbook/doc/{doc_id}/url")
async def playbook_doc_url(doc_id: str, _u: dict = Depends(_require_player)):
    """Short-lived signed URL for one PDF — 404s if the doc isn't this player's
    own team (never leaks whether the doc exists on another team). Every issued
    URL is logged (playbook_access_log) — permanent, who/what/when accountability
    record. This is the ONLY code path anywhere that can produce a viewable link
    for a doc's content, and it always requires a real rostered login."""
    async with httpx.AsyncClient() as c:
        g = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_DOCS}",
                        params={"select": "r2_key,team_id", "id": f"eq.{doc_id}", "limit": "1"},
                        headers=_scoped_headers(_u["team_id"]))
    rows = g.json() if g.status_code == 200 else []
    if not rows or rows[0].get("team_id") != _u["team_id"]:
        raise HTTPException(status_code=404, detail="Not found.")
    try:
        async with httpx.AsyncClient() as c:
            await c.post(f"{SUPABASE_URL}/rest/v1/playbook_access_log",
                        json={"team_id": _u["team_id"], "doc_id": doc_id, "email": _u["email"]},
                        headers={**_supa_headers_json(), "Prefer": "return=minimal"})
    except Exception:
        pass   # never let logging failure block a legitimate view
    return {"url": _r2_presign("GET", rows[0]["r2_key"], expires=600)}


# ── Touch Notes — private, per-player, per-play (doc page) typed notes ──────────
_PB_NOTES = "playbook_notes"


@app.get("/playbook/notes")
async def playbook_notes_list(_u: dict = Depends(_require_player)):
    """Every note belonging to the signed-in player (drives the editor + 📝 dots).
    Scoped to the token's email — a player only ever sees their own notes."""
    email = _u["email"]
    async with httpx.AsyncClient() as c:
        r = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_NOTES}",
                        params={"select": "doc_id,page,body,updated_at",
                                "email": f"eq.{email}",
                                "order": "doc_id.asc,page.asc"},
                        headers=_supa_headers_json())
    if r.status_code != 200:
        raise HTTPException(status_code=500, detail=r.text)
    return {"notes": r.json()}


@app.put("/playbook/notes/{doc_id}/{page}")
async def playbook_notes_put(doc_id: str, page: int, payload: dict = Body(...),
                             _u: dict = Depends(_require_player)):
    """Auto-save a player's note for one play (doc page). Empty text deletes it
    (so its dot clears). Keyed on (email, doc_id, page) — always the player's own."""
    email = _u["email"]
    text = (payload.get("text") or "").strip()
    async with httpx.AsyncClient() as c:
        if not text:
            r = await c.delete(f"{SUPABASE_URL}/rest/v1/{_PB_NOTES}",
                               params={"email": f"eq.{email}",
                                       "doc_id": f"eq.{doc_id}",
                                       "page": f"eq.{page}"},
                               headers={**_supa_headers_json(), "Prefer": "return=minimal"})
            if r.status_code not in (200, 204):
                raise HTTPException(status_code=500, detail=r.text)
            return {"ok": True, "deleted": True}
        row = {"email": email, "doc_id": doc_id, "page": page, "body": text,
               "updated_at": _dtmod.datetime.utcnow().isoformat() + "Z"}
        r = await c.post(f"{SUPABASE_URL}/rest/v1/{_PB_NOTES}",
                         params={"on_conflict": "email,doc_id,page"},
                         json=row,
                         headers={**_supa_headers_json(),
                                  "Prefer": "resolution=merge-duplicates,return=minimal"})
    if r.status_code not in (200, 201, 204):
        raise HTTPException(status_code=500, detail=r.text)
    return {"ok": True}


# ── Playbook Visio conversion — admin queues a Visio file; a Windows+Visio ──────
# worker (or a LibreOffice fallback worker) claims it, converts to PDF, uploads
# to R2, and the finished PDF is registered as a normal playbook_docs row.
_PB_JOBS = "playbook_jobs"
_PB_DEVICES = "playbook_converter_devices"
_PB_PAIR_TOKENS = "playbook_converter_pairing_tokens"
PB_WORKER_TOKEN = os.environ.get("PB_WORKER_TOKEN", "")
# Worker converts these to PDF. Keep in step with _convert() in BOTH pb_worker.py
# and CONVERTER/capp_binder_converter.py — a format allowed here but unhandled
# there uploads fine and then fails in conversion, which looks like a broken app.
_PB_CONVERT_EXTS = ("vsd", "vsdx", "vsdm", "ppt", "pptx", "doc", "docx", "docm",
                    "xls", "xlsx", "xlsm", "xlsb")


def _vtuple_pb(v: str) -> tuple:
    """Version compare for converter builds. NUMERIC, not lexical, so 1.10.0
    beats 1.9.0; anything unparseable sorts below every real version rather
    than winning by accident."""
    try:
        return tuple(int(x) for x in str(v).strip().split("."))
    except Exception:
        return (0,)


async def _pb_device_by_token(token: str):
    """Look up a paired local worker by its own device token (issued at pairing
    time — see /converter/register). Returns the device row or None."""
    if not token:
        return None
    async with httpx.AsyncClient() as c:
        r = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_DEVICES}",
                        params={"select": "id,email,team_id", "token": f"eq.{token}", "limit": "1"},
                        headers=_supa_headers_json())
    return r.json()[0] if r.status_code == 200 and r.json() else None


async def _worker_identity(x_worker_token: str = Header(""),
                           x_converter_version: str = Header("")):
    """Every worker call authenticates with EITHER the legacy shared
    PB_WORKER_TOKEN (used only for admin-panel-direct uploads, which have no
    coach identity to pair to) OR its own per-device token from pairing.
    Returns which kind of worker this is and, for a paired worker, the login
    it's scoped to — the claim endpoint uses this to make sure a worker only
    ever picks up ITS OWN paired coach's jobs, never another coach's, even on
    the same team."""
    if PB_WORKER_TOKEN and x_worker_token == PB_WORKER_TOKEN:
        return {"kind": "legacy", "email": None}
    dev = await _pb_device_by_token(x_worker_token)
    if dev:
        # Heartbeat, plus the version the worker reported in its header so a
        # coach can be told their converter is stale.
        beat = {"last_seen_at": _dtmod.datetime.utcnow().isoformat() + "Z"}
        # Defensive: only ever treat a real str as a version. If this is
        # somehow called outside FastAPI again, an unresolved Header object
        # must degrade to "no version", never raise into a 500.
        ver = (x_converter_version if isinstance(x_converter_version, str) else "").strip()[:20]
        try:
            async with httpx.AsyncClient() as c:
                url = f"{SUPABASE_URL}/rest/v1/{_PB_DEVICES}"
                params = {"id": f"eq.{dev['id']}"}
                hdrs = {**_supa_headers_json(), "Prefer": "return=minimal"}
                r = await c.patch(url, params=params,
                                  json={**beat, "converter_version": ver} if ver else beat,
                                  headers=hdrs)
                # ⚠ Falls back to a bare heartbeat if converter_version doesn't
                # exist yet. Without this, a server deployed before the column
                # is added would fail the PATCH and stop updating last_seen_at
                # too — every paired converter would show as OFFLINE.
                if ver and r.status_code >= 400:
                    await c.patch(url, params=params, json=beat, headers=hdrs)
        except Exception:
            pass    # heartbeat is best-effort; never blocks the claim
        return {"kind": "paired", "email": dev["email"], "team_id": dev["team_id"]}
    raise HTTPException(status_code=401, detail="Unauthorized worker")


async def _require_worker(x_worker_token: str = Header(""),
                          x_converter_version: str = Header("")):
    """Validity-only guard for the completion/error endpoints (they already
    operate on a job_id the worker only knows because it claimed that job
    itself) — accepts the legacy shared token or any paired device token.

    ⚠ BOTH headers must be declared here and forwarded. This used to call
    _worker_identity(x_worker_token) with one argument, so x_converter_version
    kept its DEFAULT — and a default of Header("") is a FastAPI Header OBJECT,
    not a string, because plain calls get no dependency resolution. It is
    truthy, so `(x_converter_version or "").strip()` raised
    "AttributeError: 'Header' object has no attribute 'strip'" and every
    /playbook/worker/complete returned 500.

    That broke the Binder end to end from Aug 17 2026: the converter did the
    whole job correctly, uploaded the PDF, reported completion, got a 500, and
    the unhandled error KILLED THE WORKER PROCESS — so nothing converted after
    it either. /playbook/worker/claim was unaffected because it takes
    _worker_identity as a real Depends(), where FastAPI fills both headers in.
    Forwarding the version also means completions now refresh the heartbeat."""
    await _worker_identity(x_worker_token, x_converter_version)


@app.post("/admin/api/playbook/jobs/sign-upload", dependencies=[Depends(_require_admin)])
async def admin_pb_job_sign_upload(payload: dict = Body(...)):
    """Presigned PUT so the admin browser uploads the raw Visio/PowerPoint file to R2."""
    import uuid as _uuid
    ext = (payload.get("ext") or "vsdx").lower().lstrip(".")
    if ext not in _PB_CONVERT_EXTS:
        raise HTTPException(status_code=400, detail="Not a convertible file.")
    team = await _team_get(slug=(payload.get("team") or "airforce"))
    if not team:
        raise HTTPException(status_code=400, detail="Unknown team.")
    key = f"{team['id']}/raw/{_uuid.uuid4().hex}.{ext}"
    return {"key": key, "put_url": _r2_presign("PUT", key, expires=900)}


@app.post("/admin/api/playbook/jobs", dependencies=[Depends(_require_admin)])
async def admin_pb_create_job(payload: dict = Body(...)):
    """Queue a conversion job after its raw bytes were uploaded to R2."""
    team = await _team_get(slug=(payload.get("team") or "airforce"))
    if not team:
        raise HTTPException(status_code=400, detail="Unknown team.")
    row = {
        "team_id":     team["id"],
        "raw_key":     (payload.get("key") or "").strip(),
        "ext":         (payload.get("ext") or "").lower().lstrip("."),
        "folder_path": (payload.get("folder") or "").strip().strip("/"),
        "title":       (payload.get("title") or "").strip(),
        "status":      "queued",
    }
    if not row["title"] or not row["raw_key"]:
        raise HTTPException(status_code=400, detail="title and key are required.")
    async with httpx.AsyncClient() as c:
        r = await c.post(f"{SUPABASE_URL}/rest/v1/{_PB_JOBS}", json=row,
                         headers={**_supa_headers_json(), "Prefer": "return=representation"})
    if r.status_code not in (200, 201):
        raise HTTPException(status_code=500, detail=r.text)
    return (r.json() or [{}])[0]


@app.get("/admin/api/playbook/jobs", dependencies=[Depends(_require_admin)])
async def admin_pb_list_jobs(team: str = "airforce"):
    t = await _team_get(slug=team)
    if not t:
        raise HTTPException(status_code=400, detail="Unknown team.")
    async with httpx.AsyncClient() as c:
        r = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_JOBS}",
                        params={"select": "id,folder_path,title,ext,status,error,"
                                          "claimed_by,claimed_at,created_at",
                                "team_id": f"eq.{t['id']}",
                                "order": "created_at.desc"},
                        headers=_supa_headers_json())
    if r.status_code != 200:
        raise HTTPException(status_code=500, detail=r.text)
    return r.json()


@app.delete("/admin/api/playbook/jobs/{job_id}", dependencies=[Depends(_require_admin)])
async def admin_pb_delete_job(job_id: str):
    async with httpx.AsyncClient() as c:
        g = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_JOBS}",
                        params={"select": "raw_key,out_key,status", "id": f"eq.{job_id}", "limit": "1"},
                        headers=_supa_headers_json())
        keys = (g.json()[0] if g.status_code == 200 and g.json() else {})
        r = await c.delete(f"{SUPABASE_URL}/rest/v1/{_PB_JOBS}",
                           params={"id": f"eq.{job_id}"},
                           headers={**_supa_headers_json(), "Prefer": "return=minimal"})
        if r.status_code not in (200, 204):
            raise HTTPException(status_code=500, detail=r.text)
        # A done job's out_key IS the live playbook_docs PDF — never delete it here.
        drop = [keys.get("raw_key")]
        if keys.get("status") != "done":
            drop.append(keys.get("out_key"))
        for k in drop:
            if k:
                try:
                    await c.delete(_r2_presign("DELETE", k, expires=300))
                except Exception:
                    pass    # orphaned R2 object is harmless
    return {"ok": True}


async def _pb_job_patch(job_id: str, fields: dict):
    async with httpx.AsyncClient() as c:
        r = await c.patch(f"{SUPABASE_URL}/rest/v1/{_PB_JOBS}",
                          params={"id": f"eq.{job_id}"},
                          json={**fields, "updated_at": _dtmod.datetime.utcnow().isoformat() + "Z"},
                          headers={**_supa_headers_json(), "Prefer": "return=minimal"})
    if r.status_code not in (200, 204):
        raise HTTPException(status_code=500, detail=r.text)


@app.post("/playbook/worker/claim")
async def pb_worker_claim(payload: dict = Body(default={}), _wid: dict = Depends(_worker_identity)):
    """Hand the next queued job to a worker: returns the job plus a signed GET for
    the raw Visio file and a signed PUT for where to drop the converted PDF.
    SCOPED (Jul 9 2026 — paired local workers): a legacy shared-token worker
    only gets jobs with no coach uploader (admin-panel-direct uploads); a
    paired worker only ever gets jobs uploaded by ITS OWN paired login — never
    another coach's, even on the same team. See BINDER LOCAL PLAN.txt."""
    import uuid as _uuid
    worker = (payload.get("worker") or "worker").strip()[:64]
    async with httpx.AsyncClient() as c:
        r = await c.post(f"{SUPABASE_URL}/rest/v1/rpc/claim_playbook_job_scoped",
                         json={"p_worker": worker, "p_uploader_email": _wid.get("email")},
                         headers=_supa_headers_json())
    if r.status_code != 200:
        raise HTTPException(status_code=500, detail=r.text)
    rows = r.json() or []
    if not rows:
        return {"job": None}
    job = rows[0]
    out_key = f"{job['team_id']}/pdfs/{_uuid.uuid4().hex}.pdf"
    await _pb_job_patch(job["id"], {"out_key": out_key})
    kind = job.get("kind") or "convert"
    job_out = {"id": job["id"], "title": job.get("title"),
               "folder_path": job.get("folder_path", ""), "ext": job.get("ext"),
               "kind": kind, "number": bool(job.get("number"))}
    resp = {
        "job": job_out,
        "raw_url": _r2_presign("GET", job["raw_key"], expires=1800),
        "out_key": out_key,
        "put_url": _r2_presign("PUT", out_key, expires=1800),
    }
    if kind == "insert":
        # An insert job also splices into an existing section: hand the worker a
        # signed GET for that PDF plus where/what to stamp. (out_key/put_url is
        # where the MERGED result goes; the doc is repointed at it on completion.)
        job_out["insert_after"] = job.get("insert_after") or 0
        job_out["label"] = job.get("label") or ""
        job_out["target_doc_id"] = job.get("target_doc_id")
        base_key = None
        if job.get("target_doc_id"):
            async with httpx.AsyncClient() as c:
                gd = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_DOCS}",
                                 params={"select": "r2_key", "id": f"eq.{job['target_doc_id']}", "limit": "1"},
                                 headers=_supa_headers_json())
                if gd.status_code == 200 and gd.json():
                    base_key = gd.json()[0].get("r2_key")
        resp["base_url"] = _r2_presign("GET", base_key, expires=1800) if base_key else None
    return resp


@app.post("/playbook/worker/complete", dependencies=[Depends(_require_worker)])
async def pb_worker_complete(payload: dict = Body(...)):
    """Worker finished a job: register the converted PDF as a playbook_docs row
    and mark the job done."""
    job_id = (payload.get("job_id") or "").strip()
    if not job_id:
        raise HTTPException(status_code=400, detail="job_id required.")
    async with httpx.AsyncClient() as c:
        g = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_JOBS}",
                        params={"select": "folder_path,title,out_key,team_id",
                                "id": f"eq.{job_id}", "limit": "1"},
                        headers=_supa_headers_json())
        if g.status_code != 200 or not g.json():
            raise HTTPException(status_code=404, detail="Job not found.")
        job = g.json()[0]
        doc = {
            "team_id":     job.get("team_id"),
            "folder_path": job.get("folder_path") or "",
            "title":       job.get("title") or "",
            "r2_key":      job.get("out_key") or "",
            "pages":       payload.get("pages"),
            "size_bytes":  payload.get("size"),
            "sort_order":  0,
        }
        d = await c.post(f"{SUPABASE_URL}/rest/v1/{_PB_DOCS}", json=doc,
                         headers={**_supa_headers_json(), "Prefer": "return=representation"})
    if d.status_code not in (200, 201):
        raise HTTPException(status_code=500, detail=d.text)
    doc_id = (d.json() or [{}])[0].get("id")
    await _pb_job_patch(job_id, {"status": "done", "doc_id": doc_id,
                                 "pages": payload.get("pages"),
                                 "size_bytes": payload.get("size"), "error": None})
    return {"ok": True, "doc_id": doc_id}


@app.post("/playbook/worker/insert-complete", dependencies=[Depends(_require_worker)])
async def pb_worker_insert_complete(payload: dict = Body(...)):
    """Worker finished an 'insert' job: point the target section at the merged
    PDF (SAME doc_id → the row keeps its tree spot, title, and player Touch
    Notes) and shift those notes past the insert point so each note stays on
    its play. This is Replace + a page-shift, not a new doc."""
    job_id = (payload.get("job_id") or "").strip()
    if not job_id:
        raise HTTPException(status_code=400, detail="job_id required.")
    async with httpx.AsyncClient() as c:
        g = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_JOBS}",
                        params={"select": "target_doc_id,out_key,insert_after,team_id",
                                "id": f"eq.{job_id}", "limit": "1"},
                        headers=_supa_headers_json())
        if g.status_code != 200 or not g.json():
            raise HTTPException(status_code=404, detail="Job not found.")
        job = g.json()[0]
        doc_id = job.get("target_doc_id")
        out_key = job.get("out_key")
        insert_after = job.get("insert_after") or 0
        if not doc_id or not out_key:
            raise HTTPException(status_code=400, detail="Insert job missing target/output.")
        gd = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_DOCS}",
                         params={"select": "r2_key,pages", "id": f"eq.{doc_id}", "limit": "1"},
                         headers=_supa_headers_json())
        if gd.status_code != 200 or not gd.json():
            raise HTTPException(status_code=404, detail="Section not found.")
        old = gd.json()[0]
        old_key = old.get("r2_key")
        new_pages = payload.get("pages")
        # How many pages were spliced in — authoritative from the worker (the
        # target's stored `pages` can be null for coach-uploaded PDFs, so don't
        # derive it from new-minus-old). Fall back to that only if not sent.
        inserted = payload.get("inserted")
        if inserted is None:
            old_pages = old.get("pages")
            inserted = (new_pages - old_pages) if isinstance(new_pages, int) and isinstance(old_pages, int) else None
        patch = {"r2_key": out_key, "size_bytes": payload.get("size"), "pages": new_pages}
        r = await c.patch(f"{SUPABASE_URL}/rest/v1/{_PB_DOCS}",
                          params={"id": f"eq.{doc_id}"}, json=patch,
                          headers={**_supa_headers_json(), "Prefer": "return=minimal"})
        if r.status_code not in (200, 204):
            raise HTTPException(status_code=500, detail=r.text)
        if inserted and inserted > 0:
            try:
                await c.post(f"{SUPABASE_URL}/rest/v1/rpc/shift_playbook_notes",
                             json={"p_doc_id": doc_id, "p_after": insert_after, "p_shift": inserted},
                             headers=_supa_headers_json())
            except Exception:
                pass    # notes shift is best-effort; the merged PDF is already live
        if old_key and old_key != out_key:
            try:
                await c.delete(_r2_presign("DELETE", old_key, expires=300))
            except Exception:
                pass    # row already points at the merged PDF; orphaned object is harmless
    await _pb_job_patch(job_id, {"status": "done", "doc_id": doc_id,
                                 "pages": payload.get("pages"),
                                 "size_bytes": payload.get("size"), "error": None})
    return {"ok": True, "doc_id": doc_id}


@app.post("/playbook/worker/error", dependencies=[Depends(_require_worker)])
async def pb_worker_error(payload: dict = Body(...)):
    """Worker couldn't convert a job — mark it errored so the admin panel shows it."""
    job_id = (payload.get("job_id") or "").strip()
    if not job_id:
        raise HTTPException(status_code=400, detail="job_id required.")
    await _pb_job_patch(job_id, {"status": "error",
                                 "error": (payload.get("error") or "conversion failed")[:500]})
    return {"ok": True}


# ── Coach upload — coaches are roster rows whose Position contains "coach"; ──
# they sign in exactly like players (email + self-set password, same token).
async def _require_coach(x_pb_token: str = Header("")):
    """Validates the session, checks the coach role, and returns the FULL user
    row (team_id resolved fresh from the DB — see _require_player)."""
    email = _pb_read_token(x_pb_token)
    if not email:
        raise HTTPException(status_code=401, detail="Please sign in again.")
    u = await _pb_get(email)
    if not u:
        raise HTTPException(status_code=401, detail="Account not found.")
    pos = (u.get("position") or "").lower()
    # "video" = Roger/video staff. Team Admins always get content-upload access
    # too, regardless of what their Position text says — a Team Admin managing
    # a program obviously needs to manage its playbook content, not just its
    # roster (Roger, Jul 8 2026: "they need the sections... I have on my admin page").
    if "coach" not in pos and "video" not in pos and not u.get("is_admin"):
        raise HTTPException(status_code=403, detail="This page is for coaches.")
    if not await _team_is_active(u["team_id"]):
        raise HTTPException(status_code=401, detail="This team's account is currently deactivated.")
    return u


async def _require_team_admin(x_pb_token: str = Header("")):
    """Team Admin gate (roster management + promoting other admins) — a role
    granted by an Owner/existing admin via is_admin, NOT tied to coach Position.
    'Coaches can't be trusted with roster power' (Roger, Jul 8 2026)."""
    email = _pb_read_token(x_pb_token)
    if not email:
        raise HTTPException(status_code=401, detail="Please sign in again.")
    u = await _pb_get(email)
    if not u:
        raise HTTPException(status_code=401, detail="Account not found.")
    if not u.get("is_admin"):
        raise HTTPException(status_code=403, detail="This page is for team admins.")
    if not await _team_is_active(u["team_id"]):
        raise HTTPException(status_code=401, detail="This team's account is currently deactivated.")
    return u


async def _doc_in_team(doc_id: str, team_id: str) -> bool:
    """True only if doc_id exists AND belongs to team_id. Used before any
    coach/team-admin write to a doc, so one team can never touch another's row
    even by guessing/replaying a doc_id."""
    async with httpx.AsyncClient() as c:
        g = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_DOCS}",
                        params={"select": "id", "id": f"eq.{doc_id}",
                                "team_id": f"eq.{team_id}", "limit": "1"},
                        headers=_scoped_headers(team_id))
    return g.status_code == 200 and bool(g.json())


async def _folder_in_team(folder_id: str, team_id: str) -> bool:
    """Same idea as _doc_in_team, for playbook_folders (empty-folder rows)."""
    async with httpx.AsyncClient() as c:
        g = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_FOLDERS}",
                        params={"select": "id", "id": f"eq.{folder_id}",
                                "team_id": f"eq.{team_id}", "limit": "1"},
                        headers=_scoped_headers(team_id))
    return g.status_code == 200 and bool(g.json())


# ── Team Admin — roster + admin management, scoped to the admin's OWN team ────
# "Coaches can't be trusted with that kind of power" (Roger, Jul 8 2026): roster
# control is a dedicated role (is_admin), independent of the coach Position
# check. A Team Admin is appointed by the Owner (or by another Team Admin on
# the SAME team) — never self-granted, never cross-team.

@app.get("/team-admin/roster")
async def team_admin_roster(_u: dict = Depends(_require_team_admin)):
    """This team's full roster (no password material)."""
    async with httpx.AsyncClient() as c:
        r = await c.get(
            f"{SUPABASE_URL}/rest/v1/{_PB_TABLE}",
            params={"select": "id,email,first_name,last_name,position,pw_hash,is_admin,created_at",
                    "team_id": f"eq.{_u['team_id']}",
                    "order": "last_name.asc,first_name.asc"},
            headers=_scoped_headers(_u["team_id"]),
        )
    if r.status_code != 200:
        raise HTTPException(status_code=500, detail=r.text)
    return [{
        "id": row["id"], "email": row["email"],
        "first_name": row.get("first_name", ""), "last_name": row.get("last_name", ""),
        "position": row.get("position", ""), "is_admin": bool(row.get("is_admin")),
        "active": bool(row.get("pw_hash")),
    } for row in r.json()]


@app.post("/team-admin/roster/upload")
async def team_admin_roster_upload(payload: dict = Body(...), _u: dict = Depends(_require_team_admin)):
    """Bulk add/update this team's roster from parsed CSV rows. Same email-
    collision protection as the Owner upload: an email already registered to a
    DIFFERENT team is skipped, never silently reassigned."""
    team_id = _u["team_id"]
    rows = payload.get("rows") or []
    processed, skipped = 0, []
    async with httpx.AsyncClient() as c:
        for raw in rows:
            email = _norm_email(raw.get("email"))
            if not email or "@" not in email:
                skipped.append({"email": raw.get("email", ""), "reason": "invalid email"})
                continue
            existing = await _pb_get(email)
            if existing and existing.get("team_id") != team_id:
                skipped.append({"email": email, "reason": "already registered to a different team"})
                continue
            row = {
                "email": email,
                "team_id": team_id,
                "first_name": (raw.get("first_name") or "").strip(),
                "last_name":  (raw.get("last_name") or "").strip(),
                "position":   (raw.get("position") or "").strip(),
            }
            r = await c.post(
                f"{SUPABASE_URL}/rest/v1/{_PB_TABLE}",
                params={"on_conflict": "email"},
                json=row,
                headers={**_scoped_headers(team_id),
                         "Prefer": "resolution=merge-duplicates,return=minimal"},
            )
            if r.status_code in (200, 201, 204):
                processed += 1
            else:
                skipped.append({"email": email, "reason": r.text[:120]})
    return {"processed": processed, "skipped": skipped, "total": len(rows)}


@app.delete("/team-admin/roster/{uid}")
async def team_admin_roster_delete(uid: str, _u: dict = Depends(_require_team_admin)):
    """Remove a player/coach from THIS team's roster only — 404s (never a plain
    delete) if the row belongs to another team, so an admin can't be tricked
    into deleting a row by id from outside their own team."""
    async with httpx.AsyncClient() as c:
        g = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_TABLE}",
                        params={"select": "id", "id": f"eq.{uid}",
                                "team_id": f"eq.{_u['team_id']}", "limit": "1"},
                        headers=_scoped_headers(_u["team_id"]))
        if g.status_code != 200 or not g.json():
            raise HTTPException(status_code=404, detail="Not found.")
        r = await c.delete(
            f"{SUPABASE_URL}/rest/v1/{_PB_TABLE}",
            params={"id": f"eq.{uid}"},
            headers={**_scoped_headers(_u["team_id"]), "Prefer": "return=minimal"},
        )
    if r.status_code not in (200, 204):
        raise HTTPException(status_code=500, detail=r.text)
    return {"ok": True}


@app.post("/team-admin/roster/{uid}/send-reset")
async def team_admin_send_reset(uid: str, _u: dict = Depends(_require_team_admin)):
    """Team Admin — email a reset link to someone on THEIR OWN roster. 404s on a
    row from another team (same guard as the delete/promote endpoints) so an id
    from outside the team can't be used to trigger mail to a stranger.
    Defined down here, not with the other reset endpoints, because
    _require_team_admin is declared below them and Depends() resolves at
    decoration time — see the import-check note in MEMORY.md."""
    async with httpx.AsyncClient() as c:
        g = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_TABLE}",
                        params={"select": "*", "id": f"eq.{uid}",
                                "team_id": f"eq.{_u['team_id']}", "limit": "1"},
                        headers=_scoped_headers(_u["team_id"]))
    rows = g.json() if g.status_code == 200 else []
    if not rows:
        raise HTTPException(status_code=404, detail="Not found.")
    try:
        email = await _pb_send_reset(rows[0])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not send: {e}")
    return {"ok": True, "email": email, "minutes": _PB_RESET_TTL_MIN}


@app.post("/team-admin/admins/{uid}")
async def team_admin_promote(uid: str, _u: dict = Depends(_require_team_admin)):
    """Promote an existing roster member (same team) to Team Admin."""
    async with httpx.AsyncClient() as c:
        g = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_TABLE}",
                        params={"select": "id", "id": f"eq.{uid}",
                                "team_id": f"eq.{_u['team_id']}", "limit": "1"},
                        headers=_scoped_headers(_u["team_id"]))
        if g.status_code != 200 or not g.json():
            raise HTTPException(status_code=404, detail="Not found.")
        r = await c.patch(f"{SUPABASE_URL}/rest/v1/{_PB_TABLE}",
                          params={"id": f"eq.{uid}"}, json={"is_admin": True},
                          headers={**_scoped_headers(_u["team_id"]), "Prefer": "return=minimal"})
    if r.status_code not in (200, 204):
        raise HTTPException(status_code=500, detail=r.text)
    return {"ok": True}


@app.delete("/team-admin/admins/{uid}")
async def team_admin_demote(uid: str, _u: dict = Depends(_require_team_admin)):
    """Demote a Team Admin (same team) back to a regular roster member. An
    admin CANNOT demote themselves — a team must always keep at least one."""
    if uid == _u["id"]:
        raise HTTPException(status_code=400, detail="You can't remove your own admin access.")
    async with httpx.AsyncClient() as c:
        g = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_TABLE}",
                        params={"select": "id", "id": f"eq.{uid}",
                                "team_id": f"eq.{_u['team_id']}", "limit": "1"},
                        headers=_scoped_headers(_u["team_id"]))
        if g.status_code != 200 or not g.json():
            raise HTTPException(status_code=404, detail="Not found.")
        r = await c.patch(f"{SUPABASE_URL}/rest/v1/{_PB_TABLE}",
                          params={"id": f"eq.{uid}"}, json={"is_admin": False},
                          headers={**_scoped_headers(_u["team_id"]), "Prefer": "return=minimal"})
    if r.status_code not in (200, 204):
        raise HTTPException(status_code=500, detail=r.text)
    return {"ok": True}


@app.post("/team-admin/logo-sign-upload")
async def team_admin_logo_sign_upload(_u: dict = Depends(_require_team_admin)):
    """Presigned PUT for THIS team's logo image — self-service, no Owner
    involvement needed. Key is always prefixed with the admin's own team_id."""
    import uuid as _uuid
    key = f"{_u['team_id']}/logo/{_uuid.uuid4().hex}"
    return {"key": key, "put_url": _r2_presign("PUT", key, expires=900)}


@app.post("/team-admin/logo")
async def team_admin_set_logo(payload: dict = Body(...), _u: dict = Depends(_require_team_admin)):
    """Save the logo key onto THIS admin's own team row (bytes already in R2
    from team_admin_logo_sign_upload above). playbook_teams has no team_id
    column of its own — ownership is enforced by matching the row's id to the
    admin's own team_id before allowing the patch, never trusting a client-
    supplied team id."""
    key = (payload.get("key") or "").strip()
    if not key:
        raise HTTPException(status_code=400, detail="key is required.")
    async with httpx.AsyncClient() as c:
        r = await c.patch(f"{SUPABASE_URL}/rest/v1/{_PB_TEAMS}",
                          params={"id": f"eq.{_u['team_id']}"},
                          json={"logo_r2_key": key},
                          headers={**_supa_headers_json(), "Prefer": "return=minimal"})
    if r.status_code not in (200, 204):
        raise HTTPException(status_code=500, detail=r.text)
    return {"ok": True}


@app.get("/team-admin/logo-catalog")
async def team_admin_logo_catalog(_u: dict = Depends(_require_team_admin)):
    """Same known-program list as the Owner's picker, for a Team Admin who
    wants to switch to (or pick, if none was set at creation) a logo already
    on file — no upload needed for any of CAPP's existing programs."""
    return {"schools": sorted(_TEAM_LOGO_NUMBERS.keys())}


@app.post("/team-admin/logo-from-catalog")
async def team_admin_logo_from_catalog(payload: dict = Body(...), _u: dict = Depends(_require_team_admin)):
    """Set THIS team's logo from the shared CAPP logo library by exact name —
    the auto-populate path, no upload/no R2 round-trip needed."""
    school = (payload.get("school") or "").strip()
    if school not in _TEAM_LOGO_NUMBERS:
        raise HTTPException(status_code=404, detail="Not in the known-programs list.")
    key = f"_team_logos/{_TEAM_LOGO_NUMBERS[school]}.png"
    async with httpx.AsyncClient() as c:
        r = await c.patch(f"{SUPABASE_URL}/rest/v1/{_PB_TEAMS}",
                          params={"id": f"eq.{_u['team_id']}"},
                          json={"logo_r2_key": key},
                          headers={**_supa_headers_json(), "Prefer": "return=minimal"})
    if r.status_code not in (200, 204):
        raise HTTPException(status_code=500, detail=r.text)
    return {"ok": True}


@app.get("/coach/playbook/folders")
async def coach_pb_folders(_u: dict = Depends(_require_coach)):
    """Every folder path a coach can upload into (docs' folders ∪ registered
    empty folders, plus all their ancestors) — scoped to the coach's own team."""
    team_id = _u["team_id"]
    async with httpx.AsyncClient() as c:
        d = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_DOCS}",
                        params={"select": "folder_path", "team_id": f"eq.{team_id}"},
                        headers=_scoped_headers(team_id))
        f = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_FOLDERS}",
                        params={"select": "folder_path", "team_id": f"eq.{team_id}"},
                        headers=_scoped_headers(team_id))
    paths = set()
    for resp in (d, f):
        if resp.status_code == 200:
            for row in resp.json():
                p = (row.get("folder_path") or "").strip().strip("/")
                if p:
                    parts = p.split("/")
                    for i in range(1, len(parts) + 1):
                        paths.add("/".join(parts[:i]))
    return {"folders": sorted(paths)}


@app.post("/coach/playbook/folders/create")
async def coach_pb_create_folder(payload: dict = Body(...), _u: dict = Depends(_require_coach)):
    """Register an empty folder so it shows in the players' tree before any
    PDF lands in it — team-scoped version of the Owner's create-folder."""
    path = (payload.get("path") or "").strip().strip("/")
    if not path:
        raise HTTPException(status_code=400, detail="path is required.")
    async with httpx.AsyncClient() as c:
        r = await c.post(f"{SUPABASE_URL}/rest/v1/{_PB_FOLDERS}",
                         params={"on_conflict": "team_id,folder_path"},
                         json={"folder_path": path, "team_id": _u["team_id"]},
                         headers={**_scoped_headers(_u["team_id"]),
                                  "Prefer": "resolution=merge-duplicates,return=representation"})
    if r.status_code not in (200, 201):
        raise HTTPException(status_code=500, detail=r.text)
    return (r.json() or [{}])[0]


@app.get("/coach/playbook/folders/list")
async def coach_pb_list_empty_folders(_u: dict = Depends(_require_coach)):
    """Just the registered EMPTY folder rows (not the docs-derived ancestor
    list from coach_pb_folders above) — for a manage/delete table."""
    async with httpx.AsyncClient() as c:
        r = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_FOLDERS}",
                        params={"select": "*", "team_id": f"eq.{_u['team_id']}",
                                "order": "folder_path.asc"},
                        headers=_scoped_headers(_u["team_id"]))
    if r.status_code != 200:
        raise HTTPException(status_code=500, detail=r.text)
    return r.json()


@app.delete("/coach/playbook/folders/{folder_id}")
async def coach_pb_delete_folder(folder_id: str, _u: dict = Depends(_require_coach)):
    """Remove an empty-folder entry — only if it belongs to the coach's own
    team (any PDFs already uploaded there are untouched either way)."""
    if not await _folder_in_team(folder_id, _u["team_id"]):
        raise HTTPException(status_code=404, detail="Folder not found.")
    async with httpx.AsyncClient() as c:
        r = await c.delete(f"{SUPABASE_URL}/rest/v1/{_PB_FOLDERS}",
                           params={"id": f"eq.{folder_id}"},
                           headers={**_scoped_headers(_u["team_id"]), "Prefer": "return=minimal"})
    if r.status_code not in (200, 204):
        raise HTTPException(status_code=500, detail=r.text)
    return {"ok": True}


def _pb_norm_path(p) -> str:
    """'/a//b/ ' -> 'a/b'. Collapses blanks so a stray slash can't create a
    folder row that no tree path will ever match."""
    return "/".join(s.strip() for s in str(p or "").split("/") if s.strip())


def _pb_under(path: str, folder: str) -> bool:
    """True if `folder` IS `path` or sits underneath it. Deliberately a plain
    prefix test rather than a PostgREST `like` filter — folder names here
    contain characters LIKE treats as wildcards ('_' matches any single char),
    so a name like '01 - OFFENSE' would silently over-match."""
    return folder == path or folder.startswith(path + "/")


@app.post("/coach/playbook/folders/rename")
async def coach_pb_rename_folder(payload: dict = Body(...), _u: dict = Depends(_require_coach)):
    """Rename/move a folder and everything under it, for THIS coach's team.

    Rewrites folder_path on every doc and every registered folder row at or
    below `from`. There is no single-statement way to do a prefix rewrite
    through PostgREST, so rows are fetched, filtered in Python, and patched
    individually — the counts involved are small (hundreds at most)."""
    team_id = _u["team_id"]
    src = _pb_norm_path(payload.get("from"))
    dst = _pb_norm_path(payload.get("to"))
    if not src or not dst:
        raise HTTPException(status_code=400, detail="Both a source and a new name are required.")
    if src == dst:
        return {"ok": True, "docs": 0, "folders": 0, "to": dst}
    # Moving a folder inside itself would orphan the whole subtree.
    if _pb_under(src, dst):
        raise HTTPException(status_code=400, detail="A folder can't be moved inside itself.")

    async with httpx.AsyncClient() as c:
        d = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_DOCS}",
                        params={"select": "id,folder_path", "team_id": f"eq.{team_id}"},
                        headers=_scoped_headers(team_id))
        f = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_FOLDERS}",
                        params={"select": "id,folder_path", "team_id": f"eq.{team_id}"},
                        headers=_scoped_headers(team_id))
        if d.status_code != 200:
            raise HTTPException(status_code=500, detail=d.text)
        docs = [x for x in d.json() if _pb_under(src, (x.get("folder_path") or ""))]
        folders = [x for x in (f.json() if f.status_code == 200 else [])
                   if _pb_under(src, (x.get("folder_path") or ""))]
        if not docs and not folders:
            raise HTTPException(status_code=404, detail="That folder no longer exists.")

        def moved(old: str) -> str:
            return dst + old[len(src):]

        for row in docs:
            r = await c.patch(f"{SUPABASE_URL}/rest/v1/{_PB_DOCS}",
                              params={"id": f"eq.{row['id']}"},
                              json={"folder_path": moved(row.get("folder_path") or "")},
                              headers={**_scoped_headers(team_id), "Prefer": "return=minimal"})
            if r.status_code not in (200, 204):
                raise HTTPException(status_code=500, detail=r.text)
        for row in folders:
            # A row may already exist at the destination path (unique on
            # team_id,folder_path) — drop this one instead of failing the rename.
            r = await c.patch(f"{SUPABASE_URL}/rest/v1/{_PB_FOLDERS}",
                              params={"id": f"eq.{row['id']}"},
                              json={"folder_path": moved(row.get("folder_path") or "")},
                              headers={**_scoped_headers(team_id), "Prefer": "return=minimal"})
            if r.status_code == 409:
                await c.delete(f"{SUPABASE_URL}/rest/v1/{_PB_FOLDERS}",
                               params={"id": f"eq.{row['id']}"},
                               headers={**_scoped_headers(team_id), "Prefer": "return=minimal"})
            elif r.status_code not in (200, 204):
                raise HTTPException(status_code=500, detail=r.text)

    print(f"[PBFOLDER] {team_id} rename {src!r} -> {dst!r} "
          f"({len(docs)} docs, {len(folders)} folders)", flush=True)
    return {"ok": True, "docs": len(docs), "folders": len(folders), "to": dst}


@app.post("/coach/playbook/folders/delete")
async def coach_pb_delete_folder_by_path(payload: dict = Body(...),
                                         _u: dict = Depends(_require_coach)):
    """Delete a folder (and its empty descendants) BY PATH, for this coach's team.

    Refuses if any document sits at or under it and says what's in the way —
    a coach clicking Delete on a game folder in the tree should never be one
    click away from wiping a season of content. Deleting the files first is a
    deliberate, separate act. (The by-id sibling endpoint above only removes a
    single empty-folder registration row.)"""
    team_id = _u["team_id"]
    path = _pb_norm_path(payload.get("path"))
    if not path:
        raise HTTPException(status_code=400, detail="path is required.")

    async with httpx.AsyncClient() as c:
        d = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_DOCS}",
                        params={"select": "id,title,folder_path", "team_id": f"eq.{team_id}"},
                        headers=_scoped_headers(team_id))
        if d.status_code != 200:
            raise HTTPException(status_code=500, detail=d.text)
        inside = [x for x in d.json() if _pb_under(path, (x.get("folder_path") or ""))]
        if inside:
            sample = ", ".join((x.get("title") or "?") for x in inside[:3])
            more = f" and {len(inside) - 3} more" if len(inside) > 3 else ""
            raise HTTPException(
                status_code=409,
                detail=(f'"{path}" still holds {len(inside)} file(s): {sample}{more}. '
                        f"Delete or move those first."))

        f = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_FOLDERS}",
                        params={"select": "id,folder_path", "team_id": f"eq.{team_id}"},
                        headers=_scoped_headers(team_id))
        rows = [x for x in (f.json() if f.status_code == 200 else [])
                if _pb_under(path, (x.get("folder_path") or ""))]
        if not rows:
            raise HTTPException(status_code=404, detail="That folder no longer exists.")
        for row in rows:
            r = await c.delete(f"{SUPABASE_URL}/rest/v1/{_PB_FOLDERS}",
                               params={"id": f"eq.{row['id']}"},
                               headers={**_scoped_headers(team_id), "Prefer": "return=minimal"})
            if r.status_code not in (200, 204):
                raise HTTPException(status_code=500, detail=r.text)

    print(f"[PBFOLDER] {team_id} delete {path!r} ({len(rows)} folder rows)", flush=True)
    return {"ok": True, "folders": len(rows)}


@app.post("/coach/playbook/sign-upload")
async def coach_pb_sign_upload(payload: dict = Body(...), _u: dict = Depends(_require_coach)):
    """Presigned PUT for a coach upload. PDFs go straight to the content area;
    Word/PowerPoint/Visio go to raw/ and get queued for conversion. R2 key is
    always prefixed with the coach's OWN team_id — never client-supplied — so a
    coach can only ever write into their own team's storage drawer."""
    import uuid as _uuid
    team_id = _u["team_id"]
    ext = (payload.get("ext") or "").lower().lstrip(".")
    if ext == "pdf":
        key = f"{team_id}/pdfs/{_uuid.uuid4().hex}.pdf"
    elif ext in _PB_CONVERT_EXTS:
        key = f"{team_id}/raw/{_uuid.uuid4().hex}.{ext}"
    else:
        raise HTTPException(status_code=400, detail="Only PDF, Word, Excel, PowerPoint, or Visio files.")
    return {"key": key, "put_url": _r2_presign("PUT", key, expires=900),
            "kind": "pdf" if ext == "pdf" else "convert"}


@app.post("/coach/playbook/docs")
async def coach_pb_create_doc(payload: dict = Body(...), _u: dict = Depends(_require_coach)):
    """Register a coach-uploaded PDF (bytes already in R2). team_id is always
    the coach's own — never taken from the request body."""
    row = {
        "team_id":     _u["team_id"],
        "folder_path": (payload.get("folder") or "").strip().strip("/"),
        "title":       (payload.get("title") or "").strip(),
        "r2_key":      (payload.get("key") or "").strip(),
        "pages":       payload.get("pages"),
        "size_bytes":  payload.get("size"),
        "sort_order":  0,
    }
    if not row["title"] or not row["r2_key"]:
        raise HTTPException(status_code=400, detail="title and key are required.")
    async with httpx.AsyncClient() as c:
        r = await c.post(f"{SUPABASE_URL}/rest/v1/{_PB_DOCS}", json=row,
                         headers={**_scoped_headers(_u["team_id"]), "Prefer": "return=representation"})
    if r.status_code not in (200, 201):
        raise HTTPException(status_code=500, detail=r.text)
    return (r.json() or [{}])[0]


@app.get("/coach/playbook/docs")
async def coach_pb_list_docs(_u: dict = Depends(_require_coach)):
    """Full contents list for the coach's own team — same shape as the Owner's
    admin_pb_list_docs, so the coach page can render the same 'Playbook
    contents' table with Replace/Delete."""
    async with httpx.AsyncClient() as c:
        r = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_DOCS}",
                        params={"select": "*", "team_id": f"eq.{_u['team_id']}",
                                "order": "folder_path.asc,sort_order.asc,title.asc"},
                        headers=_scoped_headers(_u["team_id"]))
    if r.status_code != 200:
        raise HTTPException(status_code=500, detail=r.text)
    return r.json()


@app.post("/coach/playbook/docs/{doc_id}/replace")
async def coach_pb_replace_doc(doc_id: str, payload: dict = Body(...), _u: dict = Depends(_require_coach)):
    """Swap a doc's PDF for new bytes already uploaded to R2 — team-scoped
    version of the Owner's replace. The row keeps its id/folder/title/sort
    order, so player Touch Notes stay attached. Rejects a doc that isn't this
    coach's own team."""
    if not await _doc_in_team(doc_id, _u["team_id"]):
        raise HTTPException(status_code=404, detail="Doc not found.")
    new_key = (payload.get("key") or "").strip()
    if not new_key:
        raise HTTPException(status_code=400, detail="key is required.")
    async with httpx.AsyncClient() as c:
        g = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_DOCS}",
                        params={"select": "r2_key", "id": f"eq.{doc_id}", "limit": "1"},
                        headers=_scoped_headers(_u["team_id"]))
        if g.status_code != 200 or not g.json():
            raise HTTPException(status_code=404, detail="Doc not found.")
        old_key = g.json()[0].get("r2_key")
        patch = {"r2_key": new_key, "size_bytes": payload.get("size"), "pages": payload.get("pages")}
        r = await c.patch(f"{SUPABASE_URL}/rest/v1/{_PB_DOCS}",
                          params={"id": f"eq.{doc_id}"}, json=patch,
                          headers={**_scoped_headers(_u["team_id"]), "Prefer": "return=minimal"})
        if r.status_code not in (200, 204):
            raise HTTPException(status_code=500, detail=r.text)
        if old_key and old_key != new_key:
            try:
                await c.delete(_r2_presign("DELETE", old_key, expires=300))
            except Exception:
                pass    # row already points at the new PDF; orphaned object is harmless
    return {"ok": True}


@app.post("/coach/playbook/jobs/clear-finished")
async def coach_pb_clear_finished_jobs(_u: dict = Depends(_require_coach)):
    """Clear done/error rows out of the conversion activity feed — this team's
    jobs only. Leftover raw source files are removed from R2; produced PDFs
    are live docs and untouched."""
    team_id = _u["team_id"]
    async with httpx.AsyncClient() as c:
        g = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_JOBS}",
                        params={"select": "id,raw_key,out_key,status",
                                "team_id": f"eq.{team_id}",
                                "status": "in.(done,error)"},
                        headers=_scoped_headers(team_id))
        rows = g.json() if g.status_code == 200 else []
        for j in rows:
            keys = [j.get("raw_key")]
            if j.get("status") != "done":
                keys.append(j.get("out_key"))
            for k in keys:
                if k:
                    try:
                        await c.delete(_r2_presign("DELETE", k, expires=300))
                    except Exception:
                        pass
        r = await c.delete(f"{SUPABASE_URL}/rest/v1/{_PB_JOBS}",
                           params={"team_id": f"eq.{team_id}", "status": "in.(done,error)"},
                           headers={**_scoped_headers(team_id), "Prefer": "return=minimal"})
        if r.status_code not in (200, 204):
            raise HTTPException(status_code=500, detail=r.text)
    return {"ok": True, "cleared": len(rows)}


@app.get("/coach/playbook/jobs")
async def coach_pb_list_jobs(_u: dict = Depends(_require_coach)):
    """Conversion queue state so the coach page can show real progress after
    the upload itself finishes (queued → converting → done/error) — this
    team's jobs only."""
    async with httpx.AsyncClient() as c:
        r = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_JOBS}",
                        params={"select": "id,folder_path,title,ext,status,error,created_at",
                                "team_id": f"eq.{_u['team_id']}",
                                "order": "created_at.desc", "limit": "50"},
                        headers=_scoped_headers(_u["team_id"]))
    if r.status_code != 200:
        raise HTTPException(status_code=500, detail=r.text)
    return r.json()


@app.patch("/coach/playbook/docs/{doc_id}/move")
async def coach_pb_move_doc(doc_id: str, payload: dict = Body(...), _u: dict = Depends(_require_coach)):
    """Move a doc to another folder. Same row/id, so Touch Notes stay attached.
    Rejects a doc that isn't this coach's own team (404 — never confirms another
    team's doc even exists)."""
    if not await _doc_in_team(doc_id, _u["team_id"]):
        raise HTTPException(status_code=404, detail="Doc not found.")
    folder = (payload.get("folder") or "").strip().strip("/")
    async with httpx.AsyncClient() as c:
        r = await c.patch(f"{SUPABASE_URL}/rest/v1/{_PB_DOCS}",
                          params={"id": f"eq.{doc_id}"},
                          json={"folder_path": folder},
                          headers={**_scoped_headers(_u["team_id"]), "Prefer": "return=representation"})
    if r.status_code not in (200, 204):
        raise HTTPException(status_code=500, detail=r.text)
    rows = r.json() if r.status_code == 200 else []
    if not rows:
        raise HTTPException(status_code=404, detail="Doc not found.")
    return rows[0]


@app.delete("/coach/playbook/docs/{doc_id}")
async def coach_pb_delete_doc(doc_id: str, _u: dict = Depends(_require_coach)):
    """Coach delete — same behavior as the admin delete (row + R2 object), but
    only if the doc belongs to the coach's own team."""
    if not await _doc_in_team(doc_id, _u["team_id"]):
        raise HTTPException(status_code=404, detail="Doc not found.")
    return await admin_pb_delete_doc(doc_id)


@app.post("/coach/playbook/jobs")
async def coach_pb_create_job(payload: dict = Body(...), _u: dict = Depends(_require_coach)):
    """Queue a coach upload for the worker — PowerPoint/Visio (converted to PDF)
    or a PDF that needs page numbers stamped. Numbered 1..N by default (so the
    playbook stays consistently numbered); the coach page's 'already numbered'
    option sends number=false to skip the stamp. team_id is always the coach's
    own (never client-supplied)."""
    ext = (payload.get("ext") or "").lower().lstrip(".")
    if ext != "pdf" and ext not in _PB_CONVERT_EXTS:
        raise HTTPException(status_code=400, detail="Not a convertible file.")
    row = {
        "team_id":     _u["team_id"],
        "raw_key":     (payload.get("key") or "").strip(),
        "ext":         ext,
        "uploader_email": _u["email"],   # scopes the claim to THIS coach's own paired worker
        "number":      bool(payload.get("number", True)),
        "folder_path": (payload.get("folder") or "").strip().strip("/"),
        "title":       (payload.get("title") or "").strip(),
        "status":      "queued",
    }
    if not row["title"] or not row["raw_key"]:
        raise HTTPException(status_code=400, detail="title and key are required.")
    async with httpx.AsyncClient() as c:
        r = await c.post(f"{SUPABASE_URL}/rest/v1/{_PB_JOBS}", json=row,
                         headers={**_scoped_headers(_u["team_id"]), "Prefer": "return=representation"})
    if r.status_code not in (200, 201):
        raise HTTPException(status_code=500, detail=r.text)
    return (r.json() or [{}])[0]


@app.get("/coach/playbook/doc/{doc_id}/url")
async def coach_pb_doc_url(doc_id: str, _u: dict = Depends(_require_coach)):
    """Short-lived signed GET for one of THIS team's PDFs — used by the coach
    page's Insert-Play thumbnail grid to render a section's pages. Team-scoped
    (404s another team's doc); unlike the player doc-url this is a management
    action, so it is deliberately NOT written to the player access log."""
    if not await _doc_in_team(doc_id, _u["team_id"]):
        raise HTTPException(status_code=404, detail="Not found.")
    async with httpx.AsyncClient() as c:
        g = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_DOCS}",
                        params={"select": "r2_key", "id": f"eq.{doc_id}", "limit": "1"},
                        headers=_scoped_headers(_u["team_id"]))
    if g.status_code != 200 or not g.json():
        raise HTTPException(status_code=404, detail="Not found.")
    return {"url": _r2_presign("GET", g.json()[0]["r2_key"], expires=600)}


@app.post("/coach/playbook/insert")
async def coach_pb_insert(payload: dict = Body(...), _u: dict = Depends(_require_coach)):
    """Queue an 'insert a play' job: splice a new play (PDF/Word/PowerPoint/Visio,
    already uploaded to R2) INTO an existing section at a chosen page, stamped
    with a number like '8-1'. The worker converts (if needed), stamps, merges,
    and repoints the section's PDF in place — same doc_id, so player Touch Notes
    stay attached (notes past the insert point are shifted on completion).
    team_id is always the coach's own; the section must be this team's."""
    doc_id = (payload.get("doc_id") or "").strip()
    if not doc_id or not await _doc_in_team(doc_id, _u["team_id"]):
        raise HTTPException(status_code=404, detail="Section not found.")
    ext = (payload.get("ext") or "").lower().lstrip(".")
    if ext != "pdf" and ext not in _PB_CONVERT_EXTS:
        raise HTTPException(status_code=400, detail="Only PDF, Word, Excel, PowerPoint, or Visio files.")
    raw_key = (payload.get("key") or "").strip()
    if not raw_key:
        raise HTTPException(status_code=400, detail="key is required.")
    try:
        insert_after = max(0, int(payload.get("insert_after")))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="insert_after must be a page number.")
    label = (payload.get("label") or "").strip()[:40]
    async with httpx.AsyncClient() as c:
        gd = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_DOCS}",
                         params={"select": "folder_path,title", "id": f"eq.{doc_id}", "limit": "1"},
                         headers=_scoped_headers(_u["team_id"]))
    d = gd.json()[0] if gd.status_code == 200 and gd.json() else {}
    row = {
        "team_id":       _u["team_id"],
        "raw_key":       raw_key,
        "ext":           ext,
        "uploader_email": _u["email"],   # scopes the claim to THIS coach's own paired worker
        "kind":          "insert",
        "target_doc_id": doc_id,
        "insert_after":  insert_after,
        "label":         label,
        "folder_path":   d.get("folder_path") or "",
        "title":         (d.get("title") or "section") + " ◀ insert" + (f" ({label})" if label else ""),
        "status":        "queued",
    }
    async with httpx.AsyncClient() as c:
        r = await c.post(f"{SUPABASE_URL}/rest/v1/{_PB_JOBS}", json=row,
                         headers={**_scoped_headers(_u["team_id"]), "Prefer": "return=representation"})
    if r.status_code not in (200, 201):
        raise HTTPException(status_code=500, detail=r.text)
    return (r.json() or [{}])[0]


@app.post("/coach/playbook/converter/pair-token")
async def coach_pb_pair_token(_u: dict = Depends(_require_coach)):
    """Mint a short-lived, single-use pairing token for THIS logged-in coach.
    The downloaded local worker setup exchanges it once (via /converter/register)
    for its own permanent device credential — the coach never re-enters
    credentials. See BINDER LOCAL PLAN.txt."""
    import secrets
    tok = secrets.token_urlsafe(24)
    async with httpx.AsyncClient() as c:
        r = await c.post(f"{SUPABASE_URL}/rest/v1/{_PB_PAIR_TOKENS}",
                         json={"token": tok, "email": _u["email"], "team_id": _u["team_id"]},
                         headers={**_supa_headers_json(), "Prefer": "return=minimal"})
    if r.status_code not in (200, 201, 204):
        raise HTTPException(status_code=500, detail=r.text)
    return {"pairing_token": tok}


@app.get("/coach/playbook/converter/status")
async def coach_pb_converter_status(_u: dict = Depends(_require_coach)):
    """Is a local conversion worker paired to THIS coach's own login, and has
    it checked in recently? Drives the one-time-setup prompt on a
    PowerPoint/Visio upload (a coach who only ever uploads PDFs never needs it)."""
    latest = os.environ.get("CONVERTER_VERSION", "1.0.0")
    async with httpx.AsyncClient() as c:
        r = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_DEVICES}",
                        params={"select": "device_name,last_seen_at,converter_version",
                                "email": f"eq.{_u['email']}",
                                "order": "paired_at.desc", "limit": "1"},
                        headers=_supa_headers_json())
        # Tolerate the column not existing yet (see _worker_identity).
        if r.status_code >= 400:
            r = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_DEVICES}",
                            params={"select": "device_name,last_seen_at",
                                    "email": f"eq.{_u['email']}",
                                    "order": "paired_at.desc", "limit": "1"},
                            headers=_supa_headers_json())
    rows = r.json() if r.status_code == 200 else []
    if not rows:
        return {"paired": False, "online": False, "latest_version": latest}
    last_seen = rows[0].get("last_seen_at")
    online = False
    if last_seen:
        try:
            dt = _dtmod.datetime.fromisoformat(last_seen.replace("Z", "+00:00"))
            online = (_dtmod.datetime.now(_dtmod.timezone.utc) - dt).total_seconds() < 300
        except Exception:
            online = False
    # A version is only known once the machine has run a build that reports it.
    # Older builds report nothing, which is itself the signal that it's stale.
    running = (rows[0].get("converter_version") or "").strip()
    return {"paired": True, "online": online,
            "device_name": rows[0].get("device_name"),
            "version": running or None,
            "latest_version": latest,
            "up_to_date": bool(running) and _vtuple_pb(running) >= _vtuple_pb(latest)}


@app.post("/converter/register")
async def converter_register(payload: dict = Body(...)):
    """The local worker's one-time step: exchange a pairing token (minted by an
    already-signed-in coach) for its own permanent device token. No login here
    — the one-time token IS the proof of identity, and it's consumed on first
    use so it can't be replayed onto a second machine."""
    import secrets
    tok = (payload.get("pairing_token") or "").strip()
    device_name = (payload.get("device_name") or "")[:120]
    if not tok:
        raise HTTPException(status_code=400, detail="pairing_token is required.")
    async with httpx.AsyncClient() as c:
        g = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_PAIR_TOKENS}",
                        params={"select": "email,team_id,used_at", "token": f"eq.{tok}", "limit": "1"},
                        headers=_supa_headers_json())
        rows = g.json() if g.status_code == 200 else []
        if not rows or rows[0].get("used_at"):
            raise HTTPException(status_code=401, detail="This setup link is invalid or already used.")
        claim = rows[0]
        device_token = secrets.token_urlsafe(32)
        d = await c.post(f"{SUPABASE_URL}/rest/v1/{_PB_DEVICES}",
                         json={"email": claim["email"], "team_id": claim["team_id"],
                               "device_name": device_name, "token": device_token,
                               "last_seen_at": _dtmod.datetime.utcnow().isoformat() + "Z"},
                         headers={**_supa_headers_json(), "Prefer": "return=representation"})
        if d.status_code not in (200, 201):
            raise HTTPException(status_code=500, detail=d.text)
        new_id = (d.json() or [{}])[0].get("id")
        # Enforce ONE active device per coach — pairing a new computer
        # replaces any previous one. Jobs are scoped by coach email, not by
        # device, so a coach with two live paired devices had jobs split
        # unpredictably between them (real incident, Jul 10 2026 — see
        # MEMORY.md "Binder Converter — paired local worker architecture").
        # The stale device's local worker will just start getting rejected
        # on its next claim attempt; it isn't reachable from here to notify.
        if new_id:
            await c.delete(f"{SUPABASE_URL}/rest/v1/{_PB_DEVICES}",
                           params={"email": f"eq.{claim['email']}", "id": f"neq.{new_id}"},
                           headers={**_supa_headers_json(), "Prefer": "return=minimal"})
        await c.patch(f"{SUPABASE_URL}/rest/v1/{_PB_PAIR_TOKENS}",
                     params={"token": f"eq.{tok}"},
                     json={"used_at": _dtmod.datetime.utcnow().isoformat() + "Z"},
                     headers={**_supa_headers_json(), "Prefer": "return=minimal"})
    return {"worker_token": device_token}


@app.get("/coach/playbook/positions")
async def coach_pb_positions(_u: dict = Depends(_require_coach)):
    """Distinct, non-blank Position values actually on THIS team's roster —
    drives the notification-target dropdown. No fixed list/guessing: whatever
    the team's own CSV/roster says is exactly what shows up here."""
    async with httpx.AsyncClient() as c:
        r = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_TABLE}",
                        params={"select": "position", "team_id": f"eq.{_u['team_id']}"},
                        headers=_scoped_headers(_u["team_id"]))
    if r.status_code != 200:
        raise HTTPException(status_code=500, detail=r.text)
    positions = sorted({(row.get("position") or "").strip() for row in r.json()} - {""})
    return {"positions": positions}


@app.post("/coach/playbook/notify")
async def coach_pb_notify(payload: dict = Body(...), _u: dict = Depends(_require_coach)):
    """Manually-triggered notification (email + push) — a coach picks target
    position(s) or All Team and fires it themselves; nothing here runs
    automatically off an upload. No opt-in step: every matching roster member
    gets the email (everyone has one on file); push reaches whichever of
    their devices have an active subscription (granted via OS/browser
    permission — the one part of this that can't be made fully automatic)."""
    team_id = _u["team_id"]
    positions = [p.strip() for p in (payload.get("positions") or []) if p and p.strip()]
    all_team = bool(payload.get("all_team"))
    folder_path = (payload.get("folder_path") or "").strip()
    message = (payload.get("message") or "").strip()[:500]
    if not all_team and not positions:
        raise HTTPException(status_code=400, detail="Pick at least one position, or All Team.")

    team = await _team_get(team_id=team_id)
    team_name = (team or {}).get("name", "Your team")

    async with httpx.AsyncClient() as c:
        ur = await c.get(f"{SUPABASE_URL}/rest/v1/{_PB_TABLE}",
                         params={"select": "email,first_name,position", "team_id": f"eq.{team_id}"},
                         headers=_scoped_headers(team_id))
    if ur.status_code != 200:
        raise HTTPException(status_code=500, detail=ur.text)
    all_users = ur.json()
    if all_team:
        recipients = all_users
    else:
        wanted = {p.lower() for p in positions}
        recipients = [u for u in all_users if (u.get("position") or "").strip().lower() in wanted]
    if not recipients:
        return {"ok": True, "recipients": 0, "emails_sent": 0, "push_sent": 0, "push_failed": 0}

    title = f"{team_name} Playbook Updated"
    body = (f"New content in {folder_path}." if folder_path else "The playbook has been updated.")
    if message:
        body += " " + message

    emails_sent = 0
    for u in recipients:
        try:
            _send_playbook_update_email(u["email"], u.get("first_name") or "", team_name, folder_path, message)
            emails_sent += 1
        except Exception:
            pass   # best-effort — one bad address never blocks the rest

    emails = {u["email"] for u in recipients}
    async with httpx.AsyncClient() as c:
        sr = await c.get(f"{SUPABASE_URL}/rest/v1/playbook_push_subscriptions",
                         params={"select": "*", "team_id": f"eq.{team_id}"},
                         headers=_scoped_headers(team_id))
    subs = [s for s in (sr.json() if sr.status_code == 200 else []) if s.get("email") in emails]
    push_sent = push_failed = 0
    expired_ids = []
    for sub in subs:
        status = _send_push(sub, title, body)
        if status == "ok":
            push_sent += 1
        elif status == "expired":
            expired_ids.append(sub["id"])
        else:
            push_failed += 1
    if expired_ids:
        async with httpx.AsyncClient() as c:
            for sid in expired_ids:
                try:
                    await c.delete(f"{SUPABASE_URL}/rest/v1/playbook_push_subscriptions",
                                   params={"id": f"eq.{sid}"},
                                   headers={**_supa_headers_json(), "Prefer": "return=minimal"})
                except Exception:
                    pass

    return {"ok": True, "recipients": len(recipients), "emails_sent": emails_sent,
            "push_sent": push_sent, "push_failed": push_failed}


@app.post("/playbook/worker/fail", dependencies=[Depends(_require_worker)])
async def pb_worker_fail(payload: dict = Body(...)):
    job_id = (payload.get("job_id") or "").strip()
    if not job_id:
        raise HTTPException(status_code=400, detail="job_id required.")
    await _pb_job_patch(job_id, {"status": "error",
                                 "error": (payload.get("error") or "conversion failed")[:2000]})
    return {"ok": True}


# ─────────────────────────────────────────────────────────────────────────────
# CAPP Friends
# ─────────────────────────────────────────────────────────────────────────────
from datetime import datetime as _dt, timezone as _tz, timedelta as _timedelta


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
  .badge-gray   { background: #1e2a3a; color: #8b95a1; }
  .badge-yellow { background: #2d2200; color: #facc15; }
  .badge-red2   { background: #3b0a0a; color: #f87171; }
  .loading { color: #8b95a1; font-size: 13px; padding: 20px 0; text-align: center; }
  select { background: #0d1117; border: 1px solid #2c3b55; border-radius: 7px; color: white; font-size: 13px; padding: 8px 12px; outline: none; width: 100%; cursor: pointer; }
  select:focus { border-color: #3a7ebf; }
  select:disabled { opacity: 0.4; cursor: not-allowed; }

  /* Slide-out panel */
  #slideout, #crm-slideout { position: fixed; top: 0; right: -420px; width: 420px; height: 100vh; background: #0d1117;
    border-left: 1px solid #2c3b55; z-index: 100; transition: right 0.28s ease; overflow-y: auto;
    display: flex; flex-direction: column; }
  #slideout.open, #crm-slideout.open { right: 0; }
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
    <button class="tab" onclick="showTab('crm-tab', this)">CRM</button>
    <button class="tab" onclick="showTab('gameday-tab', this)">Game Day</button>
    <button class="tab" onclick="showTab('playbook-tab', this)">Playbook</button>
    <button class="tab" onclick="showTab('pbcontent-tab', this)">Playbook Files</button>
    <button class="tab" onclick="showTab('teams-tab', this)">Binder Teams</button>
    <button class="tab" onclick="showTab('notices-tab', this)">Broadcast</button>
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

    <!-- CRM / demo prospects -->
    <div class="panel" id="crm-tab">
      <div class="card">
        <h2>Add Prospect</h2>
        <p class="small" style="margin-bottom:14px;">Track schools you've demoed. Separate from licensed Clients above.</p>
        <div class="form-row">
          <div class="form-group">
            <label>School *</label>
            <input type="text" id="p-school" placeholder="e.g. Wagner">
          </div>
          <div class="form-group">
            <label>Contact Name</label>
            <input type="text" id="p-contact" placeholder="Coach / AD name">
          </div>
        </div>
        <div class="form-row">
          <div class="form-group">
            <label>Email</label>
            <input type="text" id="p-email" placeholder="name@school.edu">
          </div>
          <div class="form-group">
            <label>Phone</label>
            <input type="text" id="p-phone" placeholder="optional">
          </div>
        </div>
        <div class="form-row">
          <div class="form-group">
            <label>Status</label>
            <select id="p-status">
              <option>Demo Done</option>
              <option>Quote/Agreement Sent</option>
              <option>Trial</option>
              <option>Paid</option>
              <option>Lost</option>
            </select>
          </div>
          <div class="form-group">
            <label>Quote / Agreement Sent Date</label>
            <input type="date" id="p-quote-date">
          </div>
        </div>
        <div class="form-group" style="margin-bottom:12px;">
          <label>Notes</label>
          <textarea id="p-notes" placeholder="Call notes, next steps, anything useful..."></textarea>
        </div>
        <button class="btn btn-primary" onclick="createProspect()">Add Prospect</button>
        <div class="result" id="p-create-result"></div>
      </div>
      <div class="card">
        <h2>Pipeline
          <button class="btn btn-primary" onclick="loadProspects()" style="float:right;font-size:12px;padding:5px 14px;">Refresh</button>
        </h2>
        <p style="color:#8b95a1;font-size:12px;margin-bottom:14px;">Click any row to edit status, dates, and notes.</p>
        <div id="prospects-table"><div class="loading">Loading...</div></div>
      </div>
      <div class="card">
        <h2>Payments — payable Invoice / Quote numbers
          <button class="btn btn-primary" onclick="loadSalesDocs()" style="float:right;font-size:12px;padding:5px 14px;">Refresh</button>
        </h2>
        <p style="color:#8b95a1;font-size:12px;margin-bottom:14px;">
          cappvcs.com/pay only accepts numbers listed here. The Sales Docs tool registers new
          docs automatically on Generate — use this form for docs issued before Jul 2026
          (matching the number printed on the customer's PDF exactly).
        </p>
        <div class="form-row">
          <div class="form-group">
            <label>Number *</label>
            <input type="text" id="sd-number" placeholder="e.g. INV-20260601-001">
          </div>
          <div class="form-group">
            <label>School *</label>
            <input type="text" id="sd-school" placeholder="e.g. SMU">
          </div>
        </div>
        <div class="form-row">
          <div class="form-group">
            <label>Type</label>
            <select id="sd-type">
              <option value="invoice">Invoice</option>
              <option value="quote">Quote</option>
              <option value="agreement">Agreement</option>
            </select>
          </div>
          <div class="form-group">
            <label>Amount (USD) *</label>
            <input type="text" id="sd-amount" placeholder="e.g. 3500 or 3500.00">
          </div>
        </div>
        <div class="form-group" style="margin-bottom:12px;">
          <label>Description (shown to the customer)</label>
          <input type="text" id="sd-desc" placeholder="e.g. Group of 6 — Year 1">
        </div>
        <button class="btn btn-primary" onclick="addSalesDoc()">Add / Update Number</button>
        <div class="result" id="sd-result"></div>
        <div id="salesdocs-table" style="margin-top:16px;"><div class="loading">Loading...</div></div>
      </div>
    </div>

    <div class="panel" id="notices-tab">
      <div class="card">
        <h2>Broadcast to All Clients</h2>
        <p class="small" style="margin-bottom:14px;">
          Shown once inside CAPP the next time each user launches it. Only the newest
          active notice is delivered, so publishing a new one supersedes the last.
        </p>
        <div style="display:grid;grid-template-columns:1fr 180px;gap:12px;">
          <input id="ntc-title" placeholder="Title, e.g. CAPP 2.6.4 is available" maxlength="120" />
          <select id="ntc-severity">
            <option value="info">Info</option>
            <option value="warning">Warning</option>
            <option value="critical">Critical</option>
          </select>
        </div>
        <textarea id="ntc-body" rows="4" placeholder="What you want every user to read." maxlength="1200"
                  style="width:100%;margin-top:12px;"></textarea>
        <div style="display:flex;align-items:center;gap:12px;margin-top:12px;flex-wrap:wrap;">
          <input id="ntc-minver" placeholder="Mandatory below version (optional, e.g. 2.6.5)"
                 style="flex:1;min-width:260px;" />
          <button class="btn btn-primary" onclick="publishNotice()">Publish in app</button>
        </div>
        <p class="small" style="margin-top:8px;">
          Leave the version blank for an informational message. Fill it in and clients older
          than that version are told the update is required.
        </p>
        <div id="ntc-msg" class="small" style="margin-top:10px;"></div>

        <div style="margin-top:22px;padding-top:18px;border-top:1px solid var(--border,#2c3b55);">
          <h3 style="margin:0 0 4px;font-size:1rem;">Email the same message</h3>
          <p class="small" style="margin-bottom:12px;">
            Reaches schools whether or not CAPP is open, and works with every installed
            version. Uses the title and message above.
          </p>
          <div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap;">
            <select id="bc-audience" onchange="previewBlast()" style="min-width:210px;">
              <option value="licensed_trial">Licensed + trial schools</option>
              <option value="licensed">Licensed schools only</option>
              <option value="all">Every active account</option>
            </select>
            <button class="btn" onclick="previewBlast()">Who gets this?</button>
            <button class="btn" onclick="testBlast()">Send test to me</button>
            <button class="btn btn-warning" onclick="sendBlast()">Send email now</button>
          </div>
          <div id="bc-preview" class="small" style="margin-top:12px;"></div>
          <div id="bc-msg" class="small" style="margin-top:8px;"></div>
        </div>
      </div>
      <div class="card" style="margin-top:18px;">
        <h2>Sent
          <button class="btn" onclick="loadNotices()" style="float:right;font-size:12px;padding:5px 14px;">Refresh</button>
        </h2>
        <div id="ntc-list"><div class="loading">Loading...</div></div>
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

    <div class="panel" id="playbook-tab">
      <div class="card">
        <h2>Add Players (roster upload)</h2>
        <p class="small" style="margin-bottom:14px;">
          Upload a CSV with columns <strong>First Name, Last Name, Position, Email</strong>
          (a header row is fine). Players sign in with their <strong>email</strong> and set
          their own password on first login. Re-uploading updates names/positions and never
          changes anyone's existing password.
        </p>
        <div class="form-group" style="margin-bottom:12px;">
          <label>CSV file</label>
          <input type="file" id="pb-file" accept=".csv,text/csv">
        </div>
        <p class="small" style="margin:8px 0;">…or paste rows (First, Last, Position, Email — one per line):</p>
        <div class="form-group" style="margin-bottom:12px;">
          <textarea id="pb-paste" placeholder="John,Smith,QB,john.smith@school.edu&#10;Mary,Jones,WR,mary.jones@school.edu"></textarea>
        </div>
        <button class="btn btn-primary" onclick="uploadPlaybook()">Add Players</button>
        <div class="result" id="pb-upload-result"></div>
      </div>
      <div class="card">
        <h2>Players
          <button class="btn btn-primary" onclick="loadPlaybookUsers()" style="float:right;font-size:12px;padding:5px 14px;">Refresh</button>
          <button class="btn btn-primary" onclick="exportPlaybookUsers()" style="float:right;font-size:12px;padding:5px 14px;margin-right:8px;">Export Excel</button>
        </h2>
        <p style="color:#8b95a1;font-size:12px;margin-bottom:14px;">
          "Set up" = the player has created their password. Delete removes their access.
        </p>
        <div id="playbook-table"><div class="loading">Loading...</div></div>
      </div>
    </div>

    <div class="panel" id="pbcontent-tab">
      <div class="card">
        <h2>Upload playbook PDFs</h2>
        <p class="small" style="margin-bottom:14px;">
          Files upload straight to Cloudflare R2. The <strong>folder</strong> you type becomes
          the section path in the portal — use <code>/</code> to nest (e.g.
          <code>Calls/Pressures</code>). Leave blank for the top level. The portal tree is built
          from these folders; the PDF's title is its file name.
        </p>
        <div class="form-row">
          <div class="form-group">
            <label>Folder path</label>
            <input type="text" id="pbc-folder" placeholder="e.g. Calls/Pressures (blank = top level)">
          </div>
          <div class="form-group">
            <label>PDF file(s)</label>
            <input type="file" id="pbc-files" accept=".pdf,application/pdf" multiple>
          </div>
        </div>
        <button class="btn btn-primary" onclick="uploadPbDocs()">Upload</button>
        <div class="result" id="pbc-upload-result"></div>
      </div>
      <div class="card">
        <h2>Upload a whole folder</h2>
        <p class="small" style="margin-bottom:14px;">
          Pick a <strong>root folder</strong> and its entire tree of subfolders is recreated in
          the portal. <strong>PDFs</strong> go live immediately. <strong>Word, Excel, Visio and PowerPoint files</strong>
          (<code>.doc/.docx/.docm/.vsd/.vsdx/.vsdm/.ppt/.pptx/.xls/.xlsx/.xlsm/.xlsb</code>) are queued and converted to PDF by the
          conversion worker, then appear automatically. Each file keeps its folder path.
        </p>
        <div class="form-row">
          <div class="form-group">
            <label>Folder</label>
            <input type="file" id="pbc-dir" webkitdirectory directory multiple>
          </div>
        </div>
        <button class="btn btn-primary" onclick="uploadPbFolder()">Upload folder</button>
        <div class="result" id="pbc-folder-result"></div>
      </div>
      <div class="card">
        <h2>Conversion jobs
          <button class="btn btn-primary" onclick="loadPbJobs()" style="float:right;font-size:12px;padding:5px 14px;">Refresh</button>
        </h2>
        <p class="small" style="margin-bottom:10px;">Visio files waiting on / being processed by the
          conversion worker. Done jobs become entries in Playbook contents below.</p>
        <div id="pbjobs-table"><div class="loading">Loading...</div></div>
      </div>
      <div class="card">
        <h2>Empty folders</h2>
        <p class="small" style="margin-bottom:14px;">
          Folders normally appear in the portal only once a PDF is uploaded into them. A folder
          registered here shows up in the players' tree <strong>even while empty</strong> — e.g.
          pre-made game-plan folders. Once files are uploaded into it, the entry here is redundant
          (but harmless). Use <code>/</code> to nest.
        </p>
        <div class="form-row">
          <div class="form-group">
            <label>Folder path</label>
            <input type="text" id="pbf-path" placeholder="e.g. 2026 GAME PLAN/01 DUQUESNE (SEP 5)">
          </div>
        </div>
        <button class="btn btn-primary" onclick="createPbFolder()">Create folder</button>
        <div class="result" id="pbf-result"></div>
        <div id="pbfolders-table" style="margin-top:14px;"><div class="loading">Loading...</div></div>
      </div>
      <div class="card">
        <h2>Playbook contents
          <button class="btn btn-primary" onclick="loadPbDocs()" style="float:right;font-size:12px;padding:5px 14px;">Refresh</button>
        </h2>
        <p class="small" style="margin-bottom:10px;"><strong>Replace</strong> swaps in a new PDF for
          that entry — same spot in the portal, and players' Touch Notes on it are kept.</p>
        <input type="file" id="pbc-replace" accept=".pdf,application/pdf" style="display:none" onchange="_pbReplacePicked()">
        <div class="result" id="pbc-replace-result"></div>
        <div id="pbcontent-table"><div class="loading">Loading...</div></div>
      </div>
      <div class="card">
        <h2>Access log
          <button class="btn btn-primary" onclick="loadPbAccessLog()" style="float:right;font-size:12px;padding:5px 14px;">Refresh</button>
        </h2>
        <p style="color:#8b95a1;font-size:12px;margin-bottom:14px;">
          Every time a player, coach, or team admin opens a PDF, it's logged here — who, which
          file, and when. There is no button anywhere in this admin panel that opens a team's
          PDF content directly; this log is the accountability record for that.
        </p>
        <div id="pbaccesslog-table"><div class="loading">Loading...</div></div>
      </div>
    </div>

    <!-- Binder Teams — the top of the multi-tenancy chain: create a team, seed
         its first Team Admin, then hand off. Roster + content after that are
         managed by that team's own admin/coaches, not from here. -->
    <div class="panel" id="teams-tab">
      <div class="card">
        <h2>Create a team</h2>
        <p class="small" style="margin-bottom:14px;">
          <strong>Slug</strong> is permanent (used as the storage prefix — pick something short
          and stable, e.g. <code>navy</code>). <strong>Name</strong> is what shows on their login
          splash. This creates the team AND its <strong>first Team Admin</strong> together in one
          step — that person signs in with the email below, sets a password, then manages their
          own roster and can promote more admins. Coaches never get roster access; that's the
          Team Admin's job only.
        </p>
        <div class="form-row">
          <div class="form-group">
            <label>Team slug</label>
            <input type="text" id="team-slug" placeholder="e.g. navy">
          </div>
          <div class="form-group">
            <label>Team name</label>
            <input type="text" id="team-name" placeholder="e.g. Navy Midshipmen">
          </div>
        </div>
        <div class="form-group" style="margin-top:10px;">
          <label>Logo (optional — auto-fills from CAPP's existing logo library)</label>
          <select id="team-logo-school"><option value="">— No logo yet (team can upload their own later) —</option></select>
        </div>
        <div class="form-row" style="margin-top:10px;">
          <div class="form-group">
            <label>First Team Admin's email</label>
            <input type="text" id="team-admin-email" placeholder="coach@school.edu">
          </div>
          <div class="form-group">
            <label>Their name (optional)</label>
            <input type="text" id="team-admin-name" placeholder="First Last">
          </div>
        </div>
        <button class="btn btn-primary" onclick="createBinderTeam()">Create team + admin</button>
        <div class="result" id="team-create-result"></div>
      </div>
      <div class="card">
        <h2>Teams
          <button class="btn btn-primary" onclick="loadBinderTeams()" style="float:right;font-size:12px;padding:5px 14px;">Refresh</button>
        </h2>
        <p style="color:#8b95a1;font-size:12px;margin-bottom:14px;">
          Toggling a team inactive signs its players/coaches out immediately and blocks new
          logins — data stays intact, nothing is deleted. "+ Add admin" here is only for adding
          a SECOND/backup admin to a team that already exists — double-check you're on the right
          row before using it (a new team's first admin is created together with the team above).
        </p>
        <div id="teams-table"><div class="loading">Loading...</div></div>
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
      <div class="so-field">
        <label>Email (for password resets)</label>
        <div class="form-group"><input type="text" id="so-email" placeholder="No email on file"></div>
      </div>
      <button class="btn btn-warning btn-sm" onclick="sendResetEmail()">Send Password Reset Email</button>
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
      <div class="so-field" id="so-seat3-row" style="display:none;">
        <label>Seat 3</label>
        <div class="seat-row">
          <div class="so-val muted" id="so-seat3">—</div>
          <button class="btn btn-warning btn-sm" onclick="resetSeat(3)">Reset</button>
        </div>
      </div>
      <div class="so-field">
        <label>Seats allowed</label>
        <div class="seat-row">
          <select id="so-seat-limit" class="btn btn-sm" style="min-width:70px;">
            <option value="1">1</option>
            <option value="2">2</option>
            <option value="3">3</option>
          </select>
          <button class="btn btn-primary btn-sm" onclick="setSeatLimit()">Save</button>
        </div>
        <div class="muted" style="font-size:11px;margin-top:4px;">
          Machines stay bound to their computer. This only changes how many
          computers the school may activate on. Lowering it never disconnects
          a machine that is already bound.
        </div>
      </div>
    </div>

    <div class="so-section">
      <h3>Trial</h3>
      <div class="so-field"><label>Status</label><div class="so-val" id="so-trial-status">—</div></div>
      <div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:10px;" id="so-trial-btns">
        <button class="btn btn-primary btn-sm" onclick="extendTrial(7)">+7 Days</button>
        <button class="btn btn-primary btn-sm" onclick="extendTrial(14)">+14 Days</button>
        <button class="btn btn-primary btn-sm" onclick="extendTrial(30)">+30 Days</button>
        <button class="btn btn-warning btn-sm" onclick="resetTrial()">Reset Trial</button>
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
    <button class="btn btn-primary btn-sm" id="so-license-btn" onclick="toggleLicensedFromSlideout()">—</button>
    <button class="btn btn-danger btn-sm" onclick="deleteFromSlideout()">Delete Account</button>
  </div>
</div>

<!-- CRM prospect slide-out -->
<div id="crm-slideout">
  <div class="so-header">
    <h2 id="crm-so-title">Prospect</h2>
    <button class="so-close" onclick="closeSlideout()">&#x2715;</button>
  </div>
  <div class="so-body">
    <div class="so-section">
      <h3>Details</h3>
      <div class="form-group" style="margin-bottom:10px;"><label>School</label><input type="text" id="crm-school"></div>
      <div class="form-group" style="margin-bottom:10px;"><label>Contact Name</label><input type="text" id="crm-contact"></div>
      <div class="form-group" style="margin-bottom:10px;"><label>Email</label><input type="text" id="crm-email"></div>
      <div class="form-group" style="margin-bottom:10px;"><label>Phone</label><input type="text" id="crm-phone"></div>
    </div>
    <div class="so-section">
      <h3>Pipeline</h3>
      <div class="form-group" style="margin-bottom:10px;">
        <label>Status</label>
        <select id="crm-status">
          <option>Demo Done</option>
          <option>Quote/Agreement Sent</option>
          <option>Trial</option>
          <option>Paid</option>
          <option>Lost</option>
        </select>
      </div>
      <div class="form-group" style="margin-bottom:10px;"><label>Quote / Agreement Sent Date</label><input type="date" id="crm-quote-date"></div>
    </div>
    <div class="so-section">
      <h3>Notes</h3>
      <div class="form-group"><textarea id="crm-notes" placeholder="Call notes, next steps..."></textarea></div>
      <button class="btn btn-primary btn-sm" onclick="saveProspect()" style="margin-top:8px;">Save</button>
      <div class="so-save-msg" id="crm-save-msg">Saved.</div>
    </div>
    <div class="so-section small" id="crm-meta"></div>
  </div>
  <div class="so-footer">
    <button class="btn btn-danger btn-sm" onclick="deleteProspect()">Delete Prospect</button>
  </div>
</div>

<script>
let _token = "";
let _currentUser = null;
let _allClients = [];
let _gameDayTimer = null;
let _gameDayGames = [];
let _prospects = [];
let _currentProspect = null;

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
  if (id === "crm-tab") { loadProspects(); loadSalesDocs(); }
  if (id === "gameday-tab") { loadGameDayStatus(); startGameDayRefresh(); }
  if (id === "playbook-tab") loadPlaybookUsers();
  if (id === "pbcontent-tab") { loadPbDocs(); loadPbJobs(); loadPbFolders(); loadPbAccessLog(); }
  if (id === "teams-tab") { loadBinderTeams(); loadTeamLogoCatalog(); }
  if (id === "notices-tab") loadNotices();
  closeSlideout();
}

function loadNotices() {
  const box = document.getElementById("ntc-list");
  box.innerHTML = '<div class="loading">Loading...</div>';
  api("GET", "/notices").then(rows => {
    if (!rows || !rows.length) { box.innerHTML = '<p class="small">Nothing sent yet.</p>'; return; }
    box.innerHTML = "<table><tr><th>Sent</th><th>Title</th><th>Severity</th><th>Requires</th><th>Status</th><th></th></tr>" +
      rows.map(n => {
        const when = String(n.created_at || "").slice(0, 16).replace("T", " ");
        const live = n.active
          ? '<span class="badge badge-green">Live</span>'
          : '<span class="badge badge-gray">Retracted</span>';
        const act = n.active
          ? '<button class="rowbtn danger" data-id="' + n.id + '" onclick="retractNotice(this.dataset.id)">Retract</button>'
          : "";
        return "<tr><td>" + when + "</td><td>" + escN(n.title) + "</td><td>" +
               escN(n.severity) + "</td><td>" + (n.min_version ? escN(n.min_version) : "-") +
               "</td><td>" + live + "</td><td>" + act + "</td></tr>";
      }).join("") + "</table>";
  });
}

function escN(s) { return String(s == null ? "" : s).replace(/</g, "&lt;"); }

// Email blasts. Deliberately three separate actions: see who gets it, send
// yourself a copy, then send for real. Mail cannot be recalled, so the real
// send should never be the first time the message has been looked at.
let _bcPreviewCount = null;

function previewBlast() {
  const aud = document.getElementById("bc-audience").value;
  const box = document.getElementById("bc-preview");
  _bcPreviewCount = null;
  box.innerHTML = "Checking...";
  api("GET", "/broadcast/preview?audience=" + encodeURIComponent(aud))
    .then(d => {
      _bcPreviewCount = d.count;
      if (!d.count) { box.innerHTML = '<b>No one matches that audience.</b>'; return; }
      const names = d.recipients.map(r =>
        pbEsc(r.school) + ' &lt;' + pbEsc(r.email) + '&gt;' + (r.licensed ? '' : ' <i>(trial)</i>')
      ).join("<br/>");
      box.innerHTML = '<b>' + d.count + ' recipient' + (d.count === 1 ? '' : 's') +
                      ':</b><br/>' + names;
    })
    .catch(e => { box.innerHTML = "Could not load recipients: " + e; });
}

function testBlast() {
  const title = document.getElementById("ntc-title").value.trim();
  const body  = document.getElementById("ntc-body").value.trim();
  const msg   = document.getElementById("bc-msg");
  if (!title || !body) { msg.textContent = "Fill in the title and message first."; return; }
  const to = prompt("Send a test copy to which address?", "roger@cappvcs.com");
  if (!to) return;
  msg.textContent = "Sending test...";
  api("POST", "/broadcast/test", { to: to, subject: title, body: body })
    .then(d => { msg.innerHTML = "<b>Test sent to " + pbEsc(d.sent_to) + ".</b> Check it before sending for real."; })
    .catch(e => { msg.textContent = "Test failed: " + e; });
}

function sendBlast() {
  const title = document.getElementById("ntc-title").value.trim();
  const body  = document.getElementById("ntc-body").value.trim();
  const aud   = document.getElementById("bc-audience").value;
  const msg   = document.getElementById("bc-msg");
  if (!title || !body) { msg.textContent = "Fill in the title and message first."; return; }
  if (_bcPreviewCount === null) {
    msg.textContent = 'Press "Who gets this?" first, so you can see the list before it goes.';
    return;
  }
  // Single quotes and no escapes: this JS lives inside a Python string, so an
  // escaped double quote silently loses a level and breaks the whole script.
  if (!confirm('Email: ' + title + ' -- send to ' + _bcPreviewCount +
               ' school(s)? This cannot be undone.')) return;
  msg.textContent = "Sending to " + _bcPreviewCount + " school(s)...";
  // confirm_count makes the server refuse if the recipient list changed since
  // the preview - so what goes out is always what was reviewed.
  api("POST", "/broadcast/send", {
      subject: title, body: body, audience: aud,
      send_email: true, show_in_app: false,
      confirm_count: _bcPreviewCount })
    .then(d => {
      let out = "<b>Sent to " + d.sent + " school(s).</b>";
      if (d.failed) {
        const bad = (d.recipients || []).filter(r => !r.ok)
          .map(r => pbEsc(r.username)).join(", ");
        out += ' <span style="color:#f08a7e;">' + d.failed + " failed: " + bad + "</span>";
      }
      msg.innerHTML = out;
      loadNotices();
    })
    .catch(e => { msg.textContent = "Send failed: " + e; });
}

function publishNotice() {
  const title = document.getElementById("ntc-title").value.trim();
  const body  = document.getElementById("ntc-body").value.trim();
  const sev   = document.getElementById("ntc-severity").value;
  const minv  = document.getElementById("ntc-minver").value.trim();
  const msg   = document.getElementById("ntc-msg");
  if (!title || !body) { msg.textContent = "Title and message are both required."; return; }
  const warn = minv
    ? "\\n\\nClients older than " + minv + " will be told the update is REQUIRED."
    : "";
  if (!confirm("Send this to every CAPP user?" + warn)) return;
  msg.textContent = "Publishing...";
  api("POST", "/notices", { title: title, body: body, severity: sev, min_version: minv })
    .then(r => {
      if (r && r.detail) { msg.textContent = r.detail; return; }
      msg.textContent = "Published. Users see it the next time they open CAPP.";
      document.getElementById("ntc-title").value = "";
      document.getElementById("ntc-body").value = "";
      document.getElementById("ntc-minver").value = "";
      loadNotices();
    });
}

function retractNotice(id) {
  if (!confirm("Retract this notice? Clients stop showing it on their next launch.")) return;
  api("PATCH", "/notices/" + id + "/retract").then(loadNotices);
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
    const TRIAL_DAYS = 7;
    const rows = data.map(c => {
      const active = c.active ? '<span class="badge badge-green">Active</span>' : '<span class="badge badge-red">Inactive</span>';
      const admin  = c.is_admin ? ' <span class="badge badge-blue">Admin</span>' : '';
      const s1     = c.seat_1_machine ? '<span class="badge badge-gray">Bound</span>' : '<span class="badge badge-green">Open</span>';
      const s2     = c.seat_2_machine ? '<span class="badge badge-gray">Bound</span>' : '<span class="badge badge-green">Open</span>';
      const s3     = (c.seat_limit || 2) >= 3
        ? (c.seat_3_machine ? ' <span class="badge badge-gray">Bound</span>' : ' <span class="badge badge-green">Open</span>')
        : '';
      const inv    = c.next_invoice_date ? c.next_invoice_date : '<span style="color:#8b95a1">—</span>';
      let licBadge;
      if (c.licensed) {
        licBadge = '<span class="badge badge-green">Licensed</span>';
      } else {
        const created = c.created_at ? new Date(c.created_at) : null;
        const daysElapsed = created ? Math.floor((Date.now() - created) / 86400000) : 0;
        const ext = c.trial_extension_days || 0;
        const remaining = TRIAL_DAYS + ext - daysElapsed;
        if (remaining <= 0) {
          licBadge = '<span class="badge badge-red2">Trial Expired</span>';
        } else {
          licBadge = `<span class="badge badge-yellow">Trial (${remaining}d left)</span>`;
        }
      }
      return `<tr class="clickable" onclick="openSlideout('${c.username}')">
        <td>${c.username}</td>
        <td>${c.client_id}</td>
        <td>${active}${admin}</td>
        <td>${licBadge}</td>
        <td>${s1}</td><td>${s2}</td>
        <td>${inv}</td>
      </tr>`;
    }).join("");
    document.getElementById("clients-table").innerHTML = `
      <table>
        <thead><tr><th>Username</th><th>Client ID</th><th>Status</th><th>License</th><th>Seat 1</th><th>Seat 2</th><th>Next Invoice</th></tr></thead>
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
  const _lim = c.seat_limit || 2;
  document.getElementById("so-seat-limit").value = String(_lim);
  // Seat 3 is only shown when the school actually has one, so the panel does
  // not imply a seat that cannot be used.
  document.getElementById("so-seat3-row").style.display = _lim >= 3 ? "" : "none";
  document.getElementById("so-seat3").textContent = c.seat_3_machine
    ? c.seat_3_machine.substring(0, 24) + "…" : "Open";
  document.getElementById("so-seat2").textContent = c.seat_2_machine
    ? c.seat_2_machine.substring(0, 24) + "…" : "Open";

  document.getElementById("so-email").value = c.email || "";
  document.getElementById("so-invoice-date").value = c.next_invoice_date || "";
  document.getElementById("so-notes").value = c.notes || "";
  document.getElementById("so-save-msg").style.display = "none";

  // Trial status
  const trialEl = document.getElementById("so-trial-status");
  const trialBtns = document.getElementById("so-trial-btns");
  if (c.licensed) {
    trialEl.textContent = "Licensed — full access";
    trialEl.style.color = "#86efac";
    trialBtns.style.display = "none";
  } else {
    trialBtns.style.display = "flex";
    const created = c.created_at ? new Date(c.created_at) : null;
    const elapsed = created ? Math.floor((Date.now() - created) / 86400000) : 0;
    const ext = c.trial_extension_days || 0;
    const remaining = Math.max(0, 7 + ext - elapsed);
    if (remaining > 0) {
      trialEl.textContent = remaining + "d remaining" + (ext > 0 ? "  (+" + ext + "d added)" : "");
      trialEl.style.color = "#fde68a";
    } else {
      trialEl.textContent = "Expired  (" + elapsed + "d elapsed, +" + ext + "d extension)";
      trialEl.style.color = "#fca5a5";
    }
  }

  const toggleBtn = document.getElementById("so-toggle-btn");
  if (c.active) {
    toggleBtn.textContent = "Deactivate";
    toggleBtn.className = "btn btn-danger btn-sm";
  } else {
    toggleBtn.textContent = "Reactivate";
    toggleBtn.className = "btn btn-success btn-sm";
  }

  const licBtn = document.getElementById("so-license-btn");
  if (c.licensed) {
    licBtn.textContent = "Revoke License";
    licBtn.className = "btn btn-warning btn-sm";
  } else {
    licBtn.textContent = "Grant License";
    licBtn.className = "btn btn-primary btn-sm";
  }

  document.getElementById("slideout").classList.add("open");
  document.getElementById("overlay").classList.add("on");
}

function closeSlideout() {
  document.getElementById("slideout").classList.remove("open");
  document.getElementById("crm-slideout").classList.remove("open");
  document.getElementById("overlay").classList.remove("on");
  _currentUser = null;
  _currentProspect = null;
}

// ── CRM / prospects ───────────────────────────────────────────────────────────

function prospectStatusBadge(status) {
  const map = {
    "Demo Done":           "badge-blue",
    "Quote/Agreement Sent": "badge-yellow",
    "Trial":               "badge-gray",
    "Paid":                "badge-green",
    "Lost":                "badge-red2",
  };
  const cls = map[status] || "badge-gray";
  return '<span class="badge ' + cls + '">' + (status || "—") + '</span>';
}

function crmEsc(s) {
  return (s == null ? "" : String(s)).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

function loadProspects() {
  document.getElementById("prospects-table").innerHTML = '<div class="loading">Loading...</div>';
  api("GET", "/prospects").then(data => {
    if (!Array.isArray(data)) {
      document.getElementById("prospects-table").innerHTML = '<div class="loading">Error loading prospects.</div>';
      return;
    }
    _prospects = data;
    if (data.length === 0) {
      document.getElementById("prospects-table").innerHTML = '<div class="loading">No prospects yet. Add one above.</div>';
      return;
    }
    const rows = data.map(p => {
      const updated = p.updated_at ? new Date(p.updated_at).toLocaleDateString() : "—";
      const quote   = p.quote_sent_date ? p.quote_sent_date : '<span style="color:#8b95a1">—</span>';
      return `<tr class="clickable" onclick="openProspect('${p.id}')">
        <td>${crmEsc(p.school)}</td>
        <td>${crmEsc(p.contact) || '<span style="color:#8b95a1">—</span>'}</td>
        <td>${prospectStatusBadge(p.status)}</td>
        <td>${quote}</td>
        <td>${updated}</td>
      </tr>`;
    }).join("");
    document.getElementById("prospects-table").innerHTML = `
      <table>
        <thead><tr><th>School</th><th>Contact</th><th>Status</th><th>Quote Sent</th><th>Updated</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>`;
  });
}

function loadSalesDocs() {
  document.getElementById("salesdocs-table").innerHTML = '<div class="loading">Loading...</div>';
  api("GET", "/salesdocs").then(data => {
    if (!Array.isArray(data)) {
      document.getElementById("salesdocs-table").innerHTML =
        '<div class="loading">Error loading payment numbers (has sales_docs_table.sql been run?).</div>';
      return;
    }
    if (data.length === 0) {
      document.getElementById("salesdocs-table").innerHTML = '<div class="loading">No payable numbers yet.</div>';
      return;
    }
    const rows = data.map(d => {
      const amt  = "$" + (d.amount_cents / 100).toLocaleString("en-US", {minimumFractionDigits: 2});
      const st   = d.status === "paid"
        ? '<span class="badge badge-green">Paid</span>'
        : (d.status === "void" ? '<span class="badge badge-red">Void</span>'
                               : '<span class="badge badge-blue">Unpaid</span>');
      const when = d.paid_at ? new Date(d.paid_at).toLocaleDateString()
                 : (d.created_at ? new Date(d.created_at).toLocaleDateString() : "—");
      const del  = d.status === "paid" ? ""
        : `<button class="btn btn-danger btn-sm" onclick="deleteSalesDoc('${crmEsc(d.number)}')">Remove</button>`;
      return `<tr>
        <td>${crmEsc(d.number)}</td>
        <td>${crmEsc(d.school)}</td>
        <td>${crmEsc(d.doc_type)}</td>
        <td>${amt}</td>
        <td>${st}</td>
        <td>${when}</td>
        <td>${del}</td>
      </tr>`;
    }).join("");
    document.getElementById("salesdocs-table").innerHTML = `
      <table>
        <thead><tr><th>Number</th><th>School</th><th>Type</th><th>Amount</th><th>Status</th><th>Date</th><th></th></tr></thead>
        <tbody>${rows}</tbody>
      </table>`;
  });
}

function addSalesDoc() {
  const res = document.getElementById("sd-result");
  const number = document.getElementById("sd-number").value.trim().toUpperCase();
  const school = document.getElementById("sd-school").value.trim();
  const amount = parseFloat(document.getElementById("sd-amount").value.replace(/[$,]/g, ""));
  if (!number || !school || !(amount > 0)) {
    res.className = "result err"; res.style.display = "block";
    res.textContent = "Number, school, and a positive amount are required.";
    return;
  }
  const body = {
    number, school,
    doc_type: document.getElementById("sd-type").value,
    description: document.getElementById("sd-desc").value.trim(),
    amount_cents: Math.round(amount * 100),
  };
  api("POST", "/salesdocs", body).then(r => {
    if (r && r.detail) {
      res.className = "result err"; res.style.display = "block";
      res.textContent = "Error: " + r.detail;
      return;
    }
    res.className = "result"; res.style.display = "block";
    res.textContent = (r.updated ? "Updated " : "Added ") + number + " — payable at cappvcs.com/pay.";
    ["sd-number","sd-school","sd-amount","sd-desc"].forEach(id => document.getElementById(id).value = "");
    loadSalesDocs();
  });
}

function deleteSalesDoc(number) {
  if (!confirm("Remove " + number + "? The customer will no longer be able to pay it online.")) return;
  api("DELETE", "/salesdocs/" + encodeURIComponent(number)).then(r => {
    if (r && r.detail) { alert("Error: " + r.detail); return; }
    loadSalesDocs();
  });
}

function createProspect() {
  const school = document.getElementById("p-school").value.trim();
  const res = document.getElementById("p-create-result");
  if (!school) {
    res.className = "result err"; res.style.display = "block";
    res.textContent = "School is required.";
    return;
  }
  const body = {
    school,
    contact: document.getElementById("p-contact").value.trim(),
    email:   document.getElementById("p-email").value.trim(),
    phone:   document.getElementById("p-phone").value.trim(),
    status:  document.getElementById("p-status").value,
    quote_sent_date: document.getElementById("p-quote-date").value || null,
    notes:   document.getElementById("p-notes").value.trim(),
  };
  api("POST", "/prospects", body).then(r => {
    if (r && r.detail) {
      res.className = "result err"; res.style.display = "block";
      res.textContent = "Error: " + r.detail;
      return;
    }
    res.className = "result"; res.style.display = "block";
    res.textContent = "Added " + school + ".";
    ["p-school","p-contact","p-email","p-phone","p-quote-date","p-notes"].forEach(id => document.getElementById(id).value = "");
    document.getElementById("p-status").value = "Demo Done";
    loadProspects();
  });
}

function openProspect(id) {
  const p = _prospects.find(x => String(x.id) === String(id));
  if (!p) return;
  _currentProspect = p;
  document.getElementById("crm-so-title").textContent = p.school || "Prospect";
  document.getElementById("crm-school").value     = p.school || "";
  document.getElementById("crm-contact").value    = p.contact || "";
  document.getElementById("crm-email").value      = p.email || "";
  document.getElementById("crm-phone").value      = p.phone || "";
  document.getElementById("crm-status").value     = p.status || "Demo Done";
  document.getElementById("crm-quote-date").value = p.quote_sent_date || "";
  document.getElementById("crm-notes").value      = p.notes || "";
  document.getElementById("crm-save-msg").style.display = "none";
  const created = p.created_at ? new Date(p.created_at).toLocaleDateString() : "—";
  document.getElementById("crm-meta").textContent = "Added " + created;
  document.getElementById("crm-slideout").classList.add("open");
  document.getElementById("overlay").classList.add("on");
}

function saveProspect() {
  if (!_currentProspect) return;
  const body = {
    school:  document.getElementById("crm-school").value.trim(),
    contact: document.getElementById("crm-contact").value.trim(),
    email:   document.getElementById("crm-email").value.trim(),
    phone:   document.getElementById("crm-phone").value.trim(),
    status:  document.getElementById("crm-status").value,
    quote_sent_date: document.getElementById("crm-quote-date").value || null,
    notes:   document.getElementById("crm-notes").value.trim(),
  };
  api("PATCH", "/prospects/" + _currentProspect.id, body).then(r => {
    const msg = document.getElementById("crm-save-msg");
    if (r && r.detail) {
      msg.style.color = "#fca5a5"; msg.textContent = "Error: " + r.detail; msg.style.display = "block";
      return;
    }
    msg.style.color = "#86efac"; msg.textContent = "Saved."; msg.style.display = "block";
    loadProspects();
  });
}

function deleteProspect() {
  if (!_currentProspect) return;
  if (!confirm("Delete " + (_currentProspect.school || "this prospect") + " permanently?")) return;
  api("DELETE", "/prospects/" + _currentProspect.id).then(r => {
    if (r && r.detail) { alert("Error: " + r.detail); return; }
    closeSlideout();
    loadProspects();
  });
}

function setSeatLimit() {
  if (!_currentUser) return;
  const n = parseInt(document.getElementById("so-seat-limit").value, 10);
  api("PATCH", "/clients/" + _currentUser.username + "/seat-limit", { seat_limit: n })
    .then(d => {
      if (d.ok) { loadClients(); closeSlideout(); }
      else alert("Error: " + JSON.stringify(d));
    });
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

function sendResetEmail() {
  if (!_currentUser) return;
  const email = document.getElementById("so-email").value.trim();
  if (!email) { alert("No email on file for " + _currentUser.username + ". Enter one and Save first."); return; }
  if (email.toLowerCase() !== (_currentUser.email || "")) {
    alert("The email box has unsaved changes — click Save Notes & Date first so the link goes to the right address.");
    return;
  }
  if (!confirm("Email a password-reset link to " + email + " for " + _currentUser.username + "?")) return;
  api("POST", "/clients/" + _currentUser.username + "/send-reset")
    .then(d => {
      if (d.ok) alert("Reset link sent to " + d.email + " (valid 60 minutes).");
      else alert("Error: " + (d.detail || JSON.stringify(d)));
    });
}

function saveDetails() {
  if (!_currentUser) return;
  const notes   = document.getElementById("so-notes").value;
  const invDate = document.getElementById("so-invoice-date").value || null;
  const email   = document.getElementById("so-email").value.trim();
  api("PATCH", "/clients/" + _currentUser.username, { notes, next_invoice_date: invDate, email })
    .then(d => {
      if (d.ok) {
        _currentUser.notes = notes;
        _currentUser.next_invoice_date = invDate;
        _currentUser.email = email.toLowerCase() || null;
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

function toggleLicensedFromSlideout() {
  if (!_currentUser) return;
  const newState = !_currentUser.licensed;
  const msg = newState
    ? "Grant full license to " + _currentUser.username + "? This removes all trial restrictions."
    : "Revoke license from " + _currentUser.username + "? They will revert to trial mode.";
  if (!confirm(msg)) return;
  api("PATCH", "/clients/" + _currentUser.username + "/license", { licensed: newState })
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

function extendTrial(days) {
  if (!_currentUser) return;
  if (!confirm("Extend trial for " + _currentUser.username + " by " + days + " day(s)?")) return;
  api("PATCH", "/clients/" + _currentUser.username + "/extend-trial", { days })
    .then(d => {
      if (d.ok) { loadClients(); closeSlideout(); }
      else alert("Error: " + JSON.stringify(d));
    });
}

function resetTrial() {
  if (!_currentUser) return;
  if (!confirm("Reset trial for " + _currentUser.username + "?\\nThis gives them a fresh 7-day trial starting now.")) return;
  api("PATCH", "/clients/" + _currentUser.username + "/reset-trial", {})
    .then(d => {
      if (d.ok) { loadClients(); closeSlideout(); }
      else alert("Error: " + JSON.stringify(d));
    });
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
// ── Playbook Portal — player roster ───────────────────────────────────────────
function pbEsc(s){ return String(s==null?"":s).replace(/[&<>"']/g,function(c){return {"&":"&amp;","<":"&lt;",">":"&gt;","\\"":"&quot;","'":"&#39;"}[c];}); }

function parseRoster(text){
  // Columns: First, Last, Position, Email (header row optional). Robust to email
  // being in any column — we pick the field containing "@".
  var rows=[];
  (text||"").split(/\\r?\\n/).forEach(function(line){
    line=line.trim(); if(!line) return;
    var low=line.toLowerCase();
    if(low.indexOf("email")>=0 && (low.indexOf("first")>=0||low.indexOf("last")>=0||low.indexOf("name")>=0||low.indexOf("position")>=0)) return; // header
    var parts=line.split(",").map(function(s){return s.trim();});
    var ei=parts.findIndex(function(p){return p.indexOf("@")>=0;});
    if(ei<0) return;
    var email=parts[ei];
    var rest=parts.filter(function(_,i){return i!==ei;});
    rows.push({first_name:rest[0]||"",last_name:rest[1]||"",position:rest[2]||"",email:email});
  });
  return rows;
}

function uploadPlaybook(){
  var res=document.getElementById("pb-upload-result");
  var fileEl=document.getElementById("pb-file");
  function doUpload(text){
    var rows=parseRoster(text);
    if(!rows.length){ res.className="result err"; res.textContent="No valid rows found (need a line with an email)."; return; }
    res.className="result"; res.textContent="Uploading "+rows.length+" player(s)...";
    api("POST","/playbook/upload",{rows:rows}).then(function(d){
      if(d && d.processed!==undefined){
        res.className="result ok";
        var msg="Added/updated "+d.processed+" player(s).";
        if(d.skipped && d.skipped.length) msg+=" Skipped "+d.skipped.length+" (invalid email).";
        res.textContent=msg;
        document.getElementById("pb-paste").value=""; fileEl.value="";
        loadPlaybookUsers();
      } else { res.className="result err"; res.textContent="Error: "+((d&&d.detail)||JSON.stringify(d)); }
    }).catch(function(e){ res.className="result err"; res.textContent="Network error: "+e; });
  }
  if(fileEl.files && fileEl.files[0]){
    var rd=new FileReader(); rd.onload=function(){ doUpload(rd.result); }; rd.readAsText(fileEl.files[0]);
  } else {
    doUpload(document.getElementById("pb-paste").value);
  }
}

var _pbUsers=[], _pbSort={key:"name",asc:true};
function loadPlaybookUsers(){
  var box=document.getElementById("playbook-table");
  box.innerHTML='<div class="loading">Loading...</div>';
  api("GET","/playbook/users").then(function(data){
    if(!Array.isArray(data)){ box.innerHTML='<div class="loading">Error loading players.</div>'; return; }
    _pbUsers=data; renderPlaybookUsers();
  }).catch(function(){ box.innerHTML='<div class="loading">Error.</div>'; });
}

function sortPlaybookUsers(key){
  if(_pbSort.key===key){ _pbSort.asc=!_pbSort.asc; } else { _pbSort={key:key,asc:true}; }
  renderPlaybookUsers();
}

function renderPlaybookUsers(){
  var box=document.getElementById("playbook-table");
  var data=_pbUsers;
  if(!data.length){ box.innerHTML='<div class="loading">No players yet. Upload a roster above.</div>'; return; }
  var key=_pbSort.key, dir=_pbSort.asc?1:-1;
  function val(u){
    if(key==="name") return (((u.last_name||"")+", "+(u.first_name||"")).trim().toLowerCase());
    if(key==="position") return (u.position||"").toLowerCase();
    if(key==="email") return (u.email||"").toLowerCase();
    if(key==="status") return u.active?1:0;
    return "";
  }
  var sorted=data.slice().sort(function(a,b){
    var va=val(a), vb=val(b);
    if(va<vb) return -dir; if(va>vb) return dir; return 0;
  });
  function hdr(label,k){
    var arrow=(_pbSort.key===k)?(_pbSort.asc?" ▲":" ▼"):"";
    return '<th style="cursor:pointer;user-select:none;white-space:nowrap" '+
           'onclick="sortPlaybookUsers(\\''+k+'\\')">'+label+arrow+'</th>';
  }
  var rows=sorted.map(function(u){
    var name=((u.last_name||"")+", "+(u.first_name||"")).replace(/^, |, $/g,"").trim();
    return "<tr>"+
      "<td>"+(pbEsc(name)||"—")+"</td>"+
      "<td>"+(pbEsc(u.position)||"—")+"</td>"+
      "<td>"+pbEsc(u.email)+"</td>"+
      "<td>"+(u.active?'<span style="color:#22c55e;">Set up</span>':'<span style="color:#8b95a1;">Pending</span>')+"</td>"+
      '<td style="white-space:nowrap">'+
        '<button class="btn btn-warning btn-sm" onclick="sendPlaybookReset(\\''+u.id+'\\',\\''+pbEsc(u.email)+'\\')">Send Password Reset</button> '+
        '<button class="btn btn-danger btn-sm" onclick="deletePlaybookUser(\\''+u.id+'\\',\\''+pbEsc(u.email)+'\\')">Delete</button>'+
      '</td>'+
    "</tr>";
  }).join("");
  var setup=data.filter(function(u){return u.active;}).length;
  box.innerHTML='<table><thead><tr>'+hdr("Name","name")+hdr("Position","position")+hdr("Email","email")+hdr("Status","status")+'<th></th></tr></thead><tbody>'+rows+'</tbody></table>'+
    '<p class="small" style="margin-top:10px;">'+data.length+' player(s), '+setup+' set up.</p>';
}

function deletePlaybookUser(id,email){
  if(!confirm("Remove "+email+"? They will lose access to the playbook.")) return;
  api("DELETE","/playbook/users/"+id).then(function(){ loadPlaybookUsers(); });
}

function sendPlaybookReset(id,email){
  if(!confirm("Email a password-reset link to "+email+"?\\nTheir current password keeps working until they use it.")) return;
  api("POST","/playbook/users/"+id+"/send-reset").then(function(d){
    if(d&&d.ok) alert("Reset link sent to "+d.email+" (valid "+(d.minutes||60)+" minutes).");
    else alert("Could not send: "+((d&&d.detail)||"unknown error"));
  }).catch(function(e){ alert("Could not send: "+e); });
}

function exportPlaybookUsers(){
  if(!_pbUsers.length){ alert("No players to export. Load the roster first."); return; }
  var key=_pbSort.key, dir=_pbSort.asc?1:-1;
  function val(u){
    if(key==="name") return (((u.last_name||"")+", "+(u.first_name||"")).trim().toLowerCase());
    if(key==="position") return (u.position||"").toLowerCase();
    if(key==="email") return (u.email||"").toLowerCase();
    if(key==="status") return u.active?1:0;
    return "";
  }
  var sorted=_pbUsers.slice().sort(function(a,b){
    var va=val(a), vb=val(b);
    if(va<vb) return -dir; if(va>vb) return dir; return 0;
  });
  function cell(s){ s=String(s==null?"":s); return '"'+s.replace(/"/g,'""')+'"'; }
  var lines=[["Name","Position","Email","Status"].map(cell).join(",")];
  sorted.forEach(function(u){
    var name=((u.last_name||"")+", "+(u.first_name||"")).replace(/^, |, $/g,"").trim();
    lines.push([name,u.position||"",u.email||"",u.active?"Set up":"Pending"].map(cell).join(","));
  });
  var blob=new Blob(["\\ufeff"+lines.join("\\r\\n")],{type:"text/csv;charset=utf-8"});
  var a=document.createElement("a");
  a.href=URL.createObjectURL(blob);
  a.download="binder_players_"+new Date().toISOString().slice(0,10)+".csv";
  document.body.appendChild(a); a.click(); document.body.removeChild(a);
  setTimeout(function(){ URL.revokeObjectURL(a.href); },1000);
}

// ── Playbook Files (R2 upload + contents) ─────────────────────────────────────
function uploadPbDocs(){
  var res=document.getElementById("pbc-upload-result");
  var folder=(document.getElementById("pbc-folder").value||"").trim().replace(/^\\/+|\\/+$/g,"");
  var files=document.getElementById("pbc-files").files;
  if(!files || !files.length){ res.className="result err"; res.textContent="Choose at least one PDF."; return; }
  if(window._pbUploadBusy){ res.className="result err"; res.textContent="An upload is already running — wait for it to finish."; return; }
  window._pbUploadBusy=true;
  var list=Array.prototype.slice.call(files), done=0, failed=0;
  res.className="result"; res.textContent="Uploading 0/"+list.length+"...";
  function next(i){
    if(i>=list.length){
      window._pbUploadBusy=false;
      res.className=failed?"result err":"result ok";
      res.textContent="Uploaded "+done+"/"+list.length+(failed?(" ("+failed+" failed)"):"")+".";
      document.getElementById("pbc-files").value=""; loadPbDocs(); return;
    }
    var f=list[i], title=f.name.replace(/\\.pdf$/i,"");
    api("POST","/playbook/docs/sign-upload",{folder:folder}).then(function(s){
      if(!s || !s.put_url) throw new Error("sign failed");
      return fetch(s.put_url,{method:"PUT",headers:{"Content-Type":"application/pdf"},body:f}).then(function(r){
        if(!r.ok) throw new Error("R2 "+r.status);
        return api("POST","/playbook/docs",{folder:folder,title:title,key:s.key,size:f.size});
      });
    }).then(function(){ done++; res.textContent="Uploading "+(done+failed)+"/"+list.length+"..."; next(i+1); })
      .catch(function(e){ failed++; res.textContent="Uploading "+(done+failed)+"/"+list.length+" (err: "+e.message+")..."; next(i+1); });
  }
  next(0);
}

function loadPbDocs(){
  var box=document.getElementById("pbcontent-table");
  box.innerHTML='<div class="loading">Loading...</div>';
  api("GET","/playbook/docs").then(function(data){
    if(!Array.isArray(data)){ box.innerHTML='<div class="loading">Error loading files.</div>'; return; }
    if(!data.length){ box.innerHTML='<div class="loading">No PDFs yet. Upload some above.</div>'; return; }
    var rows=data.map(function(d){
      return "<tr>"+
        "<td>"+(pbEsc(d.folder_path)||'<span style="color:#8b95a1">(top level)</span>')+"</td>"+
        "<td>"+pbEsc(d.title)+"</td>"+
        "<td>"+(d.size_bytes?fmtBytes(d.size_bytes):"—")+"</td>"+
        '<td style="white-space:nowrap"><button class="btn btn-primary btn-sm" onclick="replacePbDoc(\\''+d.id+'\\',\\''+pbEsc(d.title)+'\\')">Replace</button> '+
        '<button class="btn btn-danger btn-sm" onclick="deletePbDoc(\\''+d.id+'\\',\\''+pbEsc(d.title)+'\\')">Delete</button></td>'+
      "</tr>";
    }).join("");
    box.innerHTML='<table><thead><tr><th>Folder</th><th>Title</th><th>Size</th><th></th></tr></thead><tbody>'+rows+'</tbody></table>'+
      '<p class="small" style="margin-top:10px;">'+data.length+' file(s).</p>';
  }).catch(function(){ box.innerHTML='<div class="loading">Error.</div>'; });
}

function deletePbDoc(id,title){
  if(!confirm('Delete "'+title+'"? This removes it from the playbook and R2.')) return;
  api("DELETE","/playbook/docs/"+id).then(function(){ loadPbDocs(); });
}

// ── Replace one PDF in place (same doc id — keeps portal spot + Touch Notes) ──
var _pbReplaceId=null, _pbReplaceTitle="";
function replacePbDoc(id,title){
  _pbReplaceId=id; _pbReplaceTitle=title;
  var inp=document.getElementById("pbc-replace");
  inp.value=""; inp.click();
}
function _pbReplacePicked(){
  var inp=document.getElementById("pbc-replace");
  var f=inp.files && inp.files[0];
  var res=document.getElementById("pbc-replace-result");
  if(!f || !_pbReplaceId) return;
  var id=_pbReplaceId, title=_pbReplaceTitle; _pbReplaceId=null;
  if(!confirm('Replace the PDF for "'+title+'" with "'+f.name+'"? The entry keeps its folder, title, and player notes.')){
    res.className="result"; res.textContent=""; return;
  }
  res.className="result"; res.textContent='Replacing "'+title+'"...';
  api("POST","/playbook/docs/sign-upload",{}).then(function(s){
    if(!s || !s.put_url) throw new Error("sign failed");
    return fetch(s.put_url,{method:"PUT",headers:{"Content-Type":"application/pdf"},body:f}).then(function(r){
      if(!r.ok) throw new Error("R2 "+r.status);
      return api("POST","/playbook/docs/"+id+"/replace",{key:s.key,size:f.size});
    });
  }).then(function(d){
    if(d && d.ok){
      res.className="result ok";
      res.textContent='Replaced "'+title+'". Players get the new PDF the next time they open it.';
      loadPbDocs();
    } else { throw new Error((d&&d.detail)||"replace failed"); }
  }).catch(function(e){ res.className="result err"; res.textContent='Replace failed for "'+title+'": '+e.message; });
}

// ── Whole-folder upload (PDFs go live; Visio files get queued for conversion) ──
function _pbFolderOf(relPath){
  // "2026 DEF/Pressures/Zero.pdf" -> "2026 DEF/Pressures"
  var parts=String(relPath||"").split("/"); parts.pop();
  return parts.join("/").replace(/^\\/+|\\/+$/g,"");
}
function uploadPbFolder(){
  var res=document.getElementById("pbc-folder-result");
  var files=document.getElementById("pbc-dir").files;
  if(!files || !files.length){ res.className="result err"; res.textContent="Pick a folder first."; return; }
  var list=Array.prototype.slice.call(files).filter(function(f){
    return /\\.(pdf|vsdx?|vsdm|pptx?|docx?|docm|xlsx?|xlsm|xlsb)$/i.test(f.name);
  });
  if(!list.length){ res.className="result err"; res.textContent="No PDF, Word, Excel, Visio, or PowerPoint files in that folder."; return; }
  if(window._pbUploadBusy){ res.className="result err"; res.textContent="An upload is already running — wait for it to finish."; return; }
  window._pbUploadBusy=true;
  var done=0, failed=0, queued=0;
  res.className="result";
  function report(){
    res.textContent="Processed "+(done+failed)+"/"+list.length+
      " ("+done+" ok"+(queued?(", "+queued+" queued for convert"):"")+(failed?(", "+failed+" failed"):"")+")...";
  }
  report();
  function next(i){
    if(i>=list.length){
      window._pbUploadBusy=false;
      res.className=failed?"result err":"result ok";
      res.textContent="Done: "+done+"/"+list.length+" uploaded"+
        (queued?(" ("+queued+" Visio queued for conversion)"):"")+(failed?(", "+failed+" failed"):"")+".";
      document.getElementById("pbc-dir").value=""; loadPbDocs(); loadPbJobs(); return;
    }
    var f=list[i];
    var rel=f.webkitRelativePath||f.name;
    var folder=_pbFolderOf(rel);
    var m=f.name.match(/\\.(pdf|vsdx?|vsdm|pptx?|docx?|docm|xlsx?|xlsm|xlsb)$/i);
    var ext=(m?m[1]:"").toLowerCase();
    var title=f.name.replace(/\\.(pdf|vsdx?|vsdm|pptx?|docx?|docm|xlsx?|xlsm|xlsb)$/i,"");
    var p;
    if(ext==="pdf"){
      p=api("POST","/playbook/docs/sign-upload",{folder:folder}).then(function(s){
        if(!s||!s.put_url) throw new Error("sign failed");
        return fetch(s.put_url,{method:"PUT",headers:{"Content-Type":"application/pdf"},body:f}).then(function(r){
          if(!r.ok) throw new Error("R2 "+r.status);
          return api("POST","/playbook/docs",{folder:folder,title:title,key:s.key,size:f.size});
        });
      }).then(function(){ done++; });
    } else {
      p=api("POST","/playbook/jobs/sign-upload",{ext:ext}).then(function(s){
        if(!s||!s.put_url) throw new Error("sign failed");
        return fetch(s.put_url,{method:"PUT",headers:{"Content-Type":"application/octet-stream"},body:f}).then(function(r){
          if(!r.ok) throw new Error("R2 "+r.status);
          return api("POST","/playbook/jobs",{folder:folder,title:title,key:s.key,ext:ext});
        });
      }).then(function(){ done++; queued++; });
    }
    p.then(function(){ report(); next(i+1); })
     .catch(function(e){ failed++; report(); next(i+1); });
  }
  next(0);
}

// ── Conversion jobs ───────────────────────────────────────────────────────────
function loadPbJobs(){
  var box=document.getElementById("pbjobs-table");
  api("GET","/playbook/jobs").then(function(data){
    if(!Array.isArray(data)){ box.innerHTML='<div class="loading">Error loading jobs.</div>'; return; }
    if(!data.length){ box.innerHTML='<div class="loading">No conversion jobs.</div>'; return; }
    var colors={queued:"#8b95a1",converting:"#d19a2f",done:"#2f9d55",error:"#d14343"};
    var rows=data.map(function(j){
      var st=j.status||"queued";
      var badge='<span style="color:'+(colors[st]||"#8b95a1")+';font-weight:600;text-transform:capitalize">'+pbEsc(st)+'</span>';
      if(st==="error"&&j.error) badge+=' <span class="small" style="color:#d14343" title="'+pbEsc(j.error)+'">(hover)</span>';
      return "<tr>"+
        "<td>"+(pbEsc(j.folder_path)||'<span style="color:#8b95a1">(top level)</span>')+"</td>"+
        "<td>"+pbEsc(j.title)+"."+pbEsc(j.ext||"vsdx")+"</td>"+
        "<td>"+badge+"</td>"+
        '<td><button class="btn btn-danger btn-sm" onclick="deletePbJob(\\''+j.id+'\\')">Delete</button></td>'+
      "</tr>";
    }).join("");
    box.innerHTML='<table><thead><tr><th>Folder</th><th>File</th><th>Status</th><th></th></tr></thead><tbody>'+rows+'</tbody></table>';
    // Auto-refresh while anything is still in flight.
    var busy=data.some(function(j){return j.status==="queued"||j.status==="converting";});
    if(busy && !window._pbJobsTimer){
      window._pbJobsTimer=setTimeout(function(){ window._pbJobsTimer=null; loadPbJobs(); },5000);
    }
  }).catch(function(){ box.innerHTML='<div class="loading">Error.</div>'; });
}

function deletePbJob(id){
  if(!confirm("Delete this conversion job and its uploaded source file?")) return;
  api("DELETE","/playbook/jobs/"+id).then(function(){ loadPbJobs(); });
}

// ── Empty folders (visible in the portal tree before any PDFs land in them) ───
function loadPbFolders(){
  var box=document.getElementById("pbfolders-table");
  api("GET","/playbook/folders").then(function(data){
    if(!Array.isArray(data)){ box.innerHTML='<div class="loading">Error loading folders.</div>'; return; }
    if(!data.length){ box.innerHTML='<div class="loading">No registered folders.</div>'; return; }
    var rows=data.map(function(f){
      return "<tr>"+
        "<td>"+pbEsc(f.folder_path)+"</td>"+
        '<td><button class="btn btn-danger btn-sm" onclick="deletePbFolder(\\''+f.id+'\\',\\''+pbEsc(f.folder_path)+'\\')">Delete</button></td>'+
      "</tr>";
    }).join("");
    box.innerHTML='<table><thead><tr><th>Folder</th><th></th></tr></thead><tbody>'+rows+'</tbody></table>';
  }).catch(function(){ box.innerHTML='<div class="loading">Error.</div>'; });
}

function createPbFolder(){
  var res=document.getElementById("pbf-result");
  var path=(document.getElementById("pbf-path").value||"").trim().replace(/^\\/+|\\/+$/g,"");
  if(!path){ res.className="result err"; res.textContent="Type a folder path first."; return; }
  api("POST","/playbook/folders",{path:path}).then(function(d){
    if(d && d.id){ res.className="result ok"; res.textContent='Created "'+path+'".';
      document.getElementById("pbf-path").value=""; loadPbFolders(); }
    else { res.className="result err"; res.textContent="Error: "+((d&&d.detail)||JSON.stringify(d)); }
  }).catch(function(e){ res.className="result err"; res.textContent="Network error: "+e; });
}

function deletePbFolder(id,path){
  if(!confirm('Remove the empty-folder entry "'+path+'"? (Any PDFs already uploaded there are NOT affected.)')) return;
  api("DELETE","/playbook/folders/"+id).then(function(){ loadPbFolders(); });
}

// ── Access log — who viewed which PDF and when (metadata only) ────────────────
function loadPbAccessLog(){
  var box=document.getElementById("pbaccesslog-table");
  box.innerHTML='<div class="loading">Loading...</div>';
  Promise.all([api("GET","/playbook/access-log"), api("GET","/playbook/docs")]).then(function(res){
    var log=res[0], docs=res[1];
    if(!Array.isArray(log)){ box.innerHTML='<div class="loading">Error loading log.</div>'; return; }
    if(!log.length){ box.innerHTML='<div class="loading">No views logged yet.</div>'; return; }
    var titleById={};
    if(Array.isArray(docs)) docs.forEach(function(d){ titleById[d.id]=(d.folder_path?d.folder_path+"/":"")+d.title; });
    var rows=log.map(function(e){
      var when=(e.created_at||"").replace("T"," ").slice(0,19);
      return "<tr><td>"+pbEsc(e.email)+"</td><td>"+pbEsc(titleById[e.doc_id]||e.doc_id)+"</td><td>"+pbEsc(when)+"</td></tr>";
    }).join("");
    box.innerHTML='<table><thead><tr><th>Who</th><th>File</th><th>When</th></tr></thead><tbody>'+rows+'</tbody></table>'+
      '<p class="small" style="margin-top:10px;">'+log.length+' most recent view(s).</p>';
  }).catch(function(){ box.innerHTML='<div class="loading">Error.</div>'; });
}

// ── Binder Teams — the top of the multi-tenancy chain ──────────────────────
var _teams=[];
function loadBinderTeams(){
  var box=document.getElementById("teams-table");
  box.innerHTML='<div class="loading">Loading...</div>';
  api("GET","/playbook/teams").then(function(data){
    if(!Array.isArray(data)){ box.innerHTML='<div class="loading">Error loading teams.</div>'; return; }
    _teams=data; renderBinderTeams();
  }).catch(function(){ box.innerHTML='<div class="loading">Error.</div>'; });
}

function renderBinderTeams(){
  var box=document.getElementById("teams-table");
  if(!_teams.length){ box.innerHTML='<div class="loading">No teams yet — create one above.</div>'; return; }
  var rows=_teams.map(function(t){
    return '<tr>'
      +'<td>'+pbEsc(t.name)+'</td>'
      +'<td><code>'+pbEsc(t.slug)+'</code></td>'
      +'<td>'+(t.active?'<span style="color:#4caf50">Active</span>':'<span style="color:#e05555">Inactive</span>')+'</td>'
      +'<td>'
        +'<button class="btn" style="font-size:12px;padding:4px 10px;margin-right:6px;" onclick="seedBinderTeamAdmin(\\''+t.id+'\\',\\''+pbEsc(t.name).replace(/'/g,"\\\\'")+'\\')" title="Adds ANOTHER admin to THIS team — not for a new team, use the form above for that">+ Add admin</button>'
        +'<button class="btn" style="font-size:12px;padding:4px 10px;" onclick="toggleBinderTeamActive(\\''+t.id+'\\','+(!t.active)+')">'+(t.active?'Deactivate':'Activate')+'</button>'
      +'</td>'
    +'</tr>';
  }).join("");
  box.innerHTML='<table><thead><tr><th>Name</th><th>Slug</th><th>Status</th><th></th></tr></thead><tbody>'+rows+'</tbody></table>';
}

var _teamLogoCatalogLoaded=false;
function loadTeamLogoCatalog(){
  if(_teamLogoCatalogLoaded) return;
  _teamLogoCatalogLoaded=true;
  api("GET","/playbook/team-logo-catalog").then(function(d){
    var sel=document.getElementById("team-logo-school");
    (d.schools||[]).forEach(function(s){
      var o=document.createElement("option"); o.value=s; o.textContent=s; sel.appendChild(o);
    });
  }).catch(function(){});
}

function createBinderTeam(){
  var res=document.getElementById("team-create-result");
  var slug=(document.getElementById("team-slug").value||"").trim();
  var name=(document.getElementById("team-name").value||"").trim();
  var logoSchool=(document.getElementById("team-logo-school").value||"");
  var adminEmail=(document.getElementById("team-admin-email").value||"").trim().toLowerCase();
  var adminName=(document.getElementById("team-admin-name").value||"").trim();
  var first="", last="";
  if(adminName){ var parts=adminName.split(/\s+/); first=parts[0]||""; last=parts.slice(1).join(" ")||""; }
  if(!slug||!name){ res.className="result err"; res.textContent="Slug and name are both required."; return; }
  if(!adminEmail||adminEmail.indexOf("@")<0){ res.className="result err"; res.textContent="A valid admin email is required — this is who signs in first for this team."; return; }
  res.className="result"; res.textContent="Creating...";
  api("POST","/playbook/teams/create-with-admin",{
    slug:slug, name:name, logo_school:logoSchool,
    admin_email:adminEmail, admin_first_name:first, admin_last_name:last
  }).then(function(d){
    if(d && d.team && d.admin){
      res.className="result ok";
      res.textContent='Created "'+name+'"'+(logoSchool?' with logo from "'+logoSchool+'"':'')+'. '+adminEmail+' can now sign in to set up as Team Admin.';
      document.getElementById("team-slug").value=""; document.getElementById("team-name").value="";
      document.getElementById("team-logo-school").value="";
      document.getElementById("team-admin-email").value=""; document.getElementById("team-admin-name").value="";
      loadBinderTeams();
    } else if(d && d.team && !d.admin){
      res.className="result err";
      res.textContent='Team "'+name+'" was created, but seeding the admin failed: '+(d.error||"unknown error")+'. Use "+ Add admin" on its row below to retry.';
      loadBinderTeams();
    } else { res.className="result err"; res.textContent="Error: "+((d&&d.detail)||JSON.stringify(d)); }
  }).catch(function(e){ res.className="result err"; res.textContent="Network error: "+e; });
}

function seedBinderTeamAdmin(teamId, teamName){
  var email=prompt('First Team Admin for "'+teamName+'" — their email:');
  if(!email) return;
  email=email.trim();
  if(!email || email.indexOf("@")<0){ alert("That doesn't look like a valid email."); return; }
  var first=prompt("Their first name (optional):")||"";
  var last=prompt("Their last name (optional):")||"";
  api("POST","/playbook/teams/"+teamId+"/seed-admin",{email:email,first_name:first,last_name:last}).then(function(d){
    if(d && d.id){ alert(email+" can now sign in at the Binder to set a password and manage "+teamName+"'s roster."); }
    else { alert("Error: "+((d&&d.detail)||JSON.stringify(d))); }
  }).catch(function(e){ alert("Network error: "+e); });
}

function toggleBinderTeamActive(teamId, makeActive){
  api("PATCH","/playbook/teams/"+teamId,{active:makeActive}).then(function(){ loadBinderTeams(); });
}
</script>
</body>
</html>"""
