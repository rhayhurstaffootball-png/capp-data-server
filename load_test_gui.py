"""
load_test_gui.py - CAPP Server Game-Day Load Test (GUI)
=======================================================
Simulates N distinct school clients hitting the same CAPP server behavior
that SBENTRY uses on game day.

Each run writes a timestamped report with request counts, response volume,
play volume, latency, errors, and per-worker detail.
"""

import asyncio
import json
import secrets
import threading
import time
import tkinter as tk
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from tkinter import ttk
from typing import Optional

import httpx

DEFAULT_URL = "https://capp-data-server.onrender.com"
DEFAULT_WORKERS = 50
DEFAULT_DURATION = 120
ADMIN_PASSWORD = "CAPPVCS928906"
REPORT_DIR = Path(__file__).resolve().parent / "load_test_reports"

PLAYS_INTERVAL = 15
HTTP_TIMEOUT = 20
HISTORICAL_SEASON = 2025

BG = "#070a0f"
BG2 = "#0d1117"
BG3 = "#1a2230"
BORDER = "#2c3b55"
ACCENT = "#3a7ebf"
TEXT = "#e2e8f0"
TEXT_DIM = "#8b95a1"
GREEN = "#86efac"
GREEN_BG = "#14532d"
RED = "#fca5a5"
RED_BG = "#7f1d1d"
BLUE_LIGHT = "#93c5fd"

HISTORICAL_2025_GAMES = [
    {"game_id": "401628461", "league": "cfb", "home_name": "Wisconsin", "away_name": "Western Michigan"},
    {"game_id": "401628452", "league": "cfb", "home_name": "Michigan", "away_name": "Fresno State"},
    {"game_id": "401628445", "league": "cfb", "home_name": "Texas A&M", "away_name": "Texas"},
    {"game_id": "401628464", "league": "cfb", "home_name": "Iowa", "away_name": "Iowa State"},
    {"game_id": "401628458", "league": "cfb", "home_name": "Rutgers", "away_name": "Howard"},
    {"game_id": "401628455", "league": "cfb", "home_name": "Ohio State", "away_name": "Akron"},
    {"game_id": "401628441", "league": "cfb", "home_name": "LSU", "away_name": "Oklahoma"},
    {"game_id": "401628438", "league": "cfb", "home_name": "Florida State", "away_name": "Florida"},
    {"game_id": "401628449", "league": "cfb", "home_name": "Indiana", "away_name": "Florida"},
    {"game_id": "401628467", "league": "cfb", "home_name": "Nebraska", "away_name": "Colorado"},
    {"game_id": "401628470", "league": "cfb", "home_name": "Penn State", "away_name": "Bowling Green"},
    {"game_id": "401628476", "league": "cfb", "home_name": "UCLA", "away_name": "Indiana"},
    {"game_id": "401628473", "league": "cfb", "home_name": "Washington", "away_name": "Eastern Michigan"},
    {"game_id": "401628491", "league": "cfb", "home_name": "Washington", "away_name": "Northwestern"},
    {"game_id": "401628479", "league": "cfb", "home_name": "Michigan", "away_name": "Arkansas State"},
    {"game_id": "401628482", "league": "cfb", "home_name": "Nebraska", "away_name": "Northern Iowa"},
    {"game_id": "401628485", "league": "cfb", "home_name": "Nebraska", "away_name": "Illinois"},
    {"game_id": "401628494", "league": "cfb", "home_name": "Virginia Tech", "away_name": "Rutgers"},
    {"game_id": "401628497", "league": "cfb", "home_name": "Michigan", "away_name": "Minnesota"},
    {"game_id": "401628488", "league": "cfb", "home_name": "Maryland", "away_name": "Villanova"},
    {"game_id": "401628500", "league": "cfb", "home_name": "UCLA", "away_name": "Oregon"},
    {"game_id": "401628503", "league": "cfb", "home_name": "Northwestern", "away_name": "Indiana"},
    {"game_id": "401628506", "league": "cfb", "home_name": "Oregon", "away_name": "Michigan State"},
    {"game_id": "401628509", "league": "cfb", "home_name": "Wisconsin", "away_name": "Purdue"},
    {"game_id": "401628524", "league": "cfb", "home_name": "Rutgers", "away_name": "UCLA"},
    {"game_id": "401628512", "league": "cfb", "home_name": "Iowa", "away_name": "Washington"},
    {"game_id": "401628515", "league": "cfb", "home_name": "Oregon", "away_name": "Ohio State"},
    {"game_id": "401628518", "league": "cfb", "home_name": "Illinois", "away_name": "Michigan"},
    {"game_id": "401628521", "league": "cfb", "home_name": "Maryland", "away_name": "USC"},
    {"game_id": "401628527", "league": "cfb", "home_name": "Iowa", "away_name": "Northwestern"},
    {"game_id": "401628533", "league": "cfb", "home_name": "Illinois", "away_name": "Minnesota"},
    {"game_id": "401628536", "league": "cfb", "home_name": "Michigan", "away_name": "Oregon"},
    {"game_id": "401628530", "league": "cfb", "home_name": "Ohio State", "away_name": "Nebraska"},
    {"game_id": "401628542", "league": "cfb", "home_name": "UCLA", "away_name": "Iowa"},
    {"game_id": "401628554", "league": "cfb", "home_name": "Rutgers", "away_name": "Illinois"},
    {"game_id": "401628545", "league": "cfb", "home_name": "Ohio State", "away_name": "Purdue"},
    {"game_id": "401628539", "league": "cfb", "home_name": "Penn State", "away_name": "Ohio State"},
    {"game_id": "401628548", "league": "cfb", "home_name": "Maryland", "away_name": "Rutgers"},
    {"game_id": "401628551", "league": "cfb", "home_name": "Wisconsin", "away_name": "Oregon"},
    {"game_id": "401628557", "league": "cfb", "home_name": "Michigan", "away_name": "Northwestern"},
    {"game_id": "401628560", "league": "cfb", "home_name": "Nebraska", "away_name": "Wisconsin"},
    {"game_id": "401628581", "league": "cfb", "home_name": "Wake Forest", "away_name": "North Carolina A&T"},
    {"game_id": "401628566", "league": "cfb", "home_name": "Ohio State", "away_name": "Michigan"},
    {"game_id": "401628569", "league": "cfb", "home_name": "Oregon", "away_name": "Washington"},
    {"game_id": "401628584", "league": "cfb", "home_name": "Duke", "away_name": "Elon"},
    {"game_id": "401628572", "league": "cfb", "home_name": "Oregon State", "away_name": "Washington State"},
    {"game_id": "401628563", "league": "cfb", "home_name": "Northwestern", "away_name": "Illinois"},
    {"game_id": "401628590", "league": "cfb", "home_name": "Marshall", "away_name": "Stony Brook"},
    {"game_id": "401628602", "league": "cfb", "home_name": "Colgate", "away_name": "Villanova"},
    {"game_id": "401628611", "league": "cfb", "home_name": "Wofford", "away_name": "William & Mary"},
]


def expected_play_fetches(duration: int) -> int:
    return len(range(0, max(duration, 0), PLAYS_INTERVAL))


def format_bytes(num_bytes: int) -> str:
    value = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1024 or unit == "GB":
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{num_bytes} B"


def percentile(values: list[float], pct: int) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(len(ordered) * pct / 100))
    return ordered[index]


async def fetch_health_snapshot(base_url: str) -> dict:
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(f"{base_url}/health")
        if response.status_code != 200:
            return {"ok": False, "status_code": response.status_code, "body": response.text[:200]}
        payload = response.json()
        payload["ok"] = True
        return payload
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def build_verdict(report: dict) -> dict:
    totals = report["totals"]
    latency = report["latency_ms"]
    before = report.get("server_health_before", {})
    after = report.get("server_health_after", {})
    restarted = bool(before.get("started_at") and after.get("started_at") and before.get("started_at") != after.get("started_at"))
    stability_pass = totals["errors"] == 0 and not restarted
    latency_pass = latency["p95"] <= 2000 if latency["p95"] else False

    if stability_pass and latency_pass:
        recommendation = "Current setup passes this cache-only run."
    elif stability_pass:
        recommendation = "Server stayed up, but latency misses the game-day target. Upgrade Render and keep tuning."
    else:
        recommendation = "Current setup is not acceptable for game day until stability and latency improve."

    return {
        "stability_pass": stability_pass,
        "latency_pass": latency_pass,
        "restarted_during_run": restarted,
        "recommendation": recommendation,
    }


@dataclass
class WorkerState:
    worker_id: int = 0
    game_id: str = ""
    league: str = "cfb"
    account_name: str = ""
    home_name: str = "-"
    away_name: str = "-"
    status: str = "Waiting"
    requests: int = 0
    plays_fetched: int = 0
    total_plays_seen: int = 0
    bytes_received: int = 0
    last_latency: float = 0.0
    errors: int = 0
    last_error: str = ""
    last_active: float = 0.0
    latencies: list[float] = field(default_factory=list)

    def avg_latency_ms(self) -> float:
        return (sum(self.latencies) / len(self.latencies) * 1000) if self.latencies else 0.0


worker_states: list[WorkerState] = []
total_requests = 0
total_errors = 0
total_plays_fetched = 0
total_bytes_received = 0
all_latencies: list[float] = []
error_log: list[str] = []
bytes_by_endpoint: dict[str, int] = {}
created_accounts: list[tuple[str, str]] = []
state_lock = threading.Lock()
test_running = False
test_start_time = 0.0
manual_stop_event = threading.Event()
last_report_paths: tuple[Path, Path] | None = None


def _log_error(w: WorkerState, endpoint: str, detail: str):
    ts = time.strftime("%H:%M:%S")
    team = f"{w.home_name} vs {w.away_name}" if w.home_name != "-" else w.game_id
    msg = (
        f"[{ts}]  Worker {w.worker_id + 1:>2}  {team:<30}  "
        f"{endpoint:<22}  {detail}"
    )
    error_log.append(msg)
    w.last_error = detail


def build_report(
    base_url: str,
    duration: float,
    used_shared_key: bool,
    health_before: dict,
    health_after: dict,
    game_source: dict,
    cache_preflight: dict,
    warmup_summary: dict,
) -> dict:
    with state_lock:
        states = [WorkerState(**{**w.__dict__, "latencies": list(w.latencies)}) for w in worker_states]
        reqs = total_requests
        errs = total_errors
        plays = total_plays_fetched
        total_bytes = total_bytes_received
        lats = list(all_latencies)
        endpoint_bytes = dict(bytes_by_endpoint)

    lat_ms = [x * 1000 for x in lats]
    workers = []
    for worker in states:
        workers.append(
            {
                "worker_id": worker.worker_id + 1,
                "account_name": worker.account_name,
                "game_id": worker.game_id,
                "league": worker.league,
                "home_name": worker.home_name,
                "away_name": worker.away_name,
                "requests": worker.requests,
                "plays_fetches": worker.plays_fetched,
                "total_plays_seen": worker.total_plays_seen,
                "bytes_received": worker.bytes_received,
                "bytes_received_human": format_bytes(worker.bytes_received),
                "avg_latency_ms": round(worker.avg_latency_ms(), 1),
                "errors": worker.errors,
                "last_status": worker.status,
                "last_error": worker.last_error,
            }
        )

    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "server_url": base_url,
        "workers": len(states),
        "unique_games": len({w.game_id for w in states if w.game_id}),
        "duration_seconds": round(duration, 2),
        "used_shared_api_key": used_shared_key,
        "game_source": game_source,
        "polling_pattern": {
            "plays_endpoint": "GET /game/{id}/plays",
            "plays_interval_seconds": PLAYS_INTERVAL,
        },
        "totals": {
            "requests": reqs,
            "errors": errs,
            "response_bytes": total_bytes,
            "response_bytes_human": format_bytes(total_bytes),
            "plays_fetches": plays,
            "plays_returned": sum(w.total_plays_seen for w in states),
            "expected_play_fetches": len(states) * expected_play_fetches(int(duration)),
            "requests_per_second": round(reqs / duration, 2) if duration else 0,
        },
        "latency_ms": {
            "min": round(min(lat_ms), 1) if lat_ms else 0,
            "avg": round(sum(lat_ms) / len(lat_ms), 1) if lat_ms else 0,
            "p50": round(percentile(lat_ms, 50), 1) if lat_ms else 0,
            "p95": round(percentile(lat_ms, 95), 1) if lat_ms else 0,
            "p99": round(percentile(lat_ms, 99), 1) if lat_ms else 0,
            "max": round(max(lat_ms), 1) if lat_ms else 0,
        },
        "bytes_by_endpoint": {
            endpoint: {"bytes": value, "human": format_bytes(value)}
            for endpoint, value in sorted(endpoint_bytes.items())
        },
        "server_observability": {
            "memory_metrics_available_via_api": True,
            "note": (
                "Server memory, uptime, request counters, and fetcher cache "
                "state were captured from /health before and after the run."
            ),
        },
        "cache_policy": {
            "cache_only": True,
            "note": "This run used fixed 2025 game IDs, optionally warmed them through the server, and measured only the timed client-delivery phase after cache confirmation.",
        },
        "cache_preflight": cache_preflight,
        "warmup": warmup_summary,
        "server_health_before": health_before,
        "server_health_after": health_after,
        "workers_detail": workers,
    }
    report["verdict"] = build_verdict(report)
    return report


def report_summary(report: dict) -> str:
    total = report["totals"]
    lat = report["latency_ms"]
    verdict = report["verdict"]
    endpoint_lines = []
    for endpoint, info in report["bytes_by_endpoint"].items():
        endpoint_lines.append(f"  {endpoint}: {info['human']}")
    if not endpoint_lines:
        endpoint_lines.append("  GET /game/plays: 0 B")

    lines = [
        "CAPP LOAD TEST REPORT",
        "=" * 64,
        f"Generated UTC:         {report['generated_at_utc']}",
        f"Server URL:            {report['server_url']}",
        f"Workers:               {report['workers']}",
        f"Unique games:          {report['unique_games']}",
        f"Game source:           {report.get('game_source', {}).get('name', 'Unknown')}",
        f"Duration:              {report['duration_seconds']:.1f}s",
        f"Total requests:        {total['requests']}",
        f"Request rate:          {total['requests_per_second']:.2f} req/s",
        f"Errors:                {total['errors']}",
        f"Play fetches:          {total['plays_fetches']}",
        f"Plays returned:        {total['plays_returned']:,}",
        f"Response bytes:        {total['response_bytes']:,} ({total['response_bytes_human']})",
        "",
        "Verdict:",
        f"  Stability:           {'PASS' if verdict['stability_pass'] else 'FAIL'}",
        f"  Latency Target:      {'PASS' if verdict['latency_pass'] else 'FAIL'} (p95 <= 2000 ms)",
        f"  Restart Detected:    {'YES' if verdict['restarted_during_run'] else 'NO'}",
        f"  Recommendation:      {verdict['recommendation']}",
        "",
        "Latency:",
        f"  Avg:                 {lat['avg']:.1f} ms",
        f"  P95:                 {lat['p95']:.1f} ms",
        f"  P99:                 {lat['p99']:.1f} ms",
        f"  Max:                 {lat['max']:.1f} ms",
        "",
        "Bytes by endpoint:",
        *endpoint_lines,
        "",
        "Cache preflight:",
        f"  Dataset season:      {report.get('game_source', {}).get('season', '-')}",
        f"  Selected games:      {report.get('cache_preflight', {}).get('selected_games', 0)}",
        f"  Cache ready:         {report.get('cache_preflight', {}).get('cache_ready_games', 0)}",
        f"  Missing from cache:  {report.get('cache_preflight', {}).get('missing_games', 0)}",
        "",
        "Warmup phase:",
        f"  Enabled:             {'YES' if report.get('warmup', {}).get('enabled') else 'NO'}",
        f"  Warmed games:        {report.get('warmup', {}).get('warmed_games', 0)}",
        f"  Warmup requests:     {report.get('warmup', {}).get('warmup_requests', 0)}",
        f"  Warmup bytes:        {format_bytes(report.get('warmup', {}).get('warmup_bytes', 0))}",
        f"  Warmup duration:     {report.get('warmup', {}).get('duration_seconds', 0):.1f}s",
        "",
        "Server health snapshots:",
        f"  Before: status={report.get('server_health_before', {}).get('status')} ready={report.get('server_health_before', {}).get('ready')} rss={format_bytes(report.get('server_health_before', {}).get('memory', {}).get('rss_bytes', 0))}",
        f"  After:  status={report.get('server_health_after', {}).get('status')} ready={report.get('server_health_after', {}).get('ready')} rss={format_bytes(report.get('server_health_after', {}).get('memory', {}).get('rss_bytes', 0))}",
        f"  Fetcher before: cached_games={report.get('server_health_before', {}).get('fetcher', {}).get('games_cache_count', 0)} plays_cache={report.get('server_health_before', {}).get('fetcher', {}).get('plays_cache_count', 0)}",
        f"  Fetcher after:  cached_games={report.get('server_health_after', {}).get('fetcher', {}).get('games_cache_count', 0)} plays_cache={report.get('server_health_after', {}).get('fetcher', {}).get('plays_cache_count', 0)}",
    ]
    return "\n".join(lines)


def write_report(report: dict) -> tuple[Path, Path]:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = REPORT_DIR / f"load_test_report_{stamp}.json"
    txt_path = REPORT_DIR / f"load_test_report_{stamp}.txt"
    summary = report_summary(report)
    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    txt_path.write_text(summary + "\n", encoding="utf-8")
    return json_path, txt_path


async def run_worker(w: WorkerState, base_url: str, api_key: str, stop: asyncio.Event):
    global total_requests, total_errors, total_plays_fetched, total_bytes_received, all_latencies

    headers = {"x-api-key": api_key}
    plays_due = 0.0

    await asyncio.sleep(w.worker_id * 0.1)

    async with httpx.AsyncClient(headers=headers, timeout=HTTP_TIMEOUT) as client:
        while not stop.is_set():
            now = time.monotonic()
            if now < plays_due:
                await asyncio.sleep(0.25)
                continue

            with state_lock:
                w.status = "Fetching plays"

            t0 = time.perf_counter()
            try:
                r = await client.get(
                    f"{base_url}/game/{w.game_id}/plays",
                    params={"league": w.league},
                )
                elapsed = time.perf_counter() - t0
                payload = r.content or b""
                payload_bytes = len(payload)

                with state_lock:
                    total_requests += 1
                    total_bytes_received += payload_bytes
                    bytes_by_endpoint["GET /game/plays"] = bytes_by_endpoint.get("GET /game/plays", 0) + payload_bytes
                    w.requests += 1
                    w.bytes_received += payload_bytes
                    w.last_latency = elapsed
                    w.last_active = time.time()

                    if r.status_code == 200:
                        all_latencies.append(elapsed)
                        w.latencies.append(elapsed)
                        data = r.json()
                        plays_count = len(data.get("entries", []))
                        w.plays_fetched += 1
                        total_plays_fetched += 1
                        w.total_plays_seen += plays_count
                        if w.home_name == "-":
                            w.home_name = data.get("home_name", "-")
                            w.away_name = data.get("away_name", "-")
                        w.status = "Active"
                    else:
                        w.errors += 1
                        total_errors += 1
                        w.status = f"Error {r.status_code}"
                        _log_error(w, "GET /game/plays", f"HTTP {r.status_code} - {r.text[:120]}")
            except Exception as e:
                elapsed = time.perf_counter() - t0
                with state_lock:
                    total_requests += 1
                    total_errors += 1
                    w.requests += 1
                    w.errors += 1
                    w.last_latency = elapsed
                    w.status = "Timeout"
                    _log_error(w, "GET /game/plays", f"{type(e).__name__}: {e}")

            plays_due = time.monotonic() + PLAYS_INTERVAL

    with state_lock:
        w.status = "Done"


async def create_test_account(base_url: str, index: int) -> tuple[str, str]:
    username = f"loadtest_{index:02d}_{secrets.token_hex(4)}@test.invalid"
    password = secrets.token_hex(16)
    async with httpx.AsyncClient() as client:
        r = await client.post(
            f"{base_url}/register",
            json={
                "school": f"Load Test School {index:02d}",
                "email": username,
                "password": password,
            },
            timeout=15,
        )
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Registration failed: {r.status_code} {r.text}")
    data = r.json()
    return data["api_key"], username


async def delete_test_account(base_url: str, username: str):
    async with httpx.AsyncClient() as client:
        await client.delete(
            f"{base_url}/admin/api/clients/{username}",
            headers={"x-admin-token": ADMIN_PASSWORD},
            timeout=10,
        )


async def create_test_accounts(base_url: str, count: int, log_fn) -> list[tuple[str, str]]:
    accounts: list[tuple[str, str]] = []
    sem = asyncio.Semaphore(10)
    lock = asyncio.Lock()

    async def one(index: int):
        async with sem:
            account = await create_test_account(base_url, index)
            async with lock:
                accounts.append(account)
                made = len(accounts)
            if made == 1 or made == count or made % 10 == 0:
                log_fn(f"Created {made}/{count} simulated school accounts...")

    await asyncio.gather(*[one(i) for i in range(1, count + 1)])
    accounts.sort(key=lambda item: item[1])
    return accounts


def historical_game_pool(num_workers: int) -> list[dict]:
    return [dict(item) for item in HISTORICAL_2025_GAMES[:num_workers]]


def resolve_selected_games(num_workers: int, custom_game_id: str) -> tuple[list[dict], dict]:
    custom_game_id = custom_game_id.strip()
    if custom_game_id:
        match = next((item for item in HISTORICAL_2025_GAMES if item["game_id"] == custom_game_id), None)
        if match:
            game = dict(match)
        else:
            game = {
                "game_id": custom_game_id,
                "league": "cfb",
                "home_name": "Preview Game",
                "away_name": "Preview Game",
            }
        return [game], {
            "name": "Single historical preview game",
            "season": HISTORICAL_SEASON,
            "type": "historical_single_preview",
        }
    return historical_game_pool(num_workers), {
        "name": "Fixed 2025 historical game set",
        "season": HISTORICAL_SEASON,
        "type": "historical_fixed_ids",
    }


async def fetch_cache_ready_games(base_url: str, api_key: str, games: list[dict], log_fn) -> tuple[list[dict], list[dict]]:
    headers = {"x-api-key": api_key}
    ready: list[dict] = []
    missing: list[dict] = []
    sem = asyncio.Semaphore(10)
    lock = asyncio.Lock()

    async def one(game: dict):
        async with sem:
            status = 0
            error = ""
            try:
                async with httpx.AsyncClient(headers=headers, timeout=10) as client:
                    r = await client.get(f"{base_url}/game/{game['game_id']}/version")
                if r.status_code == 200:
                    status = int(r.json().get("fetched_at", 0) or 0)
                else:
                    error = f"HTTP {r.status_code}"
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"

            async with lock:
                target = ready if status > 0 else missing
                entry = dict(game)
                entry["cached_version"] = status
                if error:
                    entry["error"] = error
                target.append(entry)
                checked = len(ready) + len(missing)
                if checked == 1 or checked == len(games) or checked % 10 == 0:
                    log_fn(f"Cache preflight checked {checked}/{len(games)} historical games...")

    await asyncio.gather(*[one(game) for game in games])
    ready.sort(key=lambda item: item["game_id"])
    missing.sort(key=lambda item: item["game_id"])
    return ready, missing


async def warm_cache_games(base_url: str, api_key: str, games: list[dict], log_fn) -> dict:
    headers = {"x-api-key": api_key}
    sem = asyncio.Semaphore(5)
    lock = asyncio.Lock()
    warmed_games = 0
    warmup_requests = 0
    warmup_bytes = 0
    failures: list[str] = []
    started = time.monotonic()

    async def one(game: dict):
        nonlocal warmed_games, warmup_requests, warmup_bytes
        async with sem:
            try:
                async with httpx.AsyncClient(headers=headers, timeout=HTTP_TIMEOUT) as client:
                    response = await client.get(
                        f"{base_url}/game/{game['game_id']}/plays",
                        params={"league": game["league"]},
                    )
                payload = response.content or b""
                async with lock:
                    warmup_requests += 1
                    warmup_bytes += len(payload)
                    if response.status_code == 200:
                        warmed_games += 1
                    else:
                        failures.append(
                            f"{game['game_id']} ({game['away_name']} at {game['home_name']}): HTTP {response.status_code}"
                        )
                    checked = warmed_games + len(failures)
                    if checked == 1 or checked == len(games) or checked % 10 == 0:
                        log_fn(f"Warmed {checked}/{len(games)} historical games through the server...")
            except Exception as exc:
                async with lock:
                    warmup_requests += 1
                    failures.append(
                        f"{game['game_id']} ({game['away_name']} at {game['home_name']}): {type(exc).__name__}: {exc}"
                    )
                    checked = warmed_games + len(failures)
                    if checked == 1 or checked == len(games) or checked % 10 == 0:
                        log_fn(f"Warmed {checked}/{len(games)} historical games through the server...")

    await asyncio.gather(*[one(game) for game in games])
    duration_seconds = time.monotonic() - started
    return {
        "enabled": True,
        "warmed_games": warmed_games,
        "warmup_requests": warmup_requests,
        "warmup_bytes": warmup_bytes,
        "duration_seconds": round(duration_seconds, 2),
        "failures": failures[:10],
    }


async def cleanup_accounts(base_url: str, accounts: list[tuple[str, str]], log_fn):
    if not accounts:
        return

    failures = 0
    sem = asyncio.Semaphore(10)
    lock = asyncio.Lock()

    async def one(username: str):
        nonlocal failures
        async with sem:
            try:
                await delete_test_account(base_url, username)
            except Exception:
                async with lock:
                    failures += 1

    await asyncio.gather(*[one(username) for _, username in accounts], return_exceptions=True)
    if failures:
        log_fn(f"Cleanup finished with {failures} delete failures.")
    else:
        log_fn("Cleanup done.")


async def async_main(base_url: str, num_workers: int, duration: int, api_key_override: str, custom_game_id: str, log_fn, done_fn):
    global test_running, test_start_time, created_accounts, last_report_paths
    global total_requests, total_errors, total_plays_fetched, total_bytes_received, all_latencies, bytes_by_endpoint

    with state_lock:
        total_requests = 0
        total_errors = 0
        total_plays_fetched = 0
        total_bytes_received = 0
        all_latencies.clear()
        error_log.clear()
        bytes_by_endpoint = {}
        created_accounts.clear()
        worker_states.clear()

    manual_stop_event.clear()
    last_report_paths = None
    used_shared_key = bool(api_key_override)
    warmup_summary = {
        "enabled": True,
        "warmed_games": 0,
        "warmup_requests": 0,
        "warmup_bytes": 0,
        "duration_seconds": 0.0,
        "failures": [],
    }

    log_fn("Checking server health...")
    health_before = await fetch_health_snapshot(base_url)
    try:
        assert health_before.get("ok")
        log_fn(
            f"Server {health_before.get('status', 'unknown')} "
            f"(version {health_before.get('version', '?')}, ready={health_before.get('ready')})"
        )
    except Exception:
        log_fn(f"ERROR: Server unreachable - {health_before}")
        done_fn()
        return

    worker_accounts: list[tuple[str, str]] = []
    probe_account: tuple[str, str] | None = None
    if api_key_override:
        worker_accounts = [(api_key_override, f"shared-key-{i + 1:02d}") for i in range(num_workers)]
        discovery_key = api_key_override
        log_fn("Using the provided API key for all workers.")
    else:
        log_fn("Creating 1 probe account for cache preflight...")
        try:
            probe_account = await create_test_account(base_url, 1)
            discovery_key = probe_account[0]
            with state_lock:
                created_accounts = [probe_account]
            log_fn("Probe account created. Checking cached games before creating the rest...")
        except Exception as e:
            log_fn(f"ERROR: {e}")
            done_fn()
            return

    assignments_data, game_source = resolve_selected_games(num_workers, custom_game_id)

    if not custom_game_id and num_workers > len(HISTORICAL_2025_GAMES):
        error_msg = (
            f"ERROR: Requested {num_workers} workers but the built-in 2025 historical dataset "
            f"only has {len(HISTORICAL_2025_GAMES)} unique games."
        )
        log_fn(error_msg)
        if created_accounts:
            log_fn("Cleaning up probe account...")
            await cleanup_accounts(base_url, created_accounts, log_fn)
            log_fn(error_msg)
        done_fn()
        return

    if custom_game_id:
        log_fn(
            f"Using single-game preview mode for {custom_game_id} "
            "(warm through the server, then run one simulated client)."
        )
    else:
        log_fn(
            f"Using {len(assignments_data)} fixed {HISTORICAL_SEASON} game IDs for cache-only preflight "
            f"(no /games discovery, no ESPN lookup)."
        )
    ready_games, missing_games = await fetch_cache_ready_games(base_url, discovery_key, assignments_data, log_fn)
    cache_preflight = {
        "selected_games": len(assignments_data),
        "cache_ready_games": len(ready_games),
        "missing_games": len(missing_games),
        "missing_examples": [
            f"{item['game_id']} ({item['away_name']} at {item['home_name']})"
            for item in missing_games[:5]
        ],
    }

    if missing_games:
        log_fn(
            f"{len(missing_games)} of the selected {HISTORICAL_SEASON} games are not cached yet. "
            "Warming them through the server now..."
        )
        warmup_summary = await warm_cache_games(base_url, discovery_key, missing_games, log_fn)
        if warmup_summary["failures"]:
            examples = ", ".join(warmup_summary["failures"][:5])
            error_msg = (
                f"ERROR: Warmup failed for {len(warmup_summary['failures'])} games. "
                f"Examples: {examples}"
            )
            log_fn(error_msg)
            if created_accounts:
                log_fn("Cleaning up probe account...")
                await cleanup_accounts(base_url, created_accounts, log_fn)
                log_fn(error_msg)
            done_fn()
            return

        log_fn("Warmup finished. Re-checking server cache state...")
        ready_games, missing_games = await fetch_cache_ready_games(base_url, discovery_key, assignments_data, log_fn)
        cache_preflight = {
            "selected_games": len(assignments_data),
            "cache_ready_games": len(ready_games),
            "missing_games": len(missing_games),
            "missing_examples": [
                f"{item['game_id']} ({item['away_name']} at {item['home_name']})"
                for item in missing_games[:5]
            ],
        }
        if missing_games:
            examples = ", ".join(cache_preflight["missing_examples"]) if cache_preflight["missing_examples"] else "none listed"
            error_msg = (
                f"ERROR: Warmup completed, but {len(missing_games)} games still are not cache-ready. "
                f"Examples: {examples}"
            )
            log_fn(error_msg)
            if created_accounts:
                log_fn("Cleaning up probe account...")
                await cleanup_accounts(base_url, created_accounts, log_fn)
                log_fn(error_msg)
            done_fn()
            return
    else:
        warmup_summary["enabled"] = False

    if not api_key_override:
        remaining = num_workers - 1
        if remaining > 0:
            log_fn(f"Cache preflight passed. Creating remaining {remaining} simulated school accounts...")
            try:
                more_accounts = await create_test_accounts(base_url, remaining, log_fn)
                worker_accounts = [probe_account] + more_accounts
                with state_lock:
                    created_accounts = list(worker_accounts)
                log_fn(f"Created {len(worker_accounts)} distinct school accounts.")
            except Exception as e:
                error_msg = f"ERROR: {e}"
                log_fn(error_msg)
                if created_accounts:
                    log_fn("Cleaning up test accounts...")
                    await cleanup_accounts(base_url, created_accounts, log_fn)
                    log_fn(error_msg)
                done_fn()
                return
        else:
            worker_accounts = [probe_account]
            log_fn("Cache preflight passed. Using the probe account for the run.")

    assignments = ready_games[:num_workers]
    expected = num_workers * expected_play_fetches(duration)
    log_fn(
        f"Assigned {num_workers} unique games to {num_workers} simulated schools "
        f"(~{expected} play fetches expected)."
    )

    with state_lock:
        for i, ((_, username), game) in enumerate(zip(worker_accounts, assignments)):
            worker_states.append(
                WorkerState(
                    worker_id=i,
                    game_id=game["game_id"],
                    league=game["league"],
                    account_name=username,
                    home_name=game["home_name"],
                    away_name=game["away_name"],
                )
            )

    stop = asyncio.Event()
    test_start_time = time.time()
    test_running = True

    log_fn(
        f"Running {num_workers} workers for {duration}s "
        f"(GET /game/{{id}}/plays every {PLAYS_INTERVAL}s, one key per school)..."
    )
    tasks = [
        asyncio.create_task(run_worker(w, base_url, worker_accounts[w.worker_id][0], stop))
        for w in worker_states
    ]

    start_monotonic = time.monotonic()
    while (time.monotonic() - start_monotonic) < duration and not manual_stop_event.is_set():
        await asyncio.sleep(0.25)

    stop.set()
    await asyncio.gather(*tasks, return_exceptions=True)
    test_running = False
    actual_duration = time.monotonic() - start_monotonic

    health_after = await fetch_health_snapshot(base_url)
    report = build_report(
        base_url,
        actual_duration,
        used_shared_key,
        health_before,
        health_after,
        game_source,
        cache_preflight,
        warmup_summary,
    )
    json_path, txt_path = write_report(report)
    last_report_paths = (json_path, txt_path)

    with state_lock:
        reqs = total_requests
        errs = total_errors
        plays = total_plays_fetched
        bytes_human = format_bytes(total_bytes_received)
        accounts_to_cleanup = list(created_accounts)

    if manual_stop_event.is_set():
        log_fn(f"Stopped - {reqs} requests, {plays} play fetches, {bytes_human}, {errs} errors.")
    else:
        log_fn(f"Done - {reqs} requests, {plays} play fetches, {bytes_human}, {errs} errors.")

    log_fn(f"Report saved: {txt_path.name}")

    if accounts_to_cleanup:
        log_fn("Cleaning up test accounts...")
        await cleanup_accounts(base_url, accounts_to_cleanup, log_fn)

    done_fn()


def run_async_test(base_url, workers, duration, api_key, custom_game_id, log_fn, done_fn):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(async_main(base_url, workers, duration, api_key, custom_game_id, log_fn, done_fn))
    loop.close()


class LoadTestApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("CAPP Server - Game Day Load Test")
        self.configure(bg=BG)
        self.geometry("1140x780")
        self.resizable(True, True)
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._duration = 0
        self._start_ts = None
        self._err_log_len = 0
        self._build_ui()

    def _build_ui(self):
        self._style()

        top = tk.Frame(self, bg=BG2, pady=14)
        top.pack(fill="x")

        tk.Label(top, text="CAPP LOAD TEST", bg=BG2, fg=ACCENT, font=("Segoe UI", 14, "bold"), padx=20).pack(side="left")

        cfg = tk.Frame(top, bg=BG2)
        cfg.pack(side="left", padx=20)

        def labeled(parent, label, default, width=28):
            frame = tk.Frame(parent, bg=BG2)
            frame.pack(side="left", padx=8)
            tk.Label(frame, text=label, bg=BG2, fg=TEXT_DIM, font=("Segoe UI", 9)).pack(anchor="w")
            entry = tk.Entry(
                frame,
                width=width,
                bg=BG3,
                fg=TEXT,
                relief="flat",
                insertbackground=TEXT,
                font=("Segoe UI", 10),
                highlightthickness=1,
                highlightbackground=BORDER,
                highlightcolor=ACCENT,
            )
            entry.insert(0, default)
            entry.pack()
            return entry

        self.url_entry = labeled(cfg, "Server URL", DEFAULT_URL, 36)
        self.workers_entry = labeled(cfg, "Workers", str(DEFAULT_WORKERS), 6)
        self.duration_entry = labeled(cfg, "Duration (s)", str(DEFAULT_DURATION), 6)
        self.apikey_entry = labeled(cfg, "API Key (optional)", "", 22)
        self.preview_game_entry = labeled(cfg, "Preview Game ID (optional)", "", 16)

        btn_frame = tk.Frame(top, bg=BG2)
        btn_frame.pack(side="right", padx=20)

        self.start_btn = tk.Button(btn_frame, text="START", command=self._start, bg=ACCENT, fg="white", font=("Segoe UI", 11, "bold"), relief="flat", padx=16, pady=6, cursor="hand2")
        self.start_btn.pack(side="left", padx=6)

        self.stop_btn = tk.Button(btn_frame, text="STOP", command=self._stop, bg=RED_BG, fg=RED, font=("Segoe UI", 11, "bold"), relief="flat", padx=16, pady=6, cursor="hand2", state="disabled")
        self.stop_btn.pack(side="left", padx=6)

        stats_frame = tk.Frame(self, bg=BG3, pady=8)
        stats_frame.pack(fill="x")

        def stat_label(parent, key):
            frame = tk.Frame(parent, bg=BG3, padx=18)
            frame.pack(side="left")
            tk.Label(frame, text=key, bg=BG3, fg=TEXT_DIM, font=("Segoe UI", 8, "bold")).pack()
            label = tk.Label(frame, text="-", bg=BG3, fg=BLUE_LIGHT, font=("Segoe UI", 13, "bold"))
            label.pack()
            return label

        self.lbl_elapsed = stat_label(stats_frame, "ELAPSED")
        self.lbl_rps = stat_label(stats_frame, "REQ / SEC")
        self.lbl_total = stat_label(stats_frame, "TOTAL REQS")
        self.lbl_errors = stat_label(stats_frame, "ERRORS")
        self.lbl_avg_lat = stat_label(stats_frame, "AVG LATENCY")
        self.lbl_p95 = stat_label(stats_frame, "P95 LATENCY")
        self.lbl_plays = stat_label(stats_frame, "PLAY FETCHES")
        self.lbl_bytes = stat_label(stats_frame, "RESP BYTES")

        prog_frame = tk.Frame(self, bg=BG, pady=4, padx=12)
        prog_frame.pack(fill="x")
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(prog_frame, variable=self.progress_var, maximum=100, style="CAPP.Horizontal.TProgressbar")
        self.progress_bar.pack(fill="x")

        table_frame = tk.Frame(self, bg=BG)
        table_frame.pack(fill="both", expand=True, padx=12, pady=6)

        cols = ("#", "Account", "Home Team", "Away Team", "Game ID", "League", "Status", "Fetches", "Plays", "Bytes", "Latency", "Errors")
        self.tree = ttk.Treeview(table_frame, columns=cols, show="headings", style="CAPP.Treeview")

        widths = [40, 160, 140, 140, 100, 55, 130, 65, 70, 85, 75, 55]
        for col, width in zip(cols, widths):
            self.tree.heading(col, text=col)
            self.tree.column(col, width=width, anchor="center", stretch=False)
        self.tree.column("Account", anchor="w")
        self.tree.column("Home Team", anchor="w")
        self.tree.column("Away Team", anchor="w")
        self.tree.column("Status", anchor="w")

        vsb = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y")
        self.tree.pack(fill="both", expand=True)

        self.tree.tag_configure("active", background=GREEN_BG, foreground=GREEN)
        self.tree.tag_configure("fetching", background="#1e3a5f", foreground=BLUE_LIGHT)
        self.tree.tag_configure("waiting", background=BG2, foreground=TEXT_DIM)
        self.tree.tag_configure("error", background=RED_BG, foreground=RED)
        self.tree.tag_configure("done", background=BG2, foreground=TEXT_DIM)

        err_outer = tk.Frame(self, bg=BG2)
        err_outer.pack(fill="x", padx=12, pady=(0, 6))
        tk.Label(err_outer, text="ERROR LOG", bg=BG2, fg=TEXT_DIM, font=("Segoe UI", 8, "bold"), padx=6, pady=3).pack(anchor="w")
        err_inner = tk.Frame(err_outer, bg=BG2)
        err_inner.pack(fill="x")
        self.err_text = tk.Text(err_inner, height=5, bg=BG2, fg=RED, font=("Consolas", 9), relief="flat", state="disabled", wrap="none", highlightthickness=1, highlightbackground=BORDER)
        err_scroll = ttk.Scrollbar(err_inner, orient="vertical", command=self.err_text.yview)
        self.err_text.configure(yscrollcommand=err_scroll.set)
        err_scroll.pack(side="right", fill="y")
        self.err_text.pack(fill="x")

        log_frame = tk.Frame(self, bg=BG2, pady=4)
        log_frame.pack(fill="x", padx=12, pady=(0, 8))
        self.log_var = tk.StringVar(value="Ready.")
        tk.Label(log_frame, textvariable=self.log_var, bg=BG2, fg=TEXT_DIM, font=("Consolas", 10), anchor="w", padx=8).pack(fill="x")

        report_frame = tk.Frame(self, bg=BG2, pady=4)
        report_frame.pack(fill="x", padx=12, pady=(0, 10))
        self.report_var = tk.StringVar(value="No report generated yet.")
        tk.Label(report_frame, textvariable=self.report_var, bg=BG2, fg=TEXT_DIM, font=("Consolas", 9), anchor="w", padx=8).pack(fill="x")

    def _style(self):
        style = ttk.Style(self)
        style.theme_use("clam")
        style.configure("CAPP.Treeview", background=BG3, foreground=TEXT, fieldbackground=BG3, rowheight=24, font=("Consolas", 10))
        style.configure("CAPP.Treeview.Heading", background=BG2, foreground=TEXT_DIM, font=("Segoe UI", 9, "bold"), relief="flat")
        style.map("CAPP.Treeview", background=[("selected", ACCENT)])
        style.configure("CAPP.Horizontal.TProgressbar", troughcolor=BG3, background=ACCENT, darkcolor=ACCENT, lightcolor=ACCENT, bordercolor=BG3)

    def _populate_table(self, n: int):
        self.tree.delete(*self.tree.get_children())
        self._err_log_len = 0
        self.err_text.config(state="normal")
        self.err_text.delete("1.0", "end")
        self.err_text.config(state="disabled")
        self.report_var.set("No report generated yet.")
        for i in range(n):
            self.tree.insert("", "end", iid=str(i), values=(i + 1, "-", "-", "-", "-", "-", "Waiting", 0, 0, "-", "-", 0), tags=("waiting",))

    def _tag_for(self, status: str) -> str:
        s = status.lower()
        if "active" in s:
            return "active"
        if "fetch" in s:
            return "fetching"
        if "error" in s or "timeout" in s:
            return "error"
        if "done" in s:
            return "done"
        return "waiting"

    def _update_row(self, ws: WorkerState):
        latency = f"{ws.last_latency * 1000:.0f} ms" if ws.last_latency else "-"
        values = (
            ws.worker_id + 1,
            ws.account_name,
            ws.home_name,
            ws.away_name,
            ws.game_id,
            ws.league.upper(),
            ws.status,
            ws.plays_fetched,
            ws.total_plays_seen,
            format_bytes(ws.bytes_received),
            latency,
            ws.errors,
        )
        try:
            self.tree.item(str(ws.worker_id), values=values, tags=(self._tag_for(ws.status),))
        except Exception:
            return

    def _start(self):
        if self._running:
            return

        base_url = self.url_entry.get().strip().rstrip("/")
        try:
            workers = int(self.workers_entry.get())
            duration = int(self.duration_entry.get())
        except ValueError:
            self.log_var.set("ERROR: Workers and Duration must be numbers.")
            return

        if workers <= 0 or duration <= 0:
            self.log_var.set("ERROR: Workers and Duration must be greater than zero.")
            return

        api_key = self.apikey_entry.get().strip()
        custom_game_id = self.preview_game_entry.get().strip()

        if custom_game_id:
            workers = 1

        self._running = True
        self._duration = duration
        self._start_ts = None
        self.start_btn.config(state="disabled")
        self.stop_btn.config(state="normal")
        self.progress_var.set(0)
        self._populate_table(workers)

        self._thread = threading.Thread(
            target=run_async_test,
            args=(base_url, workers, duration, api_key, custom_game_id, self._log, self._on_done),
            daemon=True,
        )
        self._thread.start()
        self._poll_ui()

    def _stop(self):
        self.log_var.set("Stopping...")
        self.stop_btn.config(state="disabled")
        manual_stop_event.set()

    def _log(self, msg: str):
        self.after(0, lambda: self.log_var.set(msg))

    def _on_done(self):
        self.after(0, self._finish)

    def _finish(self):
        self._running = False
        self.start_btn.config(state="normal")
        self.stop_btn.config(state="disabled")
        self.progress_var.set(100)
        self._refresh_stats()
        if last_report_paths:
            self.report_var.set(f"Report: {last_report_paths[1]} | JSON: {last_report_paths[0]}")

    def _poll_ui(self):
        if self._start_ts is None and test_start_time:
            self._start_ts = test_start_time

        self._refresh_stats()

        with state_lock:
            states_copy = list(worker_states)
            new_errors = list(error_log[self._err_log_len:])
            self._err_log_len = len(error_log)

        for ws in states_copy:
            self._update_row(ws)

        if new_errors:
            self.err_text.config(state="normal")
            for line in new_errors:
                self.err_text.insert("end", line + "\n")
            self.err_text.see("end")
            self.err_text.config(state="disabled")

        if self._running or test_running:
            self.after(500, self._poll_ui)

    def _refresh_stats(self):
        elapsed = (time.time() - test_start_time) if test_start_time else 0

        with state_lock:
            reqs = total_requests
            errs = total_errors
            plays = total_plays_fetched
            total_bytes = total_bytes_received
            lats = list(all_latencies)

        rps = reqs / elapsed if elapsed > 0 else 0
        avg_lat = (sum(lats) / len(lats) * 1000) if lats else 0
        p95 = percentile([x * 1000 for x in lats], 95) if lats else 0

        self.lbl_elapsed.config(text=f"{elapsed:.0f}s")
        self.lbl_rps.config(text=f"{rps:.1f}")
        self.lbl_total.config(text=str(reqs))
        self.lbl_errors.config(text=str(errs), fg=RED if errs else GREEN)
        self.lbl_avg_lat.config(text=f"{avg_lat:.0f} ms" if avg_lat else "-")
        self.lbl_p95.config(text=f"{p95:.0f} ms" if p95 else "-")
        self.lbl_plays.config(text=str(plays))
        self.lbl_bytes.config(text=format_bytes(total_bytes))

        if self._duration and test_start_time:
            pct = min(100, elapsed / self._duration * 100)
            self.progress_var.set(pct)


if __name__ == "__main__":
    app = LoadTestApp()
    app.mainloop()
