"""
load_test.py — CAPP Server Game-Day Load Test
==============================================
Simulates N concurrent school clients hitting the server the same way
the real CAPP app does on game day.

Usage:
    python load_test.py                              # 50 workers, 120s, live server
    python load_test.py --workers 50 --duration 180
    python load_test.py --url http://localhost:8000  # local dev server
    python load_test.py --api-key <key>              # skip auto-create/cleanup

Each worker runs the real CAPP polling loop:
    1. GET /games          — scoreboard, every GAMES_INTERVAL seconds
    2. GET /game/{id}/version — lightweight version check per live game
    3. GET /game/{id}/plays   — full play data only when version changes
"""

import asyncio
import argparse
import secrets
import statistics
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

import httpx

# ── Polling intervals (match real CAPP client) ────────────────────────────────
GAMES_INTERVAL   = 30   # seconds between /games polls
VERSION_INTERVAL = 60   # seconds between /version polls per game
HTTP_TIMEOUT     = 20   # seconds per request

# ── Default config ────────────────────────────────────────────────────────────
DEFAULT_URL      = "https://capp-data-server.onrender.com"
DEFAULT_WORKERS  = 50
DEFAULT_DURATION = 120  # seconds

# Fallback game IDs used when no live games are found (recent CFB games)
FALLBACK_GAME_IDS = [
    ("401628438", "cfb"),
    ("401628441", "cfb"),
    ("401628445", "cfb"),
    ("401628449", "cfb"),
    ("401628452", "cfb"),
]


# ── Stats collector ───────────────────────────────────────────────────────────
@dataclass
class Stats:
    latencies: list = field(default_factory=list)
    errors:    dict = field(default_factory=lambda: defaultdict(int))
    counts:    dict = field(default_factory=lambda: defaultdict(int))
    lock:      asyncio.Lock = field(default_factory=asyncio.Lock)

    async def record(self, endpoint: str, elapsed: float, status: int):
        async with self.lock:
            self.counts[endpoint] += 1
            if status < 400:
                self.latencies.append(elapsed)
            else:
                self.errors[status] += 1

    def summary(self, duration: float) -> str:
        total   = sum(self.counts.values())
        err_total = sum(self.errors.values())
        rps     = total / duration if duration else 0
        lines   = [
            "",
            "=" * 58,
            "  CAPP LOAD TEST — RESULTS",
            "=" * 58,
            f"  Duration:        {duration:.1f}s",
            f"  Total requests:  {total}",
            f"  Requests/sec:    {rps:.1f}",
            f"  Errors:          {err_total}",
            "",
            "  By endpoint:",
        ]
        for ep, cnt in sorted(self.counts.items()):
            lines.append(f"    {ep:<30} {cnt:>6} reqs")

        if self.errors:
            lines += ["", "  Error breakdown:"]
            for code, cnt in sorted(self.errors.items()):
                lines.append(f"    HTTP {code}:  {cnt}")

        if self.latencies:
            s = sorted(self.latencies)
            p = lambda pct: s[int(len(s) * pct / 100)]
            lines += [
                "",
                "  Latency (successful requests):",
                f"    min   {min(s)*1000:.0f} ms",
                f"    avg   {statistics.mean(s)*1000:.0f} ms",
                f"    p50   {p(50)*1000:.0f} ms",
                f"    p95   {p(95)*1000:.0f} ms",
                f"    p99   {p(99)*1000:.0f} ms",
                f"    max   {max(s)*1000:.0f} ms",
            ]
        lines.append("=" * 58)
        return "\n".join(lines)


# ── HTTP helpers ──────────────────────────────────────────────────────────────
async def get(client: httpx.AsyncClient, url: str, stats: Stats,
              label: str, **kwargs) -> Optional[dict]:
    t0 = time.perf_counter()
    try:
        r = await client.get(url, timeout=HTTP_TIMEOUT, **kwargs)
        elapsed = time.perf_counter() - t0
        await stats.record(label, elapsed, r.status_code)
        if r.status_code == 200:
            return r.json()
        return None
    except Exception as e:
        elapsed = time.perf_counter() - t0
        await stats.record(label, elapsed, 0)
        return None


# ── Single worker — simulates one school's CAPP client ───────────────────────
async def worker(worker_id: int, base_url: str, api_key: str,
                 game_ids: list, stats: Stats, stop: asyncio.Event):
    headers = {"x-api-key": api_key}
    version_seen = {}   # game_id -> last fetched_at

    async with httpx.AsyncClient(headers=headers) as client:
        games_due    = 0.0
        version_due  = 0.0

        while not stop.is_set():
            now = time.monotonic()

            # ── Poll /games ───────────────────────────────────────────────────
            if now >= games_due:
                data = await get(client, f"{base_url}/games", stats, "GET /games",
                                 params={"league": "all"})
                if data:
                    live = [(g["game_id"], g.get("league", "cfb"))
                            for g in data if g.get("status") == "in"]
                    if live:
                        game_ids = live          # switch to real live games
                games_due = time.monotonic() + GAMES_INTERVAL

            # ── Poll version + fetch plays for each game ──────────────────────
            if now >= version_due:
                for gid, league in game_ids[:8]:  # cap at 8 games per worker
                    ver_data = await get(
                        client, f"{base_url}/game/{gid}/version",
                        stats, "GET /game/version")
                    if ver_data:
                        new_ver = ver_data.get("fetched_at", 0)
                        if version_seen.get(gid, 0) != new_ver:
                            version_seen[gid] = new_ver
                            await get(
                                client, f"{base_url}/game/{gid}/plays",
                                stats, "GET /game/plays",
                                params={"league": league})
                version_due = time.monotonic() + VERSION_INTERVAL

            await asyncio.sleep(0.5)


# ── Progress printer ──────────────────────────────────────────────────────────
async def progress(stats: Stats, duration: float, stop: asyncio.Event):
    start = time.monotonic()
    while not stop.is_set():
        await asyncio.sleep(10)
        elapsed = time.monotonic() - start
        total = sum(stats.counts.values())
        errs  = sum(stats.errors.values())
        rps   = total / elapsed if elapsed else 0
        lat   = f"{statistics.mean(stats.latencies)*1000:.0f}ms avg" if stats.latencies else "—"
        pct   = min(100, elapsed / duration * 100)
        print(f"  [{pct:3.0f}%] {elapsed:.0f}s | {total} reqs | {rps:.1f} req/s | "
              f"{lat} | {errs} errors", flush=True)


# ── Account management ────────────────────────────────────────────────────────
async def create_test_account(base_url: str) -> tuple[str, str]:
    """Register a throwaway test account. Returns (api_key, username)."""
    username = f"loadtest_{secrets.token_hex(4)}@test.invalid"
    password = secrets.token_hex(16)
    async with httpx.AsyncClient() as client:
        r = await client.post(f"{base_url}/register", json={
            "school": "Load Test School",
            "email":  username,
            "password": password,
        }, timeout=15)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Could not create test account: {r.status_code} {r.text}")
    data = r.json()
    return data["api_key"], username


async def delete_test_account(base_url: str, username: str, admin_password: str):
    """Delete the throwaway account via the admin API."""
    async with httpx.AsyncClient() as client:
        r = await client.delete(
            f"{base_url}/admin/api/clients/{username}",
            headers={"x-admin-token": admin_password},
            timeout=10,
        )
    if r.status_code in (200, 204):
        print(f"  Test account '{username}' cleaned up.")
    else:
        print(f"  Warning: could not delete test account ({r.status_code}) — delete manually.")


# ── Entry point ───────────────────────────────────────────────────────────────
async def main():
    parser = argparse.ArgumentParser(description="CAPP server game-day load test")
    parser.add_argument("--url",       default=DEFAULT_URL,     help="Server base URL")
    parser.add_argument("--workers",   type=int, default=DEFAULT_WORKERS,  help="Concurrent clients")
    parser.add_argument("--duration",  type=int, default=DEFAULT_DURATION, help="Test duration (seconds)")
    parser.add_argument("--api-key",   default="",  help="Use existing API key (skips account creation)")
    parser.add_argument("--admin-pw",  default="CAPPVCS928906", help="Admin password for cleanup")
    args = parser.parse_args()

    base_url  = args.url.rstrip("/")
    owned_key = False
    api_key   = args.api_key
    username  = ""

    print(f"\n  CAPP Load Test")
    print(f"  Server:   {base_url}")
    print(f"  Workers:  {args.workers}")
    print(f"  Duration: {args.duration}s")

    # ── Health check ──────────────────────────────────────────────────────────
    try:
        async with httpx.AsyncClient() as c:
            r = await c.get(f"{base_url}/health", timeout=10)
        assert r.status_code == 200
        print(f"  Health:   OK (version {r.json().get('version', '?')})")
    except Exception as e:
        print(f"  ERROR: Server not reachable — {e}")
        sys.exit(1)

    # ── API key ───────────────────────────────────────────────────────────────
    if not api_key:
        print("  Creating temporary test account...")
        api_key, username = await create_test_account(base_url)
        owned_key = True
        print(f"  Test account: {username}")

    # ── Discover game IDs ─────────────────────────────────────────────────────
    print("  Fetching live games...")
    game_ids = []
    try:
        async with httpx.AsyncClient(headers={"x-api-key": api_key}) as c:
            r = await c.get(f"{base_url}/games", params={"league": "all"}, timeout=15)
        if r.status_code == 200:
            live = [(g["game_id"], g.get("league", "cfb"))
                    for g in r.json() if g.get("status") == "in"]
            game_ids = live
    except Exception:
        pass

    if game_ids:
        print(f"  Live games found: {len(game_ids)} — using real game data")
    else:
        game_ids = FALLBACK_GAME_IDS
        print(f"  No live games — using {len(game_ids)} fallback historical game IDs")

    # ── Run test ──────────────────────────────────────────────────────────────
    print(f"\n  Starting {args.workers} workers...\n")
    stats = Stats()
    stop  = asyncio.Event()

    tasks = [
        asyncio.create_task(worker(i, base_url, api_key, list(game_ids), stats, stop))
        for i in range(args.workers)
    ]
    tasks.append(asyncio.create_task(progress(stats, args.duration, stop)))

    start = time.monotonic()
    await asyncio.sleep(args.duration)
    stop.set()
    await asyncio.gather(*tasks, return_exceptions=True)
    actual_duration = time.monotonic() - start

    # ── Results ───────────────────────────────────────────────────────────────
    print(stats.summary(actual_duration))

    # ── Cleanup ───────────────────────────────────────────────────────────────
    if owned_key and username:
        await delete_test_account(base_url, username, args.admin_pw)


if __name__ == "__main__":
    asyncio.run(main())
