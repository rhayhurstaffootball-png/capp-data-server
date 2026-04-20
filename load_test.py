"""
load_test.py — CAPP Server Game-Day Load Test
==============================================
Simulates N concurrent school clients, each watching their OWN game —
exactly how real game day works: 50 schools, 50 different games.

Usage:
    python load_test.py                              # 50 workers, 120s, live server
    python load_test.py --workers 50 --duration 180
    python load_test.py --url http://localhost:8000  # local dev server
    python load_test.py --api-key <key>              # skip auto-create/cleanup

Each worker is assigned ONE game and runs the real CAPP polling loop:
    1. GET /game/{id}/version  — lightweight version check every VERSION_INTERVAL s
    2. GET /game/{id}/plays    — full play data only when version changes
    3. GET /games              — scoreboard refresh every GAMES_INTERVAL s
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
VERSION_INTERVAL = 60   # seconds between /version polls per assigned game
HTTP_TIMEOUT     = 20   # seconds per request

# ── Default config ────────────────────────────────────────────────────────────
DEFAULT_URL      = "https://capp-data-server.onrender.com"
DEFAULT_WORKERS  = 50
DEFAULT_DURATION = 120  # seconds

# Fallback: one full week of real CFB games (Week 8, 2024) — 50+ games
FALLBACK_GAME_IDS = [
    ("401628438", "cfb"), ("401628441", "cfb"), ("401628445", "cfb"),
    ("401628449", "cfb"), ("401628452", "cfb"), ("401628455", "cfb"),
    ("401628458", "cfb"), ("401628461", "cfb"), ("401628464", "cfb"),
    ("401628467", "cfb"), ("401628470", "cfb"), ("401628473", "cfb"),
    ("401628476", "cfb"), ("401628479", "cfb"), ("401628482", "cfb"),
    ("401628485", "cfb"), ("401628488", "cfb"), ("401628491", "cfb"),
    ("401628494", "cfb"), ("401628497", "cfb"), ("401628500", "cfb"),
    ("401628503", "cfb"), ("401628506", "cfb"), ("401628509", "cfb"),
    ("401628512", "cfb"), ("401628515", "cfb"), ("401628518", "cfb"),
    ("401628521", "cfb"), ("401628524", "cfb"), ("401628527", "cfb"),
    ("401628530", "cfb"), ("401628533", "cfb"), ("401628536", "cfb"),
    ("401628539", "cfb"), ("401628542", "cfb"), ("401628545", "cfb"),
    ("401628548", "cfb"), ("401628551", "cfb"), ("401628554", "cfb"),
    ("401628557", "cfb"), ("401628560", "cfb"), ("401628563", "cfb"),
    ("401628566", "cfb"), ("401628569", "cfb"), ("401628572", "cfb"),
    ("401628575", "cfb"), ("401628578", "cfb"), ("401628581", "cfb"),
    ("401628584", "cfb"), ("401628587", "cfb"), ("401628590", "cfb"),
    ("401628593", "cfb"), ("401628596", "cfb"), ("401628599", "cfb"),
    ("401628602", "cfb"),
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

    def summary(self, duration: float, num_workers: int, num_games: int) -> str:
        total     = sum(self.counts.values())
        err_total = sum(self.errors.values())
        rps       = total / duration if duration else 0
        lines     = [
            "",
            "=" * 58,
            "  CAPP LOAD TEST — RESULTS",
            "=" * 58,
            f"  Duration:        {duration:.1f}s",
            f"  Workers:         {num_workers} (1 school per worker)",
            f"  Unique games:    {num_games}",
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
    except Exception:
        elapsed = time.perf_counter() - t0
        await stats.record(label, elapsed, 0)
        return None


# ── Single worker — simulates ONE school watching ONE game ────────────────────
async def worker(worker_id: int, base_url: str, api_key: str,
                 game_id: str, league: str,
                 stats: Stats, stop: asyncio.Event):
    """Each worker is assigned exactly one game — just like a real school."""
    headers       = {"x-api-key": api_key}
    last_version  = -1   # -1 ensures first version check always triggers a plays fetch
    games_due     = 0.0
    version_due   = 0.0

    # Stagger startup so all 50 workers don't fire simultaneously
    await asyncio.sleep(worker_id * 0.1)

    async with httpx.AsyncClient(headers=headers) as client:
        while not stop.is_set():
            now = time.monotonic()

            # ── Check version of this worker's assigned game ──────────────────
            if now >= version_due:
                ver_data = await get(
                    client, f"{base_url}/game/{game_id}/version",
                    stats, "GET /game/version")
                if ver_data:
                    new_ver = ver_data.get("fetched_at", 0)
                    if new_ver != last_version:
                        last_version = new_ver
                        await get(
                            client, f"{base_url}/game/{game_id}/plays",
                            stats, "GET /game/plays",
                            params={"league": league})
                version_due = time.monotonic() + VERSION_INTERVAL

            # ── Refresh scoreboard (all teams do this to track other scores) ──
            if now >= games_due:
                await get(client, f"{base_url}/games", stats, "GET /games",
                          params={"league": "all"})
                games_due = time.monotonic() + GAMES_INTERVAL

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
        lat   = (f"{statistics.mean(stats.latencies)*1000:.0f}ms avg"
                 if stats.latencies else "—")
        pct   = min(100, elapsed / duration * 100)
        print(f"  [{pct:3.0f}%] {elapsed:.0f}s | {total} reqs | "
              f"{rps:.1f} req/s | {lat} | {errs} errors", flush=True)


# ── Account management ────────────────────────────────────────────────────────
async def create_test_account(base_url: str) -> tuple[str, str]:
    username = f"loadtest_{secrets.token_hex(4)}@test.invalid"
    password = secrets.token_hex(16)
    async with httpx.AsyncClient() as client:
        r = await client.post(f"{base_url}/register", json={
            "school":   "Load Test School",
            "email":    username,
            "password": password,
        }, timeout=15)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Could not create test account: {r.status_code} {r.text}")
    data = r.json()
    return data["api_key"], username


async def delete_test_account(base_url: str, username: str, admin_password: str):
    async with httpx.AsyncClient() as client:
        r = await client.delete(
            f"{base_url}/admin/api/clients/{username}",
            headers={"x-admin-token": admin_password},
            timeout=10,
        )
    if r.status_code in (200, 204):
        print(f"  Test account '{username}' cleaned up.")
    else:
        print(f"  Warning: could not delete test account ({r.status_code})")


# ── Entry point ───────────────────────────────────────────────────────────────
async def main():
    parser = argparse.ArgumentParser(description="CAPP server game-day load test")
    parser.add_argument("--url",      default=DEFAULT_URL)
    parser.add_argument("--workers",  type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--duration", type=int, default=DEFAULT_DURATION)
    parser.add_argument("--api-key",  default="")
    parser.add_argument("--admin-pw", default="CAPPVCS928906")
    args = parser.parse_args()

    base_url = args.url.rstrip("/")
    api_key  = args.api_key
    username = ""

    print(f"\n  CAPP Load Test  —  {args.workers} schools, each watching their own game")
    print(f"  Server:   {base_url}")
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
        print(f"  Test account: {username}")

    # ── Discover game IDs — one per worker ───────────────────────────────────
    print("  Fetching live games...")
    game_pool = []
    try:
        async with httpx.AsyncClient(headers={"x-api-key": api_key}) as c:
            r = await c.get(f"{base_url}/games", params={"league": "all"}, timeout=15)
        if r.status_code == 200:
            game_pool = [(g["game_id"], g.get("league", "cfb"))
                         for g in r.json() if g.get("status") == "in"]
    except Exception:
        pass

    if game_pool:
        print(f"  Live games found: {len(game_pool)}")
    else:
        game_pool = FALLBACK_GAME_IDS
        print(f"  No live games — using {len(game_pool)} historical fallback game IDs")

    # Assign one unique game per worker (cycle if more workers than games)
    assignments = [game_pool[i % len(game_pool)] for i in range(args.workers)]
    unique_games = len(set(gid for gid, _ in assignments))
    print(f"  Game assignments: {unique_games} unique games across {args.workers} workers")

    # ── Run test ──────────────────────────────────────────────────────────────
    print(f"\n  Starting workers (staggered 100ms apart)...\n")
    stats = Stats()
    stop  = asyncio.Event()

    tasks = [
        asyncio.create_task(
            worker(i, base_url, api_key, gid, league, stats, stop)
        )
        for i, (gid, league) in enumerate(assignments)
    ]
    tasks.append(asyncio.create_task(progress(stats, args.duration, stop)))

    start = time.monotonic()
    await asyncio.sleep(args.duration)
    stop.set()
    await asyncio.gather(*tasks, return_exceptions=True)
    actual_duration = time.monotonic() - start

    print(stats.summary(actual_duration, args.workers, unique_games))

    if username:
        await delete_test_account(base_url, username, args.admin_pw)


if __name__ == "__main__":
    asyncio.run(main())
