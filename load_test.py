"""
load_test.py - CAPP Server Game-Day Load Test
=============================================
CLI harness that simulates many distinct school clients hitting the CAPP
server the way SBENTRY does on game day.

Default behavior:
  - one API key per worker (one simulated school per worker)
  - one unique game per worker
  - GET /game/{id}/plays every 15 seconds
  - writes a timestamped JSON + TXT report after every run
"""

import argparse
import asyncio
import json
import secrets
import statistics
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import httpx

DEFAULT_URL = "https://capp-data-server.onrender.com"
DEFAULT_WORKERS = 50
DEFAULT_DURATION = 120
DEFAULT_ADMIN_PW = "CAPPVCS928906"
REPORT_DIR = Path(__file__).resolve().parent / "load_test_reports"

PLAYS_INTERVAL = 15
HTTP_TIMEOUT = 20

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
    before_started = before.get("started_at")
    after_started = after.get("started_at")
    restarted = bool(before_started and after_started and before_started != after_started)
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
class WorkerResult:
    worker_id: int
    account_name: str
    game_id: str
    league: str
    home_name: str = "-"
    away_name: str = "-"
    requests: int = 0
    plays_fetches: int = 0
    total_plays_seen: int = 0
    bytes_received: int = 0
    errors: int = 0
    last_status: str = "Waiting"
    last_error: str = ""
    latencies: list[float] = field(default_factory=list)

    def avg_latency_ms(self) -> float:
        return (sum(self.latencies) / len(self.latencies) * 1000) if self.latencies else 0.0


@dataclass
class Stats:
    latencies: list[float] = field(default_factory=list)
    errors: dict = field(default_factory=lambda: defaultdict(int))
    counts: dict = field(default_factory=lambda: defaultdict(int))
    plays_counts: list[int] = field(default_factory=list)
    bytes_by_endpoint: dict = field(default_factory=lambda: defaultdict(int))
    worker_results: dict = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def ensure_worker(self, worker_id: int, account_name: str, game_id: str, league: str):
        async with self.lock:
            self.worker_results.setdefault(
                worker_id,
                WorkerResult(
                    worker_id=worker_id,
                    account_name=account_name,
                    game_id=game_id,
                    league=league,
                ),
            )

    async def record(
        self,
        worker_id: int,
        endpoint: str,
        elapsed: float,
        status: int,
        bytes_received: int,
        play_count: int = 0,
        home_name: str = "-",
        away_name: str = "-",
        error_detail: str = "",
    ):
        async with self.lock:
            self.counts[endpoint] += 1
            self.bytes_by_endpoint[endpoint] += bytes_received

            worker = self.worker_results[worker_id]
            worker.requests += 1
            worker.bytes_received += bytes_received
            worker.last_status = f"HTTP {status}" if status else "EXC"
            if home_name != "-":
                worker.home_name = home_name
                worker.away_name = away_name

            if 200 <= status < 400:
                self.latencies.append(elapsed)
                self.plays_counts.append(play_count)
                worker.latencies.append(elapsed)
                worker.plays_fetches += 1
                worker.total_plays_seen += play_count
                worker.last_status = "OK"
            else:
                self.errors[status] += 1
                worker.errors += 1
                worker.last_error = error_detail

    def total_requests(self) -> int:
        return sum(self.counts.values())

    def total_errors(self) -> int:
        return sum(self.errors.values())

    def total_bytes(self) -> int:
        return sum(self.bytes_by_endpoint.values())

    def summary(self, duration: float, num_workers: int) -> str:
        total = self.total_requests()
        err_total = self.total_errors()
        rps = total / duration if duration else 0
        lines = [
            "",
            "=" * 64,
            "  CAPP LOAD TEST - RESULTS",
            "=" * 64,
            f"  Duration:              {duration:.1f}s",
            f"  Workers:               {num_workers}",
            f"  Total requests:        {total}",
            f"  Requests/sec:          {rps:.1f}",
            f"  Errors:                {err_total}",
            f"  Total response bytes:  {self.total_bytes():,} ({format_bytes(self.total_bytes())})",
            "",
            "  By endpoint:",
        ]
        for ep, cnt in sorted(self.counts.items()):
            ep_bytes = self.bytes_by_endpoint.get(ep, 0)
            lines.append(f"    {ep:<24} {cnt:>6} reqs   {format_bytes(ep_bytes):>10}")

        if self.plays_counts:
            avg_plays = sum(self.plays_counts) / len(self.plays_counts)
            lines += [
                "",
                f"  Plays fetches:         {len(self.plays_counts)}",
                f"  Avg plays/response:    {avg_plays:.0f}",
                f"  Total plays returned:  {sum(self.plays_counts):,}",
            ]

        if self.errors:
            lines += ["", "  Error breakdown:"]
            for code, cnt in sorted(self.errors.items()):
                label = "EXC" if code == 0 else f"HTTP {code}"
                lines.append(f"    {label}: {cnt}")

        if self.latencies:
            lines += [
                "",
                "  Latency (successful requests):",
                f"    min   {min(self.latencies) * 1000:.0f} ms",
                f"    avg   {statistics.mean(self.latencies) * 1000:.0f} ms",
                f"    p50   {percentile(self.latencies, 50) * 1000:.0f} ms",
                f"    p95   {percentile(self.latencies, 95) * 1000:.0f} ms",
                f"    p99   {percentile(self.latencies, 99) * 1000:.0f} ms",
        f"    max   {max(self.latencies) * 1000:.0f} ms",
    ]
        lines += [
            "",
            "  Server observability:",
            "    Health snapshots are included in the saved report.",
            "=" * 64,
        ]
        return "\n".join(lines)


def build_report(
    stats: Stats,
    duration: float,
    num_workers: int,
    base_url: str,
    used_shared_key: bool,
    unique_games: int,
    expected_fetches: int,
    health_before: dict,
    health_after: dict,
) -> dict:
    total_requests = stats.total_requests()
    total_errors = stats.total_errors()
    total_bytes = stats.total_bytes()
    latencies_ms = [x * 1000 for x in stats.latencies]
    workers = []
    for worker in sorted(stats.worker_results.values(), key=lambda item: item.worker_id):
        workers.append(
            {
                "worker_id": worker.worker_id + 1,
                "account_name": worker.account_name,
                "game_id": worker.game_id,
                "league": worker.league,
                "home_name": worker.home_name,
                "away_name": worker.away_name,
                "requests": worker.requests,
                "plays_fetches": worker.plays_fetches,
                "total_plays_seen": worker.total_plays_seen,
                "bytes_received": worker.bytes_received,
                "bytes_received_human": format_bytes(worker.bytes_received),
                "avg_latency_ms": round(worker.avg_latency_ms(), 1),
                "errors": worker.errors,
                "last_status": worker.last_status,
                "last_error": worker.last_error,
            }
        )

    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "server_url": base_url,
        "workers": num_workers,
        "unique_games": unique_games,
        "duration_seconds": round(duration, 2),
        "used_shared_api_key": used_shared_key,
        "polling_pattern": {
            "plays_endpoint": "GET /game/{id}/plays",
            "plays_interval_seconds": PLAYS_INTERVAL,
        },
        "totals": {
            "requests": total_requests,
            "errors": total_errors,
            "response_bytes": total_bytes,
            "response_bytes_human": format_bytes(total_bytes),
            "plays_fetches": len(stats.plays_counts),
            "plays_returned": sum(stats.plays_counts),
            "expected_play_fetches": expected_fetches,
            "requests_per_second": round(total_requests / duration, 2) if duration else 0,
        },
        "latency_ms": {
            "min": round(min(latencies_ms), 1) if latencies_ms else 0,
            "avg": round(statistics.mean(latencies_ms), 1) if latencies_ms else 0,
            "p50": round(percentile(latencies_ms, 50), 1) if latencies_ms else 0,
            "p95": round(percentile(latencies_ms, 95), 1) if latencies_ms else 0,
            "p99": round(percentile(latencies_ms, 99), 1) if latencies_ms else 0,
            "max": round(max(latencies_ms), 1) if latencies_ms else 0,
        },
        "bytes_by_endpoint": {
            endpoint: {
                "bytes": value,
                "human": format_bytes(value),
            }
            for endpoint, value in sorted(stats.bytes_by_endpoint.items())
        },
        "errors_by_status": {
            ("EXC" if code == 0 else f"HTTP {code}"): count
            for code, count in sorted(stats.errors.items())
        },
        "server_observability": {
            "health_endpoint_available": True,
            "memory_metrics_available_via_api": True,
            "note": (
                "Server memory, uptime, request counters, and fetcher cache "
                "state were captured from /health before and after the run."
            ),
        },
        "cache_policy": {
            "cache_only": True,
            "note": "This run used only game IDs already returned by /games and did not probe fallback IDs.",
        },
        "server_health_before": health_before,
        "server_health_after": health_after,
        "workers_detail": workers,
    }
    report["verdict"] = build_verdict(report)
    return report


def write_report(report: dict, summary_text: str) -> tuple[Path, Path]:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = REPORT_DIR / f"load_test_report_{stamp}.json"
    txt_path = REPORT_DIR / f"load_test_report_{stamp}.txt"
    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    txt_path.write_text(summary_text + "\n\nJSON report: " + str(json_path), encoding="utf-8")
    return json_path, txt_path


def _health_line(snapshot: dict, label: str) -> list[str]:
    if not snapshot.get("ok"):
        return [f"  {label}: unavailable ({snapshot})"]
    memory = snapshot.get("memory", {}).get("rss_bytes", 0)
    fetcher = snapshot.get("fetcher", {})
    return [
        f"  {label}: status={snapshot.get('status')} ready={snapshot.get('ready')}",
        f"    uptime={snapshot.get('uptime_seconds', 0)}s rss={format_bytes(memory)}",
        f"    cached_games={fetcher.get('games_cache_count', 0)} plays_cache={fetcher.get('plays_cache_count', 0)} initial_poll_complete={fetcher.get('initial_poll_complete')}",
    ]


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
        "Server health snapshots:",
        *_health_line(report.get("server_health_before", {}), "Before"),
        *_health_line(report.get("server_health_after", {}), "After"),
    ]
    return "\n".join(lines)


async def get(
    client: httpx.AsyncClient,
    url: str,
    stats: Stats,
    worker_id: int,
    label: str,
    **kwargs,
):
    t0 = time.perf_counter()
    try:
        r = await client.get(url, timeout=HTTP_TIMEOUT, **kwargs)
        elapsed = time.perf_counter() - t0
        payload = r.content or b""
        play_count = 0
        home_name = "-"
        away_name = "-"
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, dict):
                play_count = len(data.get("entries", []))
                home_name = data.get("home_name", "-")
                away_name = data.get("away_name", "-")
        await stats.record(
            worker_id,
            label,
            elapsed,
            r.status_code,
            len(payload),
            play_count=play_count,
            home_name=home_name,
            away_name=away_name,
            error_detail=r.text[:120] if r.status_code >= 400 else "",
        )
        return r
    except Exception as exc:
        elapsed = time.perf_counter() - t0
        await stats.record(
            worker_id,
            label,
            elapsed,
            0,
            0,
            error_detail=f"{type(exc).__name__}: {exc}",
        )
        return None


async def worker(
    worker_id: int,
    account_name: str,
    base_url: str,
    api_key: str,
    game_id: str,
    league: str,
    stats: Stats,
    stop: asyncio.Event,
):
    headers = {"x-api-key": api_key}
    plays_due = 0.0

    await stats.ensure_worker(worker_id, account_name, game_id, league)
    await asyncio.sleep(worker_id * 0.1)

    async with httpx.AsyncClient(headers=headers) as client:
        while not stop.is_set():
            now = time.monotonic()
            if now >= plays_due:
                await get(
                    client,
                    f"{base_url}/game/{game_id}/plays",
                    stats,
                    worker_id,
                    "GET /game/plays",
                    params={"league": league},
                )
                plays_due = time.monotonic() + PLAYS_INTERVAL
            await asyncio.sleep(0.25)


async def progress(stats: Stats, duration: float, stop: asyncio.Event):
    start = time.monotonic()
    while not stop.is_set():
        await asyncio.sleep(10)
        elapsed = time.monotonic() - start
        total = stats.total_requests()
        errs = stats.total_errors()
        rps = total / elapsed if elapsed else 0
        lat = f"{statistics.mean(stats.latencies) * 1000:.0f}ms avg" if stats.latencies else "-"
        pct = min(100, elapsed / duration * 100) if duration else 0
        plays = len(stats.plays_counts)
        payload = format_bytes(stats.total_bytes())
        print(
            f"  [{pct:3.0f}%] {elapsed:.0f}s | {total} reqs | "
            f"{rps:.1f} req/s | {lat} | {plays} plays fetches | "
            f"{payload} | {errs} errors",
            flush=True,
        )


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
        raise RuntimeError(f"Could not create test account: {r.status_code} {r.text}")
    data = r.json()
    return data["api_key"], username


async def create_test_accounts(base_url: str, count: int) -> list[tuple[str, str]]:
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
                print(f"  Created {made}/{count} simulated school accounts...", flush=True)

    await asyncio.gather(*[one(i) for i in range(1, count + 1)])
    accounts.sort(key=lambda item: item[1])
    return accounts


async def delete_test_account(base_url: str, username: str, admin_password: str):
    async with httpx.AsyncClient() as client:
        r = await client.delete(
            f"{base_url}/admin/api/clients/{username}",
            headers={"x-admin-token": admin_password},
            timeout=10,
        )
    if r.status_code not in (200, 204):
        raise RuntimeError(f"{username}: HTTP {r.status_code}")


async def cleanup_accounts(base_url: str, accounts: list[tuple[str, str]], admin_password: str):
    if not accounts:
        return

    failures = 0
    sem = asyncio.Semaphore(10)
    lock = asyncio.Lock()

    async def one(username: str):
        nonlocal failures
        async with sem:
            try:
                await delete_test_account(base_url, username, admin_password)
            except Exception:
                async with lock:
                    failures += 1

    await asyncio.gather(*[one(username) for _, username in accounts], return_exceptions=True)
    if failures:
        print(f"  Cleanup finished with {failures} delete failures.")
    else:
        print("  Cleanup done.")


async def main():
    parser = argparse.ArgumentParser(description="CAPP server game-day load test")
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--duration", type=int, default=DEFAULT_DURATION)
    parser.add_argument("--api-key", default="")
    parser.add_argument("--admin-pw", default=DEFAULT_ADMIN_PW)
    args = parser.parse_args()

    if args.workers <= 0 or args.duration <= 0:
        print("ERROR: workers and duration must be greater than zero.")
        sys.exit(1)

    base_url = args.url.rstrip("/")

    print(f"\n  CAPP Load Test - {args.workers} schools, each watching a unique game")
    print(f"  Server:   {base_url}")
    print(f"  Duration: {args.duration}s")
    print(f"  Pattern:  GET /game/{{id}}/plays every {PLAYS_INTERVAL}s per worker")

    health_before = await fetch_health_snapshot(base_url)
    try:
        assert health_before.get("ok")
        print(
            f"  Health:   {health_before.get('status', 'unknown')} "
            f"(version {health_before.get('version', '?')}, ready={health_before.get('ready')})"
        )
    except Exception:
        print(f"  ERROR: Server not reachable - {health_before}")
        sys.exit(1)

    created_accounts: list[tuple[str, str]] = []
    used_shared_key = bool(args.api_key)
    if used_shared_key:
        worker_accounts = [(args.api_key, f"shared-key-{i + 1:02d}") for i in range(args.workers)]
        print("  Using the provided API key for all workers.")
    else:
        print(f"  Creating {args.workers} simulated school accounts...")
        worker_accounts = await create_test_accounts(base_url, args.workers)
        created_accounts = list(worker_accounts)
        print(f"  Created {len(worker_accounts)} distinct school accounts.")

    discovery_key = worker_accounts[0][0]

    print("  Fetching games from server...")
    game_pool = []
    try:
        async with httpx.AsyncClient(headers={"x-api-key": discovery_key}, timeout=15) as client:
            r = await client.get(f"{base_url}/games", params={"league": "all"})
        if r.status_code == 200:
            game_pool = [(g["game_id"], g.get("league", "cfb")) for g in r.json() if g.get("game_id")]
    except Exception:
        game_pool = []

    if game_pool:
        print(f"  Server has {len(game_pool)} cached games - using only cached server data")
    else:
        print("  ERROR: /games returned no cached games. This load test is cache-only and will not probe fallback IDs.")
        if created_accounts:
            await cleanup_accounts(base_url, created_accounts, args.admin_pw)
        sys.exit(1)

    unique_games = []
    seen = set()
    for gid, league in game_pool:
        if gid not in seen:
            unique_games.append((gid, league))
            seen.add(gid)

    if len(unique_games) < args.workers:
        print(
            f"  ERROR: Need {args.workers} unique games for this run, "
            f"but only found {len(unique_games)}."
        )
        if created_accounts:
            await cleanup_accounts(base_url, created_accounts, args.admin_pw)
        sys.exit(1)

    assignments = unique_games[:args.workers]
    expected_fetches = args.workers * expected_play_fetches(args.duration)
    print(f"  Game assignments: {len(assignments)} unique games across {args.workers} workers")
    print(f"  Expected plays fetches: ~{expected_fetches}")
    print("  Starting workers (staggered 100ms apart)...\n")

    stats = Stats()
    stop = asyncio.Event()

    tasks = [
        asyncio.create_task(
            worker(i, worker_accounts[i][1], base_url, worker_accounts[i][0], gid, league, stats, stop)
        )
        for i, (gid, league) in enumerate(assignments)
    ]
    tasks.append(asyncio.create_task(progress(stats, args.duration, stop)))

    start = time.monotonic()
    await asyncio.sleep(args.duration)
    stop.set()
    await asyncio.gather(*tasks, return_exceptions=True)
    actual_duration = time.monotonic() - start

    health_after = await fetch_health_snapshot(base_url)

    report = build_report(
        stats,
        actual_duration,
        args.workers,
        base_url,
        used_shared_key,
        len(assignments),
        expected_fetches,
        health_before,
        health_after,
    )
    summary = report_summary(report)
    print(summary)
    json_path, txt_path = write_report(report, summary)
    print(f"  Report written: {txt_path}")
    print(f"  JSON report:    {json_path}")

    if created_accounts:
        await cleanup_accounts(base_url, created_accounts, args.admin_pw)


if __name__ == "__main__":
    asyncio.run(main())
