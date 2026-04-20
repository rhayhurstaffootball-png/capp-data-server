"""
load_test_gui.py — CAPP Server Load Test (GUI)
===============================================
Visual game-day simulation: 50 schools, each watching their own game.
Shows live per-school status — team names, play counts, latency, status.

Run:
    python load_test_gui.py
"""

import asyncio
import secrets
import threading
import time
import tkinter as tk
from tkinter import ttk
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

import httpx

# ── Config ────────────────────────────────────────────────────────────────────
DEFAULT_URL      = "https://capp-data-server.onrender.com"
DEFAULT_WORKERS  = 50
DEFAULT_DURATION = 120
ADMIN_PASSWORD   = "CAPPVCS928906"

GAMES_INTERVAL   = 30
VERSION_INTERVAL = 60
HTTP_TIMEOUT     = 20

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

# ── Colors ────────────────────────────────────────────────────────────────────
BG          = "#070a0f"
BG2         = "#0d1117"
BG3         = "#1a2230"
BORDER      = "#2c3b55"
ACCENT      = "#3a7ebf"
TEXT        = "#e2e8f0"
TEXT_DIM    = "#8b95a1"
GREEN       = "#86efac"
GREEN_BG    = "#14532d"
YELLOW      = "#fcd34d"
YELLOW_BG   = "#78350f"
RED         = "#fca5a5"
RED_BG      = "#7f1d1d"
BLUE_LIGHT  = "#93c5fd"


# ── Shared worker state (written by asyncio thread, read by tkinter) ──────────
@dataclass
class WorkerState:
    worker_id:     int    = 0
    game_id:       str    = ""
    league:        str    = "cfb"
    home_name:     str    = "—"
    away_name:     str    = "—"
    status:        str    = "Waiting"
    version_checks: int   = 0
    plays_fetched: int    = 0
    play_count:    int    = 0
    last_latency:  float  = 0.0
    errors:        int    = 0
    last_error:    str    = ""
    last_active:   float  = 0.0


# ── Global shared state ───────────────────────────────────────────────────────
worker_states: list[WorkerState] = []
total_requests  = 0
total_errors    = 0
all_latencies:  list[float] = []
error_log:      list[str]   = []   # timestamped error messages for the log panel
state_lock      = threading.Lock()
test_running    = False
test_start_time = 0.0
owned_username  = ""
owned_api_key   = ""


# ── Async worker ──────────────────────────────────────────────────────────────
def _log_error(w: WorkerState, endpoint: str, detail: str):
    """Append a timestamped error entry to the global error log (called under state_lock)."""
    ts      = time.strftime("%H:%M:%S")
    team    = f"{w.home_name} vs {w.away_name}" if w.home_name != "—" else w.game_id
    msg     = f"[{ts}]  Worker {w.worker_id+1:>2}  {team:<30}  {endpoint:<22}  {detail}"
    error_log.append(msg)
    w.last_error = detail


async def run_worker(w: WorkerState, base_url: str, api_key: str,
                     stop: asyncio.Event):
    global total_requests, total_errors, all_latencies

    headers      = {"x-api-key": api_key}
    last_version = 0
    version_due  = 0.0
    games_due    = 0.0

    await asyncio.sleep(w.worker_id * 0.1)

    async with httpx.AsyncClient(headers=headers, timeout=HTTP_TIMEOUT) as client:
        while not stop.is_set():
            now = time.monotonic()

            # ── Version check + plays fetch ───────────────────────────────────
            if now >= version_due:
                with state_lock:
                    w.status = "Checking version"
                t0 = time.perf_counter()
                try:
                    r = await client.get(
                        f"{base_url}/game/{w.game_id}/version")
                    elapsed = time.perf_counter() - t0
                    with state_lock:
                        total_requests += 1
                        w.last_latency = elapsed
                        w.last_active  = time.time()
                        if r.status_code == 200:
                            all_latencies.append(elapsed)
                            w.version_checks += 1
                            data = r.json()
                            new_ver = data.get("fetched_at", 0)
                            if new_ver != last_version:
                                last_version = new_ver
                                w.status = "Fetching plays"
                        else:
                            w.errors += 1
                            total_errors += 1
                            w.status = f"Error {r.status_code}"
                            _log_error(w, "GET /game/version",
                                       f"HTTP {r.status_code} — {r.text[:120]}")
                except Exception as e:
                    elapsed = time.perf_counter() - t0
                    with state_lock:
                        total_requests += 1
                        total_errors   += 1
                        w.errors       += 1
                        w.status       = "Timeout"
                        _log_error(w, "GET /game/version", f"{type(e).__name__}: {e}")

                # Fetch plays if version changed
                if w.status == "Fetching plays":
                    t0 = time.perf_counter()
                    try:
                        r = await client.get(
                            f"{base_url}/game/{w.game_id}/plays",
                            params={"league": w.league})
                        elapsed = time.perf_counter() - t0
                        with state_lock:
                            total_requests += 1
                            w.last_latency  = elapsed
                            w.last_active   = time.time()
                            if r.status_code == 200:
                                all_latencies.append(elapsed)
                                data = r.json()
                                w.plays_fetched += 1
                                w.play_count     = len(data.get("entries", []))
                                if w.home_name == "—":
                                    w.home_name = data.get("home_name", "—")
                                    w.away_name = data.get("away_name", "—")
                                w.status = "Active"
                            else:
                                w.errors += 1
                                total_errors += 1
                                w.status = f"Error {r.status_code}"
                                _log_error(w, "GET /game/plays",
                                           f"HTTP {r.status_code} — {r.text[:120]}")
                    except Exception as e:
                        with state_lock:
                            total_requests += 1
                            total_errors   += 1
                            w.errors       += 1
                            w.status       = "Timeout"
                            _log_error(w, "GET /game/plays", f"{type(e).__name__}: {e}")
                else:
                    with state_lock:
                        if w.status not in ("Error", "Timeout") and not w.status.startswith("Error"):
                            w.status = "Polling"

                version_due = time.monotonic() + VERSION_INTERVAL

            # ── Scoreboard refresh ────────────────────────────────────────────
            if now >= games_due:
                with state_lock:
                    if w.status == "Polling":
                        w.status = "Refreshing scoreboard"
                t0 = time.perf_counter()
                try:
                    r = await client.get(f"{base_url}/games",
                                         params={"league": "all"})
                    elapsed = time.perf_counter() - t0
                    with state_lock:
                        total_requests += 1
                        w.last_latency  = elapsed
                        w.last_active   = time.time()
                        if r.status_code == 200:
                            all_latencies.append(elapsed)
                            if w.status == "Refreshing scoreboard":
                                w.status = "Polling"
                        else:
                            w.errors += 1
                            total_errors += 1
                            _log_error(w, "GET /games",
                                       f"HTTP {r.status_code} — {r.text[:120]}")
                except Exception as e:
                    with state_lock:
                        total_requests += 1
                        total_errors   += 1
                        w.errors       += 1
                        _log_error(w, "GET /games", f"{type(e).__name__}: {e}")
                games_due = time.monotonic() + GAMES_INTERVAL

            await asyncio.sleep(0.25)

    with state_lock:
        w.status = "Done"


# ── Account helpers ───────────────────────────────────────────────────────────
async def create_test_account(base_url: str) -> tuple[str, str]:
    username = f"loadtest_{secrets.token_hex(4)}@test.invalid"
    password = secrets.token_hex(16)
    async with httpx.AsyncClient() as c:
        r = await c.post(f"{base_url}/register", json={
            "school": "Load Test School",
            "email": username, "password": password,
        }, timeout=15)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Registration failed: {r.status_code} {r.text}")
    d = r.json()
    return d["api_key"], username


async def delete_test_account(base_url: str, username: str):
    async with httpx.AsyncClient() as c:
        await c.delete(
            f"{base_url}/admin/api/clients/{username}",
            headers={"x-admin-token": ADMIN_PASSWORD}, timeout=10)


async def fetch_live_games(base_url: str, api_key: str) -> list:
    try:
        async with httpx.AsyncClient(headers={"x-api-key": api_key}) as c:
            r = await c.get(f"{base_url}/games",
                            params={"league": "all"}, timeout=15)
        if r.status_code == 200:
            return [(g["game_id"], g.get("league", "cfb"))
                    for g in r.json() if g.get("status") == "in"]
    except Exception:
        pass
    return []


# ── Main async runner ─────────────────────────────────────────────────────────
async def async_main(base_url: str, num_workers: int, duration: int,
                     api_key_override: str, log_fn, done_fn):
    global test_running, test_start_time, owned_username, owned_api_key

    api_key  = api_key_override
    username = ""

    log_fn("Checking server health...")
    try:
        async with httpx.AsyncClient() as c:
            r = await c.get(f"{base_url}/health", timeout=10)
        assert r.status_code == 200
        log_fn(f"Server OK  (version {r.json().get('version','?')})")
    except Exception as e:
        log_fn(f"ERROR: Server unreachable — {e}")
        done_fn()
        return

    if not api_key:
        log_fn("Creating temporary test account...")
        try:
            api_key, username = await create_test_account(base_url)
            with state_lock:
                owned_username = username
                owned_api_key  = api_key
            log_fn(f"Test account: {username}")
        except Exception as e:
            log_fn(f"ERROR: {e}")
            done_fn()
            return

    log_fn("Fetching live games...")
    game_pool = await fetch_live_games(base_url, api_key)
    if game_pool:
        log_fn(f"Live games: {len(game_pool)} found")
    else:
        game_pool = FALLBACK_GAME_IDS
        log_fn(f"No live games — using {len(game_pool)} historical fallback IDs")

    assignments = [game_pool[i % len(game_pool)] for i in range(num_workers)]
    unique = len(set(g for g, _ in assignments))
    log_fn(f"Assigning {unique} unique games to {num_workers} workers...")

    with state_lock:
        worker_states.clear()
        for i, (gid, league) in enumerate(assignments):
            ws = WorkerState(worker_id=i, game_id=gid, league=league)
            worker_states.append(ws)

    stop = asyncio.Event()
    test_start_time = time.time()
    test_running    = True

    log_fn(f"Running {num_workers} workers for {duration}s...\n")
    tasks = [asyncio.create_task(run_worker(w, base_url, api_key, stop))
             for w in worker_states]

    await asyncio.sleep(duration)
    stop.set()
    await asyncio.gather(*tasks, return_exceptions=True)

    test_running = False
    log_fn("\nTest complete.")

    if username:
        log_fn("Cleaning up test account...")
        await delete_test_account(base_url, username)
        log_fn("Done.")

    done_fn()


def run_async_test(base_url, workers, duration, api_key, log_fn, done_fn):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(
        async_main(base_url, workers, duration, api_key, log_fn, done_fn))
    loop.close()


# ── GUI ───────────────────────────────────────────────────────────────────────
class LoadTestApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("CAPP Server — Game Day Load Test")
        self.configure(bg=BG)
        self.geometry("1100x750")
        self.resizable(True, True)
        self._build_ui()
        self._thread: Optional[threading.Thread] = None
        self._running = False

    # ── UI construction ───────────────────────────────────────────────────────
    def _build_ui(self):
        self._style()

        # ── Top bar ───────────────────────────────────────────────────────────
        top = tk.Frame(self, bg=BG2, pady=14)
        top.pack(fill="x", padx=0)

        tk.Label(top, text="CAPP  LOAD TEST", bg=BG2, fg=ACCENT,
                 font=("Segoe UI", 14, "bold"), padx=20).pack(side="left")

        # Config fields
        cfg = tk.Frame(top, bg=BG2)
        cfg.pack(side="left", padx=20)

        def labeled(parent, label, default, width=28):
            f = tk.Frame(parent, bg=BG2)
            f.pack(side="left", padx=8)
            tk.Label(f, text=label, bg=BG2, fg=TEXT_DIM,
                     font=("Segoe UI", 9)).pack(anchor="w")
            e = tk.Entry(f, width=width, bg=BG3, fg=TEXT, relief="flat",
                         insertbackground=TEXT, font=("Segoe UI", 10),
                         highlightthickness=1, highlightbackground=BORDER,
                         highlightcolor=ACCENT)
            e.insert(0, default)
            e.pack()
            return e

        self.url_entry      = labeled(cfg, "Server URL",  DEFAULT_URL, 36)
        self.workers_entry  = labeled(cfg, "Workers",     str(DEFAULT_WORKERS), 6)
        self.duration_entry = labeled(cfg, "Duration (s)", str(DEFAULT_DURATION), 6)
        self.apikey_entry   = labeled(cfg, "API Key (optional)", "", 22)

        # Buttons
        btn_frame = tk.Frame(top, bg=BG2)
        btn_frame.pack(side="right", padx=20)

        self.start_btn = tk.Button(
            btn_frame, text="▶  START", command=self._start,
            bg=ACCENT, fg="white", font=("Segoe UI", 11, "bold"),
            relief="flat", padx=16, pady=6, cursor="hand2",
            activebackground="#4a8ecf", activeforeground="white")
        self.start_btn.pack(side="left", padx=6)

        self.stop_btn = tk.Button(
            btn_frame, text="■  STOP", command=self._stop,
            bg=RED_BG, fg=RED, font=("Segoe UI", 11, "bold"),
            relief="flat", padx=16, pady=6, cursor="hand2",
            state="disabled", activebackground="#991b1b", activeforeground=RED)
        self.stop_btn.pack(side="left", padx=6)

        # ── Stats bar ─────────────────────────────────────────────────────────
        stats_frame = tk.Frame(self, bg=BG3, pady=8)
        stats_frame.pack(fill="x")

        def stat_label(parent, key):
            f = tk.Frame(parent, bg=BG3, padx=18)
            f.pack(side="left")
            tk.Label(f, text=key, bg=BG3, fg=TEXT_DIM,
                     font=("Segoe UI", 8, "bold")).pack()
            lbl = tk.Label(f, text="—", bg=BG3, fg=BLUE_LIGHT,
                           font=("Segoe UI", 13, "bold"))
            lbl.pack()
            return lbl

        self.lbl_elapsed   = stat_label(stats_frame, "ELAPSED")
        self.lbl_rps       = stat_label(stats_frame, "REQ / SEC")
        self.lbl_total     = stat_label(stats_frame, "TOTAL REQS")
        self.lbl_errors    = stat_label(stats_frame, "ERRORS")
        self.lbl_avg_lat   = stat_label(stats_frame, "AVG LATENCY")
        self.lbl_p95       = stat_label(stats_frame, "P95 LATENCY")
        self.lbl_plays     = stat_label(stats_frame, "TOTAL PLAYS")
        self.lbl_active    = stat_label(stats_frame, "ACTIVE WORKERS")

        # Progress bar
        prog_frame = tk.Frame(self, bg=BG, pady=4, padx=12)
        prog_frame.pack(fill="x")
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(
            prog_frame, variable=self.progress_var,
            maximum=100, style="CAPP.Horizontal.TProgressbar")
        self.progress_bar.pack(fill="x")

        # ── Table ─────────────────────────────────────────────────────────────
        table_frame = tk.Frame(self, bg=BG)
        table_frame.pack(fill="both", expand=True, padx=12, pady=6)

        cols = ("#", "Home Team", "Away Team", "Game ID", "League",
                "Status", "Ver Checks", "Plays Fetched", "Play Count",
                "Latency", "Errors", "Last Error")
        self.tree = ttk.Treeview(table_frame, columns=cols,
                                 show="headings", style="CAPP.Treeview")

        widths = [40, 150, 150, 100, 55, 140, 75, 85, 75, 75, 55, 280]
        for col, w in zip(cols, widths):
            self.tree.heading(col, text=col)
            self.tree.column(col, width=w, anchor="center", stretch=False)
        self.tree.column("Home Team",  anchor="w")
        self.tree.column("Away Team",  anchor="w")
        self.tree.column("Status",     anchor="w")
        self.tree.column("Last Error", anchor="w")

        vsb = ttk.Scrollbar(table_frame, orient="vertical",
                             command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y")
        self.tree.pack(fill="both", expand=True)

        # Row tags for status colors
        self.tree.tag_configure("active",   background=GREEN_BG,  foreground=GREEN)
        self.tree.tag_configure("fetching", background="#1e3a5f",  foreground=BLUE_LIGHT)
        self.tree.tag_configure("polling",  background=BG3,        foreground=TEXT)
        self.tree.tag_configure("waiting",  background=BG2,        foreground=TEXT_DIM)
        self.tree.tag_configure("error",    background=RED_BG,     foreground=RED)
        self.tree.tag_configure("done",     background=BG2,        foreground=TEXT_DIM)

        # ── Error log panel ───────────────────────────────────────────────────
        err_outer = tk.Frame(self, bg=BG2)
        err_outer.pack(fill="x", padx=12, pady=(0, 6))

        tk.Label(err_outer, text="ERROR LOG", bg=BG2, fg=TEXT_DIM,
                 font=("Segoe UI", 8, "bold"), padx=6, pady=3).pack(anchor="w")

        err_inner = tk.Frame(err_outer, bg=BG2)
        err_inner.pack(fill="x")

        self.err_text = tk.Text(
            err_inner, height=5, bg=BG2, fg=RED,
            font=("Consolas", 9), relief="flat",
            state="disabled", wrap="none",
            highlightthickness=1, highlightbackground=BORDER)
        err_scroll = ttk.Scrollbar(err_inner, orient="vertical",
                                   command=self.err_text.yview)
        self.err_text.configure(yscrollcommand=err_scroll.set)
        err_scroll.pack(side="right", fill="y")
        self.err_text.pack(fill="x")

        # ── Status strip ──────────────────────────────────────────────────────
        log_frame = tk.Frame(self, bg=BG2, pady=4)
        log_frame.pack(fill="x", padx=12, pady=(0, 8))
        self.log_var = tk.StringVar(value="Ready.")
        tk.Label(log_frame, textvariable=self.log_var, bg=BG2, fg=TEXT_DIM,
                 font=("Consolas", 10), anchor="w", padx=8).pack(fill="x")
        self._err_log_len = 0

    def _style(self):
        s = ttk.Style(self)
        s.theme_use("clam")
        s.configure("CAPP.Treeview",
                     background=BG3, foreground=TEXT,
                     fieldbackground=BG3, rowheight=24,
                     font=("Consolas", 10))
        s.configure("CAPP.Treeview.Heading",
                     background=BG2, foreground=TEXT_DIM,
                     font=("Segoe UI", 9, "bold"), relief="flat")
        s.map("CAPP.Treeview", background=[("selected", ACCENT)])
        s.configure("CAPP.Horizontal.TProgressbar",
                     troughcolor=BG3, background=ACCENT,
                     darkcolor=ACCENT, lightcolor=ACCENT, bordercolor=BG3)

    # ── Table helpers ─────────────────────────────────────────────────────────
    def _populate_table(self, n):
        self.tree.delete(*self.tree.get_children())
        self._err_log_len = 0
        self.err_text.config(state="normal")
        self.err_text.delete("1.0", "end")
        self.err_text.config(state="disabled")
        for i in range(n):
            self.tree.insert("", "end", iid=str(i),
                             values=(i + 1, "—", "—", "—", "—",
                                     "Waiting", 0, 0, 0, "—", 0, ""),
                             tags=("waiting",))

    def _tag_for(self, status: str) -> str:
        s = status.lower()
        if "active"    in s: return "active"
        if "fetch"     in s: return "fetching"
        if "poll"      in s or "scoreboard" in s or "version" in s: return "polling"
        if "error"     in s or "timeout"    in s: return "error"
        if "done"      in s: return "done"
        return "waiting"

    def _update_row(self, ws: WorkerState):
        lat = f"{ws.last_latency*1000:.0f} ms" if ws.last_latency else "—"
        vals = (
            ws.worker_id + 1,
            ws.home_name, ws.away_name,
            ws.game_id, ws.league.upper(),
            ws.status,
            ws.version_checks, ws.plays_fetched,
            ws.play_count if ws.play_count else "—",
            lat, ws.errors,
            ws.last_error,
        )
        tag = self._tag_for(ws.status)
        try:
            self.tree.item(str(ws.worker_id), values=vals, tags=(tag,))
        except Exception:
            pass

    # ── Controls ──────────────────────────────────────────────────────────────
    def _start(self):
        if self._running:
            return
        self._running     = True
        self._stop_signal = False

        base_url = self.url_entry.get().strip().rstrip("/")
        try:
            workers  = int(self.workers_entry.get())
            duration = int(self.duration_entry.get())
        except ValueError:
            self.log_var.set("ERROR: Workers and Duration must be numbers.")
            self._running = False
            return
        api_key  = self.apikey_entry.get().strip()

        self.start_btn.config(state="disabled")
        self.stop_btn.config(state="normal")

        self._populate_table(workers)
        self._duration = duration
        self._start_ts = None

        self._thread = threading.Thread(
            target=run_async_test,
            args=(base_url, workers, duration, api_key,
                  self._log, self._on_done),
            daemon=True)
        self._thread.start()
        self._poll_ui()

    def _stop(self):
        # Signal is handled by the async stop event timing out naturally;
        # we just cut the duration short by replacing with a very small sleep.
        self._stop_signal = True
        self.log_var.set("Stopping...")
        self.stop_btn.config(state="disabled")

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

    # ── Live UI refresh ───────────────────────────────────────────────────────
    def _poll_ui(self):
        if not worker_states:
            if self._running:
                self.after(200, self._poll_ui)
            return

        if self._start_ts is None and test_start_time:
            self._start_ts = test_start_time

        self._refresh_stats()

        with state_lock:
            states_copy = list(worker_states)
            new_errors  = list(error_log[self._err_log_len:])
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
        now = time.time()
        elapsed = (now - test_start_time) if test_start_time else 0

        with state_lock:
            reqs   = total_requests
            errs   = total_errors
            lats   = list(all_latencies)
            states = list(worker_states)

        rps     = reqs / elapsed if elapsed > 0 else 0
        avg_lat = (sum(lats) / len(lats) * 1000) if lats else 0
        p95     = 0
        if lats:
            s = sorted(lats)
            p95 = s[int(len(s) * 0.95)] * 1000

        total_plays  = sum(w.play_count    for w in states)
        active_count = sum(1 for w in states if w.status not in ("Waiting", "Done", ""))

        self.lbl_elapsed.config(text=f"{elapsed:.0f}s")
        self.lbl_rps.config(text=f"{rps:.1f}")
        self.lbl_total.config(text=str(reqs))
        self.lbl_errors.config(
            text=str(errs), fg=RED if errs else GREEN)
        self.lbl_avg_lat.config(text=f"{avg_lat:.0f} ms" if avg_lat else "—")
        self.lbl_p95.config(text=f"{p95:.0f} ms" if p95 else "—")
        self.lbl_plays.config(text=str(total_plays))
        self.lbl_active.config(text=str(active_count))

        if self._duration and test_start_time:
            pct = min(100, elapsed / self._duration * 100)
            self.progress_var.set(pct)


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app = LoadTestApp()
    app.mainloop()
