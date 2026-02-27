"""
CAPP QC Monitor
Standalone quality control dashboard for monitoring live game data
from the CAPP data server. Detects anomalies and alerts the operator.
"""

import tkinter as tk
from tkinter import ttk
import customtkinter as ctk
import threading
import time
import requests
import winsound
from datetime import datetime

SERVER_URL   = "https://capp-data-server.onrender.com"
POLL_INTERVAL = 30

ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")

BG_DEEP  = "#06080c"
BG_MID   = "#0e1116"
BG_CARD  = "#161b22"
ACCENT   = "#3a7ebf"
BORDER   = "#2c3b55"
MUTED    = "#8b95a1"
WHITE    = "#ffffff"
GREEN    = "#2ea043"
ORANGE   = "#d97706"
RED      = "#cf3130"

# ============================================================
# Anomaly Detection Engine
# ============================================================

class AnomalyChecker:
    """Runs all QC checks on a list of CAPP-mapped play entries."""

    # Valid positive score increments:
    #   0 = no score, 1 = EP good, 2 = 2PT conversion, 3 = safety/FG-unlikely but valid
    #   6 = TD only (EP follows separately), 7 = TD+EP bundled by ESPN, 8 = TD+2PT bundled
    VALID_INCREMENTS = {0, 1, 2, 3, 6, 7, 8}

    # These negative deltas are expected lag artifacts when ESPN bundles TD+EP/2PT into
    # a single score update — they are NOT genuine score regressions.
    BUNDLED_LAG_ARTIFACTS = {-7, -8}

    STUCK_THRESHOLD  = 4

    def check(self, entries, home_name="Home", away_name="Away"):
        issues = []
        if not entries:
            return issues
        self._check_stuck_clock(entries, issues)
        self._check_score_jumps(entries, home_name, away_name, issues)
        self._check_missing_ep(entries, home_name, away_name, issues)
        self._check_zero_field_position(entries, issues)
        return issues

    def _check_stuck_clock(self, entries, issues):
        streak = 1
        for i in range(1, len(entries)):
            c = entries[i]
            p = entries[i - 1]
            if (c.get("clock") == p.get("clock")
                    and c.get("quarter") == p.get("quarter")
                    and str(c.get("down", "")) not in ("KO", "EP", "2PT")):
                streak += 1
                if streak == self.STUCK_THRESHOLD:
                    issues.append({
                        "severity": "WARNING",
                        "type": "stuck_clock",
                        "message": f"Clock stuck at {c.get('clock')} for {streak}+ plays in Q{c.get('quarter')}",
                        "play_index": i,
                    })
            else:
                streak = 1

    def _check_score_jumps(self, entries, home_name, away_name, issues):
        for i in range(1, len(entries)):
            hd = entries[i].get("home_score", 0) - entries[i - 1].get("home_score", 0)
            ad = entries[i].get("away_score", 0) - entries[i - 1].get("away_score", 0)
            for delta, team in ((hd, home_name), (ad, away_name)):
                if delta == 0:
                    continue
                if delta in self.BUNDLED_LAG_ARTIFACTS:
                    # Lag mirror of bundled TD+EP/2PT — expected data artifact, not a real regression
                    issues.append({"severity": "INFO", "type": "bundled_score_artifact",
                        "message": f"{team} bundled-score lag artifact ({delta:+d}) at play #{i + 1}",
                        "play_index": i})
                elif delta < 0:
                    issues.append({"severity": "ERROR", "type": "score_regression",
                        "message": f"{team} score decreased by {abs(delta)} at play #{i + 1}",
                        "play_index": i})
                elif delta not in self.VALID_INCREMENTS:
                    issues.append({"severity": "WARNING", "type": "invalid_score_jump",
                        "message": f"{team} score jumped by {delta} at play #{i + 1} (unexpected value)",
                        "play_index": i})

    def _check_missing_ep(self, entries, home_name, away_name, issues):
        for i in range(1, len(entries)):
            hd = entries[i].get("home_score", 0) - entries[i - 1].get("home_score", 0)
            ad = entries[i].get("away_score", 0) - entries[i - 1].get("away_score", 0)
            if hd == 6 or ad == 6:
                scorer = home_name if hd == 6 else away_name
                n1 = str(entries[i].get("down", "")) if i < len(entries) else ""
                n2 = str(entries[i + 1].get("down", "")) if i + 1 < len(entries) else ""
                if n1 not in ("EP", "2PT") and n2 not in ("EP", "2PT"):
                    # If the +6 delta lands on a KO, the missing EP is on the
                    # preceding TD — point to that row instead of the kickoff.
                    flag_idx = i - 1 if n1 == "KO" and i > 0 else i
                    issues.append({"severity": "WARNING", "type": "missing_ep",
                        "message": f"TD by {scorer} at play #{flag_idx + 1} — no EP or 2PT row follows",
                        "play_index": flag_idx})

    def _check_zero_field_position(self, entries, issues):
        count = sum(
            1 for e in entries
            if e.get("field_position") == 0
            and str(e.get("down", "")) in ("1", "2", "3", "4")
        )
        if count > 3:
            issues.append({"severity": "INFO", "type": "missing_fp",
                "message": f"{count} scrimmage plays have field_position=0 (missing data)",
                "play_index": 0})


# ============================================================
# QC Monitor Application
# ============================================================

class QCMonitor:
    def __init__(self, root):
        self.root = root
        self.root.title("CAPP QC Monitor")
        self.root.geometry("1340x820")
        self.root.configure(bg=BG_DEEP)

        self._monitored    = {}        # game_id -> game dict
        self._play_counts  = {}        # game_id -> int
        self._game_entries = {}        # game_id -> entries list
        self._seen_keys    = set()     # dedup alert keys
        self._all_alerts   = []        # list of alert dicts
        self._paused       = False
        self._selected_id  = None
        self._checker      = AnomalyChecker()
        self._game_iid_map = {}        # iid -> game_id
        self._qc_game_list         = []    # all games from last historical QC run
        self._qc_games_with_issues = set() # game IDs that had ERROR or WARNING
        self._filter_issues_only   = False

        self._build_ui()
        self._start_polling()

    # ─── UI Construction ─────────────────────────────────────

    def _build_ui(self):
        # Header
        hdr = ctk.CTkFrame(self.root, fg_color=BG_CARD, corner_radius=0, height=62)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)

        ctk.CTkLabel(hdr, text="CAPP QC Monitor",
                     font=ctk.CTkFont("Segoe UI", 22, "bold"),
                     text_color=WHITE).pack(side="left", padx=24, pady=14)

        self.alert_badge = ctk.CTkLabel(hdr, text="",
            font=ctk.CTkFont("Segoe UI", 13, "bold"), text_color=RED)
        self.alert_badge.pack(side="right", padx=20)

        self.pause_btn = ctk.CTkButton(hdr, text="Pause",
            command=self._toggle_pause,
            font=ctk.CTkFont("Segoe UI", 13, "bold"),
            fg_color=ORANGE, hover_color="#c86000",
            width=90, height=34, corner_radius=8)
        self.pause_btn.pack(side="right", padx=(0, 8), pady=14)

        ctk.CTkButton(hdr, text="Clear Alerts", command=self._clear_alerts,
            font=ctk.CTkFont("Segoe UI", 13, "bold"),
            fg_color=BG_CARD, hover_color=BORDER,
            width=110, height=34, corner_radius=8,
            border_width=1, border_color=BORDER).pack(side="right", padx=(0, 4))

        self.status_lbl = ctk.CTkLabel(hdr, text="Starting...",
            font=ctk.CTkFont("Segoe UI", 13), text_color=MUTED)
        self.status_lbl.pack(side="right", padx=16)

        # Body
        body = ctk.CTkFrame(self.root, fg_color=BG_DEEP, corner_radius=0)
        body.pack(fill="both", expand=True, padx=8, pady=8)

        # Left: game list
        left = ctk.CTkFrame(body, fg_color=BG_MID, corner_radius=8, width=290)
        left.pack(side="left", fill="y", padx=(0, 6))
        left.pack_propagate(False)

        ctk.CTkLabel(left, text="Live Games",
                     font=ctk.CTkFont("Segoe UI", 14, "bold"),
                     text_color=MUTED).pack(anchor="w", padx=12, pady=(10, 4))

        btnrow = ctk.CTkFrame(left, fg_color="transparent")
        btnrow.pack(fill="x", padx=8, pady=(0, 6))
        ctk.CTkButton(btnrow, text="Monitor All", command=self._monitor_all,
            font=ctk.CTkFont("Segoe UI", 12, "bold"),
            fg_color=ACCENT, hover_color="#4a8ecf",
            height=30, corner_radius=6).pack(side="left", expand=True, padx=(0, 3))
        ctk.CTkButton(btnrow, text="Clear All", command=self._monitor_none,
            font=ctk.CTkFont("Segoe UI", 12, "bold"),
            fg_color=BG_CARD, hover_color=BORDER,
            height=30, corner_radius=6,
            border_width=1, border_color=BORDER).pack(side="left", expand=True, padx=(3, 0))

        self._build_game_list(left)

        # Historical Test section
        ctk.CTkLabel(left, text="Historical Test",
                     font=ctk.CTkFont("Segoe UI", 13, "bold"),
                     text_color=MUTED).pack(anchor="w", padx=12, pady=(12, 4))

        hist_frame = ctk.CTkFrame(left, fg_color=BG_CARD, corner_radius=8)
        hist_frame.pack(fill="x", padx=8, pady=(0, 8))

        def lbl(parent, text):
            ctk.CTkLabel(parent, text=text,
                         font=ctk.CTkFont("Segoe UI", 11),
                         text_color=MUTED).pack(side="left", padx=(8, 2))

        row1 = ctk.CTkFrame(hist_frame, fg_color="transparent")
        row1.pack(fill="x", pady=(8, 4))
        lbl(row1, "League:")
        self.hist_league = ctk.StringVar(value="CFB")
        ctk.CTkOptionMenu(row1, variable=self.hist_league,
                          values=["CFB", "NFL"],
                          font=ctk.CTkFont("Segoe UI", 12),
                          fg_color=BG_MID, button_color=ACCENT,
                          button_hover_color="#4a8ecf",
                          dropdown_fg_color=BG_CARD,
                          width=80, height=28).pack(side="left", padx=(0, 4))

        row2 = ctk.CTkFrame(hist_frame, fg_color="transparent")
        row2.pack(fill="x", pady=(0, 4))
        lbl(row2, "Year:")
        self.hist_year = ctk.StringVar(value="2025")
        ctk.CTkOptionMenu(row2, variable=self.hist_year,
                          values=["2025", "2024", "2023"],
                          font=ctk.CTkFont("Segoe UI", 12),
                          fg_color=BG_MID, button_color=ACCENT,
                          button_hover_color="#4a8ecf",
                          dropdown_fg_color=BG_CARD,
                          width=80, height=28).pack(side="left", padx=(0, 4))
        lbl(row2, "Wk:")
        self.hist_week = ctk.StringVar(value="1")
        ctk.CTkOptionMenu(row2, variable=self.hist_week,
                          values=[str(i) for i in range(0, 16)],
                          font=ctk.CTkFont("Segoe UI", 12),
                          fg_color=BG_MID, button_color=ACCENT,
                          button_hover_color="#4a8ecf",
                          dropdown_fg_color=BG_CARD,
                          width=60, height=28).pack(side="left")

        self.run_qc_btn = ctk.CTkButton(hist_frame, text="Run QC Check",
                                         command=self._run_historical_qc,
                                         font=ctk.CTkFont("Segoe UI", 12, "bold"),
                                         fg_color=ACCENT, hover_color="#4a8ecf",
                                         height=32, corner_radius=6)
        self.run_qc_btn.pack(fill="x", padx=8, pady=(4, 10))

        self.hist_progress = ctk.CTkLabel(hist_frame, text="",
                                           font=ctk.CTkFont("Segoe UI", 11),
                                           text_color=MUTED)
        self.hist_progress.pack(pady=(0, 8))

        # ── QC Summary ────────────────────────────────────────
        ctk.CTkLabel(left, text="QC Summary",
                     font=ctk.CTkFont("Segoe UI", 13, "bold"),
                     text_color=MUTED).pack(anchor="w", padx=12, pady=(8, 4))

        qc_card = ctk.CTkFrame(left, fg_color=BG_CARD, corner_radius=8)
        qc_card.pack(fill="x", padx=8, pady=(0, 10))

        self.summary_lbl = ctk.CTkLabel(qc_card, text="Run a QC check to see results",
            font=ctk.CTkFont("Segoe UI", 12, "bold"), text_color=MUTED, anchor="w")
        self.summary_lbl.pack(fill="x", padx=12, pady=(10, 6))

        # Breakdown rows
        bd = ctk.CTkFrame(qc_card, fg_color="transparent")
        bd.pack(fill="x", padx=10, pady=(0, 4))

        def _bd_row(attr, dot_color, label_text):
            row = ctk.CTkFrame(bd, fg_color="transparent")
            row.pack(fill="x", pady=1)
            ctk.CTkLabel(row, text="●", font=ctk.CTkFont("Segoe UI", 9),
                         text_color=dot_color, width=14).pack(side="left")
            ctk.CTkLabel(row, text=label_text,
                         font=ctk.CTkFont("Segoe UI", 11), text_color=MUTED,
                         width=104, anchor="w").pack(side="left", padx=(2, 0))
            val = ctk.CTkLabel(row, text="—",
                               font=ctk.CTkFont("Segoe UI", 11, "bold"),
                               text_color=MUTED, anchor="w")
            val.pack(side="left")
            setattr(self, attr, val)

        _bd_row("bd_score_lbl", RED,    "Score errors")
        _bd_row("bd_clock_lbl", ORANGE, "Clock issues")
        _bd_row("bd_ep_lbl",    MUTED,  "Missing EP")

        ctk.CTkFrame(qc_card, fg_color=BORDER, height=1,
                     corner_radius=0).pack(fill="x", padx=10, pady=(4, 4))

        self.bd_plays_lbl = ctk.CTkLabel(qc_card, text="",
            font=ctk.CTkFont("Segoe UI", 11), text_color=MUTED, anchor="w")
        self.bd_plays_lbl.pack(fill="x", padx=12, pady=(0, 8))

        self.filter_btn = ctk.CTkButton(qc_card, text="Show Issues Only",
            command=self._toggle_issue_filter,
            font=ctk.CTkFont("Segoe UI", 12, "bold"),
            fg_color=BG_MID, hover_color=BORDER,
            height=30, corner_radius=6,
            border_width=1, border_color=BORDER,
            state="disabled")
        self.filter_btn.pack(fill="x", padx=8, pady=(0, 10))

        # Right
        right = ctk.CTkFrame(body, fg_color="transparent")
        right.pack(side="left", fill="both", expand=True)

        # Alert feed (top 40%)
        af = ctk.CTkFrame(right, fg_color=BG_MID, corner_radius=8)
        af.pack(fill="both", expand=False, pady=(0, 6), ipady=4)
        af.pack_propagate(False)
        af.configure(height=280)

        ah = ctk.CTkFrame(af, fg_color="transparent")
        ah.pack(fill="x", padx=12, pady=(8, 4))
        ctk.CTkLabel(ah, text="Alert Feed",
                     font=ctk.CTkFont("Segoe UI", 14, "bold"),
                     text_color=MUTED).pack(side="left")
        self.sound_var = ctk.BooleanVar(value=True)
        ctk.CTkCheckBox(ah, text="Sound Alerts", variable=self.sound_var,
                        font=ctk.CTkFont("Segoe UI", 12),
                        checkbox_width=18, checkbox_height=18).pack(side="right")
        self._build_alert_tree(af)

        # Play log (bottom 60%)
        pf = ctk.CTkFrame(right, fg_color=BG_MID, corner_radius=8)
        pf.pack(fill="both", expand=True)

        ph = ctk.CTkFrame(pf, fg_color="transparent")
        ph.pack(fill="x", padx=12, pady=(8, 4))
        self.play_lbl = ctk.CTkLabel(ph, text="Play Log — double-click a game to monitor",
                                      font=ctk.CTkFont("Segoe UI", 13, "bold"),
                                      text_color=MUTED)
        self.play_lbl.pack(side="left")
        self.play_count_lbl = ctk.CTkLabel(ph, text="",
                                            font=ctk.CTkFont("Segoe UI", 12),
                                            text_color=MUTED)
        self.play_count_lbl.pack(side="right")
        self._build_play_tree(pf)

    def _build_game_list(self, parent):
        s = ttk.Style()
        s.configure("GL.Treeview", background=BG_CARD, foreground=WHITE,
                    fieldbackground=BG_CARD, rowheight=34,
                    font=("Segoe UI", 12), borderwidth=0)
        s.configure("GL.Treeview.Heading", background=BG_MID, foreground=MUTED,
                    font=("Segoe UI", 11, "bold"), borderwidth=0, relief="flat")
        s.map("GL.Treeview",
              background=[("selected", ACCENT)], foreground=[("selected", WHITE)])

        fr = ctk.CTkFrame(parent, fg_color="transparent")
        fr.pack(fill="both", expand=True, padx=4, pady=(0, 8))

        self.game_tree = ttk.Treeview(fr, columns=("dot", "game", "st"),
                                       show="headings", style="GL.Treeview",
                                       selectmode="browse")
        self.game_tree.heading("dot",  text="")
        self.game_tree.heading("game", text="Matchup")
        self.game_tree.heading("st",   text="QC")
        self.game_tree.column("dot",  width=22,  anchor="center", stretch=False)
        self.game_tree.column("game", width=196, anchor="w")
        self.game_tree.column("st",   width=46,  anchor="center", stretch=False)

        sb = ttk.Scrollbar(fr, orient="vertical", command=self.game_tree.yview)
        self.game_tree.configure(yscrollcommand=sb.set)
        self.game_tree.pack(side="left", fill="both", expand=True)
        sb.pack(side="right", fill="y")

        self.game_tree.tag_configure("on",    foreground=GREEN)
        self.game_tree.tag_configure("off",   foreground=MUTED)
        self.game_tree.tag_configure("alert", foreground=RED)

        self.game_tree.bind("<Double-Button-1>", self._toggle_monitor)
        self.game_tree.bind("<<TreeviewSelect>>", self._on_game_select)

    def _build_alert_tree(self, parent):
        s = ttk.Style()
        s.configure("AL.Treeview", background=BG_CARD, foreground=WHITE,
                    fieldbackground=BG_CARD, rowheight=26,
                    font=("Segoe UI", 12), borderwidth=0)
        s.configure("AL.Treeview.Heading", background=BG_MID, foreground=MUTED,
                    font=("Segoe UI", 11, "bold"), borderwidth=0, relief="flat")
        s.map("AL.Treeview",
              background=[("selected", ACCENT)], foreground=[("selected", WHITE)])

        fr = ctk.CTkFrame(parent, fg_color="transparent")
        fr.pack(fill="both", expand=True, padx=8, pady=(0, 8))

        cols = ("time", "sev", "game", "issue")
        self.alert_tree = ttk.Treeview(fr, columns=cols, show="headings",
                                        style="AL.Treeview", selectmode="browse")
        self.alert_tree.heading("time",  text="Time")
        self.alert_tree.heading("sev",   text="Sev")
        self.alert_tree.heading("game",  text="Game")
        self.alert_tree.heading("issue", text="Issue")
        self.alert_tree.column("time",  width=76,  anchor="center", stretch=False)
        self.alert_tree.column("sev",   width=72,  anchor="center", stretch=False)
        self.alert_tree.column("game",  width=190, anchor="w",      stretch=False)
        self.alert_tree.column("issue", width=500, anchor="w")

        asb = ttk.Scrollbar(fr, orient="vertical", command=self.alert_tree.yview)
        self.alert_tree.configure(yscrollcommand=asb.set)
        self.alert_tree.pack(side="left", fill="both", expand=True)
        asb.pack(side="right", fill="y")

        self.alert_tree.tag_configure("error",   foreground=RED)
        self.alert_tree.tag_configure("warning", foreground=ORANGE)
        self.alert_tree.tag_configure("info",    foreground=MUTED)
        self.alert_tree.tag_configure("ok",      foreground=GREEN)

    def _build_play_tree(self, parent):
        s = ttk.Style()
        s.configure("PL.Treeview", background=BG_CARD, foreground=WHITE,
                    fieldbackground=BG_CARD, rowheight=26,
                    font=("Segoe UI", 11), borderwidth=0)
        s.configure("PL.Treeview.Heading", background=BG_MID, foreground=MUTED,
                    font=("Segoe UI", 10, "bold"), borderwidth=0, relief="flat")
        s.map("PL.Treeview",
              background=[("selected", ACCENT)], foreground=[("selected", WHITE)])

        fr = ctk.CTkFrame(parent, fg_color="transparent")
        fr.pack(fill="both", expand=True, padx=8, pady=(0, 8))

        cols = ("#", "HS", "AS", "Clock", "Qtr", "Dn", "Dist", "Gain", "FP", "Poss", "Play Text", "QC Issue")
        self.play_tree = ttk.Treeview(fr, columns=cols, show="headings",
                                       style="PL.Treeview", selectmode="browse")
        widths = {"#": 36, "HS": 38, "AS": 38, "Clock": 58, "Qtr": 34,
                  "Dn": 34, "Dist": 38, "Gain": 38, "FP": 44,
                  "Poss": 130, "Play Text": 300, "QC Issue": 200}
        for col in cols:
            self.play_tree.heading(col, text=col)
            self.play_tree.column(col, width=widths[col],
                                  anchor="w" if col in ("Poss", "Play Text", "QC Issue") else "center",
                                  stretch=(col == "Play Text"))

        vsb = ttk.Scrollbar(fr, orient="vertical",   command=self.play_tree.yview)
        hsb = ttk.Scrollbar(fr, orient="horizontal", command=self.play_tree.xview)
        self.play_tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        self.play_tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        fr.rowconfigure(0, weight=1)
        fr.columnconfigure(0, weight=1)

        self.play_tree.tag_configure("flagged", background="#2a0f0f", foreground=RED)
        self.play_tree.tag_configure("odd",     background="#111720")
        self.play_tree.tag_configure("even",    background=BG_CARD)

    # ─── Polling ──────────────────────────────────────────────

    def _start_polling(self):
        threading.Thread(target=self._poll_loop, daemon=True).start()

    def _poll_loop(self):
        while True:
            if not self._paused:
                try:
                    self._poll_once()
                except Exception as e:
                    self.root.after(0, lambda err=e: self.status_lbl.configure(
                        text=f"Poll error: {err}", text_color=RED))
            time.sleep(POLL_INTERVAL)

    def _poll_once(self):
        r = requests.get(f"{SERVER_URL}/games", params={"league": "all"}, timeout=15)
        r.raise_for_status()
        live = [g for g in r.json() if g.get("status") == "in"]
        now = datetime.now().strftime("%I:%M:%S %p")
        self.root.after(0, self._update_game_list, live, now)

        for g in live:
            gid = g["game_id"]
            if gid not in self._monitored:
                continue
            try:
                league = g.get("league", "cfb")
                pr = requests.get(f"{SERVER_URL}/game/{gid}/plays",
                                  params={"league": league}, timeout=20)
                pr.raise_for_status()
                data    = pr.json()
                entries = data.get("entries", [])
                hname   = data.get("home_name", "Home")
                aname   = data.get("away_name", "Away")

                self._game_entries[gid] = entries

                # Stalled play count check
                prev = self._play_counts.get(gid, -1)
                curr = len(entries)
                self._play_counts[gid] = curr
                if 0 < prev == curr:
                    key = f"{gid}:stalled:{curr}"
                    if key not in self._seen_keys:
                        self._seen_keys.add(key)
                        self.root.after(0, self._add_alert, g, "INFO",
                            f"Play count unchanged at {curr} plays since last poll")

                # Anomaly checks
                for issue in self._checker.check(entries, hname, aname):
                    pidx = issue.get("play_index", -1)
                    server_qc = entries[pidx].get("qc_issue", "") if 0 <= pidx < len(entries) else None
                    # Score-related issues are NEVER assumed auto-fixed — an
                    # empty server qc_issue could mean the pipeline produced
                    # wrong data that the QC check didn't catch (e.g. EP on
                    # wrong team for a punt return TD).  Only clock/fp issues
                    # can be safely downgraded when the server says clean.
                    _score_types = {"score_regression", "invalid_score_jump",
                                    "missing_ep", "bundled_score_artifact"}
                    is_score_issue = issue["type"] in _score_types
                    auto_fixed = (not is_score_issue
                                  and server_qc is not None and server_qc == ""
                                  and issue["severity"] in ("ERROR", "WARNING"))
                    msg = issue["message"]
                    if auto_fixed:
                        msg = f"{msg}  [auto-fixed by pipeline — no red row in CAPP]"
                    sev = "INFO" if auto_fixed else issue["severity"]
                    key = f"{gid}:{issue['type']}:{msg}"
                    if key not in self._seen_keys:
                        self._seen_keys.add(key)
                        self.root.after(0, self._add_alert, g, sev, msg)

                if self._selected_id == gid:
                    self.root.after(0, self._refresh_play_log,
                                    gid, hname, aname)
            except Exception as e:
                self.root.after(0, self._add_alert, g, "ERROR",
                                f"Failed to fetch plays: {e}")

    # ─── UI Updates ───────────────────────────────────────────

    def _update_game_list(self, live_games, timestamp):
        live_ids   = {g["game_id"] for g in live_games}
        gid_to_iid = {v: k for k, v in self._game_iid_map.items()}

        # Remove stale
        for iid, gid in list(self._game_iid_map.items()):
            if gid not in live_ids:
                try:
                    self.game_tree.delete(iid)
                except Exception:
                    pass
                del self._game_iid_map[iid]

        gid_to_iid = {v: k for k, v in self._game_iid_map.items()}

        for g in live_games:
            gid   = g["game_id"]
            home  = g.get("home_team", g.get("home", ""))[:16]
            away  = g.get("away_team", g.get("away", ""))[:16]
            label = f"{away} @ {home}"
            mon   = gid in self._monitored
            dot   = "●" if mon else "○"
            has_alert = any(a["game_id"] == gid for a in self._all_alerts)
            tag   = "alert" if has_alert else ("on" if mon else "off")
            qc    = "ON" if mon else "—"

            if gid in gid_to_iid:
                iid = gid_to_iid[gid]
                self.game_tree.item(iid, values=(dot, label, qc), tags=(tag,))
            else:
                iid = self.game_tree.insert("", "end",
                                             values=(dot, label, qc), tags=(tag,))
                self._game_iid_map[iid] = gid

        cnt = len(live_games)
        self.status_lbl.configure(
            text=f"{cnt} live game{'s' if cnt != 1 else ''}  |  {timestamp}",
            text_color=GREEN if cnt > 0 else MUTED)

    def _add_alert(self, game, severity, message):
        now   = datetime.now().strftime("%H:%M:%S")
        home  = game.get("home_team", game.get("home", ""))[:12]
        away  = game.get("away_team", game.get("away", ""))[:12]
        label = f"{away} / {home}"
        gid   = game.get("game_id", "")

        self._all_alerts.insert(0, {"time": now, "severity": severity,
                                     "game": label, "message": message,
                                     "game_id": gid})
        self.alert_tree.insert("", 0,
            values=(now, severity, label, message),
            tags=(severity.lower(),))

        errs  = sum(1 for a in self._all_alerts if a["severity"] == "ERROR")
        warns = sum(1 for a in self._all_alerts if a["severity"] == "WARNING")
        if errs + warns:
            self.alert_badge.configure(
                text=f"⚠  {errs} ERR   {warns} WARN")
        else:
            self.alert_badge.configure(text="")

        if self.sound_var.get() and severity in ("ERROR", "WARNING"):
            freq = 1200 if severity == "ERROR" else 800
            threading.Thread(
                target=lambda: winsound.Beep(freq, 300), daemon=True).start()

        # Mark game row red
        for iid, gid2 in self._game_iid_map.items():
            if gid2 == gid:
                self.game_tree.item(iid, tags=("alert",))

    def _refresh_play_log(self, gid, hname, aname):
        entries = self._game_entries.get(gid, [])

        self.play_tree.delete(*self.play_tree.get_children())
        for i, e in enumerate(entries):
            # Use the server's qc_issue field — this is the ground truth for
            # what CAPP will show as red rows.  Issues fixed by the pipeline
            # (inferred EPs, clock fixes, field position fills) will have
            # qc_issue="" even if the local checker might flag them.
            qc = e.get("qc_issue", "")
            tag = "flagged" if qc else ("odd" if i % 2 else "even")
            self.play_tree.insert("", "end", tags=(tag,), values=(
                i + 1,
                e.get("home_score", ""),
                e.get("away_score", ""),
                e.get("clock", ""),
                e.get("quarter", ""),
                e.get("down", ""),
                e.get("distance", ""),
                e.get("gain", ""),
                e.get("field_position", ""),
                e.get("possession", ""),
                e.get("play_text", ""),
                qc,
            ))

        children = self.play_tree.get_children()
        if children:
            self.play_tree.see(children[-1])

        self.play_count_lbl.configure(text=f"{len(entries)} plays")
        self.play_lbl.configure(
            text=f"Play Log — {aname} @ {hname}", text_color=WHITE)

    # ─── Controls ─────────────────────────────────────────────

    def _toggle_monitor(self, event):
        sel = self.game_tree.selection()
        if not sel:
            return
        gid = self._game_iid_map.get(sel[0])
        if not gid:
            return
        if gid in self._monitored:
            del self._monitored[gid]
        else:
            vals = self.game_tree.item(sel[0], "values")
            self._monitored[gid] = {"game_id": gid}
        self._refresh_game_row(sel[0], gid)

    def _refresh_game_row(self, iid, gid):
        mon = gid in self._monitored
        has_alert = any(a["game_id"] == gid for a in self._all_alerts)
        dot = "●" if mon else "○"
        qc  = "ON" if mon else "—"
        tag = "alert" if has_alert else ("on" if mon else "off")
        vals = self.game_tree.item(iid, "values")
        self.game_tree.item(iid, values=(dot, vals[1], qc), tags=(tag,))

    def _on_game_select(self, event):
        sel = self.game_tree.selection()
        if not sel:
            return
        gid = self._game_iid_map.get(sel[0])
        if not gid:
            return
        self._selected_id = gid
        entries = self._game_entries.get(gid)
        if entries is not None:
            self._refresh_play_log(gid, "Home", "Away")
        else:
            self.play_lbl.configure(
                text="Play Log — monitoring will load plays on next poll",
                text_color=MUTED)

    def _monitor_all(self):
        for iid, gid in self._game_iid_map.items():
            self._monitored[gid] = {"game_id": gid}
            self._refresh_game_row(iid, gid)

    def _monitor_none(self):
        self._monitored.clear()
        for iid, gid in self._game_iid_map.items():
            self._refresh_game_row(iid, gid)

    def _run_historical_qc(self):
        league = self.hist_league.get().lower()
        year   = int(self.hist_year.get())
        week   = int(self.hist_week.get())
        self.run_qc_btn.configure(state="disabled", text="Running...")
        self.hist_progress.configure(text="Fetching games...")
        threading.Thread(target=self._historical_qc_worker,
                         args=(league, year, week), daemon=True).start()

    def _historical_qc_worker(self, league, year, week):
        try:
            r = requests.get(f"{SERVER_URL}/games",
                             params={"league": league, "year": year, "week": week},
                             timeout=15)
            r.raise_for_status()
            games = r.json()

            if not games:
                self.root.after(0, lambda: self.hist_progress.configure(
                    text="No games found for that week."))
                self.root.after(0, lambda: self.run_qc_btn.configure(
                    state="normal", text="Run QC Check"))
                return

            total  = len(games)
            issues_found = 0
            games_with_issues = set()
            games_with_score  = set()
            games_with_clock  = set()
            games_with_ep     = set()
            total_plays       = 0
            flagged_play_keys = set()   # (gid, play_index) — unique flagged plays

            for idx, g in enumerate(games):
                gid    = g["game_id"]
                gleague = g.get("league", league)
                home   = g.get("home_team", g.get("home", ""))
                away   = g.get("away_team", g.get("away", ""))

                self.root.after(0, lambda i=idx, t=total, a=away, h=home:
                    self.hist_progress.configure(
                        text=f"Checking {i+1}/{t}: {a[:12]} @ {h[:12]}"))

                try:
                    pr = requests.get(f"{SERVER_URL}/game/{gid}/plays",
                                      params={"league": gleague}, timeout=20)
                    pr.raise_for_status()
                    data    = pr.json()
                    entries = data.get("entries", [])
                    hname   = data.get("home_name", home)
                    aname   = data.get("away_name", away)

                    self._game_entries[gid] = entries
                    total_plays += len(entries)

                    # Add to game list display
                    self.root.after(0, self._add_historical_game_row, g)

                    issues = self._checker.check(entries, hname, aname)
                    for issue in issues:
                        sev  = issue["severity"]
                        typ  = issue["type"]
                        pidx = issue.get("play_index", -1)
                        server_qc = entries[pidx].get("qc_issue", "") if 0 <= pidx < len(entries) else None
                        # Score-related issues are NEVER assumed auto-fixed — an
                        # empty server qc_issue could mean the pipeline produced
                        # wrong data that the QC check didn't catch (e.g. EP on
                        # wrong team for a punt return TD).  Only clock/fp issues
                        # can be safely downgraded when the server says clean.
                        _score_types = {"score_regression", "invalid_score_jump",
                                        "missing_ep", "bundled_score_artifact"}
                        is_score_issue = typ in _score_types
                        auto_fixed = (not is_score_issue
                                      and server_qc is not None and server_qc == ""
                                      and sev in ("ERROR", "WARNING"))
                        msg = issue["message"]
                        if auto_fixed:
                            msg = f"{msg}  [auto-fixed — no red row in CAPP]"
                        disp_sev = "INFO" if auto_fixed else sev
                        # Only count toward "games with issues" if genuinely unfixed
                        if sev in ("ERROR", "WARNING") and not auto_fixed:
                            games_with_issues.add(gid)
                            if pidx >= 0:
                                flagged_play_keys.add((gid, pidx))
                            if typ in ("score_regression", "invalid_score_jump"):
                                games_with_score.add(gid)
                            elif typ == "stuck_clock":
                                games_with_clock.add(gid)
                            elif typ == "missing_ep":
                                games_with_ep.add(gid)
                        issues_found += 1
                        key = f"{gid}:{typ}:{msg}"
                        if key not in self._seen_keys:
                            self._seen_keys.add(key)
                            self.root.after(0, self._add_alert, g, disp_sev, msg)

                    if not issues:
                        self.root.after(0, self._add_alert, g, "OK",
                                        f"No issues found — {len(entries)} plays checked")

                except Exception as e:
                    self.root.after(0, self._add_alert, g, "ERROR",
                                    f"Failed to fetch plays: {e}")

            summary = f"Done — {total} games, {issues_found} issue(s) found"
            self.root.after(0, lambda: self.hist_progress.configure(
                text=summary, text_color=GREEN if issues_found == 0 else ORANGE))
            self.root.after(0, self._finish_historical_qc,
                            list(games), games_with_issues, total,
                            games_with_score, games_with_clock, games_with_ep,
                            total_plays, len(flagged_play_keys))

        except Exception as e:
            self.root.after(0, lambda: self.hist_progress.configure(
                text=f"Error: {e}", text_color=RED))

        self.root.after(0, lambda: self.run_qc_btn.configure(
            state="normal", text="Run QC Check"))

    def _add_historical_game_row(self, g):
        gid  = g["game_id"]
        home = g.get("home_team", g.get("home", ""))[:16]
        away = g.get("away_team", g.get("away", ""))[:16]
        label = f"{away} @ {home}"
        existing = {v for v in self._game_iid_map.values()}
        if gid not in existing:
            iid = self.game_tree.insert("", "end",
                                         values=("○", label, "—"), tags=("off",))
            self._game_iid_map[iid] = gid

    def _finish_historical_qc(self, qc_game_list, games_with_issues, total,
                              games_with_score, games_with_clock, games_with_ep,
                              total_plays, flagged_plays):
        self._qc_game_list         = qc_game_list
        self._qc_games_with_issues = games_with_issues
        n = len(games_with_issues)

        # Refresh the QC column in the game tree to show ✓ / ⚠ status
        gid_to_iid = {v: k for k, v in self._game_iid_map.items()}
        for g in qc_game_list:
            gid = g["game_id"]
            iid = gid_to_iid.get(gid)
            if iid:
                has  = gid in games_with_issues
                vals = self.game_tree.item(iid, "values")
                self.game_tree.item(iid,
                    values=(vals[0], vals[1], "⚠" if has else "✓"),
                    tags=("alert" if has else "off",))

        # Main summary line
        if n == 0:
            text  = f"✓  All {total} games are clean"
            color = GREEN
        else:
            text  = f"{n} of {total} games have issues"
            color = RED if n / max(total, 1) > 0.3 else ORANGE
        self.summary_lbl.configure(text=text, text_color=color)

        # Breakdown labels
        ns = len(games_with_score)
        nc = len(games_with_clock)
        ne = len(games_with_ep)

        def _fmt(count):
            return f"{count} game{'s' if count != 1 else ''}" if count else "none"

        self.bd_score_lbl.configure(text=_fmt(ns),
            text_color=RED    if ns else MUTED)
        self.bd_clock_lbl.configure(text=_fmt(nc),
            text_color=ORANGE if nc else MUTED)
        self.bd_ep_lbl.configure(text=_fmt(ne),
            text_color=MUTED)

        # Play-level stats
        pct = (flagged_plays / total_plays * 100) if total_plays else 0
        plays_color = GREEN if pct < 2 else (ORANGE if pct < 5 else RED)
        self.bd_plays_lbl.configure(
            text=f"{total_plays:,} plays scanned · {pct:.1f}% flagged",
            text_color=plays_color)

        lbl = "Show All Games" if self._filter_issues_only else "Show Issues Only"
        self.filter_btn.configure(state="normal", text=lbl)

    def _toggle_issue_filter(self):
        if not self._qc_game_list:
            return
        self._filter_issues_only = not self._filter_issues_only
        if self._filter_issues_only:
            self.filter_btn.configure(text="← Show All Games",
                fg_color=ACCENT, hover_color="#4a8ecf", border_color=ACCENT)
        else:
            self.filter_btn.configure(text="Show Issues Only",
                fg_color=BG_MID, hover_color=BORDER, border_color=BORDER)
        self._repopulate_game_tree()

    def _repopulate_game_tree(self):
        """Rebuild the game tree based on current filter state."""
        for iid in list(self._game_iid_map.keys()):
            try:
                self.game_tree.delete(iid)
            except Exception:
                pass
        self._game_iid_map.clear()

        games = self._qc_game_list
        if self._filter_issues_only:
            games = [g for g in games if g["game_id"] in self._qc_games_with_issues]

        for g in games:
            gid   = g["game_id"]
            home  = g.get("home_team", g.get("home", ""))[:16]
            away  = g.get("away_team", g.get("away", ""))[:16]
            label = f"{away} @ {home}"
            has   = gid in self._qc_games_with_issues
            iid   = self.game_tree.insert("", "end",
                values=("○", label, "⚠" if has else "✓"),
                tags=("alert" if has else "off",))
            self._game_iid_map[iid] = gid

    def _toggle_pause(self):
        self._paused = not self._paused
        if self._paused:
            self.pause_btn.configure(text="Resume",
                                     fg_color=GREEN, hover_color="#1a6030")
            self.status_lbl.configure(text="Paused", text_color=ORANGE)
        else:
            self.pause_btn.configure(text="Pause",
                                     fg_color=ORANGE, hover_color="#c86000")
            self.status_lbl.configure(text="Resuming...", text_color=MUTED)

    def _clear_alerts(self):
        self._all_alerts.clear()
        self._seen_keys.clear()
        self.alert_tree.delete(*self.alert_tree.get_children())
        self.alert_badge.configure(text="")
        for iid, gid in self._game_iid_map.items():
            self._refresh_game_row(iid, gid)


if __name__ == "__main__":
    root = ctk.CTk()
    QCMonitor(root)
    root.mainloop()
