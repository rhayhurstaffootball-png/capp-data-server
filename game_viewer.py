import tkinter as tk
from tkinter import ttk
import customtkinter as ctk
import threading
import requests

SERVER_URL = "https://capp-data-server.onrender.com"

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

GAME_STATUS_MAP = {
    "pre":  ("Scheduled", MUTED),
    "in":   ("LIVE",      GREEN),
    "post": ("Final",     MUTED),
}

DOWN_COLORS = {
    "KO":  "#4a90d9",
    "EP":  "#2ea043",
    "2PT": "#2ea043",
    "1":   WHITE,
    "2":   WHITE,
    "3":   ORANGE,
    "4":   RED,
    "OT":  "#d97706",
}


class GameViewer:
    def __init__(self, root):
        self.root = root
        self.root.title("CAPP Live — Game Viewer")
        self.root.geometry("1400x820")
        self.root.configure(bg=BG_DEEP)
        self._selected_game = None
        self._build_ui()
        self._load_games()

    # ─────────────────────────────────────────────────────────────
    # UI Construction
    # ─────────────────────────────────────────────────────────────
    def _build_ui(self):
        # ── Header ──
        header = ctk.CTkFrame(self.root, fg_color=BG_CARD, corner_radius=0, height=60)
        header.pack(fill="x")
        header.pack_propagate(False)
        ctk.CTkLabel(header, text="CAPP Live  —  Game Viewer",
                     font=ctk.CTkFont("Segoe UI", 22, "bold"),
                     text_color=WHITE).pack(side="left", padx=24, pady=14)
        self.status_label = ctk.CTkLabel(header, text="",
                                          font=ctk.CTkFont("Segoe UI", 13),
                                          text_color=MUTED)
        self.status_label.pack(side="right", padx=20)

        # ── Filter bar ──
        fbar = ctk.CTkFrame(self.root, fg_color=BG_MID, corner_radius=0, height=50)
        fbar.pack(fill="x")
        fbar.pack_propagate(False)

        def lbl(parent, text):
            return ctk.CTkLabel(parent, text=text,
                                font=ctk.CTkFont("Segoe UI", 13), text_color=MUTED)

        def omenu(parent, var, values, w=110):
            return ctk.CTkOptionMenu(parent, variable=var, values=values,
                                     font=ctk.CTkFont("Segoe UI", 13),
                                     fg_color=BG_CARD, button_color=ACCENT,
                                     button_hover_color="#4a8ecf",
                                     dropdown_fg_color=BG_CARD,
                                     width=w, height=32)

        lbl(fbar, "League:").pack(side="left", padx=(16, 4), pady=10)
        self.league_var = ctk.StringVar(value="CFB")
        omenu(fbar, self.league_var, ["CFB", "NFL", "All"]).pack(side="left", padx=(0, 12))

        lbl(fbar, "Year:").pack(side="left", padx=(0, 4))
        self.year_var = ctk.StringVar(value="2025")
        omenu(fbar, self.year_var, ["2025", "2024", "2023"], w=90).pack(side="left", padx=(0, 12))

        lbl(fbar, "Week:").pack(side="left", padx=(0, 4))
        self.week_var = ctk.StringVar(value="1")
        omenu(fbar, self.week_var,
              [str(i) for i in range(0, 16)] + ["Post"],
              w=80).pack(side="left", padx=(0, 12))

        lbl(fbar, "Type:").pack(side="left", padx=(0, 4))
        self.stype_var = ctk.StringVar(value="Regular")
        omenu(fbar, self.stype_var, ["Regular", "Postseason"], w=130).pack(side="left", padx=(0, 16))

        self.load_btn = ctk.CTkButton(fbar, text="Load Games", command=self._load_games,
                                       font=ctk.CTkFont("Segoe UI", 13, "bold"),
                                       fg_color=ACCENT, hover_color="#4a8ecf",
                                       width=120, height=34, corner_radius=8)
        self.load_btn.pack(side="left", padx=(0, 8))

        self.game_count_label = ctk.CTkLabel(fbar, text="",
                                              font=ctk.CTkFont("Segoe UI", 12),
                                              text_color=MUTED)
        self.game_count_label.pack(side="left", padx=8)

        # ── Main split ──
        main = ctk.CTkFrame(self.root, fg_color=BG_DEEP, corner_radius=0)
        main.pack(fill="both", expand=True)

        # Left: game list
        left = ctk.CTkFrame(main, fg_color=BG_MID, corner_radius=0, width=340)
        left.pack(side="left", fill="y", padx=(8, 4), pady=8)
        left.pack_propagate(False)

        ctk.CTkLabel(left, text="Games",
                     font=ctk.CTkFont("Segoe UI", 14, "bold"),
                     text_color=MUTED).pack(anchor="w", padx=12, pady=(10, 4))

        self._build_game_tree(left)

        # Right: plays panel
        right = ctk.CTkFrame(main, fg_color=BG_MID, corner_radius=0)
        right.pack(side="left", fill="both", expand=True, padx=(4, 8), pady=8)

        # Scoreboard strip
        self.score_frame = ctk.CTkFrame(right, fg_color=BG_CARD, corner_radius=8, height=54)
        self.score_frame.pack(fill="x", padx=8, pady=(8, 4))
        self.score_frame.pack_propagate(False)
        self.score_label = ctk.CTkLabel(self.score_frame,
                                         text="Select a game to load plays",
                                         font=ctk.CTkFont("Segoe UI", 15, "bold"),
                                         text_color=MUTED)
        self.score_label.pack(expand=True)

        self.play_count_label = ctk.CTkLabel(right, text="",
                                              font=ctk.CTkFont("Segoe UI", 12),
                                              text_color=MUTED)
        self.play_count_label.pack(anchor="w", padx=12, pady=(0, 2))

        self._build_play_tree(right)

    def _build_game_tree(self, parent):
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("Games.Treeview",
            background=BG_CARD, foreground=WHITE, fieldbackground=BG_CARD,
            rowheight=36, font=("Segoe UI", 12), borderwidth=0)
        style.configure("Games.Treeview.Heading",
            background=BG_MID, foreground=MUTED,
            font=("Segoe UI", 11, "bold"), borderwidth=0, relief="flat")
        style.map("Games.Treeview",
            background=[("selected", ACCENT)], foreground=[("selected", WHITE)])

        frame = ctk.CTkFrame(parent, fg_color="transparent")
        frame.pack(fill="both", expand=True, padx=4, pady=(0, 8))

        cols = ("matchup", "status")
        self.game_tree = ttk.Treeview(frame, columns=cols, show="headings",
                                       style="Games.Treeview", selectmode="browse")
        self.game_tree.heading("matchup", text="Matchup")
        self.game_tree.heading("status",  text="Status")
        self.game_tree.column("matchup", width=220, anchor="w")
        self.game_tree.column("status",  width=80,  anchor="center", stretch=False)

        gsb = ttk.Scrollbar(frame, orient="vertical", command=self.game_tree.yview)
        self.game_tree.configure(yscrollcommand=gsb.set)
        self.game_tree.pack(side="left", fill="both", expand=True)
        gsb.pack(side="right", fill="y")

        self.game_tree.tag_configure("live",  foreground=GREEN)
        self.game_tree.tag_configure("final", foreground="#666666")
        self.game_tree.tag_configure("pre",   foreground=WHITE)

        self.game_tree.bind("<<TreeviewSelect>>", self._on_game_select)

    def _build_play_tree(self, parent):
        style = ttk.Style()
        style.configure("Plays.Treeview",
            background=BG_CARD, foreground=WHITE, fieldbackground=BG_CARD,
            rowheight=30, font=("Segoe UI", 12), borderwidth=0)
        style.configure("Plays.Treeview.Heading",
            background=BG_MID, foreground=MUTED,
            font=("Segoe UI", 11, "bold"), borderwidth=0, relief="flat")
        style.map("Plays.Treeview",
            background=[("selected", ACCENT)], foreground=[("selected", WHITE)])

        frame = ctk.CTkFrame(parent, fg_color="transparent")
        frame.pack(fill="both", expand=True, padx=8, pady=(0, 8))

        cols = ("#", "HS", "AS", "Clock", "Qtr", "Dn", "Dist",
                "Gain", "FP", "Poss", "RC", "HTO", "ATO", "Play Text")
        self.play_tree = ttk.Treeview(frame, columns=cols, show="headings",
                                       style="Plays.Treeview", selectmode="browse")

        widths = {
            "#": 40, "HS": 45, "AS": 45, "Clock": 65, "Qtr": 40,
            "Dn": 40, "Dist": 45, "Gain": 45, "FP": 50,
            "Poss": 140, "RC": 40, "HTO": 40, "ATO": 40, "Play Text": 400,
        }
        anchors = {
            "#": "center", "HS": "center", "AS": "center", "Clock": "center",
            "Qtr": "center", "Dn": "center", "Dist": "center", "Gain": "center",
            "FP": "center", "Poss": "w", "RC": "center", "HTO": "center",
            "ATO": "center", "Play Text": "w",
        }
        for col in cols:
            self.play_tree.heading(col, text=col)
            self.play_tree.column(col, width=widths[col], anchor=anchors[col],
                                  stretch=(col == "Play Text"))

        vsb = ttk.Scrollbar(frame, orient="vertical",   command=self.play_tree.yview)
        hsb = ttk.Scrollbar(frame, orient="horizontal", command=self.play_tree.xview)
        self.play_tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        self.play_tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        frame.rowconfigure(0, weight=1)
        frame.columnconfigure(0, weight=1)

        # Row tags
        self.play_tree.tag_configure("odd",  background="#111720")
        self.play_tree.tag_configure("even", background=BG_CARD)
        self.play_tree.tag_configure("ko",   foreground="#4a90d9")
        self.play_tree.tag_configure("ep",   foreground=GREEN)
        self.play_tree.tag_configure("td",   foreground=ORANGE)
        self.play_tree.tag_configure("q3",   foreground="#a0c0ff")
        self.play_tree.tag_configure("q4",   foreground="#ffb060")
        self.play_tree.tag_configure("ot",   foreground=RED)

    # ─────────────────────────────────────────────────────────────
    # Data Loading
    # ─────────────────────────────────────────────────────────────
    def _load_games(self):
        self.load_btn.configure(state="disabled", text="Loading...")
        self.status_label.configure(text="Fetching games...")
        threading.Thread(target=self._fetch_games, daemon=True).start()

    def _fetch_games(self):
        try:
            league = self.league_var.get().lower()
            year_str = self.year_var.get()
            week_str = self.week_var.get()
            stype = 3 if self.stype_var.get() == "Postseason" else 2

            params = {"league": league}
            if year_str:
                params["year"] = int(year_str)
            if week_str and week_str != "Post":
                params["week"] = int(week_str)
            if stype == 3:
                params["seasontype"] = 3

            r = requests.get(f"{SERVER_URL}/games", params=params, timeout=15)
            r.raise_for_status()
            games = r.json()
            self.root.after(0, self._populate_games, games)
        except Exception as e:
            self.root.after(0, lambda: self.status_label.configure(
                text=f"Error: {e}", text_color=RED))
            self.root.after(0, lambda: self.load_btn.configure(
                state="normal", text="Load Games"))

    def _populate_games(self, games):
        self.game_tree.delete(*self.game_tree.get_children())
        self._game_data = {}

        for g in games:
            status = g.get("status", "pre")
            label, _ = GAME_STATUS_MAP.get(status, (status, MUTED))
            home = g.get("home_team", g.get("home", ""))
            away = g.get("away_team", g.get("away", ""))
            hs = g.get("home_score", 0)
            as_ = g.get("away_score", 0)

            if status == "post":
                matchup = f"{away} {as_}  {hs} {home}"
            elif status == "in":
                matchup = f"{away} {as_}  {hs} {home}"
            else:
                matchup = f"{away} @ {home}"

            tag = "live" if status == "in" else ("final" if status == "post" else "pre")
            iid = self.game_tree.insert("", "end",
                values=(matchup, label), tags=(tag,))
            self._game_data[iid] = g

        total = len(games)
        self.game_count_label.configure(text=f"{total} game{'s' if total != 1 else ''}")
        self.status_label.configure(text="Games loaded", text_color=MUTED)
        self.load_btn.configure(state="normal", text="Load Games")

    def _on_game_select(self, event):
        sel = self.game_tree.selection()
        if not sel:
            return
        game = self._game_data.get(sel[0])
        if not game:
            return
        self._selected_game = game
        self._load_plays(game)

    def _load_plays(self, game):
        game_id = game.get("game_id")
        league  = game.get("league", "cfb")
        home = game.get("home_team", game.get("home", ""))
        away = game.get("away_team", game.get("away", ""))
        self.score_label.configure(
            text=f"Loading plays for  {away}  vs  {home}...", text_color=MUTED)
        self.play_tree.delete(*self.play_tree.get_children())
        self.play_count_label.configure(text="")
        threading.Thread(target=self._fetch_plays,
                         args=(game_id, league), daemon=True).start()

    def _fetch_plays(self, game_id, league):
        try:
            r = requests.get(f"{SERVER_URL}/game/{game_id}/plays",
                             params={"league": league}, timeout=20)
            r.raise_for_status()
            data = r.json()
            self.root.after(0, self._populate_plays, data)
        except Exception as e:
            self.root.after(0, lambda: self.score_label.configure(
                text=f"Error loading plays: {e}", text_color=RED))

    def _populate_plays(self, data):
        entries     = data.get("entries", [])
        actual_home = data.get("actual_home", 0)
        actual_away = data.get("actual_away", 0)
        home_name   = data.get("home_name", "Home")
        away_name   = data.get("away_name", "Away")
        status      = data.get("status", "post")

        status_txt = "LIVE" if status == "in" else ("Final" if status == "post" else "Scheduled")
        self.score_label.configure(
            text=f"{away_name}  {actual_away}  —  {actual_home}  {home_name}     [{status_txt}]",
            text_color=GREEN if status == "in" else WHITE)

        self.play_tree.delete(*self.play_tree.get_children())

        for i, e in enumerate(entries):
            down = str(e.get("down", ""))
            qtr  = str(e.get("quarter", ""))
            row_tag = "odd" if i % 2 else "even"

            if down in ("KO",):
                type_tag = "ko"
            elif down in ("EP", "2PT"):
                type_tag = "ep"
            elif qtr == "OT":
                type_tag = "ot"
            elif qtr == "4":
                type_tag = "q4"
            elif qtr == "3":
                type_tag = "q3"
            else:
                type_tag = row_tag

            self.play_tree.insert("", "end", tags=(row_tag, type_tag), values=(
                i + 1,
                e.get("home_score", ""),
                e.get("away_score", ""),
                e.get("clock", ""),
                qtr,
                down,
                e.get("distance", ""),
                e.get("gain", ""),
                e.get("field_position", ""),
                e.get("possession", ""),
                e.get("run_clock", ""),
                e.get("home_time_out", ""),
                e.get("away_time_out", ""),
                e.get("play_text", ""),
            ))

        self.play_count_label.configure(
            text=f"{len(entries)} plays loaded")


if __name__ == "__main__":
    root = ctk.CTk()
    GameViewer(root)
    root.mainloop()
