import tkinter as tk
from tkinter import ttk
import customtkinter as ctk
import threading
import requests

SERVER_URL = "https://capp-data-server.onrender.com"

ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")

BG_DEEP   = "#06080c"
BG_MID    = "#0e1116"
BG_CARD   = "#161b22"
ACCENT    = "#3a7ebf"
BORDER    = "#2c3b55"
MUTED     = "#8b95a1"
WHITE     = "#ffffff"
GREEN     = "#2ea043"
ORANGE    = "#d97706"
RED       = "#cf3130"

STATUS_MAP = {
    "STATUS_SCHEDULED":   ("Scheduled", MUTED),
    "STATUS_IN_PROGRESS": ("LIVE",      GREEN),
    "STATUS_HALFTIME":    ("Halftime",  ORANGE),
    "STATUS_FINAL":       ("Final",     RED),
    "STATUS_POSTPONED":   ("Postponed", MUTED),
    "STATUS_CANCELED":    ("Canceled",  MUTED),
}

LEAGUE_COLORS = {
    "cfb": "#4a90d9",
    "nfl": "#c84b31",
}


class GameViewer:
    def __init__(self, root):
        self.root = root
        self.root.title("CAPP Live — Game Viewer")
        self.root.geometry("1100x700")
        self.root.configure(bg=BG_DEEP)
        self._build_ui()
        self._refresh()

    def _build_ui(self):
        # ── Header ──
        header = ctk.CTkFrame(self.root, fg_color=BG_CARD, corner_radius=0, height=64)
        header.pack(fill="x", side="top")
        header.pack_propagate(False)

        ctk.CTkLabel(
            header, text="CAPP Live  —  Game Viewer",
            font=ctk.CTkFont("Segoe UI", 22, "bold"), text_color=WHITE
        ).pack(side="left", padx=24, pady=16)

        self.status_label = ctk.CTkLabel(
            header, text="Connecting...",
            font=ctk.CTkFont("Segoe UI", 13), text_color=MUTED
        )
        self.status_label.pack(side="right", padx=24)

        self.refresh_btn = ctk.CTkButton(
            header, text="Refresh", command=self._refresh,
            font=ctk.CTkFont("Segoe UI", 13, "bold"),
            fg_color=ACCENT, hover_color="#4a8ecf",
            width=100, height=36, corner_radius=8
        )
        self.refresh_btn.pack(side="right", padx=(0, 12), pady=14)

        # ── Filter bar ──
        filter_bar = ctk.CTkFrame(self.root, fg_color=BG_MID, corner_radius=0, height=48)
        filter_bar.pack(fill="x", side="top")
        filter_bar.pack_propagate(False)

        ctk.CTkLabel(
            filter_bar, text="League:",
            font=ctk.CTkFont("Segoe UI", 13), text_color=MUTED
        ).pack(side="left", padx=(20, 6), pady=12)

        self.league_var = ctk.StringVar(value="All")
        self.league_menu = ctk.CTkOptionMenu(
            filter_bar, variable=self.league_var,
            values=["All", "CFB", "NFL"],
            command=self._apply_filter,
            font=ctk.CTkFont("Segoe UI", 13),
            fg_color=BG_CARD, button_color=ACCENT,
            button_hover_color="#4a8ecf", dropdown_fg_color=BG_CARD,
            width=120, height=32
        )
        self.league_menu.pack(side="left", padx=(0, 20))

        ctk.CTkLabel(
            filter_bar, text="Status:",
            font=ctk.CTkFont("Segoe UI", 13), text_color=MUTED
        ).pack(side="left", padx=(0, 6))

        self.status_var = ctk.StringVar(value="All")
        self.status_menu = ctk.CTkOptionMenu(
            filter_bar, variable=self.status_var,
            values=["All", "Scheduled", "Live", "Final"],
            command=self._apply_filter,
            font=ctk.CTkFont("Segoe UI", 13),
            fg_color=BG_CARD, button_color=ACCENT,
            button_hover_color="#4a8ecf", dropdown_fg_color=BG_CARD,
            width=140, height=32
        )
        self.status_menu.pack(side="left")

        self.count_label = ctk.CTkLabel(
            filter_bar, text="",
            font=ctk.CTkFont("Segoe UI", 13), text_color=MUTED
        )
        self.count_label.pack(side="right", padx=24)

        # ── Treeview ──
        tree_frame = ctk.CTkFrame(self.root, fg_color=BG_MID, corner_radius=0)
        tree_frame.pack(fill="both", expand=True, padx=0, pady=0)

        style = ttk.Style()
        style.theme_use("clam")
        style.configure("Games.Treeview",
            background=BG_CARD,
            foreground=WHITE,
            fieldbackground=BG_CARD,
            rowheight=38,
            font=("Segoe UI", 13),
            borderwidth=0,
        )
        style.configure("Games.Treeview.Heading",
            background=BG_MID,
            foreground=MUTED,
            font=("Segoe UI", 12, "bold"),
            borderwidth=0,
            relief="flat",
        )
        style.map("Games.Treeview",
            background=[("selected", ACCENT)],
            foreground=[("selected", WHITE)],
        )
        style.map("Games.Treeview.Heading",
            background=[("active", BG_CARD)],
        )

        columns = ("league", "away", "home", "status")
        self.tree = ttk.Treeview(
            tree_frame, columns=columns, show="headings",
            style="Games.Treeview", selectmode="browse"
        )

        self.tree.heading("league", text="League")
        self.tree.heading("away",   text="Away Team")
        self.tree.heading("home",   text="Home Team")
        self.tree.heading("status", text="Status")

        self.tree.column("league", width=90,  anchor="center", stretch=False)
        self.tree.column("away",   width=320, anchor="w")
        self.tree.column("home",   width=320, anchor="w")
        self.tree.column("status", width=120, anchor="center", stretch=False)

        scrollbar = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)

        self.tree.pack(side="left", fill="both", expand=True, padx=(12, 0), pady=12)
        scrollbar.pack(side="right", fill="y", pady=12, padx=(0, 8))

        # Row tag colors
        self.tree.tag_configure("live",      foreground=GREEN)
        self.tree.tag_configure("final",     foreground="#888888")
        self.tree.tag_configure("scheduled", foreground=WHITE)
        self.tree.tag_configure("odd",       background="#111720")
        self.tree.tag_configure("even",      background=BG_CARD)

        self._all_games = []

    def _refresh(self):
        self.refresh_btn.configure(state="disabled", text="Loading...")
        self.status_label.configure(text="Fetching games...")
        threading.Thread(target=self._fetch_games, daemon=True).start()

    def _fetch_games(self):
        try:
            r = requests.get(f"{SERVER_URL}/games", timeout=15)
            r.raise_for_status()
            games = r.json()
            self.root.after(0, self._load_games, games)
        except Exception as e:
            self.root.after(0, self._on_error, str(e))

    def _load_games(self, games):
        self._all_games = games
        self._apply_filter()
        self.refresh_btn.configure(state="normal", text="Refresh")
        self.status_label.configure(text=f"Last updated: just now")

    def _apply_filter(self, *_):
        league_filter = self.league_var.get()
        status_filter = self.status_var.get()

        filtered = []
        for g in self._all_games:
            if league_filter != "All" and g["league"].upper() != league_filter:
                continue
            label, _ = STATUS_MAP.get(g["status"], (g["status"], MUTED))
            if status_filter != "All" and label.lower() != status_filter.lower():
                continue
            filtered.append(g)

        self._populate_tree(filtered)

    def _populate_tree(self, games):
        self.tree.delete(*self.tree.get_children())

        for i, g in enumerate(games):
            league = g["league"].upper()
            away   = g["away"]
            home   = g["home"]
            raw_status = g["status"]
            label, _ = STATUS_MAP.get(raw_status, (raw_status, MUTED))

            if "LIVE" in label or "IN_PROGRESS" in raw_status:
                status_tag = "live"
            elif "Final" in label:
                status_tag = "final"
            else:
                status_tag = "scheduled"

            row_tag = "odd" if i % 2 else "even"

            self.tree.insert("", "end",
                values=(league, away, home, label),
                tags=(status_tag, row_tag)
            )

        total = len(games)
        self.count_label.configure(text=f"{total} game{'s' if total != 1 else ''}")

    def _on_error(self, msg):
        self.status_label.configure(text=f"Error: {msg}", text_color=RED)
        self.refresh_btn.configure(state="normal", text="Refresh")


if __name__ == "__main__":
    root = ctk.CTk()
    GameViewer(root)
    root.mainloop()
