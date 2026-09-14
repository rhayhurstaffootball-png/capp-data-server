"""Primary Backup - NCAA's play-by-play, kept on the server while a game is played (Roger, Sep 14 2026).

Roger: "NCAA becomes the Backup Data Resolve Keeps a play by play on the Server and when that is Ready by Quarter,
Half, and Full game the Backup is Availible button is active." Two SBENTRY buttons: PRIMARY BACKUP (this, NCAA) and
SECONDARY BACKUP (Roger's Bleacher Report upload, main.py /backup-data). Both feed the same Resolve logic
(SBENTRY/backup_resolve.py), so this builds the SAME "capp-backup-data" items the Backup Data Builder makes.

⚠ WHY A COPY IS KEPT (measured Sep 14 2026, same host, same Air Force contest 6604047): NCAA publishes a game-day
version (team named on the spot "1 and 10 at NDS25" on ~95% of rows, drive-start lines, no kickoff or timeout lines)
and later REWRITES the game (no team on any spot, kickoff + timeout lines added). When differs by game - Nebraska was
already rewritten on the Sep 13 morning copy, another game still had codes on Sep 14. So every change is saved:
    primary_backup/<espn_game_id>/latest.json   the newest copy
    primary_backup/<espn_game_id>/coded.json    the newest copy that still names the team on most spots
    primary_backup/<espn_game_id>/status.json   small - what SBENTRY's notice asks every few minutes
Saving runs on its own thread and swallows every error: a second source must never touch play delivery.
"""
import hashlib
import json
import os
import re
import threading
import time

import ncaa_check
import ncaa_live
import ncaa_match as M

FORMAT = "capp-backup-data"
BUCKET = "capp-workflow"
CODED_SHARE = 0.5          # a copy "has codes" when at least half its down-and-distance lines name the spot team
MIN_SAVE_GAP = 60          # seconds between saves of one game unless its status changed (the poller runs every ~10s)

_last = {}
_lock = threading.Lock()


# ── readiness ─────────────────────────────────────────────────────────────────

def ready_through(status, period):
    """How many quarters are finished: a final game is all of them; otherwise every quarter before the current one."""
    try:
        p = int(period or 0)
    except (TypeError, ValueError):
        p = 0
    if str(status or "").strip().lower() == "final":
        return max(4, p)
    return max(0, min(p - 1, 4))


def ready_label(status, through):
    if str(status or "").strip().lower() == "final":
        return "Full Game"
    return {1: "1st Quarter", 2: "Half", 3: "3rd Quarter", 4: "4th Quarter"}.get(through, "")


def coded_share(pbp):
    """Share of rows with a down-and-distance line that name the spot team."""
    n = coded = 0
    for p in (pbp or {}).get("plays") or []:
        down, _dist, side, yard = ncaa_check.situation(p.get("drive_text"))
        if down is None or yard is None:
            continue
        n += 1
        coded += bool(side)
    return (coded / n) if n else 0.0


# ── saving (called from the live play feed) ───────────────────────────────────

def _storage_url(gid, name):
    base = os.environ.get("SUPABASE_URL", "").rstrip("/")
    return f"{base}/storage/v1/object/{BUCKET}/primary_backup/{gid}/{name}"


def _put(gid, name, doc):
    import httpx
    key = os.environ.get("SUPABASE_SERVICE_KEY", "")
    headers = {"Authorization": f"Bearer {key}", "apikey": key, "Content-Type": "application/json", "x-upsert": "true"}
    with httpx.Client(timeout=20) as client:
        r = client.put(_storage_url(gid, name), content=json.dumps(doc).encode(), headers=headers)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"store {name}: HTTP {r.status_code} {r.text[:120]}")


def _signature(pbp):
    body = [(p.get("quarter"), p.get("clock"), p.get("drive_text"), p.get("text")) for p in pbp.get("plays") or []]
    return hashlib.sha1(json.dumps([pbp.get("status"), pbp.get("period"), body]).encode("utf-8")).hexdigest()


def build_documents(espn_game_id, ncaa_game_id, pbp, now=None):
    """{name: document} to store for one NCAA fetch. Pure - tested on its own."""
    now = now or time.time()
    stamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now))
    share = coded_share(pbp)
    status, period = pbp.get("status", ""), pbp.get("period")
    through = ready_through(status, period)
    copy = {"format": "capp-primary-backup", "version": 1, "espn_game_id": str(espn_game_id),
            "ncaa_game_id": str(ncaa_game_id or ""), "saved_at": stamp, "status": status, "period": period,
            "ready_through": through, "coded_share": round(share, 3), "teams": pbp.get("teams") or {},
            "plays": pbp.get("plays") or []}
    docs = {"latest.json": copy}
    if share >= CODED_SHARE:
        docs["coded.json"] = copy
    docs["status.json"] = {"espn_game_id": str(espn_game_id), "saved_at": stamp, "status": status, "period": period,
                           "ready_through": through, "label": ready_label(status, through),
                           "plays": len(copy["plays"]), "coded_share": copy["coded_share"]}
    return docs


def note_pbp(espn_game_id, ncaa_game_id, pbp, put=None, background=True):
    """Keep this NCAA fetch if it changed. Never raises, never blocks the feed (the store runs on its own thread)."""
    try:
        if not (pbp or {}).get("available") or not pbp.get("plays"):
            return False
        if put is None and not os.environ.get("SUPABASE_URL"):
            return False
        gid = str(espn_game_id)
        sig, now, status = _signature(pbp), time.time(), str(pbp.get("status") or "")
        with _lock:
            prev = _last.get(gid)
            if prev and prev["sig"] == sig:
                return False
            if prev and prev["status"] == status and now - prev["at"] < MIN_SAVE_GAP:
                return False
            _last[gid] = {"sig": sig, "at": now, "status": status}
        docs = json.loads(json.dumps(build_documents(gid, ncaa_game_id, pbp, now)))   # own copy: the feed keeps working on pbp
        store = put or _put

        def work():
            try:
                for name in ("latest.json", "coded.json", "status.json"):   # status last: it never points at a copy not yet stored
                    if name in docs:
                        store(gid, name, docs[name])
            except Exception as e:
                with _lock:
                    _last.pop(gid, None)                                    # try again on the next poll
                print(f"WARNING: primary backup save failed for {gid}: {type(e).__name__}: {e}", flush=True)

        if background:
            threading.Thread(target=work, daemon=True, name=f"primary-backup-{gid}").start()
        else:
            work()
        return True
    except Exception as e:
        print(f"WARNING: primary backup note failed for {espn_game_id}: {type(e).__name__}: {e}", flush=True)
        return False


# ── which copy, and the items Resolve reads ───────────────────────────────────

def choose_copy(latest, coded):
    """The copy with team codes unless the newest copy is further along (a later quarter is ready)."""
    if coded and (not latest or int(coded.get("ready_through") or 0) >= int(latest.get("ready_through") or 0)):
        return coded
    return latest


def _team_forms(team):
    return [ncaa_live._fold(v) for v in (team.get("short"), team.get("full"), team.get("abbrev"),
                                          (team.get("seo") or "").replace("-", " ")) if v]


def _timeout_item(text, teams):
    """("tv_timeout", None) | ("team_timeout", "home"/"away") | None when the team can't be told apart - never guess."""
    m = ncaa_live._TIMEOUT_RE.match(text)
    who = (m.group(1) if m else "").strip()
    if ncaa_live._fold(who) in ("other", "", "official", "officials", "media", "tv", "injury"):
        return "tv_timeout", None
    hs = ncaa_live._name_score(who, _team_forms(teams.get("home") or {}))
    aws = ncaa_live._name_score(who, _team_forms(teams.get("away") or {}))
    if hs == aws:
        return None
    return "team_timeout", ("home" if hs > aws else "away")


def spot_code_sides(entries, plays, home_name, away_name):
    """{spot code: "home"/"away"} learned from ESPN plays lined up with NCAA's that agree on the yard - the same votes
    the live NCAA check uses (ncaa_check.verify_entries). A code with a tie is left out."""
    votes = {}
    other = {home_name: away_name, away_name: home_name}
    for q in sorted({M._quarter(e.get("quarter")) for e in entries} - {0}):
        mine = [e for e in entries if M._quarter(e.get("quarter")) == q and not M.is_admin_line(e.get("play_text", ""))]
        theirs = [t for t in plays if M._quarter(t.get("quarter")) == q and not M.is_admin_line(t.get("text", ""))]
        for a, b in M._align_quarter(mine, theirs):
            if a is None or b is None or not ncaa_check._good_pair(mine[a], theirs[b], None):
                continue
            s1, s2 = _SNAP.match(str(mine[a].get("play_text") or "")), _SNAP.match(str(theirs[b].get("text") or ""))
            if s1 and s2 and abs(int(s1.group(1)) * 60 + int(s1.group(2)) - int(s2.group(1)) * 60 - int(s2.group(2))) > 5:
                continue                      # two different written snap times are two plays (backup_resolve.SNAP_TOLERANCE)
            _d, _dist, sd, yd = ncaa_check.situation(theirs[b].get("drive_text"))
            fp = ncaa_check._int(mine[a].get("field_position"))
            poss = mine[a].get("possession")
            if not (sd and yd and yd != 50 and fp and abs(fp) == yd and poss in other):
                continue
            owner = poss if fp < 0 else other[poss]
            v = votes.setdefault(sd, {home_name: 0, away_name: 0})
            v[owner] += 1
    out = {}
    for code, v in votes.items():
        if v[home_name] != v[away_name]:
            out[code] = "home" if v[home_name] > v[away_name] else "away"
    return out


_SNAP = re.compile(r"^\s*\((\d{1,2}):(\d{2})\)")


def _clock_of(p, timeout=False):
    """(clock, is_snap). Only the crew's "(MM:SS)" at the start of the text is a SNAP time (and a timeout line's
    "clock MM:SS" - the time of that stoppage). Resolve changes a clock only from a snap time.

    ⚠ MEASURED Sep 14 2026: NCAA's clock FIELD is not the snap. Rewritten copies, 125 games, 11,758 plays with a written
    snap: field == own snap 1,245, == the NEXT play's snap 1,410, between the two 8,400. NCAA also writes ", clock 00:00"
    at the END of a field goal line (Fresno Q2: key 0:06). Using them moved Fresno's Q2/Q3 first plays 15:00 -> 14:53 /
    14:55. The field still helps find the pair, so it is kept - marked not a snap. A 0:00 that is not written as the
    snap is no clock (NCAA writes 00:00 on every extra point: 50/50 game-day, 471/475 rewritten)."""
    text = str(p.get("text") or "")
    m = _SNAP.match(text)
    if m:
        return "%d:%02d" % (int(m.group(1)), int(m.group(2))), True
    tc = M.text_clock(text)
    if timeout and tc:
        return tc, True
    c = tc or M.norm_clock(p.get("clock"))
    return (c, False) if M.clock_secs(c) else ("", False)


def to_backup(copy, entries, home_name, away_name, espn_game_id):
    """Resolve's backup data from a kept NCAA copy: only the finished quarters, in NCAA's order."""
    plays = copy.get("plays") or []
    teams = copy.get("teams") or {}
    through = int(copy.get("ready_through") or 0)
    sides = spot_code_sides(entries, plays, home_name, away_name)
    items, skipped = [], 0
    for p in plays:
        q = M._quarter(p.get("quarter"))
        text = str(p.get("text") or "").strip()
        if not q or q > through or not text:
            continue
        low = text.lower()
        clock, is_snap = _clock_of(p, timeout=low.startswith("timeout"))
        base = {"quarter": q, "clock": clock, "clock_secs": M.clock_secs(clock), "clock_is_snap": is_snap,
                "text": text, "score_home": p.get("home_score"), "score_away": p.get("away_score")}
        if low.startswith("timeout"):
            kind = _timeout_item(text, teams)
            if kind is None:
                skipped += 1
                continue
            it = dict(base, kind=kind[0])
            if kind[1]:
                it["timeout_side"] = kind[1]
        elif "kick attempt" in low or "extra point" in low:
            # No clock is borrowed: Middle Tennessee Q1 took the touchdown's SNAP (9:27) where the try is at 9:08.
            it = dict(base, kind="extra_point")
        elif M.is_admin_line(text):
            continue
        elif " kickoff " in f" {low} ":
            it = dict(base, kind="kickoff")
        else:
            down, dist, sd, yd = ncaa_check.situation(p.get("drive_text"))
            if down is None:
                skipped += 1
                continue
            it = dict(base, kind="play", down=int(down), distance=dist, goal_to_go=False,
                      spot_code=sd or "", spot_yard=yd, spot_side=sides.get(sd) if sd else None)
            if not sd and yd is not None:
                it["spot_row_side"] = True     # no team named: the yard, on the side the game already has
        items.append(it)
    per_q = {}
    for seq, it in enumerate(items, 1):
        it["seq"] = seq
        if it["kind"] in ("play", "kickoff", "extra_point", "two_point"):
            per_q[str(it["quarter"])] = per_q.get(str(it["quarter"]), 0) + 1
    status = copy.get("status", "")
    return {
        "format": FORMAT, "version": 1, "source": "primary", "espn_game_id": str(espn_game_id),
        "built_at": time.strftime("%Y-%m-%dT%H:%M:%S"), "saved_at": copy.get("saved_at"),
        "ready": {"through": through, "label": ready_label(status, through)},
        "has_timeouts": any(it["kind"] in ("team_timeout", "tv_timeout", "two_minute") for it in items),
        "code_map": sides, "coded_share": copy.get("coded_share"),
        "summary": {"plays_per_quarter": dict(sorted(per_q.items())), "plays": sum(per_q.values()),
                    "skipped": skipped},
        "items": items, "unparsed": [], "errors": [],
    }
