"""SERVER COPY of SBENTRY/quarter_swap.py (the coach app's CBS-to-table-row converter, proven live Sep 19 2026 on
SMU Q1 / MTSU Q1 swaps). Used by cbs_tail.py to draw CBS plays the primary feed has not published yet. Pure logic.
⚠ Keep the row rules in step with SBENTRY/quarter_swap.py - a change to one is a change to both."""

import re

PROMPT_SHARE = 0.10
NOTE = "Backup source"
PLAY_KINDS = ("play", "kickoff", "extra_point", "two_point")

_GAIN = re.compile(r"\bfor (-?\d+) yards?\b", re.I)
_FG_LEN = re.compile(r"(\d+)[\s-]*(?:yds?|yards?)\s+field goal|field goal attempt from (\d+) yards?", re.I)
_RUN = re.compile(r"\b(rushed|rushes|rush|scrambles?|scrambled|kneels?|kneeled)\b", re.I)
_KNEEL = re.compile(r"\bkneel", re.I)
_NO_GAIN = re.compile(r"\bno play\b|intercept|\bpunts?\b", re.I)
_TD = re.compile(r"touchdown", re.I)
_PEN_ONLY = re.compile(r"^\s*(\(\d{1,2}:\d{2}\)\s*)?penalty\b", re.I)
_PRE_SNAP = re.compile(r"false start|delay of game|offside|encroach|neutral zone|illegal substitution|too many men|"
                       r"illegal formation|illegal shift|illegal motion|illegal procedure|illegal snap|disconcerting", re.I)


def _int(v):
    try:
        return int(float(str(v).strip()))
    except (TypeError, ValueError):
        return None


def _secs(clock):
    try:
        m, s = str(clock).strip().split(":")
        return int(m) * 60 + int(s)
    except (ValueError, AttributeError):
        return None


def _same_quarter(row, q):
    return str(row.get("quarter", "")).strip() == str(q).strip()


def _name(side, home, away):
    return home if side == "home" else (away if side == "away" else "")


def _spot(item, side):
    """Field position from the backup's spot: minus = the team with the ball's own side (convert_field_position)."""
    y = _int(item.get("spot_yard"))
    if y is None:
        return None
    if y == 50:
        return -50
    if side and item.get("spot_side"):
        return -y if item["spot_side"] == side else y
    return None


def _to_goal(fp):
    """Yards to the goal line from a field position (own side negative)."""
    fp = _int(fp)
    if fp is None:
        return None
    return 100 + fp if fp < 0 else fp


def part_of_previous_play(text):
    """A line that is only a penalty and whose foul is NOT a before-the-snap procedure foul - it belongs to the play it
    happened during or after (Roger, Sep 14 2026)."""
    text = str(text or "")
    return bool(_PEN_ONLY.match(text)) and not _PRE_SNAP.search(text)


def _with_scores(items):
    """(item, score before) over EVERY item in game order. CBS items carry score_*_before (cbs_backup.py); NCAA's
    score_home / score_away is the score AFTER the play (MEASURED Sep 14 2026 on UCD @ SMU's copy: the touchdown line
    itself reads 6-0), so the score before is the previous item's."""
    last = (0, 0)
    for it in items:
        hb, ab = it.get("score_home_before"), it.get("score_away_before")
        before = (hb, ab) if hb is not None and ab is not None else last
        ha, aa = it.get("score_home"), it.get("score_away")
        if it.get("kind") in PLAY_KINDS:
            last = (ha, aa) if ha is not None and aa is not None else before
        yield it, before


def _play_row(it, before, prev, home, away):
    kind = it.get("kind")
    side = it.get("team_side")
    text = str(it.get("text") or "")
    prev = prev or {}
    row = {"home_score": before[0], "away_score": before[1], "clock": it.get("clock") or prev.get("clock", ""),
           "quarter": str(it.get("quarter")), "possession": _name(side, home, away) or prev.get("possession", ""),
           "run_clock": "No", "home_time_out": "No", "away_time_out": "No", "play_text": text, "qc_issue": NOTE}
    if kind == "kickoff":
        row.update(down="KO", distance=0, gain=0, field_position=-35)
        return row
    if kind in ("extra_point", "two_point"):
        row.update(down="EP" if kind == "extra_point" else "2PT", distance=3, gain=0, field_position=3)
        return row
    dist = _int(it.get("distance"))
    fp = _spot(it, side)
    if fp is None:
        fp = prev.get("field_position", 0)
    if it.get("goal_to_go") and side and it.get("spot_side") == side:
        # "Goal" on the offense's own side cannot be - the distance the play before left.
        pd, pg = _int(prev.get("distance")), _int(prev.get("gain"))
        same = prev.get("possession") == row["possession"] and str(prev.get("down", "")).isdigit()
        dist = (pd - (pg or 0)) if same and pd is not None and pd - (pg or 0) > 0 else 10
    if "field goal" in text.lower():
        m = _FG_LEN.search(text)
        gain = _int(next((g for g in m.groups() if g), None)) if m else 0
        down = "FG"
    else:
        down = str(it["down"]) if it.get("down") is not None else str(prev.get("down") or "1")
        g = _GAIN.search(text)
        gain = 0 if (_NO_GAIN.search(text) or not g) else int(g.group(1))
    row.update(down=down, distance=dist if dist is not None else 10, gain=gain or 0, field_position=fp)
    if (down.isdigit() and _RUN.search(text) and not _TD.search(text)
            and (gain or 0) < (dist if dist is not None else 10)):
        row["run_clock"] = "Yes"
    return row


def _timeout_row(it, nxt, before, home, away, timeout_text=None):
    side = it.get("timeout_side")
    team = _name(side, home, away)
    base = nxt or {}
    clock = it.get("clock") or base.get("clock", "")
    down, dist = str(base.get("down", "")), base.get("distance", 10)
    if not down.isdigit():                   # before a kickoff / try: no down to carry
        down, dist = "1", 10
    s = _secs(clock)
    tail = (", clock %02d:%02d" % divmod(s, 60)) if s is not None else ""
    text = f"Timeout {team}{tail}"
    if timeout_text:
        text = timeout_text("team", team, clock, text)
    return {"home_score": before[0], "away_score": before[1], "clock": clock, "quarter": str(it.get("quarter")),
            "down": down, "distance": dist, "gain": 0, "field_position": base.get("field_position", 0),
            "possession": base.get("possession", ""), "run_clock": "No",
            "home_time_out": "Yes" if side == "home" else "No", "away_time_out": "Yes" if side == "away" else "No",
            "play_text": text, "qc_issue": NOTE}


def _after_pass(plays):
    """Rules that need the NEXT snap: run clock on a change of possession, a kneel's yards."""
    for k, r in enumerate(plays):
        nxt = plays[k + 1] if k + 1 < len(plays) else None
        if r["run_clock"] == "Yes" and nxt is not None and nxt.get("possession") != r.get("possession"):
            r["run_clock"] = "No"
        if _KNEEL.search(str(r.get("play_text") or "")) and not _GAIN.search(str(r.get("play_text") or "")):
            here, there = _to_goal(r.get("field_position")), _to_goal((nxt or {}).get("field_position"))
            if (nxt is not None and nxt.get("possession") == r.get("possession") and str(nxt.get("down", "")).isdigit()
                    and here is not None and there is not None):
                r["gain"] = here - there
            else:
                r["gain"] = -1
            if r["gain"] >= (_int(r.get("distance")) or 10):
                r["run_clock"] = "No"


def rows_for_quarter(backup, q, home_name, away_name, timeout_text=None):
    """The backup's rows for quarter q (1-4), in the backup's order."""
    items = sorted(backup.get("items") or [], key=lambda it: it.get("seq", 0))
    out, waiting, plays = [], [], []
    for it, before in _with_scores(items):
        if _int(it.get("quarter")) != _int(q):
            continue
        kind = it.get("kind")
        if kind == "team_timeout" and it.get("timeout_side") in ("home", "away"):
            waiting.append((it, before))
            continue
        if kind not in PLAY_KINDS:
            continue                             # TV timeouts / two-minute warnings - not needed
        text = str(it.get("text") or "").strip()
        if kind == "play" and plays and not waiting and part_of_previous_play(text):
            plays[-1]["play_text"] = f"{plays[-1]['play_text']} {text}".strip()   # part of the play it happened during
            continue
        row = _play_row(it, before, plays[-1] if plays else None, home_name, away_name)
        for t, tb in waiting:
            out.append(_timeout_row(t, row, tb, home_name, away_name, timeout_text))
        waiting = []
        out.append(row)
        plays.append(row)
    for t, tb in waiting:
        out.append(_timeout_row(t, plays[-1] if plays else None, tb, home_name, away_name, timeout_text))
    _after_pass(plays)
    return out


def swap_quarter(rows, backup, q, home_name, away_name, timeout_text=None):
    """(new table rows, info). Quarter q's rows are replaced by the backup's, at the place the quarter's first row was;
    every other row is untouched and keeps its order. info["removed"] / ["removed_at"] let Undo put them back."""
    new = rows_for_quarter(backup, q, home_name, away_name, timeout_text)
    if not new:
        return list(rows), {"swapped": False, "why": "The backup source has no plays for that quarter."}
    idx = [i for i, r in enumerate(rows) if _same_quarter(r, q)]
    if idx:
        pos = idx[0]
    else:
        pos = next((i for i, r in enumerate(rows) if (_int(r.get("quarter")) or 99) > _int(q)), len(rows))
    before = [r for r in rows[:pos] if not _same_quarter(r, q)]
    after = [r for r in rows[pos:] if not _same_quarter(r, q)]
    out = before + [dict(r, _backup="swapped") for r in new] + after
    return out, {"swapped": True, "quarter": str(q), "removed": [rows[i] for i in idx], "removed_at": idx,
                 "added": len(new), "position": len(before)}


def quarters_to_offer(quarter_issues, feed_quarters, status, ready_through, already_asked, share=PROMPT_SHARE,
                      table_quarters=None):
    """Finished quarters (1-4) worth the prompt: SHARE or more of the quarter's rows have an issue, the backup has that
    quarter, and the coach was never asked about it. A quarter is finished once the feed has a play in a later quarter
    or the game is over.

    `table_quarters` = the quarters actually IN THE TABLE, and a quarter that is not there is never offered.
    ⚠ WHY (Roger, Sep 17 2026, live in Syracuse @ Pitt): *"I was working on the 4th quarter when a Alert popped up
    that the 2nd quarter had a higher than 10% error rate.. so I clicked yes used the back up and then the 2nd and
    4th quarter was loaded."* The prompt read the FEED's quarters, so it offered Q2 while the table held only Q4;
    `swap_quarter` found no Q2 rows to replace, fell through to "insert before the first later quarter", and put 59
    backup rows for a quarter he was not working on on top of his Q4 segment (his log: "[swap] Q2 now from the backup
    source: 0 rows out, 59 in"). Same rule he set for Resolve on Sep 15: "It should only resolve the plays currently
    in the Table." `feed_quarters` still decides what is FINISHED - that is a different question and it needs the
    feed, not the table.
    Left as None = every quarter, so the pure tests and any caller that has no table keep their old behaviour.
    """
    qs = [_int(x) for x in (feed_quarters or []) if _int(x)]
    top = max(qs) if qs else 0
    over = str(status or "").strip().lower() in ("post", "final")
    asked = {str(a) for a in (already_asked or ())}
    on_table = None if table_quarters is None else {_int(x) for x in table_quarters if _int(x)}
    out = []
    for key, c in (quarter_issues or {}).items():
        q = _int(key)
        if not q or q > 4 or str(q) in asked:
            continue
        if on_table is not None and q not in on_table:
            continue
        if not (q < top or over) or q > int(ready_through or 0):
            continue
        if float((c or {}).get("share") or 0) >= share:
            out.append(q)
    return sorted(out)


def prompt_text(q):
    return f"Q{q} SCOREBOARDS HAVE OVER A 10% ERROR RATE... WOULD YOU LIKE TO USE THE BACKUP SOURCE?"
