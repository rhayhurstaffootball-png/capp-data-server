"""Plays ESPN removed OUTRIGHT: remembered per game, reported to keyed clients as withdrawn.

Houston @ Texas Tech Q1, Sep 18 2026 (live): a reviewed pass ("10 yards to the TTU01") was replaced by the crew with
a NEW play id ("11 yards ... TOUCHDOWN ... CALL OVERTURNED"), and the old id vanished from ESPN's feed; same at 2:40
(an incompletion became a completion). ncaa_check.for_keyed_client only knows a replaced entry when ESPN still shows
both (the "superseded" rule), so nothing was reported and every client that had drawn the old rows kept them - two
duplicate boards, each one shifting every later clip in Catapult. The client has taken rows down on the server's
explicit word since Sep 14 2026 ("withdrawn_keys"); it never removes a row merely because a key is absent, and that
stays true - THIS is the explicit word.

Rule: a key this process has served for a game that is missing from a fresh fetch, while a play with a HIGHER ESPN
sequence number is still present (so it is gone from the middle, not from a tail the summary may be late on), on
MISSING_POLLS consecutive fresh fetches, is withdrawn - and stays reported for the rest of the game so a client that
polls later still takes it down. A key that comes back is a play again. "gap:" rows (the check's own adds) are the
client's existing rule and are not tracked here. Process memory only: a restart forgets, which costs nothing - a
client that never saw the old key has nothing to take down.
"""
import threading
import time

MISSING_POLLS = 2          # consecutive fresh fetches a key must be missing from the middle of the feed
TTL_SECONDS = 12 * 3600    # forget a game nobody has asked about for this long

_LOCK = threading.Lock()
_GAMES = {}                # game_id -> {"seen": {key: seq}, "missing": {key: n}, "withdrawn": set(),
                           #             "fetched_at": last payload stamp, "touched": time}


def _seq(e):
    try:
        return int(str(e.get("espn_seq") or "0"))
    except (TypeError, ValueError):
        return 0


def note(game_id, payload):
    """Update the memory for `game_id` from a payload about to be served; return the keys to report withdrawn."""
    entries = (payload or {}).get("entries") or []
    keys = {}
    for e in entries:
        k = str(e.get("entry_key") or "")
        if k and not k.startswith("gap:"):
            keys[k] = _seq(e)
    if not keys:
        return []
    now = time.time()
    with _LOCK:
        for gid in [g for g, st in _GAMES.items() if now - st["touched"] > TTL_SECONDS]:
            del _GAMES[gid]
        st = _GAMES.setdefault(game_id, {"seen": {}, "missing": {}, "withdrawn": set(), "fetched_at": None,
                                         "touched": now})
        st["touched"] = now
        stamp = (payload or {}).get("fetched_at")
        fresh = stamp != st["fetched_at"]
        st["fetched_at"] = stamp
        top = max(keys.values())
        for k in list(st["seen"]):
            if k in keys:
                st["missing"].pop(k, None)
                st["withdrawn"].discard(k)          # back in the feed: it is a play again
            elif fresh and st["seen"][k] < top:     # gone from the MIDDLE of the feed
                st["missing"][k] = st["missing"].get(k, 0) + 1
                if st["missing"][k] >= MISSING_POLLS:
                    st["withdrawn"].add(k)
        st["seen"].update(keys)
        return sorted(st["withdrawn"])


def forget(game_id=None):
    """Testing / admin: drop one game's memory, or all of it."""
    with _LOCK:
        if game_id is None:
            _GAMES.clear()
        else:
            _GAMES.pop(game_id, None)
