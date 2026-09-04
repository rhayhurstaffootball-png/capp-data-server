"""CollegeFootballData live play-by-play — a SECOND opinion on a quiet game.

Built Sep 3 2026, the night Colorado @ Georgia Tech sat "In Progress" at 7:49 of
the 2nd for 40+ minutes with zero plays while two other live games fed normally.

⚠ READ THIS BEFORE TREATING CFBD AS A SAFETY NET.
Measured that same night, all three games live at once:

    game                        ESPN plays   CFBD plays
    Colorado @ Georgia Tech          0            0      <- BOTH DARK
    Ark-Pine Bluff @ Missouri       86           84
    Idaho @ Utah                    34           30

CFBD did NOT have the missing game either. Both feeds ultimately derive from the
same stadium/officiating data, so when a game goes dark it goes dark everywhere.
**CFBD is not protection against a dead game feed.** What it IS good for:
  - ESPN-side outages (endpoint retired, envelope change, auth, rate limiting),
    where CFBD keeps working because it is a different vendor.
  - Telling the two cases APART, which is the real value. "ESPN dark but CFBD
    has plays" and "both dark" need opposite responses, and without a second
    source they look identical — silence.

API shape notes, learned the hard way:
  - The parameter is `gameId`, NOT `id`. Passing `id` returns 400
    "Validation Failed", which reads like an outage and is not one.
  - Plays are nested under `drives[].plays[]`. There is no top-level `plays`
    array; reading one gives 0 for a perfectly healthy game.
  - A game with nothing published answers **400 "No plays found for game."**,
    not 200-with-empty. A 400 here is DATA, not a failure.
  - CFBD uses the same game ids as ESPN, so no id mapping is needed.
"""

import json
import os
import threading
import time
import urllib.error
import urllib.request

CFBD_BASE = os.environ.get("CFBD_BASE", "https://apinext.collegefootballdata.com")
_TIMEOUT = 12

# Never let a second opinion cost more than it is worth: one lookup per game per
# window, and never on the hot path of serving plays.
_CACHE_SECONDS = 45
_cache: dict = {}
_lock = threading.Lock()


def _api_key() -> str:
    return (os.environ.get("CFBD_API_KEY") or "").strip()


def available() -> bool:
    """False when no key is configured — the caller must degrade quietly rather
    than report a problem that is really just missing configuration."""
    return bool(_api_key())


def _call(path: str):
    """(status, parsed_json). A 400 is returned, not raised — CFBD answers 400
    with a meaningful body for 'no plays yet'."""
    req = urllib.request.Request(
        f"{CFBD_BASE}{path}",
        headers={"Authorization": f"Bearer {_api_key()}",
                 "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
            return resp.status, json.loads(resp.read() or b"null")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read() or b"null")
        except Exception:
            return e.code, None
    except Exception:
        return 0, None


def live_play_count(game_id) -> dict:
    """How many plays CFBD has for this game right now.

    Returns {"available", "plays", "period", "clock", "status", "note"}.
    Never raises and never blocks longer than the timeout — this is a
    corroborating signal, not a dependency.
    """
    gid = str(game_id or "").strip()
    if not gid:
        return {"available": False, "plays": 0, "note": "no game id"}
    if not available():
        return {"available": False, "plays": 0, "note": "CFBD_API_KEY not set"}

    now = time.time()
    with _lock:
        hit = _cache.get(gid)
        if hit and now - hit[0] < _CACHE_SECONDS:
            return dict(hit[1])

    status, body = _call(f"/live/plays?gameId={gid}")
    if status == 200 and isinstance(body, dict):
        plays = sum(len(d.get("plays") or []) for d in (body.get("drives") or []))
        out = {"available": True, "plays": plays,
               "period": body.get("period"), "clock": body.get("clock"),
               "status": body.get("status", ""), "note": ""}
    elif status == 400 and isinstance(body, dict):
        # Expected for a game with nothing published — this is the answer, not
        # an error. See the module docstring.
        out = {"available": True, "plays": 0, "status": "",
               "note": str(body.get("message") or "no plays")}
    else:
        out = {"available": False, "plays": 0,
               "note": f"CFBD unreachable (HTTP {status})"}

    with _lock:
        _cache[gid] = (now, dict(out))
    return out
