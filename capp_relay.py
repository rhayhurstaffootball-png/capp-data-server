from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse
from typing import Dict, Optional
import asyncio
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="CAPP VNC Relay")

_sessions:    Dict[str, Dict[str, Optional[WebSocket]]]  = {}
_send_locks:  Dict[str, Dict[str, asyncio.Lock]]         = {}

_KEEPALIVE_INTERVAL = 10

INSTALLER_PATH = "/opt/capp_installer/CAPP_Setup_2.7.0.exe"
INSTALLER_NAME = "CAPP_Setup_2.7.0.exe"

# CAPP Binder Converter — the per-coach local conversion worker. Hosted here
# (not Supabase) because Supabase's project-wide Storage upload limit rejects
# it on the free plan, same reason the installer above lives here too.
CONVERTER_PATH = "/opt/capp_converter/CAPP_Binder_Converter.exe"
CONVERTER_NAME = "CAPP_Binder_Converter.exe"

# Pairing-token embedding: when a coach's browser requests the converter with
# ?t=<pairing_token>, the token is appended to the EXE bytes behind this
# marker instead of being downloaded as a separate pairing_token.txt file.
# The EXE reads its own trailing bytes on first launch to recover it, so the
# ONLY thing that ever touches the coach's Desktop/Downloads is the EXE
# itself — which self-deletes a couple seconds after install. See
# CONVERTER/capp_binder_converter.py and "T:\BINDER LOCAL PLAN.txt".
_PAIR_MARKER = b"\n<<CAPP_PAIR_TOKEN>>"


async def _keepalive_task(websocket: WebSocket, lock: asyncio.Lock):
    """Periodically send a lightweight keepalive on this WebSocket."""
    while True:
        await asyncio.sleep(_KEEPALIVE_INTERVAL)
        async with lock:
            try:
                await websocket.send_text('{"type":"keepalive"}')
            except Exception:
                try:
                    await websocket.close()
                except Exception:
                    pass
                break


@app.websocket("/vnc/{machine_id}/{role}")
async def vnc_relay(
    websocket: WebSocket,
    machine_id: str,
    role: str,
    x_api_key: str = Query(None),
):
    if role not in ("host", "viewer"):
        await websocket.close(code=4000)
        return

    session_key = machine_id
    await websocket.accept()
    logger.info(f"[{session_key}] {role} connected")

    if session_key not in _sessions:
        _sessions[session_key]   = {"host": None, "viewer": None}
        _send_locks[session_key] = {"host": asyncio.Lock(), "viewer": asyncio.Lock()}

    # A second connection for the same machine+role REPLACES the first in
    # this table, which already makes the old socket unreachable -- routing
    # looks the peer up here -- but it was never CLOSED, so the fd leaked.
    # Aug 14-15 2026: one host reconnecting ~96x/min held 2,283 orphaned
    # sockets open, exhausted the fd limit and took the relay down twice.
    # Closing the superseded socket is invisible to correct use: the relay
    # only ever supports ONE host and ONE viewer per machine.
    _old = _sessions[session_key].get(role)
    if _old is not None and _old is not websocket:
        try:
            await _old.close(code=4001)
        except Exception:
            pass
        logger.info("[%s] closed superseded %s connection" % (session_key, role))

    _sessions[session_key][role]   = websocket
    _send_locks[session_key][role] = asyncio.Lock()

    other_role = "viewer" if role == "host" else "host"
    my_lock    = _send_locks[session_key][role]

    ka = asyncio.create_task(_keepalive_task(websocket, my_lock))

    try:
        while True:
            data = await websocket.receive()
            if data.get("type") == "websocket.disconnect":
                break

            other_ws   = _sessions.get(session_key, {}).get(other_role)
            other_lock = _send_locks.get(session_key, {}).get(other_role)

            if other_ws is not None and other_lock is not None:
                async with other_lock:
                    try:
                        if data.get("bytes") is not None:
                            await other_ws.send_bytes(data["bytes"])
                        elif data.get("text") is not None:
                            await other_ws.send_text(data["text"])
                    except Exception:
                        pass
    except Exception:
        pass
    finally:
        ka.cancel()
        logger.info(f"[{session_key}] {role} disconnected")
        if session_key in _sessions:
            if _sessions[session_key][role] is websocket:
                _sessions[session_key][role] = None
                if all(v is None for v in _sessions[session_key].values()):
                    del _sessions[session_key]
                    del _send_locks[session_key]


@app.get("/sessions")
def sessions():
    connected = [
        mid for mid, roles in _sessions.items()
        if roles.get("host") is not None
    ]
    return {"connected": connected}


@app.get("/health")
def health():
    return {"status": "ok", "sessions": len(_sessions)}


@app.get("/installer/download")
def installer_download():
    if not os.path.exists(INSTALLER_PATH):
        raise HTTPException(status_code=404, detail="Installer not available")
    return FileResponse(
        INSTALLER_PATH,
        media_type="application/octet-stream",
        filename=INSTALLER_NAME,
    )


def _stream_with_suffix(path: str, suffix: bytes):
    def gen():
        with open(path, "rb") as f:
            while True:
                chunk = f.read(1024 * 1024)
                if not chunk:
                    break
                yield chunk
        yield suffix
    return gen()


@app.get("/converter/download")
def converter_download(t: str = Query(None)):
    if not os.path.exists(CONVERTER_PATH):
        raise HTTPException(status_code=404, detail="Converter not available")
    if not t:
        # Plain download (admin/manual/no coach session) — unchanged, full
        # Range support via FileResponse for resumable downloads.
        return FileResponse(
            CONVERTER_PATH,
            media_type="application/octet-stream",
            filename=CONVERTER_NAME,
        )
    # Coach setup flow: embed this one-time pairing token into the EXE bytes
    # so no separate token file is ever written to disk. Single-shot stream
    # (no Range support on this branch) — fine for a one-time setup download.
    suffix = _PAIR_MARKER + t.encode("utf-8")
    total_size = os.path.getsize(CONVERTER_PATH) + len(suffix)
    headers = {
        "Content-Disposition": f'attachment; filename="{CONVERTER_NAME}"',
        "Content-Length": str(total_size),
    }
    return StreamingResponse(
        _stream_with_suffix(CONVERTER_PATH, suffix),
        media_type="application/octet-stream",
        headers=headers,
    )


# =============================================================================
# Workflow cloud sync  (added Aug 28 2026)
# =============================================================================
# Lets every seat on ONE login share Workflow notes and the OFF/DEF/KICKS/PFF
# toggles. Storage is namespaced by a hash of the API KEY, and both seats of a
# licence share one key -- so "same login sees the same data" falls out of the
# namespace rather than needing any per-user logic.
#
# WHY THIS ARRIVED LATE: the client has been pushing to these URLs since
# Mar 31 2026 and the routes were NEVER deployed. _cloud_push_async() is
# fire-and-forget with a swallowed exception, so five months of 404s were
# completely silent. Confirmed Aug 28 2026: no relay backup ever contained
# them and /root/workflow_sync did not exist.
#
# ⚠ THE CLIENT SENDS THREE PATH SEGMENTS: /workflow/{client_id}/{filename}.
# The original patch in _dev_tools/relay_workflow_patch.py declared only TWO
# (/workflow/{filename}), so deploying it verbatim would still have 404'd.
# The client_id in the path is accepted and DELIBERATELY IGNORED for storage --
# it is a plain string like "airforce" and therefore guessable, whereas the
# API key is not. Namespacing on the key keeps seats isolated by something
# unguessable while still being shared between the seats that matter.
import hashlib as _wf_hashlib
import json as _wf_json
import os as _wf_os
import re as _wf_re
from pathlib import Path as _WfPath
from fastapi import Request as _WfRequest
from fastapi.responses import JSONResponse as _WfJSONResponse

WORKFLOW_STORE = _WfPath("/root/workflow_sync")

# A workflow_data.json with a full season for 51 teams measures ~142 KB, so
# 5 MB is ~35x headroom. The cap exists because THIS BOX FILLED ITS DISK ON
# AUG 14 2026 and took the relay down for two days -- an unbounded write
# endpoint is exactly how that happens again.
_WF_MAX_BYTES = 5_000_000
_WF_MAX_FILES = 400          # ~51 team_* files + 2 shared today


def _wf_ns(api_key: str) -> _WfPath:
    h = _wf_hashlib.sha256(api_key.encode()).hexdigest()[:24]
    p = WORKFLOW_STORE / h
    p.mkdir(parents=True, exist_ok=True)
    return p


def _wf_key_from_request(request: _WfRequest) -> str:
    key = request.headers.get("x-api-key", "").strip()
    # Length-only sanity check. The relay cannot verify a key against Supabase
    # (no credentials here, by design -- it is a byte forwarder), so this stops
    # junk/empty namespaces without pretending to be authentication. Real
    # isolation comes from the key being unguessable.
    if not (20 <= len(key) <= 200):
        raise HTTPException(status_code=401, detail="Missing or malformed API key")
    return key


def _wf_safe_name(filename: str) -> str:
    """Filename guard that still accepts REAL team names.

    ⚠ The client pushes `team_{team}` where team is only stripped of slashes --
    so "team_AIR FORCE" (space), "team_Hawai'i" (apostrophe) and
    "team_St. Thomas (MN)" (period, parens) all arrive here. The original
    patch's ^[a-zA-Z0-9_-]+$ rejected every one of them with a 400, which would
    have looked like "team extras just don't sync".
    """
    if not filename or len(filename) > 120:
        raise HTTPException(status_code=400, detail="Invalid filename")
    # Reject anything that could escape the namespace directory.
    if ("/" in filename or "\\" in filename or "\x00" in filename
            or ".." in filename or filename.startswith(".")):
        raise HTTPException(status_code=400, detail="Invalid filename")
    if not _wf_re.match(r"^[A-Za-z0-9 _\-().'&+]+$", filename):
        raise HTTPException(status_code=400, detail="Invalid filename")
    return filename


@app.get("/workflow/{client_id}/meta")
async def workflow_meta(client_id: str, request: _WfRequest):
    """{filename: mtime} so a client can poll cheaply and pull only what moved.

    ⚠ Declared BEFORE the generic /{filename} route -- FastAPI matches in
    declaration order, so the other way round "meta" would be read as a
    filename and this would never run.
    """
    ns = _wf_ns(_wf_key_from_request(request))
    return _WfJSONResponse({f.stem: f.stat().st_mtime for f in ns.glob("*.json")})


@app.get("/workflow/{client_id}/{filename}")
async def workflow_get(client_id: str, filename: str, request: _WfRequest):
    ns = _wf_ns(_wf_key_from_request(request))
    path = ns / f"{_wf_safe_name(filename)}.json"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Not found")
    try:
        return _WfJSONResponse(_wf_json.loads(path.read_text(encoding="utf-8")))
    except Exception:
        # A half-written file must not become a permanent 500 for that seat.
        raise HTTPException(status_code=404, detail="Not found")


@app.put("/workflow/{client_id}/{filename}")
async def workflow_put(client_id: str, filename: str, request: _WfRequest):
    ns = _wf_ns(_wf_key_from_request(request))
    name = _wf_safe_name(filename)
    raw = await request.body()
    if len(raw) > _WF_MAX_BYTES:
        raise HTTPException(status_code=413, detail="Workflow file too large")
    try:
        body = _wf_json.loads(raw.decode("utf-8"))
    except Exception:
        raise HTTPException(status_code=400, detail="Body must be JSON")
    path = ns / f"{name}.json"
    if not path.exists() and len(list(ns.glob("*.json"))) >= _WF_MAX_FILES:
        raise HTTPException(status_code=507, detail="Too many workflow files")
    # Atomic: a crash or a concurrent GET must never see a half-written file,
    # which would otherwise wipe a seat's notes on the next merge.
    tmp = ns / f"{name}.json.tmp"
    tmp.write_text(_wf_json.dumps(body, ensure_ascii=False), encoding="utf-8")
    _wf_os.replace(tmp, path)
    return _WfJSONResponse({"ok": True})


# =============================================================================
# Scoreboard Designer cloud sync  (added Sep 1 2026)
# =============================================================================
# Roger: "If you save a layout on one computer, if you log onto another computer
# with the same login, the layout is available."
#
# Same namespace trick as the workflow routes above -- storage keyed on
# sha256(api_key)[:24], and both seats of a licence share one key, so "same
# login sees the same data" needs no per-user logic. The client_id in the path
# is accepted and ignored for storage (guessable string vs unguessable key).
#
# ⚠ WHY THERE ARE ASSET ROUTES AND NOT JUST LAYOUT JSON: a layout file is NOT
# self-contained. SBEDITOR_qt.save_layout() stores only BASENAMES for the
# background, per-element images and the font; load_layout_by_name() re-joins
# them against the local BGIMAGES/Fonts folders and silently skips whatever is
# missing (every resolve is guarded by os.path.exists). Syncing the JSON alone
# would hand seat B a layout with no background and the wrong font, showing no
# error at all. The bytes have to travel with it.
from fastapi.responses import Response as _WfResponse

_SB_STORE = _WfPath("/root/sb_sync")

# Backgrounds are PNG/JPG, not a 16 KB JSON -- this is the one endpoint here
# that moves real bytes. Caps are deliberately tight: THIS BOX FILLED ITS DISK
# ON AUG 14 2026 and took the relay down for two days.
_SB_MAX_LAYOUT_BYTES = 2_000_000
_SB_MAX_ASSET_BYTES = 10_000_000
_SB_MAX_LAYOUTS = 300
_SB_MAX_ASSETS = 300                      # per kind
_SB_KINDS = {"bg", "font"}
_SB_EXTS = {
    "bg": {".png", ".jpg", ".jpeg"},
    "font": {".ttf", ".otf", ".ttc"},
}


def _sb_ns(api_key: str, *parts) -> _WfPath:
    h = _wf_hashlib.sha256(api_key.encode()).hexdigest()[:24]
    p = _SB_STORE.joinpath(h, *parts)
    p.mkdir(parents=True, exist_ok=True)
    return p


def _sb_safe_name(filename: str, kind: str = None) -> str:
    """Same permissive rule as _wf_safe_name -- layout names are FREE TEXT typed
    into a dialog and will contain spaces, apostrophes and periods. When `kind`
    is given the extension must also be one we expect to serve."""
    if not filename or len(filename) > 160:
        raise HTTPException(status_code=400, detail="Invalid filename")
    if ("/" in filename or "\\" in filename or "\x00" in filename
            or ".." in filename or filename.startswith(".")):
        raise HTTPException(status_code=400, detail="Invalid filename")
    if not _wf_re.match(r"^[A-Za-z0-9 _\-().'&+]+$", filename):
        raise HTTPException(status_code=400, detail="Invalid filename")
    if kind is not None:
        if _wf_os.path.splitext(filename)[1].lower() not in _SB_EXTS[kind]:
            raise HTTPException(status_code=400, detail="Unsupported file type")
    return filename


def _sb_kind(kind: str) -> str:
    if kind not in _SB_KINDS:
        raise HTTPException(status_code=400, detail="Unknown asset kind")
    return kind


def _sb_write_atomic(path: _WfPath, raw: bytes):
    """A half-written background must never be readable -- the next seat to pull
    would cache a truncated image and the layout would render wrong with no
    error (same failure class the workflow routes guard against)."""
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_bytes(raw)
    _wf_os.replace(tmp, path)


@app.get("/sbsync/{client_id}/meta")
async def sb_meta(client_id: str, request: _WfRequest):
    """Everything a client needs to decide what to move, in one call.

    layouts: {name: mtime}         -- pull only what is newer than local
    assets:  {kind: {name: sha}}   -- skip an upload whose bytes already match,
                                      and pull only assets a layout references.

    ⚠ Declared BEFORE the generic routes below: FastAPI matches in declaration
    order, so otherwise "meta" would be read as a layout name.
    """
    key = _wf_key_from_request(request)
    layouts = {f.stem: f.stat().st_mtime for f in _sb_ns(key, "layouts").glob("*.json")}
    assets = {}
    for kind in sorted(_SB_KINDS):
        d = _sb_ns(key, "assets", kind)
        entry = {}
        for f in d.iterdir():
            if f.is_file() and not f.name.endswith(".tmp"):
                entry[f.name] = _wf_hashlib.sha256(f.read_bytes()).hexdigest()[:32]
        assets[kind] = entry
    return _WfJSONResponse({"layouts": layouts, "assets": assets})


@app.get("/sbsync/{client_id}/layout/{name}")
async def sb_layout_get(client_id: str, name: str, request: _WfRequest):
    ns = _sb_ns(_wf_key_from_request(request), "layouts")
    path = ns / f"{_sb_safe_name(name)}.json"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Not found")
    try:
        return _WfJSONResponse(_wf_json.loads(path.read_text(encoding="utf-8")))
    except Exception:
        raise HTTPException(status_code=404, detail="Not found")


@app.put("/sbsync/{client_id}/layout/{name}")
async def sb_layout_put(client_id: str, name: str, request: _WfRequest):
    ns = _sb_ns(_wf_key_from_request(request), "layouts")
    safe = _sb_safe_name(name)
    raw = await request.body()
    if len(raw) > _SB_MAX_LAYOUT_BYTES:
        raise HTTPException(status_code=413, detail="Layout too large")
    try:
        body = _wf_json.loads(raw.decode("utf-8"))
    except Exception:
        raise HTTPException(status_code=400, detail="Body must be JSON")
    path = ns / f"{safe}.json"
    if not path.exists() and len(list(ns.glob("*.json"))) >= _SB_MAX_LAYOUTS:
        raise HTTPException(status_code=507, detail="Too many layouts")
    _sb_write_atomic(path, _wf_json.dumps(body, ensure_ascii=False).encode("utf-8"))
    return _WfJSONResponse({"ok": True})


@app.get("/sbsync/{client_id}/asset/{kind}/{name}")
async def sb_asset_get(client_id: str, kind: str, name: str, request: _WfRequest):
    kind = _sb_kind(kind)
    ns = _sb_ns(_wf_key_from_request(request), "assets", kind)
    path = ns / _sb_safe_name(name, kind)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Not found")
    return _WfResponse(path.read_bytes(), media_type="application/octet-stream")


@app.put("/sbsync/{client_id}/asset/{kind}/{name}")
async def sb_asset_put(client_id: str, kind: str, name: str, request: _WfRequest):
    kind = _sb_kind(kind)
    ns = _sb_ns(_wf_key_from_request(request), "assets", kind)
    safe = _sb_safe_name(name, kind)
    raw = await request.body()
    if not raw:
        raise HTTPException(status_code=400, detail="Empty body")
    if len(raw) > _SB_MAX_ASSET_BYTES:
        raise HTTPException(status_code=413, detail="Asset too large")
    path = ns / safe
    if not path.exists() and sum(1 for f in ns.iterdir() if f.is_file()) >= _SB_MAX_ASSETS:
        raise HTTPException(status_code=507, detail="Too many assets")
    _sb_write_atomic(path, raw)
    return _WfJSONResponse({"ok": True, "sha": _wf_hashlib.sha256(raw).hexdigest()[:32]})
