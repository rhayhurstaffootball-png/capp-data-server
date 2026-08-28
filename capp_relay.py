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

INSTALLER_PATH = "/opt/capp_installer/CAPP_Setup_2.6.7.exe"
INSTALLER_NAME = "CAPP_Setup_2.6.7.exe"

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
