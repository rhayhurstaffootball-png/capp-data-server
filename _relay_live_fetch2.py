from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query
from fastapi.responses import FileResponse
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

INSTALLER_PATH = "/opt/capp_installer/CAPP_Setup_2.1.10.exe"
INSTALLER_NAME = "CAPP_Setup_2.1.10.exe"

# CAPP Binder Converter — the per-coach local conversion worker. Hosted here
# (not Supabase) because Supabase's project-wide Storage upload limit rejects
# it on the free plan, same reason the installer above lives here too.
CONVERTER_PATH = "/opt/capp_converter/CAPP_Binder_Converter.exe"
CONVERTER_NAME = "CAPP_Binder_Converter.exe"


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


@app.get("/converter/download")
def converter_download():
    if not os.path.exists(CONVERTER_PATH):
        raise HTTPException(status_code=404, detail="Converter not available")
    return FileResponse(
        CONVERTER_PATH,
        media_type="application/octet-stream",
        filename=CONVERTER_NAME,
    )
