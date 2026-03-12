from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from typing import Dict, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="CAPP VNC Relay")

_sessions: Dict[str, Dict[str, Optional[WebSocket]]] = {}

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
        _sessions[session_key] = {"host": None, "viewer": None}
    _sessions[session_key][role] = websocket

    other_role = "viewer" if role == "host" else "host"

    try:
        while True:
            data = await websocket.receive()
            if data.get("type") == "websocket.disconnect":
                break
            other_ws = _sessions.get(session_key, {}).get(other_role)
            if other_ws is not None:
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
        logger.info(f"[{session_key}] {role} disconnected")
        if session_key in _sessions:
            _sessions[session_key][role] = None
            if all(v is None for v in _sessions[session_key].values()):
                del _sessions[session_key]

@app.get("/health")
def health():
    return {"status": "ok", "sessions": len(_sessions)}
