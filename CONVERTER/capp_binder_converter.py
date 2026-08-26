"""CAPP Binder Converter â€” the coach-facing, per-machine background worker.

Distributed as a single signed EXE. Fully invisible: no window, no console,
no tray icon â€” matches the dev-machine pb_worker.py exactly. A coach downloads
this from the Binder's "Complete Setup" screen alongside a one-time pairing
token; on first launch it installs itself, registers to auto-start with
Windows, pairs to that coach's own login, and from then on only ever converts
files THAT coach uploaded â€” never another coach's, even on the same team.
See "T:\\BINDER LOCAL PLAN.txt" for the full design.

This is the compiled counterpart to capp-data-server/pb_worker.py (Roger's own
dev-machine worker, which still exists for admin-panel-direct uploads under the
legacy shared token). Logic is intentionally kept in sync with pb_worker.py's
claim/convert/insert/number pipeline; this file adds the parts a distributable
EXE needs that a dev script run from a shared drive doesn't: frozen-aware
paths, a bundled copy of the Visio Converter's font toolkit (pb_worker.py on
Roger's own PC reaches it via a hardcoded path that only exists on HIS
machine), and the one-time self-install + auto-start step.
"""
import atexit
import datetime
import json
import os
import pathlib
import re
import shutil
import socket
import sys
import tempfile
import time
import traceback
import urllib.request

SERVER = "https://capp-data-server.onrender.com"
POLL_SECONDS = 5
WORKER_NAME = socket.gethostname()[:64]
APP_NAME = "CAPP Binder Converter"

# This build's version. Stamped by build_converter.ps1 -Version at build time,
# exactly like AGENT_VERSION in capp_agent.py. An installed converter compares
# this against GET /converter/version (the CONVERTER_VERSION env var on Render)
# and silently updates itself when Render reports a higher one â€” so BOTH have to
# move for a release to reach anybody: upload the new exe AND bump the env var.
CONVERTER_VERSION = "1.2.6"

_FROZEN = bool(getattr(sys, "frozen", False))

# â”€â”€ where this install lives â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Frozen: a stable per-user folder (survives the exe being re-launched from
# anywhere). Source/dev run: next to this script, same as pb_worker.py.
if _FROZEN:
    _APP_DIR = pathlib.Path(os.environ["LOCALAPPDATA"]) / "CAPP Binder Converter"
else:
    _APP_DIR = pathlib.Path(__file__).parent
_APP_DIR.mkdir(parents=True, exist_ok=True)

_LOG_PATH = _APP_DIR / "converter.log"
_DEVICE_TOKEN_PATH = _APP_DIR / "device_token.json"
_PAIRING_TOKEN_PATH = _APP_DIR / "pairing_token.txt"


def log(msg: str) -> None:
    line = f"[{datetime.datetime.now():%m-%d %H:%M:%S}] {msg}"
    if not _FROZEN:
        print(line, flush=True)
    try:
        with open(_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def _trim_log() -> None:
    try:
        if _LOG_PATH.exists() and _LOG_PATH.stat().st_size > 1_000_000:
            keep = _LOG_PATH.read_text(encoding="utf-8", errors="ignore").splitlines()[-500:]
            _LOG_PATH.write_text("\n".join(keep) + "\n", encoding="utf-8")
    except Exception:
        pass


# â”€â”€ one-time self-install (frozen only) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# A coach double-clicks the downloaded EXE from wherever their browser put it
# (usually Downloads/Desktop). The "Complete Setup" screen embeds the coach's
# one-time pairing token INTO the downloaded EXE's own bytes (see the relay's
# /converter/download?t=... route) â€” there is no separate pairing_token.txt
# download anymore. First launch: pull the token out of this file's own
# trailing bytes, copy the CLEAN (token-stripped) bytes into the stable
# per-user folder so the permanent install never carries the secret, relaunch
# that installed copy passing the token as a one-time startup argument,
# register to auto-start with Windows (HKCU Run key â€” no admin rights
# needed), and exit this one â€” which then gets deleted from Downloads a
# couple seconds later. Every launch after that is already running from
# _APP_DIR, so this whole function is a no-op.
#
# A pairing_token.txt file next to the EXE (the OLD flow) is still honored as
# a fallback for manual/admin use, so nothing breaks if someone drops one in
# by hand.
_PAIR_MARKER = b"\n<<CAPP_PAIR_TOKEN>>"


def _extract_embedded_token(exe_path: pathlib.Path) -> str | None:
    """Read this EXE's own trailing bytes for an embedded pairing token.
    Returns (token, marker_offset) via a 2-tuple, or (None, None) if absent."""
    try:
        data = exe_path.read_bytes()
    except OSError:
        return None, None
    idx = data.rfind(_PAIR_MARKER)
    if idx == -1:
        return None, None
    tok = data[idx + len(_PAIR_MARKER):].decode("utf-8", errors="ignore").strip()
    return (tok, idx) if tok else (None, None)


def _find_downloaded_token(folder: pathlib.Path) -> pathlib.Path | None:
    """A pairing_token.txt downloaded alongside the EXE lands in the same
    folder as the EXE itself (browser default download location). If the
    coach re-ran 'Complete Setup' before (or the browser already had a file
    by that name), the browser saves it as 'pairing_token (1).txt' etc â€” an
    exact-name match would miss it and leave the real token sitting in
    Downloads forever. Glob for any variant and take the newest."""
    try:
        candidates = sorted(folder.glob("pairing_token*.txt"),
                            key=lambda p: p.stat().st_mtime, reverse=True)
    except OSError:
        return None
    return candidates[0] if candidates else None


def _self_delete_downloaded_exe(exe_path: pathlib.Path) -> None:
    """Best-effort cleanup so the coach's Downloads folder doesn't keep a
    stray copy of the installer sitting around after setup â€” Windows won't
    let a running process delete its own file, so this hands it off to a
    hidden, detached shell that waits for us to exit first."""
    try:
        import subprocess
        CREATE_NO_WINDOW = 0x08000000
        subprocess.Popen(
            ["cmd", "/c", "ping", "127.0.0.1", "-n", "2", ">", "nul",
             "&", "del", "/f", "/q", str(exe_path)],
            creationflags=CREATE_NO_WINDOW, close_fds=True)
    except Exception as e:
        log(f"(couldn't schedule cleanup of downloaded exe: {e})")


# The installed EXE is ALWAYS this name, whatever the download was called.
#
# âš  WHY (Aug 19 2026): this used to install to `_APP_DIR / exe_path.name`, i.e.
# whatever name the browser gave the download. A coach who downloads twice gets
# "CAPP_Binder_Converter (1).exe", a third time "(4).exe", and EACH install
# registers its own filename for autostart and leaves the previous binaries on
# disk AND RUNNING. Roger's machine had three copies and a pair of JULY builds
# still running a month later, claiming every Binder job and failing all of them
# with "unsupported extension: xlsx" because they predated Excel support. The
# new build installed fine and simply never got the work.
#
# A fixed name means a re-download replaces the same file, self-update always
# targets the copy that is actually running, and autostart cannot fragment.
_CANONICAL_EXE_NAME = "CAPP_Binder_Converter.exe"


def _stop_other_converters() -> int:
    """Kill any other running converter, whatever filename it was installed as.

    âš  Load-bearing for two reasons: an old copy holds the job queue and keeps
    failing work the new build could do, and a running process locks its own
    EXE so the install copy would fail outright.

    Matches on the image name PREFIX so the "(1)"/"(4)" variants are caught, and
    excludes our own PID so a converter never kills itself.
    """
    if os.name != "nt":
        return 0
    try:
        import subprocess
        me = os.getpid()
        ps = (
            "Get-Process | Where-Object { $_.Name -like 'CAPP_Binder_Converter*' "
            f"-and $_.Id -ne {me} }} | ForEach-Object {{ $_.Id; Stop-Process -Id $_.Id -Force }}"
        )
        r = subprocess.run(
            ["powershell", "-NoProfile", "-NonInteractive", "-Command", ps],
            capture_output=True, text=True, timeout=30,
            creationflags=0x08000000)
        killed = [ln for ln in (r.stdout or "").split() if ln.strip().isdigit()]
        if killed:
            log(f"Stopped {len(killed)} older converter process(es): {', '.join(killed)}")
        return len(killed)
    except Exception as e:
        log(f"(couldn't stop older converters: {e})")
        return 0


def _remove_stale_installs(keep: pathlib.Path) -> None:
    """Delete previously-installed EXEs that used a download-suffixed name.

    Best-effort and deliberately narrow: only files in _APP_DIR whose name
    starts with 'CAPP_Binder_Converter' and is not the canonical one.
    """
    try:
        for p in _APP_DIR.glob("CAPP_Binder_Converter*.exe"):
            if p.resolve() == keep.resolve():
                continue
            try:
                p.unlink()
                log(f"Removed stale converter copy: {p.name}")
            except OSError:
                pass          # still locked; it is harmless once autostart points elsewhere
    except Exception:
        pass


def _self_install_if_needed() -> None:
    if not _FROZEN:
        return
    exe_path = pathlib.Path(sys.executable).resolve()
    installed_path = _APP_DIR / _CANONICAL_EXE_NAME
    if exe_path == installed_path:
        return   # already running from the installed location
    log(f"First launch from {exe_path} - installing to {installed_path} ...")
    # Stop older copies FIRST: one of them may be holding installed_path open,
    # and any that survive keep claiming jobs this build should be doing.
    _stop_other_converters()

    embedded_token, marker_offset = _extract_embedded_token(exe_path)
    try:
        if marker_offset is not None:
            # Copy only the clean EXE bytes (drop the embedded token) so the
            # permanent installed copy never carries the secret on disk.
            with open(exe_path, "rb") as src, open(installed_path, "wb") as dst:
                remaining = marker_offset
                while remaining > 0:
                    chunk = src.read(min(1024 * 1024, remaining))
                    if not chunk:
                        break
                    dst.write(chunk)
                    remaining -= len(chunk)
            shutil.copystat(exe_path, installed_path)
        else:
            shutil.copy2(exe_path, installed_path)
    except Exception as e:
        log(f"FATAL: could not install to {installed_path}: {e}")
        sys.exit(1)
    # Legacy fallback: a pairing_token.txt dropped next to the EXE by hand
    # (or by an older build of the Binder setup screen). Glob for any
    # variant since a repeat download gets suffixed "pairing_token (1).txt".
    stray_tokens = []
    try:
        stray_tokens = list(exe_path.parent.glob("pairing_token*.txt"))
    except OSError:
        pass
    newest_token = _find_downloaded_token(exe_path.parent)
    if newest_token and not _PAIRING_TOKEN_PATH.exists():
        try:
            shutil.move(str(newest_token), str(_PAIRING_TOKEN_PATH))
        except Exception as e:
            log(f"(couldn't move {newest_token.name} into place: {e})")
    for t in stray_tokens:
        if t.exists():
            try:
                t.unlink()
            except Exception:
                pass
    _register_autostart(installed_path)
    # Now that autostart points at the canonical copy, drop any older
    # download-suffixed EXEs so they can never be launched again.
    _remove_stale_installs(installed_path)
    log("Installed. Launching the installed copy and exiting this one...")
    try:
        if embedded_token:
            # Hand the token to the fresh process as a startup argument
            # instead of a file â€” it's consumed by a single HTTP call within
            # a second of launch and never touches disk. (The only residual
            # exposure is that the token is briefly visible in this
            # process's command line, e.g. to Task Manager, for that same
            # second â€” an acceptable trade against a file that would
            # otherwise sit in Downloads indefinitely.)
            import subprocess
            CREATE_NO_WINDOW = 0x08000000
            subprocess.Popen([str(installed_path), "--pair-token", embedded_token],
                             creationflags=CREATE_NO_WINDOW, close_fds=True)
        else:
            os.startfile(str(installed_path))   # detached â€” survives this process exiting
    except Exception as e:
        log(f"FATAL: could not launch installed copy: {e}")
        sys.exit(1)
    _self_delete_downloaded_exe(exe_path)
    sys.exit(0)


def _register_autostart(exe_path: pathlib.Path) -> None:
    """HKCU Run key â€” starts silently with Windows, no admin rights, no
    scheduled-task/service complexity. Idempotent (just overwrites the value)."""
    try:
        import winreg
        key = winreg.OpenKey(winreg.HKEY_CURRENT_USER,
                             r"Software\Microsoft\Windows\CurrentVersion\Run",
                             0, winreg.KEY_SET_VALUE)
        winreg.SetValueEx(key, "CAPPBinderConverter", 0, winreg.REG_SZ, f'"{exe_path}"')
        winreg.CloseKey(key)
        log("Registered to start automatically with Windows.")
    except Exception as e:
        log(f"(couldn't register auto-start: {e} â€” you'll need to re-launch it after a restart)")


_self_install_if_needed()

# â”€â”€ single instance guard (second launch exits quietly) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# A named Windows mutex, not a loopback socket bind â€” real-world testing
# showed two copies (a coach re-running "Complete Setup" while an older
# instance was still alive) BOTH ending up alive and BOTH polling for jobs,
# each with its own separate never-primed Visio state â€” which is what was
# actually causing seemingly-random single-file failures in a batch (the job
# claimed by the "invisible" second instance hit its own cold-start race).
# A named mutex is the standard, reliable Windows single-instance primitive:
# the OS itself guarantees only one process can hold it, and releases it
# automatically even if a process is killed rather than exiting cleanly â€”
# a loopback socket can also do this, but is more exposed to interference
# from VPN/EDR/security software intercepting or virtualizing localhost,
# which is a real concern on a managed/military machine.
try:
    import win32api
    import win32event
    import winerror
    _guard = win32event.CreateMutex(None, False, "Global\\CAPPBinderConverterSingleton")
    if win32api.GetLastError() == winerror.ERROR_ALREADY_EXISTS:
        sys.exit(0)
except ImportError:
    # pywin32 not available (dev/source run only, never in the frozen EXE) â€”
    # fall back to the old loopback-socket guard so `python capp_binder_
    # converter.py` still behaves sanely for local testing.
    _guard = socket.socket()
    try:
        _guard.bind(("127.0.0.1", 47654))
    except OSError:
        sys.exit(0)


# â”€â”€ paired identity â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Every coach pairs THEIR OWN computer to THEIR OWN login, once. From then on
# the server only ever hands THIS worker jobs THAT coach uploaded â€” never
# another coach's, even on the same team. See BINDER LOCAL PLAN.txt.
def _load_device_token() -> str:
    if not _DEVICE_TOKEN_PATH.exists():
        return ""
    try:
        return json.loads(_DEVICE_TOKEN_PATH.read_text(encoding="utf-8")).get("worker_token", "")
    except Exception:
        return ""


def _register_with_token(tok: str, fatal: bool = True) -> bool:
    """Exchange a one-time pairing token for this device's permanent token.

    `fatal=False` is used when we are RE-pairing a machine that already holds a
    device_token.json: a failed re-pair must fall back to the existing
    credential rather than kill a converter that was working."""
    try:
        req = urllib.request.Request(
            SERVER + "/converter/register",
            data=json.dumps({"pairing_token": tok, "device_name": WORKER_NAME}).encode(),
            method="POST", headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=30) as r:
            data = json.load(r)
        device_token = data.get("worker_token")
        if not device_token:
            raise RuntimeError("server did not return a worker_token")
        _DEVICE_TOKEN_PATH.write_text(json.dumps({"worker_token": device_token}), encoding="utf-8")
        log("Paired this computer to your CAPP Binder login â€” it will only ever "
            "convert YOUR OWN uploads.")
        return True
    except Exception as e:
        if fatal:
            log(f"FATAL: pairing failed ({e}). Redo 'Complete Setup' from the Binder to try again.")
            sys.exit(1)
        log(f"Re-pairing failed ({e}); keeping the device credential already on disk.")
        return False


def _register_with_pairing_token_file() -> None:
    """Legacy/fallback path: a pairing_token.txt was dropped next to this
    install by hand (or by an older Binder build). Not used by the normal
    coach flow anymore â€” that hands the token in via --pair-token instead."""
    tok = _PAIRING_TOKEN_PATH.read_text(encoding="utf-8").strip()
    try:
        _register_with_token(tok)
    finally:
        try:
            _PAIRING_TOKEN_PATH.unlink()
        except Exception:
            pass


# â”€â”€ silent self-update â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Same shape as the CAPP Nodes Agent's check (GET the server's current version,
# compare tuples, download, swap the exe via a script that waits for this
# process to exit, relaunch) with one deliberate difference: the Agent shows an
# "Update Available" window with Update Now / Later. This converter is designed
# to be completely invisible â€” no window, no console, no tray â€” so there is
# nobody to click "Later" and prompting would break that contract. It updates
# itself quietly and only says so in converter.log.
#
# Two rules keep it from being disruptive:
#   - frozen only. A source/dev run has no exe to replace.
#   - IDLE only. The check is made from the "no job claimed" branch of the main
#     loop, so an update can never interrupt a conversion halfway through and
#     lose a coach's upload.
_UPDATE_EVERY_SECONDS = 6 * 60 * 60      # this thing runs for weeks between reboots
_MIN_SANE_EXE_BYTES = 20_000_000         # real build is ~128MB; guards a partial download


def _vtuple(v):
    try:
        return tuple(int(x) for x in str(v).strip().split("."))
    except Exception:
        return (0,)


def _http_json(url: str, timeout: int = 15) -> dict:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8") or "{}")


def _check_for_update():
    """Server's converter version if it's newer than this build, else None."""
    try:
        server_v = str(_http_json(f"{SERVER}/converter/version").get("version", "")).strip()
        if server_v and _vtuple(server_v) > _vtuple(CONVERTER_VERSION):
            return server_v
    except Exception as e:
        log(f"(update check failed: {e})")
    return None


def _self_update(new_version: str) -> bool:
    """Replace this exe with the current build and relaunch. On success the
    process exits and never returns."""
    if not _FROZEN:
        return False
    try:
        running_exe = pathlib.Path(sys.executable).resolve()
        log(f"update available: v{new_version} (running v{CONVERTER_VERSION}) â€” downloading ...")
        url = _http_json(f"{SERVER}/converter/download").get("download_url", "")
        if not url:
            log("  update aborted: server returned no download link")
            return False

        # NOTE: no ?t= pairing token on this URL. The relay only appends the
        # token trailer when one is requested, so this comes down as a CLEAN
        # exe â€” which is exactly right. This machine is already paired and its
        # device_token.json is untouched by the swap.
        # Streamed, not http_get() â€” that buffers the whole body in memory, and
        # this payload is ~128MB on a machine that is also running Office.
        new_path = running_exe.with_suffix(running_exe.suffix + ".new")
        with urllib.request.urlopen(url, timeout=600) as r, open(new_path, "wb") as f:
            while True:
                chunk = r.read(1024 * 512)
                if not chunk:
                    break
                f.write(chunk)
        size = new_path.stat().st_size if new_path.exists() else 0
        if size < _MIN_SANE_EXE_BYTES:
            log(f"  update aborted: download looked incomplete ({size} bytes)")
            try:
                new_path.unlink()
            except Exception:
                pass
            return False

        # Office has to be let go before the exe is replaced, or the COM
        # servers outlive the swap and the relaunched copy fights them.
        try:
            _reset_office()
        except Exception:
            pass

        pid = os.getpid()
        bat = pathlib.Path(tempfile.gettempdir()) / "capp_binder_converter_swap.bat"
        bat.write_text(
            "@echo off\r\n"
            ":loop\r\n"
            f'tasklist /FI "PID eq {pid}" 2>NUL | find /I "{pid}" >NUL\r\n'
            "if not errorlevel 1 ( timeout /t 1 /nobreak >NUL & goto loop )\r\n"
            f'copy /Y "{new_path}" "{running_exe}" >NUL\r\n'
            f'del /Q "{new_path}" >NUL\r\n'
            f'start "" "{running_exe}"\r\n'
            'del /Q "%~f0" >NUL\r\n',
            encoding="utf-8")
        log(f"  downloaded v{new_version}; swapping and restarting ...")
        import subprocess
        subprocess.Popen(["cmd", "/c", str(bat)], creationflags=0x08000000)  # CREATE_NO_WINDOW
        os._exit(0)
    except Exception as e:
        log(f"  update failed: {e}")
        return False


def _maybe_update(state: dict) -> None:
    """Called from the idle branch of the main loop. Rate-limited so a converter
    sitting idle for a week isn't hitting the server constantly."""
    if not _FROZEN:
        return
    now = time.time()
    if now - state.get("last_update_check", 0) < _UPDATE_EVERY_SECONDS:
        return
    state["last_update_check"] = now
    newer = _check_for_update()
    if newer:
        _self_update(newer)


def _worker_token() -> str:
    saved = _load_device_token()
    # A --pair-token is a DELIBERATE act: the coach just clicked "Setup
    # Converter" in the Binder while signed in, which means "bind THIS computer
    # to THAT account, now". So it has to OUTRANK any device_token.json already
    # on disk.
    #
    # This used to check `saved` first and return early, which made re-pairing
    # structurally impossible: once a machine had ever paired, every future
    # setup silently reused the old credential and NEVER called
    # /converter/register. If the server side of that pairing was gone (the
    # device row unpaired from the admin panel, or replaced when another
    # machine paired to the same coach), the worker was left holding a token
    # the server rejects, polling into 401s forever -- while the Binder's setup
    # screen waited for a device that could never appear. Re-downloading the
    # installer could not fix it, because reinstalling is the very path that
    # was short-circuited. Hit for real by a coach Aug 12-26 2026: 8 pairing
    # tokens minted, every one left unredeemed.
    if "--pair-token" in sys.argv:
        i = sys.argv.index("--pair-token")
        tok = sys.argv[i + 1] if i + 1 < len(sys.argv) else ""
        if tok:
            # Only fatal if there is no existing credential to fall back on --
            # a failed re-pair must never take out a converter that was working.
            if _register_with_token(tok, fatal=not saved):
                fresh = _load_device_token()
                if fresh:
                    return fresh
            if saved:
                return saved
    if saved:
        return saved
    if _PAIRING_TOKEN_PATH.exists():
        _register_with_pairing_token_file()
        saved = _load_device_token()
        if saved:
            return saved
    log("FATAL: not paired (no device_token.json / --pair-token / pairing_token.txt). "
        "Run 'Complete Setup' from the Binder to pair this computer.")
    sys.exit(1)


TOKEN = _worker_token()

# â”€â”€ Visio Converter toolkit â€” bundled into this EXE (not a hardcoded dev path) â”€
# Gives every coach's worker the same font fidelity Roger's own worker has: the
# portable Fonts bundle loaded into the session, and the variable-font hybrid
# (Visio can't embed a variable font like Bahnschrift into vector PDF; those
# pages get re-rendered EMF->GDI+ raster).
if _FROZEN:
    _VTP_DIR = os.path.join(sys._MEIPASS, "VisioToPPT_bundle")
else:
    _VTP_DIR = r"C:\Users\roger.hayhurst.ctr\VisioToPPT"   # dev-machine fallback only
vtp = None
try:
    sys.path.insert(0, _VTP_DIR)
    import visio_to_ppt as vtp
except Exception as _e:
    vtp = None
    _VTP_IMPORT_ERR = str(_e)

_NAMEFILE = None
_FVAR_CACHE = {}
_WARNED_FONTS = set()


def _font_name_to_file():
    global _NAMEFILE
    if _NAMEFILE is None:
        m = {}
        try:
            m = dict(vtp.installed_font_file_map())
            for d in vtp.font_dirs():
                try:
                    entries = os.listdir(d)
                except OSError:
                    continue
                for fn in entries:
                    if fn.lower().endswith(vtp.FONT_EXTS):
                        p = os.path.join(d, fn)
                        for nm in vtp.ConvertWorker._font_family_names(p):
                            m.setdefault(nm, p)
        except Exception:
            pass
        _NAMEFILE = m
    return _NAMEFILE


def _font_is_variable(fam):
    key = (fam or "").strip().lower()
    if not key:
        return False
    if key in _FVAR_CACHE:
        return _FVAR_CACHE[key]
    m = _font_name_to_file()
    path = m.get(key)
    if not path:
        pre = key + " "
        path = next((p for n, p in m.items() if n == key or n.startswith(pre)), None)
    res = vtp._font_has_fvar(path) if path else False
    _FVAR_CACHE[key] = res
    return res


def _basefont_family(bf):
    bf = bf.split("+", 1)[-1]
    bf = bf.split("-", 1)[0]
    return bf.strip()


def _raster_dpi_for(w_in, h_in, dpi=300, cap_mp=220):
    if w_in > 0 and h_in > 0:
        mp = (w_in * dpi) * (h_in * dpi) / 1e6
        if mp > cap_mp:
            dpi = int(((cap_mp * 1e6) / (w_in * h_in)) ** 0.5)
    return max(150, dpi)


def _log_missing_fonts(doc):
    if vtp is None:
        return
    try:
        used = vtp.collect_used_fonts(doc)
        have = set(_font_name_to_file().keys())
        for f in sorted(used):
            fl = (f or "").strip().lower()
            if not fl or fl in _WARNED_FONTS:
                continue
            if fl not in have and not any(n == fl or n.startswith(fl + " ") for n in have):
                _WARNED_FONTS.add(fl)
                log(f'  WARNING: font "{f}" is not on this PC or in the Fonts bundle â€” '
                    f'Visio will substitute it (layout may shift).')
    except Exception:
        pass


def _hybrid_fix_variable_fonts(fg_pages, pdf_path):
    if vtp is None:
        return
    try:
        import fitz
        need = set()
        with fitz.open(pdf_path) as d:
            n_pdf = d.page_count
            for i in range(n_pdf):
                for f in d[i].get_fonts(full=True):
                    fam = _basefont_family(f[3])
                    if _font_is_variable(fam):
                        need.add(i)
                        if fam.lower() not in _WARNED_FONTS:
                            _WARNED_FONTS.add(fam.lower())
                            log(f'  "{fam}" is a variable font â€” its pages get '
                                f'rasterized at high DPI (vector can\'t embed it)')
                        break
        if not need:
            return
        if len(fg_pages) != n_pdf:
            log("  (page count mismatch â€” skipping variable-font raster pass)")
            return
        png_for = {}
        with tempfile.TemporaryDirectory(prefix="pbhybrid_") as tmp:
            for fi in sorted(need):
                try:
                    pg = fg_pages[fi]
                    w_in = float(pg.PageSheet.CellsU("PageWidth").ResultIU)
                    h_in = float(pg.PageSheet.CellsU("PageHeight").ResultIU)
                    emf = os.path.join(tmp, f"p{fi}.emf")
                    pg.Export(emf)
                    img = os.path.join(tmp, f"p{fi}.png")
                    vtp.emf_to_image(emf, img, w_in, h_in, dpi=_raster_dpi_for(w_in, h_in))
                    png_for[fi] = img
                except Exception as e:
                    log(f"  (raster of page {fi + 1} failed: {e} â€” leaving it vector)")
            if not png_for:
                return
            src = fitz.open(pdf_path)
            out = fitz.open()
            for i in range(src.page_count):
                rect = src[i].rect
                if i in png_for:
                    p = out.new_page(width=rect.width, height=rect.height)
                    p.insert_image(p.rect, filename=png_for[i])
                else:
                    out.insert_pdf(src, from_page=i, to_page=i)
            tmp_out = pdf_path + ".hyb"
            out.save(tmp_out, deflate=True, garbage=3)
            out.close()
            src.close()
            os.replace(tmp_out, pdf_path)
        log(f"  rasterized {len(png_for)} variable-font page(s)")
    except Exception as e:
        log(f"  (variable-font pass skipped: {e})")


def api(path: str, body: dict) -> dict:
    # x-converter-version rides on every call so the server can record which
    # build each paired machine is actually running. It costs nothing (the
    # worker already calls in constantly to claim jobs) and it's the only way
    # a coach can be told their converter is stale â€” the exe is invisible, so
    # there is nothing to look at on the machine itself.
    req = urllib.request.Request(
        SERVER + path, data=json.dumps(body).encode(), method="POST",
        headers={"x-worker-token": TOKEN,
                 "x-converter-version": CONVERTER_VERSION,
                 "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)


def http_get(url: str, dest: pathlib.Path) -> None:
    with urllib.request.urlopen(url, timeout=300) as r:
        dest.write_bytes(r.read())


def http_put(url: str, src: pathlib.Path) -> None:
    req = urllib.request.Request(url, data=src.read_bytes(), method="PUT",
                                 headers={"Content-Type": "application/pdf"})
    with urllib.request.urlopen(req, timeout=300):
        pass


# â”€â”€ warm Office instances (launched on first use, reused across jobs) â”€â”€â”€â”€â”€â”€â”€â”€
_VISIO = None
_PP = None
_WORD = None
_XL = None


_VISIO_FAIL_STREAK = 0


# ⚠ DispatchEx, NEVER Dispatch, for every Office app below.
#
# Dispatch() ATTACHES to an already-running instance if the coach has one open.
# The converter then drives THEIR Excel/Word/Visio: it sets Visible = False and
# DisplayAlerts = False on the app they are working in, opens files in it, and
# quits it when done. Roger saw Excel open on screen during a Binder conversion
# (Aug 19 2026) -- on a coach's machine the same code can make their own open
# workbook disappear mid-edit.
#
# DispatchEx() forces a NEW out-of-process instance every time, so the converter
# stays completely invisible and can never touch the user's session. This is
# what "no window, no console, no tray" was always supposed to mean.


def _get_visio():
    global _VISIO, _VISIO_FAIL_STREAK
    if _VISIO is None:
        import win32com.client
        log("starting Visio (kept warm for later jobs)...")
        _VISIO = win32com.client.DispatchEx("Visio.Application")
        # Visio's automation endpoint isn't always fully up the instant
        # Dispatch() returns â€” worse right after a previous instance was
        # just quit, since a rapid relaunch can briefly trip up Office's own
        # licensing/activation check. Retry for up to ~20s before giving up
        # (was ~5s â€” too short for that check to clear on a busy machine).
        last_err = None
        for _attempt in range(40):
            try:
                _VISIO.Visible = False
                _VISIO.AlertResponse = 1
                last_err = None
                break
            except Exception as e:
                last_err = e
                time.sleep(0.5)
        if last_err is not None:
            try:
                _VISIO.Quit()
            except Exception:
                pass
            _VISIO = None
            _VISIO_FAIL_STREAK += 1
            hint = ""
            if _VISIO_FAIL_STREAK >= 3:
                hint = (" â€” this has failed 3+ times in a row now; if it keeps "
                        "happening, open Visio directly on this PC and check "
                        "File > Account for its activation status.")
            raise RuntimeError(f"Visio did not become ready in time ({last_err}){hint}")
        _VISIO_FAIL_STREAK = 0
    return _VISIO


def _get_powerpoint():
    global _PP
    if _PP is None:
        import win32com.client
        log("starting PowerPoint (kept warm for later jobs)...")
        _PP = win32com.client.DispatchEx("PowerPoint.Application")
        # PowerPoint is the one Office app whose Application.Visible CANNOT be
        # forced False -- COM raises on the assignment, which is why this block
        # is best-effort per setting rather than one try. Everything else that
        # keeps a headless worker quiet is still applied: alerts off (a modal
        # nobody can see would hang the job), macros disabled to match the Word
        # and Excel paths, and the window minimised so a coach does not get a
        # deck flashing up mid-conversion.
        for attr, val in (("DisplayAlerts", 1),        # ppAlertsNone
                          ("AutomationSecurity", 3),   # msoAutomationSecurityForceDisable
                          ("WindowState", 2)):         # ppWindowMinimized
            try:
                setattr(_PP, attr, val)
            except Exception:
                pass
        try:
            _PP.Visible = False        # works on some builds, raises on most
        except Exception:
            pass
    return _PP


def _get_word():
    global _WORD
    if _WORD is None:
        import win32com.client
        log("starting Word (kept warm for later jobs)...")
        _WORD = win32com.client.DispatchEx("Word.Application")
        try:
            _WORD.Visible = False
            # Word is far more dialog-happy than PowerPoint: "convert this .doc
            # format?", "recover this file?", "open read-only?" all block on a
            # modal nobody can see on a headless worker, hanging the job.
            _WORD.DisplayAlerts = 0
            # msoAutomationSecurityForceDisable â€” a .doc/.docm can carry an
            # AutoOpen macro that would otherwise run the moment we open it.
            # Same intent as the "macros off" flag on the Visio OpenEx call.
            _WORD.AutomationSecurity = 3
        except Exception:
            pass
    return _WORD


def _get_excel():
    global _XL
    if _XL is None:
        import win32com.client
        log("starting Excel (kept warm for later jobs)...")
        _XL = win32com.client.DispatchEx("Excel.Application")
        try:
            _XL.Visible = False
            _XL.DisplayAlerts = False
            # A workbook can carry a Workbook_Open handler and .xlsm can carry
            # macros outright â€” same intent as the Word AutomationSecurity and
            # the "macros off" flag on the Visio OpenEx call.
            _XL.AutomationSecurity = 3     # msoAutomationSecurityForceDisable
            _XL.EnableEvents = False
            # "This workbook contains links to other data sources" is a modal
            # nobody can see on a headless worker â€” it would hang the job.
            _XL.AskToUpdateLinks = False
        except Exception:
            pass
    return _XL


def _reset_office() -> None:
    global _VISIO, _PP, _WORD, _XL
    for app in (_VISIO, _PP, _WORD, _XL):
        if app is not None:
            try:
                app.Quit()
            except Exception:
                pass
    _VISIO = _PP = _WORD = _XL = None


atexit.register(_reset_office)


def convert_visio(src: str, out: str) -> None:
    visio = _get_visio()
    doc = visio.Documents.OpenEx(os.path.normpath(src), 0x2 | 0x80)
    try:
        _log_missing_fonts(doc)
        fg = [doc.Pages.Item(i) for i in range(1, doc.Pages.Count + 1)
              if not bool(doc.Pages.Item(i).Background)]
        doc.ExportAsFixedFormat(1, os.path.normpath(out), 1, 0)
        _hybrid_fix_variable_fonts(fg, os.path.normpath(out))
    finally:
        doc.Close()


def convert_powerpoint(src: str, out: str) -> None:
    pp = _get_powerpoint()
    pres = pp.Presentations.Open(os.path.normpath(src), ReadOnly=True,
                                 Untitled=False, WithWindow=False)
    try:
        pres.SaveAs(os.path.normpath(out), 32)
    finally:
        pres.Close()


def convert_word(src: str, out: str) -> None:
    """Word COM ExportAsFixedFormat (wdExportFormatPDF=17) â€” native vector PDF,
    same as the Visio/PowerPoint paths rather than a print-to-PDF raster."""
    word = _get_word()
    # ConfirmConversions=False stops the legacy-format prompt on old .doc files;
    # AddToRecentFiles=False keeps a coach's Word MRU list clean.
    doc = word.Documents.Open(os.path.normpath(src), ConfirmConversions=False,
                              ReadOnly=True, AddToRecentFiles=False, Visible=False)
    try:
        doc.ExportAsFixedFormat(os.path.normpath(out), 17)
    finally:
        doc.Close(0)   # wdDoNotSaveChanges â€” never leave a "save?" modal behind


def convert_excel(src: str, out: str) -> None:
    """Excel COM ExportAsFixedFormat (xlTypePDF=0) â€” native vector PDF, same as
    the other Office paths.

    Spreadsheets need a page-setup pass first, which the other formats don't.
    A Word/PowerPoint/Visio file already knows its own page size; a worksheet is
    an unbounded grid, so Excel's default is to slice it into letter-size tiles
    and emit the left-hand columns on one page, the next few columns pages
    later. A 14-column call sheet exported raw came out as 7 pages showing 5
    columns each â€” to a coach that reads as a broken upload, not a wide sheet.

    So per sheet:
      * fit to ONE page wide, unlimited pages tall â€” the whole row stays on one
        page and long lists still spill downward, which is how a call sheet or
        a personnel chart is meant to read;
      * orientation from the used range's own shape (wider than tall ->
        landscape), rather than forcing one on every sheet.
    An existing print area is left alone â€” if a coach has already set one up,
    that intent wins; this only decides how what's printed gets laid out.
    """
    xl = _get_excel()
    wb = xl.Workbooks.Open(os.path.normpath(src), ReadOnly=True, UpdateLinks=0,
                           IgnoreReadOnlyRecommended=True)
    try:
        # Each PageSetup write is a round-trip to the printer driver; batching
        # them behind PrintCommunication turns a slow per-property crawl into
        # one apply. On a multi-sheet workbook this is the difference between
        # seconds and minutes.
        try:
            xl.PrintCommunication = False
        except Exception:
            pass
        printable = 0
        for ws in wb.Worksheets:
            try:
                used = ws.UsedRange
                if used.Cells.Count <= 1 and not str(used.Value or "").strip():
                    continue                       # genuinely empty sheet
                printable += 1
                ps = ws.PageSetup
                ps.Orientation = 2 if used.Width > used.Height else 1
                ps.Zoom = False
                ps.FitToPagesWide = 1
                ps.FitToPagesTall = False
            except Exception as e:
                # One odd sheet must not sink the workbook â€” it still exports,
                # just with whatever page setup it already had.
                log(f"  (page setup skipped for a sheet: {e})")
        try:
            xl.PrintCommunication = True
        except Exception:
            pass
        if not printable:
            # Excel raises a bare "Document not saved" here, which tells a coach
            # nothing. Fail with something they can act on.
            raise RuntimeError("that spreadsheet has no printable content")
        wb.ExportAsFixedFormat(0, os.path.normpath(out))
    finally:
        wb.Close(False)    # never leave a "save changes?" modal behind


def page_count(pdf: pathlib.Path):
    try:
        import fitz
        with fitz.open(pdf) as d:
            return d.page_count
    except Exception:
        return None


def normalize_pdf(pdf_path: pathlib.Path) -> None:
    try:
        import collections
        import fitz
    except ImportError:
        log("  (PyMuPDF not available â€” skipping page normalization)")
        return
    book = fitz.open(pdf_path)
    if book.page_count == 0:
        book.close()
        return
    counts = collections.Counter()
    for i in range(book.page_count):
        r = book[i].rect
        counts[(round(r.width, 1), round(r.height, 1))] += 1
    (tw, th), same = counts.most_common(1)[0]
    off_size = book.page_count - same
    if off_size == 0:
        book.close()
        return
    out = fitz.open()
    for i in range(book.page_count):
        r = book[i].rect
        if abs(r.width - tw) < 0.6 and abs(r.height - th) < 0.6:
            out.insert_pdf(book, from_page=i, to_page=i)
            continue
        page = out.new_page(width=tw, height=th)
        scale = min(tw / r.width, th / r.height)
        w, h = r.width * scale, r.height * scale
        x, y = (tw - w) / 2.0, (th - h) / 2.0
        page.show_pdf_page(fitz.Rect(x, y, x + w, y + h), book, i)
    book.close()
    tmp = pdf_path.with_suffix(".norm.pdf")
    out.save(tmp, deflate=True)
    out.close()
    tmp.replace(pdf_path)
    log(f"  normalized {off_size} page(s) to {tw / 72:.1f} x {th / 72:.1f} in")


def _stamp_label(page, label: str) -> None:
    """Same style as the Visio Converter's own booklet stamp â€” bottom-center,
    small white pill â€” so every coach's numbering matches Roger's exactly."""
    if not label:
        return
    import fitz
    r = page.rect
    fs = 13
    tw = fitz.get_text_length(label, fontname="helv", fontsize=fs)
    cx = r.width / 2.0
    baseline = r.height - 24
    box = fitz.Rect(cx - tw / 2 - 6, baseline - fs - 1, cx + tw / 2 + 6, baseline + 4)
    page.draw_rect(box, fill=(1, 1, 1), color=(0.7, 0.7, 0.7), width=0.4)
    page.insert_text((cx - tw / 2, baseline), label,
                     fontname="helv", fontsize=fs, color=(0, 0, 0))


def _number_pages(pdf_path) -> None:
    import fitz
    book = fitz.open(pdf_path)
    try:
        for i in range(book.page_count):
            _stamp_label(book[i], str(i + 1))
        tmp = str(pdf_path) + ".num"
        book.save(tmp, deflate=True, garbage=3)
    finally:
        book.close()
    os.replace(tmp, str(pdf_path))
    log(f"  stamped page numbers 1-{page_count(pdf_path)}")


def _label_for(base_label: str, j: int) -> str:
    base_label = base_label or ""
    if j == 0:
        return base_label
    m = re.search(r"(\d+)\s*$", base_label)
    if m:
        return base_label[:m.start(1)] + str(int(m.group(1)) + j)
    return f"{base_label}-{j + 1}" if base_label else ""


def _merge_insert(base_path, play_path, out_path, insert_after: int, label: str) -> int:
    import fitz
    base = fitz.open(base_path)
    play = fitz.open(play_path)
    try:
        inserted = play.page_count
        n = base.page_count
        p = max(0, min(int(insert_after), n))
        if n > 0:
            br = base[min(p, n - 1)].rect
            bw, bh = br.width, br.height
        else:
            pr = play[0].rect
            bw, bh = pr.width, pr.height
        out = fitz.open()
        if p > 0:
            out.insert_pdf(base, from_page=0, to_page=p - 1)
        for j in range(play.page_count):
            pg = out.new_page(width=bw, height=bh)
            r = play[j].rect
            scale = min(bw / r.width, bh / r.height) if r.width and r.height else 1.0
            w, h = r.width * scale, r.height * scale
            x, y = (bw - w) / 2.0, (bh - h) / 2.0
            pg.show_pdf_page(fitz.Rect(x, y, x + w, y + h), play, j)
            _stamp_label(pg, _label_for(label, j))
        if p < n:
            out.insert_pdf(base, from_page=p, to_page=n - 1)
        out.save(out_path, deflate=True, garbage=3)
        out.close()
        return inserted
    finally:
        base.close()
        play.close()


def _convert(ext: str, src: str, out: str) -> None:
    # Keep in step with _PB_CONVERT_EXTS in main.py and _convert in pb_worker.py â€”
    # a format the server accepts but this doesn't handle uploads fine and then
    # dies here, which reads to a coach as a broken app.
    if ext in ("vsd", "vsdx", "vsdm"):
        convert_visio(src, out)
    elif ext in ("ppt", "pptx"):
        convert_powerpoint(src, out)
    elif ext in ("doc", "docx", "docm"):
        convert_word(src, out)
    elif ext in ("xls", "xlsx", "xlsm", "xlsb"):
        convert_excel(src, out)
    else:
        raise RuntimeError(f"unsupported extension: {ext}")


def _convert_with_retry(ext: str, src: str, out: str) -> None:
    """One retry with a fresh Office instance on failure â€” paced with a short
    pause first. Several files landing at once used to cause back-to-back
    instant Visio kill+relaunch cycles (this retry stacked on top of
    _get_visio()'s own retry-then-relaunch), which made a transient
    licensing-check hiccup on the machine worse, not better. A few seconds'
    breathing room before tearing down and relaunching gives that check a
    chance to actually clear instead of getting hit again immediately."""
    try:
        _convert(ext, src, out)
    except Exception as e:
        log(f"  convert failed ({e}); pausing before retrying with a fresh Office instance...")
        time.sleep(3)
        _reset_office()
        _convert(ext, src, out)


def process_insert(claim: dict) -> None:
    job = claim["job"]
    ext = (job.get("ext") or "").lower()
    insert_after = int(job.get("insert_after") or 0)
    label = job.get("label") or ""
    base_url = claim.get("base_url")
    if not base_url:
        raise RuntimeError("insert job has no target section PDF")
    log(f"  inserting a play into {job.get('folder_path','')}/{job.get('title','')} "
        f"after page {insert_after} (label '{label}') ...")
    with tempfile.TemporaryDirectory(prefix="pbinsert_") as tmp:
        tmpp = pathlib.Path(tmp)
        raw = tmpp / f"in.{ext or 'pdf'}"
        http_get(claim["raw_url"], raw)
        if ext == "pdf":
            play = raw
        else:
            play = tmpp / "play.pdf"
            _convert_with_retry(ext, str(raw), str(play))
            if not play.exists() or play.stat().st_size == 0:
                raise RuntimeError("conversion produced no PDF")
        base = tmpp / "base.pdf"
        http_get(base_url, base)
        out = tmpp / "out.pdf"
        inserted = _merge_insert(base, play, out, insert_after, label)
        http_put(claim["put_url"], out)
        api("/playbook/worker/insert-complete",
            {"job_id": job["id"], "pages": page_count(out),
             "size": out.stat().st_size, "inserted": inserted})
    log(f"  inserted: {job.get('title','')}")


def process(claim: dict) -> None:
    if (claim["job"].get("kind") or "convert") == "insert":
        process_insert(claim)
        return
    job = claim["job"]
    ext = (job.get("ext") or "").lower()
    number = bool(job.get("number"))
    title = job.get("title") or "untitled"
    log(f"  {'numbering' if ext == 'pdf' else 'converting'} [{ext}] "
        f"{job.get('folder_path','')}/{title} ...")
    with tempfile.TemporaryDirectory(prefix="pbworker_") as tmp:
        out = pathlib.Path(tmp) / "out.pdf"
        if ext == "pdf":
            http_get(claim["raw_url"], out)
            if not out.exists() or out.stat().st_size == 0:
                raise RuntimeError("uploaded PDF was empty")
        else:
            src = pathlib.Path(tmp) / f"in.{ext}"
            http_get(claim["raw_url"], src)
            _convert_with_retry(ext, str(src), str(out))
            if not out.exists() or out.stat().st_size == 0:
                raise RuntimeError("conversion produced no PDF")
        normalize_pdf(out)
        if number:
            _number_pages(out)
        http_put(claim["put_url"], out)
        api("/playbook/worker/complete",
            {"job_id": job["id"], "pages": page_count(out), "size": out.stat().st_size})
    log(f"  done: {title}")


def main() -> None:
    import pythoncom
    pythoncom.CoInitialize()
    _trim_log()
    log(f"{APP_NAME} v{CONVERTER_VERSION} '{WORKER_NAME}' polling {SERVER} every "
        f"{POLL_SECONDS}s (paired mode; installed at {_APP_DIR}).")
    # Check once at startup, before any job is claimed â€” a converter that has
    # been off for a month should come back current rather than run a stale
    # build until the first 6-hour tick.
    _upd_state = {"last_update_check": 0.0}
    _maybe_update(_upd_state)
    if vtp is None:
        log(f"converter toolkit NOT loaded ({globals().get('_VTP_IMPORT_ERR', 'missing')}) â€” "
            f"font protections off, plain export only")
    else:
        try:
            loaded = vtp.register_session_fonts()
            atexit.register(vtp.unregister_session_fonts, loaded)
            log(f"loaded {len(loaded or [])} bundled font file(s) into the session")
        except Exception as e:
            log(f"(session font load failed: {e})")
    # Prime Visio once, quietly, before the first real job ever reaches it.
    # The very first Visio.Application launch in this process's lifetime can
    # lose a one-time COM marshaling race ("Property 'Visio.Application.
    # Visible' can not be set") no matter how long _get_visio() retries â€”
    # but that race only ever happens once per process, pass or fail. Eating
    # that failure here means the coach's actual first upload never sees it,
    # instead of it landing on whichever file happens to be queued first.
    try:
        _get_visio()
        log("Visio primed and ready.")
    except Exception as e:
        log(f"(Visio priming hit the known first-launch hiccup, as expected: {e} â€” now warmed up for real jobs)")
    # ⚠ NOTHING below may be allowed to end this loop. On Aug 19 2026 a server
    # bug made /playbook/worker/complete return 500 after a job had converted
    # perfectly, and the worker stopped running -- so every later upload sat at
    # "converting 0 of 1 files" forever, because nothing was left to claim them.
    # A coach reads that as "the Binder is broken and does nothing".
    # One bad request must cost one job, never the worker.
    while True:
        try:
            _work_once(_upd_state)
        except Exception as e:
            log(f"UNEXPECTED error in the worker loop ({e}) -- continuing anyway")
            log(traceback.format_exc())
            time.sleep(POLL_SECONDS)


def _work_once(_upd_state) -> None:
    """One poll cycle: claim a job if there is one, run it, report the result."""
    try:
        claim = api("/playbook/worker/claim", {"worker": WORKER_NAME})
    except Exception as e:
        log(f"claim failed ({e}); retrying in {POLL_SECONDS}s")
        time.sleep(POLL_SECONDS)
        return
    if not claim.get("job"):
        _maybe_update(_upd_state)   # idle only, never mid-conversion
        time.sleep(POLL_SECONDS)
        return
    job_id = claim["job"]["id"]
    try:
        process(claim)
    except Exception as e:
        log(f"  FAILED: {e}")
        log(traceback.format_exc())
        try:
            api("/playbook/worker/error", {"job_id": job_id, "error": str(e)})
        except Exception as e2:
            # The error report can fail for the SAME reason the job did -- on
            # Aug 19 complete AND error both 500'd. Log it instead of swallowing
            # it, so a job left mid-flight on the server is visible from here.
            log(f"  (could not report the failure to the server either: {e2})")


if __name__ == "__main__":
    main()
