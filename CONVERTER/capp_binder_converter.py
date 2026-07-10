"""CAPP Binder Converter — the coach-facing, per-machine background worker.

Distributed as a single signed EXE. Fully invisible: no window, no console,
no tray icon — matches the dev-machine pb_worker.py exactly. A coach downloads
this from the Binder's "Complete Setup" screen alongside a one-time pairing
token; on first launch it installs itself, registers to auto-start with
Windows, pairs to that coach's own login, and from then on only ever converts
files THAT coach uploaded — never another coach's, even on the same team.
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

_FROZEN = bool(getattr(sys, "frozen", False))

# ── where this install lives ─────────────────────────────────────────────────
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


# ── one-time self-install (frozen only) ──────────────────────────────────────
# A coach double-clicks the downloaded EXE from wherever their browser put it
# (usually Downloads/Desktop). The "Complete Setup" screen embeds the coach's
# one-time pairing token INTO the downloaded EXE's own bytes (see the relay's
# /converter/download?t=... route) — there is no separate pairing_token.txt
# download anymore. First launch: pull the token out of this file's own
# trailing bytes, copy the CLEAN (token-stripped) bytes into the stable
# per-user folder so the permanent install never carries the secret, relaunch
# that installed copy passing the token as a one-time startup argument,
# register to auto-start with Windows (HKCU Run key — no admin rights
# needed), and exit this one — which then gets deleted from Downloads a
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
    by that name), the browser saves it as 'pairing_token (1).txt' etc — an
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
    stray copy of the installer sitting around after setup — Windows won't
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


def _self_install_if_needed() -> None:
    if not _FROZEN:
        return
    exe_path = pathlib.Path(sys.executable).resolve()
    installed_path = _APP_DIR / exe_path.name
    if exe_path == installed_path:
        return   # already running from the installed location
    log(f"First launch from {exe_path} — installing to {installed_path} ...")
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
    log("Installed. Launching the installed copy and exiting this one...")
    try:
        if embedded_token:
            # Hand the token to the fresh process as a startup argument
            # instead of a file — it's consumed by a single HTTP call within
            # a second of launch and never touches disk. (The only residual
            # exposure is that the token is briefly visible in this
            # process's command line, e.g. to Task Manager, for that same
            # second — an acceptable trade against a file that would
            # otherwise sit in Downloads indefinitely.)
            import subprocess
            CREATE_NO_WINDOW = 0x08000000
            subprocess.Popen([str(installed_path), "--pair-token", embedded_token],
                             creationflags=CREATE_NO_WINDOW, close_fds=True)
        else:
            os.startfile(str(installed_path))   # detached — survives this process exiting
    except Exception as e:
        log(f"FATAL: could not launch installed copy: {e}")
        sys.exit(1)
    _self_delete_downloaded_exe(exe_path)
    sys.exit(0)


def _register_autostart(exe_path: pathlib.Path) -> None:
    """HKCU Run key — starts silently with Windows, no admin rights, no
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
        log(f"(couldn't register auto-start: {e} — you'll need to re-launch it after a restart)")


_self_install_if_needed()

# ── single instance guard (second launch exits quietly) ─────────────────────
_guard = socket.socket()
try:
    _guard.bind(("127.0.0.1", 47654))   # different port than pb_worker.py's 47653 — both can run on the same PC
except OSError:
    sys.exit(0)


# ── paired identity ───────────────────────────────────────────────────────────
# Every coach pairs THEIR OWN computer to THEIR OWN login, once. From then on
# the server only ever hands THIS worker jobs THAT coach uploaded — never
# another coach's, even on the same team. See BINDER LOCAL PLAN.txt.
def _load_device_token() -> str:
    if not _DEVICE_TOKEN_PATH.exists():
        return ""
    try:
        return json.loads(_DEVICE_TOKEN_PATH.read_text(encoding="utf-8")).get("worker_token", "")
    except Exception:
        return ""


def _register_with_token(tok: str) -> None:
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
        log("Paired this computer to your CAPP Binder login — it will only ever "
            "convert YOUR OWN uploads.")
    except Exception as e:
        log(f"FATAL: pairing failed ({e}). Redo 'Complete Setup' from the Binder to try again.")
        sys.exit(1)


def _register_with_pairing_token_file() -> None:
    """Legacy/fallback path: a pairing_token.txt was dropped next to this
    install by hand (or by an older Binder build). Not used by the normal
    coach flow anymore — that hands the token in via --pair-token instead."""
    tok = _PAIRING_TOKEN_PATH.read_text(encoding="utf-8").strip()
    try:
        _register_with_token(tok)
    finally:
        try:
            _PAIRING_TOKEN_PATH.unlink()
        except Exception:
            pass


def _worker_token() -> str:
    saved = _load_device_token()
    if saved:
        return saved
    if "--pair-token" in sys.argv:
        i = sys.argv.index("--pair-token")
        tok = sys.argv[i + 1] if i + 1 < len(sys.argv) else ""
        if tok:
            _register_with_token(tok)
            saved = _load_device_token()
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

# ── Visio Converter toolkit — bundled into this EXE (not a hardcoded dev path) ─
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
                log(f'  WARNING: font "{f}" is not on this PC or in the Fonts bundle — '
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
                            log(f'  "{fam}" is a variable font — its pages get '
                                f'rasterized at high DPI (vector can\'t embed it)')
                        break
        if not need:
            return
        if len(fg_pages) != n_pdf:
            log("  (page count mismatch — skipping variable-font raster pass)")
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
                    log(f"  (raster of page {fi + 1} failed: {e} — leaving it vector)")
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
    req = urllib.request.Request(
        SERVER + path, data=json.dumps(body).encode(), method="POST",
        headers={"x-worker-token": TOKEN, "Content-Type": "application/json"})
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


# ── warm Office instances (launched on first use, reused across jobs) ────────
_VISIO = None
_PP = None


_VISIO_FAIL_STREAK = 0


def _get_visio():
    global _VISIO, _VISIO_FAIL_STREAK
    if _VISIO is None:
        import win32com.client
        log("starting Visio (kept warm for later jobs)...")
        _VISIO = win32com.client.Dispatch("Visio.Application")
        # Visio's automation endpoint isn't always fully up the instant
        # Dispatch() returns — worse right after a previous instance was
        # just quit, since a rapid relaunch can briefly trip up Office's own
        # licensing/activation check. Retry for up to ~20s before giving up
        # (was ~5s — too short for that check to clear on a busy machine).
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
                hint = (" — this has failed 3+ times in a row now; if it keeps "
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
        _PP = win32com.client.Dispatch("PowerPoint.Application")
    return _PP


def _reset_office() -> None:
    global _VISIO, _PP
    for app in (_VISIO, _PP):
        if app is not None:
            try:
                app.Quit()
            except Exception:
                pass
    _VISIO = _PP = None


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
        log("  (PyMuPDF not available — skipping page normalization)")
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
    """Same style as the Visio Converter's own booklet stamp — bottom-center,
    small white pill — so every coach's numbering matches Roger's exactly."""
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
    if ext in ("vsd", "vsdx", "vsdm"):
        convert_visio(src, out)
    elif ext in ("ppt", "pptx"):
        convert_powerpoint(src, out)
    else:
        raise RuntimeError(f"unsupported extension: {ext}")


def _convert_with_retry(ext: str, src: str, out: str) -> None:
    """One retry with a fresh Office instance on failure — paced with a short
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
    log(f"{APP_NAME} '{WORKER_NAME}' polling {SERVER} every {POLL_SECONDS}s "
        f"(paired mode; installed at {_APP_DIR}).")
    if vtp is None:
        log(f"converter toolkit NOT loaded ({globals().get('_VTP_IMPORT_ERR', 'missing')}) — "
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
    # Visible' can not be set") no matter how long _get_visio() retries —
    # but that race only ever happens once per process, pass or fail. Eating
    # that failure here means the coach's actual first upload never sees it,
    # instead of it landing on whichever file happens to be queued first.
    try:
        _get_visio()
        log("Visio primed and ready.")
    except Exception as e:
        log(f"(Visio priming hit the known first-launch hiccup, as expected: {e} — now warmed up for real jobs)")
    while True:
        try:
            claim = api("/playbook/worker/claim", {"worker": WORKER_NAME})
        except Exception as e:
            log(f"claim failed ({e}); retrying in {POLL_SECONDS}s")
            time.sleep(POLL_SECONDS)
            continue
        if not claim.get("job"):
            time.sleep(POLL_SECONDS)
            continue
        job_id = claim["job"]["id"]
        try:
            process(claim)
        except Exception as e:
            log(f"  FAILED: {e}")
            log(traceback.format_exc())
            try:
                api("/playbook/worker/error", {"job_id": job_id, "error": str(e)})
            except Exception:
                pass


if __name__ == "__main__":
    main()
