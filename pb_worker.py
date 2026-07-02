"""CAPP Binder conversion worker.

Runs on a Windows PC that has Microsoft Visio and PowerPoint installed
(Roger's machine). Polls the CAPP server for queued playbook conversion jobs
(uploaded .vsd/.vsdx/.vsdm/.ppt/.pptx files), converts each to PDF via Office
COM, normalizes page sizes, uploads the PDF to R2 through the server's
presigned URL, and marks the job done. PDFs then appear in the Binder portal
automatically.

Designed to run HEADLESS (pythonw.exe, no console): all output also goes to
pb_worker.log next to this script, and the coach upload page shows a live
"Conversion activity" feed from the server. A Startup-folder shortcut launches
it at logon; a localhost port guard prevents double instances.

Office apps are kept WARM between jobs (launch once, reuse) — app startup was
most of the per-file time. On a conversion error the apps are reset and the
job retried once with a fresh instance.

Setup: PB_WORKER_TOKEN in the .env next to this script (same value as Render).
Run:   pythonw pb_worker.py   (or python pb_worker.py for a console)
"""
import atexit
import datetime
import json
import os
import pathlib
import socket
import sys
import tempfile
import time
import traceback
import urllib.request

SERVER = "https://capp-data-server.onrender.com"
POLL_SECONDS = 5
WORKER_NAME = socket.gethostname()[:64]

_HERE = pathlib.Path(__file__).parent
_ENV_PATH = _HERE / ".env"
_LOG_PATH = _HERE / "pb_worker.log"

# ── single instance guard (second launch exits quietly) ─────────────────────
_guard = socket.socket()
try:
    _guard.bind(("127.0.0.1", 47653))
except OSError:
    sys.exit(0)


def log(msg: str) -> None:
    line = f"[{datetime.datetime.now():%m-%d %H:%M:%S}] {msg}"
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


def _worker_token() -> str:
    tok = os.environ.get("PB_WORKER_TOKEN", "")
    if not tok and _ENV_PATH.exists():
        for line in _ENV_PATH.read_text().splitlines():
            line = line.strip()
            if line.startswith("PB_WORKER_TOKEN") and "=" in line:
                tok = line.split("=", 1)[1].strip().strip('"').strip("'")
    if not tok:
        log("FATAL: PB_WORKER_TOKEN not set (env var or .env next to this script).")
        sys.exit(1)
    return tok


TOKEN = _worker_token()


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


def _get_visio():
    global _VISIO
    if _VISIO is None:
        import win32com.client
        log("starting Visio (kept warm for later jobs)...")
        _VISIO = win32com.client.Dispatch("Visio.Application")
        _VISIO.Visible = False
        _VISIO.AlertResponse = 1          # auto-answer any modal prompt with OK
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
    """Visio COM native vector PDF export (visFixedFormatPDF=1, intent=print,
    all pages) — same call the Visio Converter's PDF mode uses."""
    visio = _get_visio()
    doc = visio.Documents.OpenEx(os.path.normpath(src), 0x2 | 0x80)  # RO + macros off
    try:
        doc.ExportAsFixedFormat(1, os.path.normpath(out), 1, 0)
    finally:
        doc.Close()


def convert_powerpoint(src: str, out: str) -> None:
    """PowerPoint COM SaveAs PDF (ppSaveAsPDF=32)."""
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
    """Scale every page to the document's most common page size (off-size pages
    fit aspect-preserved + centered via show_pdf_page, so vector stays vector).
    Same behavior as the Visio Converter's booklet normalization — keeps a
    converted doc reading at one steady size in the Binder."""
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


def _convert(ext: str, src: str, out: str) -> None:
    if ext in ("vsd", "vsdx", "vsdm"):
        convert_visio(src, out)
    elif ext in ("ppt", "pptx"):
        convert_powerpoint(src, out)
    else:
        raise RuntimeError(f"unsupported extension: {ext}")


def process(claim: dict) -> None:
    job = claim["job"]
    ext = (job.get("ext") or "").lower()
    title = job.get("title") or "untitled"
    log(f"  converting [{ext}] {job.get('folder_path','')}/{title} ...")
    with tempfile.TemporaryDirectory(prefix="pbworker_") as tmp:
        src = pathlib.Path(tmp) / f"in.{ext}"
        out = pathlib.Path(tmp) / "out.pdf"
        http_get(claim["raw_url"], src)
        try:
            _convert(ext, str(src), str(out))
        except Exception as e:
            log(f"  convert failed ({e}); retrying with a fresh Office instance...")
            _reset_office()
            _convert(ext, str(src), str(out))
        if not out.exists() or out.stat().st_size == 0:
            raise RuntimeError("conversion produced no PDF")
        normalize_pdf(out)
        http_put(claim["put_url"], out)
        api("/playbook/worker/complete",
            {"job_id": job["id"], "pages": page_count(out), "size": out.stat().st_size})
    log(f"  done: {title}")


def main() -> None:
    import pythoncom
    pythoncom.CoInitialize()
    _trim_log()
    log(f"CAPP Binder worker '{WORKER_NAME}' polling {SERVER} every {POLL_SECONDS}s "
        f"(headless; log = {_LOG_PATH.name}).")
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
