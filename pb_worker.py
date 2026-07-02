"""CAPP Binder conversion worker.

Runs on a Windows PC that has Microsoft Visio and PowerPoint installed
(Roger's machine). Polls the CAPP server for queued playbook conversion jobs
(uploaded .vsd/.vsdx/.vsdm/.ppt/.pptx files), converts each to PDF via Office
COM, uploads the PDF to R2 through the server's presigned URL, and marks the
job done. PDFs then appear in the Binder portal automatically.

Setup:
  - PB_WORKER_TOKEN must be in the .env file next to this script (same value
    as the Render env var).
  - Close/kill any stale VISIO.EXE before starting (a zombie instance holds
    file locks and pops modal dialogs that hang conversions).

Run:  python pb_worker.py
Stop: Ctrl+C
"""
import json
import os
import pathlib
import socket
import tempfile
import time
import traceback
import urllib.request

SERVER = "https://capp-data-server.onrender.com"
POLL_SECONDS = 15
WORKER_NAME = socket.gethostname()[:64]

_ENV_PATH = pathlib.Path(__file__).with_name(".env")


def _worker_token() -> str:
    tok = os.environ.get("PB_WORKER_TOKEN", "")
    if not tok and _ENV_PATH.exists():
        for line in _ENV_PATH.read_text().splitlines():
            line = line.strip()
            if line.startswith("PB_WORKER_TOKEN") and "=" in line:
                tok = line.split("=", 1)[1].strip().strip('"').strip("'")
    if not tok:
        raise SystemExit("PB_WORKER_TOKEN not set (env var or .env next to this script).")
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


def convert_visio(src: str, out: str) -> None:
    """Visio COM native vector PDF export (same call the Visio Converter's PDF
    mode uses: visFixedFormatPDF=1, intent=print, all pages)."""
    import pythoncom
    import win32com.client
    pythoncom.CoInitialize()
    visio = None
    try:
        visio = win32com.client.Dispatch("Visio.Application")
        visio.Visible = False
        visio.AlertResponse = 1          # auto-answer any modal prompt with OK
        doc = visio.Documents.OpenEx(os.path.normpath(src), 0x2 | 0x80)  # RO + macros off
        try:
            doc.ExportAsFixedFormat(1, os.path.normpath(out), 1, 0)
        finally:
            doc.Close()
    finally:
        if visio is not None:
            try:
                visio.Quit()
            except Exception:
                pass
        pythoncom.CoUninitialize()


def convert_powerpoint(src: str, out: str) -> None:
    """PowerPoint COM SaveAs PDF (ppSaveAsPDF=32)."""
    import pythoncom
    import win32com.client
    pythoncom.CoInitialize()
    pp = None
    try:
        pp = win32com.client.Dispatch("PowerPoint.Application")
        pres = pp.Presentations.Open(os.path.normpath(src), ReadOnly=True,
                                     Untitled=False, WithWindow=False)
        try:
            pres.SaveAs(os.path.normpath(out), 32)
        finally:
            pres.Close()
    finally:
        if pp is not None:
            try:
                pp.Quit()
            except Exception:
                pass
        pythoncom.CoUninitialize()


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
        print("  (PyMuPDF not available — skipping page normalization)")
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
    print(f"  normalized {off_size} page(s) to {tw / 72:.1f} x {th / 72:.1f} in")


def process(claim: dict) -> None:
    job = claim["job"]
    ext = (job.get("ext") or "").lower()
    title = job.get("title") or "untitled"
    print(f"  converting [{ext}] {job.get('folder_path','')}/{title} ...")
    with tempfile.TemporaryDirectory(prefix="pbworker_") as tmp:
        src = pathlib.Path(tmp) / f"in.{ext}"
        out = pathlib.Path(tmp) / "out.pdf"
        http_get(claim["raw_url"], src)
        if ext in ("vsd", "vsdx", "vsdm"):
            convert_visio(str(src), str(out))
        elif ext in ("ppt", "pptx"):
            convert_powerpoint(str(src), str(out))
        else:
            raise RuntimeError(f"unsupported extension: {ext}")
        if not out.exists() or out.stat().st_size == 0:
            raise RuntimeError("conversion produced no PDF")
        normalize_pdf(out)
        http_put(claim["put_url"], out)
        api("/playbook/worker/complete",
            {"job_id": job["id"], "pages": page_count(out), "size": out.stat().st_size})
    print(f"  done: {title}")


def main() -> None:
    print(f"CAPP Binder worker '{WORKER_NAME}' polling {SERVER} every {POLL_SECONDS}s. Ctrl+C to stop.")
    while True:
        try:
            claim = api("/playbook/worker/claim", {"worker": WORKER_NAME})
        except Exception as e:
            print(f"claim failed ({e}); retrying in {POLL_SECONDS}s")
            time.sleep(POLL_SECONDS)
            continue
        if not claim.get("job"):
            time.sleep(POLL_SECONDS)
            continue
        job_id = claim["job"]["id"]
        try:
            process(claim)
        except Exception as e:
            print(f"  FAILED: {e}")
            traceback.print_exc()
            try:
                api("/playbook/worker/error", {"job_id": job_id, "error": str(e)})
            except Exception:
                pass


if __name__ == "__main__":
    main()
