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
import re
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


# ── paired local worker (Jul 9 2026) ─────────────────────────────────────────
# Each coach pairs THEIR OWN computer to THEIR OWN CAPP Binder login, once —
# from then on the server only ever hands THIS worker jobs THAT coach uploaded
# (never another coach's, even on the same team). See "BINDER LOCAL PLAN.txt".
# The (future) one-click setup installer drops a one-time pairing_token.txt
# next to this script before first launch; the worker exchanges it for its own
# permanent device_token.json and never needs it again. A machine with neither
# file falls back to the legacy shared PB_WORKER_TOKEN (.env) — that worker
# only ever receives admin-panel-direct uploads (no coach to pair to), never a
# coach's files.
_DEVICE_TOKEN_PATH = _HERE / "device_token.json"
_PAIRING_TOKEN_PATH = _HERE / "pairing_token.txt"


def _load_device_token() -> str:
    if not _DEVICE_TOKEN_PATH.exists():
        return ""
    try:
        return json.loads(_DEVICE_TOKEN_PATH.read_text(encoding="utf-8")).get("worker_token", "")
    except Exception:
        return ""


def _register_with_pairing_token() -> None:
    """One-time: exchange the setup's pairing token for this machine's own
    permanent device token. The pairing token is deleted either way (used or
    invalid) so it can never be reused or copied onto a second machine."""
    tok = _PAIRING_TOKEN_PATH.read_text(encoding="utf-8").strip()
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
    finally:
        try:
            _PAIRING_TOKEN_PATH.unlink()
        except Exception:
            pass


def _worker_token() -> str:
    saved = _load_device_token()
    if saved:
        return saved
    if _PAIRING_TOKEN_PATH.exists():
        _register_with_pairing_token()
        saved = _load_device_token()
        if saved:
            return saved
    tok = os.environ.get("PB_WORKER_TOKEN", "")
    if not tok and _ENV_PATH.exists():
        for line in _ENV_PATH.read_text().splitlines():
            line = line.strip()
            if line.startswith("PB_WORKER_TOKEN") and "=" in line:
                tok = line.split("=", 1)[1].strip().strip('"').strip("'")
    if not tok:
        log("FATAL: this computer isn't paired (no device_token.json / pairing_token.txt) "
            "and no PB_WORKER_TOKEN is set. Run 'Complete Setup' from the Binder to pair "
            "it to your login, or set PB_WORKER_TOKEN in .env for the legacy shared worker.")
        sys.exit(1)
    return tok


TOKEN = _worker_token()

# ── Visio Converter toolkit (same PC) — reused for font fidelity ─────────────
# Gives the worker the converter's proven protections: the portable Fonts
# bundle loaded into the session (so coach files using fonts this PC lacks
# still render right), and the variable-font hybrid (Visio can't embed a
# variable font like Bahnschrift into vector PDF → those pages come out with
# wrong glyphs; re-render just those pages via EMF→GDI+ raster).
_VTP_DIR = r"C:\Users\roger.hayhurst.ctr\VisioToPPT"
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
    """family-name (lower) -> font file, from installed fonts + the converter's
    Fonts bundle (mirrors ConvertWorker._name_to_file)."""
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
    """'BCDIEE+Bahnschrift-Bold' -> 'Bahnschrift' (same as the converter)."""
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
    """Name any font the file uses that is neither installed here nor in the
    Fonts bundle — Visio will substitute it and layout can shift."""
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
                    f'Visio will substitute it (layout may shift). Add its file to '
                    f'{_VTP_DIR}\\Fonts to fix.')
    except Exception:
        pass


def _hybrid_fix_variable_fonts(fg_pages, pdf_path):
    """Re-render variable-font pages as high-DPI images inside the PDF —
    the converter's proven hybrid (vector everywhere else)."""
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
                _VISIO.AlertResponse = 1  # auto-answer any modal prompt with OK
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
    """Visio COM native vector PDF export (visFixedFormatPDF=1, intent=print,
    all pages) — same call the Visio Converter's PDF mode uses — plus the
    converter's missing-font warning and variable-font hybrid raster pass."""
    visio = _get_visio()
    doc = visio.Documents.OpenEx(os.path.normpath(src), 0x2 | 0x80)  # RO + macros off
    try:
        _log_missing_fonts(doc)
        fg = [doc.Pages.Item(i) for i in range(1, doc.Pages.Count + 1)
              if not bool(doc.Pages.Item(i).Background)]
        doc.ExportAsFixedFormat(1, os.path.normpath(out), 1, 0)
        _hybrid_fix_variable_fonts(fg, os.path.normpath(out))
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


def _number_pages(pdf_path) -> None:
    """Stamp booklet page numbers 1..N bottom-center (white pill) on every page —
    same style as _stamp_label / the Visio Converter's _stamp_page_numbers, so a
    coach upload comes out numbered like the rest of the playbook. Each section
    (this doc) is numbered on its own, starting at 1."""
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


def _label_for(base_label: str, j: int) -> str:
    """Labels for a multi-page insert. A single play is the common case (j=0 →
    the label as typed, e.g. '8-1'). If more than one page comes in, number them
    from the label's trailing integer: '8-1' → 8-1, 8-2, 8-3."""
    base_label = base_label or ""
    if j == 0:
        return base_label
    m = re.search(r"(\d+)\s*$", base_label)
    if m:
        return base_label[:m.start(1)] + str(int(m.group(1)) + j)
    return f"{base_label}-{j + 1}" if base_label else ""


def _stamp_label(page, label: str) -> None:
    """Draw one page-number label bottom-center in a small white pill — the SAME
    style as the Visio Converter's booklet stamp (_stamp_page_numbers), so an
    inserted play's number matches the numbers already printed on the section's
    pages."""
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


def _merge_insert(base_path, play_path, out_path, insert_after: int, label: str) -> None:
    """Build base[0..P-1] + the play page(s) + base[P..end]. Each inserted page
    is fit-centered onto the SECTION's page size (so it reads at the same
    dimensions as the rest of the booklet) and stamped with its label."""
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


def process_insert(claim: dict) -> None:
    """An 'insert a play' job: convert the play (or pass a PDF straight through),
    splice it into the target section at the chosen page, and repoint the section
    at the merged PDF via /playbook/worker/insert-complete."""
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
            # Already a PDF — no Office conversion, just (optionally) number it.
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
    log(f"CAPP Binder worker '{WORKER_NAME}' polling {SERVER} every {POLL_SECONDS}s "
        f"(headless; log = {_LOG_PATH.name}).")
    if vtp is None:
        log(f"converter toolkit NOT loaded ({globals().get('_VTP_IMPORT_ERR', 'missing')}) — "
            f"font protections off, plain Visio export only")
    else:
        try:
            loaded = vtp.register_session_fonts()
            atexit.register(vtp.unregister_session_fonts, loaded)
            log(f"loaded {len(loaded or [])} bundled font file(s) into the session "
                f"({_VTP_DIR}\\Fonts)")
        except Exception as e:
            log(f"(session font load failed: {e})")
    # Prime Visio once, quietly, before the first real job ever reaches it.
    # The very first Visio.Application launch in this process's lifetime can
    # lose a one-time COM marshaling race ("Property 'Visio.Application.
    # Visible' can not be set") no matter how long _get_visio() retries —
    # but that race only ever happens once per process, pass or fail. Eating
    # that failure here means the first real upload never sees it, instead
    # of it landing on whichever file happens to be queued first.
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
