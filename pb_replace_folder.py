#!/usr/bin/env python3
"""Replace a whole Binder folder from newly converted section PDFs.

WHY THIS EXISTS
---------------
The coach page's "Upload folder" is ADDITIVE ONLY -- it walks the tree and
inserts, with no match step and no upsert (`pb_worker_complete` and
`POST /coach/playbook/docs` are plain POSTs, no `on_conflict`). Point it at a
folder that already exists and you get TWO of every play, with the OLD rows
still holding the players' Touch Notes.

So a re-release has to go through per-doc REPLACE, which swaps the PDF bytes
but keeps the row id -- tree spot, title, sort order and Touch Notes all
survive. Notes are keyed (email, doc_id, page); deleting a doc loses them with
no recovery, and also re-keys `r2_key`, forcing every "Save offline" player to
re-download.

Chain, per doc:
    GET  /admin/api/playbook/docs?team=<slug>      -> current tree
    match on (folder_path, title)
    POST /admin/api/playbook/docs/sign-upload      -> {key, put_url}
    PUT  put_url                                   -> the PDF bytes (R2)
    POST /admin/api/playbook/docs/{id}/replace     -> {key, size, pages}

Creates nothing and deletes nothing. New or missing sections are REPORTED for
you to handle by hand -- silently creating rows is how you end up with a
duplicate tree under the wrong root.

USAGE
-----
    # always dry-run first and read the match count
    python pb_replace_folder.py --src "T:\\NEW DEF PLAYBOOK\\New folder" \\
                                --prefix "2026 AF DEF PLAYBOOK"

    # then, once MATCHED is what you expect and NEW/MISSING are empty
    python pb_replace_folder.py --src "..." --prefix "..." --apply

THE GOTCHA THAT COST AN HOUR ON AUG 4 2026
------------------------------------------
The Binder's root name is NOT the source folder's name. Binder root is
`2026 AF DEF PLAYBOOK`; the Visio source is `2026 AFA DEFENSIVE PLAYBOOK`, and
the converter's output folder may be named anything at all ("New folder").
Every subfolder BELOW the root matches exactly -- only the root differs.
So --prefix is required and is the knob that matters. 0 matched + everything
"NEW" means the prefix is wrong, not that the playbook is new.

Stdlib only -- no pip install (see the HARD RULE). PyMuPDF (`fitz`) is used for
exact page counts if it happens to be importable, and skipped if not.
"""

import argparse
import json
import os
import sys
import urllib.error
import urllib.request

DEFAULT_BASE = "https://capp-data-server.onrender.com"
DEFAULT_TEAM = "airforce"

try:                        # optional: exact page counts
    import fitz             # PyMuPDF
except Exception:
    fitz = None


# ── path helpers ─────────────────────────────────────────────────────────────

def norm(path):
    """Binder folder_path shape: forward slashes, no leading/trailing slash."""
    return (path or "").replace("\\", "/").strip("/")


def under(path, prefix):
    """Is `path` at or below `prefix`, matching whole SEGMENTS?

    ⚠ Deliberately not `startswith`. These folder names make naive matching
    over-reach: `01 OFFENSE` would match `01 OFFENSE LINE` and pull a whole
    unrelated section into the replace set.
    """
    p, q = norm(path).casefold(), norm(prefix).casefold()
    if not q:
        return True
    return p == q or p.startswith(q + "/")


def key_of(folder, title):
    """Match key. Casefolded so a case difference reads as a MATCH, not as a
    NEW doc -- a false NEW is the dangerous direction (it leaves the real doc
    stale and untouched)."""
    return (norm(folder).casefold(), (title or "").strip().casefold())


def page_count(path):
    if fitz is None:
        return None
    try:
        with fitz.open(path) as d:
            return d.page_count
    except Exception:
        return None


# ── http ─────────────────────────────────────────────────────────────────────

def api(base, token, method, path, payload=None):
    url = base.rstrip("/") + path
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("X-Admin-Token", token)
    if data:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            body = r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", "replace")[:300]
        raise SystemExit(f"\n{method} {path} failed: HTTP {e.code}\n{detail}")
    return json.loads(body) if body.strip() else {}


def put_bytes(url, path):
    with open(path, "rb") as f:
        body = f.read()
    req = urllib.request.Request(url, data=body, method="PUT")
    req.add_header("Content-Type", "application/pdf")
    req.add_header("Content-Length", str(len(body)))
    with urllib.request.urlopen(req, timeout=600) as r:
        if r.status not in (200, 201, 204):
            raise SystemExit(f"storage PUT returned {r.status}")
    return len(body)


# ── scan ─────────────────────────────────────────────────────────────────────

def scan_local(src, prefix):
    """Every PDF under src -> (folder_path, title, abs path), mirroring how the
    Binder stores it: folder = prefix + the path relative to src."""
    out = []
    for root, _dirs, files in os.walk(src):
        rel = os.path.relpath(root, src)
        rel = "" if rel == "." else norm(rel)
        folder = norm(prefix + ("/" + rel if rel else ""))
        for f in sorted(files):
            if f.lower().endswith(".pdf"):
                out.append((folder, os.path.splitext(f)[0], os.path.join(root, f)))
    return sorted(out, key=lambda r: (r[0], r[1]))


def main():
    ap = argparse.ArgumentParser(description="Replace a Binder folder in place.")
    ap.add_argument("--src", required=True, help="folder of converted section PDFs")
    ap.add_argument("--prefix", help="Binder root folder (DEFAULTS to basename of --src, "
                                     "which is usually WRONG -- pass it explicitly)")
    ap.add_argument("--team", default=DEFAULT_TEAM)
    ap.add_argument("--base", default=DEFAULT_BASE)
    ap.add_argument("--token", default=os.environ.get("CAPP_ADMIN_TOKEN", ""))
    ap.add_argument("--apply", action="store_true", help="actually write (default is dry-run)")
    args = ap.parse_args()

    if not os.path.isdir(args.src):
        raise SystemExit(f"--src is not a folder: {args.src}")
    if not args.token:
        raise SystemExit("No admin token. Pass --token or set CAPP_ADMIN_TOKEN.")
    prefix = norm(args.prefix or os.path.basename(os.path.abspath(args.src)))

    local = scan_local(args.src, prefix)
    if not local:
        raise SystemExit(f"No PDFs under {args.src}")

    docs = api(args.base, args.token, "GET",
               f"/admin/api/playbook/docs?team={args.team}")
    scoped = [d for d in docs if under(d.get("folder_path"), prefix)]

    # ⚠ Abort on pre-existing twins. Duplicate (folder, title) means a folder
    # upload already ran twice; replacing would refresh one twin and leave the
    # other stale, which is worse than doing nothing.
    seen, dupes = {}, []
    for d in scoped:
        k = key_of(d.get("folder_path"), d.get("title"))
        if k in seen:
            dupes.append(k)
        seen[k] = d

    matched, new = [], []
    for folder, title, path in local:
        k = key_of(folder, title)
        (matched if k in seen else new).append((folder, title, path, seen.get(k)))
    local_keys = {key_of(f, t) for f, t, _ in local}
    missing = [d for d in scoped if key_of(d.get("folder_path"), d.get("title")) not in local_keys]

    print(f"\nBinder team   : {args.team}")
    print(f"Binder prefix : {prefix}")
    print(f"Local source  : {args.src}")
    print(f"\n{len(local)} local PDFs | {len(docs)} docs in Binder "
          f"({len(scoped)} under this prefix)")
    print(f"  MATCHED (will be replaced) : {len(matched)}")
    print(f"  NEW     (not in Binder)    : {len(new)}")
    print(f"  MISSING (in Binder only)   : {len(missing)}")

    if dupes:
        print("\nABORT -- the Binder already holds duplicate (folder, title) pairs:")
        for f, t in sorted(set(dupes)):
            print(f"    {f} :: {t}")
        print("A folder upload was run twice. Clean those up before replacing.")
        return 2

    if new:
        print("\nNEW -- not in the Binder, nothing will be created:")
        for folder, title, _p, _d in new:
            print(f"    {folder} :: {title}")
    if missing:
        print("\nMISSING -- in the Binder but not in the source, left untouched:")
        for d in missing:
            print(f"    {norm(d.get('folder_path'))} :: {d.get('title')}")

    if not matched:
        print("\nNothing matched. That almost always means --prefix is wrong "
              f"(tried '{prefix}').")
        if scoped:
            print("Folders under that prefix in the Binder:")
        else:
            roots = sorted({norm(d.get("folder_path")).split("/")[0] for d in docs})
            print("Binder root folders are:")
            for r in roots:
                print(f"    {r}")
        return 1

    if not args.apply:
        print("\nDRY RUN -- nothing written. Re-run with --apply once the "
              "numbers above look right.")
        return 0

    print(f"\nReplacing {len(matched)} docs...\n")
    done = failed = 0
    for i, (folder, title, path, doc) in enumerate(matched, 1):
        size = os.path.getsize(path)
        pages = page_count(path)
        try:
            s = api(args.base, args.token, "POST",
                    "/admin/api/playbook/docs/sign-upload", {"team": args.team})
            put_bytes(s["put_url"], path)
            body = {"key": s["key"], "size": size}
            if pages is not None:
                body["pages"] = pages
            api(args.base, args.token, "POST",
                f"/admin/api/playbook/docs/{doc['id']}/replace", body)
            done += 1
            print(f"  [{i}/{len(matched)}] ok   {folder} :: {title}  "
                  f"({size:,} bytes{f', {pages} pg' if pages else ''})")
        except SystemExit as e:
            failed += 1
            print(f"  [{i}/{len(matched)}] FAIL {folder} :: {title}  {e}")

    print(f"\n{done} replaced, {failed} failed.")
    if done:
        print("Doc ids preserved -> Touch Notes and tree positions intact. "
              "Nothing was re-numbered.")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
