"""
ONE-TIME SCRIPT — bulk-uploads TEAM_LOGOS_NUMBERED/*.png to R2 so the Binder
can auto-populate a known program's logo at team-creation time (no manual
upload needed for any of the ~263 schools already in CAPP's logo library).

Run this ONCE from Roger's PC (where TEAM_LOGOS_NUMBERED/ lives) after
filling in the 3 R2 values below — copy them from the SAME Render env vars
already set on capp-data-server (R2_ACCOUNT_ID / R2_ACCESS_KEY_ID /
R2_SECRET_KEY). Uploads to a SHARED, non-team-scoped R2 prefix
"_team_logos/{number}.png" — these are public brand assets, not private
per-team content, so they intentionally sit outside the {team_id}/... prefix
used everywhere else.

Usage:
    python upload_team_logos_to_r2.py

Safe to re-run — it just re-uploads (overwrites) each file, no duplicates.
"""
import os
import sys
import hashlib
import hmac
import datetime
import urllib.parse
import urllib.request

# ── FILL THESE IN (from Render env vars on capp-data-server) ────────────────
R2_ACCOUNT_ID = "8b95c3cd59fb1c1f9a554c1d797deaf6"      # e.g. 8b95c3cd59fb1c1f9a554c1d797deaf6 (same as Cloudflare Account ID)
R2_ACCESS_KEY_ID = "d2d745eeb46802a0ab12e89995ffbee9"
R2_SECRET_KEY = "82bda38962cec74feede981e5bf3a7a997afc861d39610098121b738071eb3da"
R2_BUCKET = "capp-playbook"
# ──────────────────────────────────────────────────────────────────────────

LOGO_DIR = os.path.join(os.path.dirname(__file__), "..", "TEAM_LOGOS_NUMBERED")
LOGO_DIR = os.path.normpath(LOGO_DIR)


def r2_presign_put(key: str, expires: int = 900) -> str:
    host = f"{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
    region, service = "auto", "s3"
    now = datetime.datetime.utcnow()
    amzdate, datestamp = now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")
    scope = f"{datestamp}/{region}/{service}/aws4_request"
    canon_uri = "/" + R2_BUCKET + "/" + urllib.parse.quote(key, safe="/~")
    q = {
        "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
        "X-Amz-Credential": f"{R2_ACCESS_KEY_ID}/{scope}",
        "X-Amz-Date": amzdate,
        "X-Amz-Expires": str(expires),
        "X-Amz-SignedHeaders": "host",
    }
    canon_qs = "&".join(f"{urllib.parse.quote(k, safe='~')}={urllib.parse.quote(v, safe='~')}"
                        for k, v in sorted(q.items()))
    canon_req = "\n".join([
        "PUT", canon_uri, canon_qs, f"host:{host}\n", "host", "UNSIGNED-PAYLOAD"])
    sts = "\n".join(["AWS4-HMAC-SHA256", amzdate, scope,
                     hashlib.sha256(canon_req.encode()).hexdigest()])

    def _s(k, m):
        return hmac.new(k, m.encode(), hashlib.sha256).digest()

    kdate = _s(("AWS4" + R2_SECRET_KEY).encode(), datestamp)
    ksig = _s(_s(_s(kdate, region), service), "aws4_request")
    sig = hmac.new(ksig, sts.encode(), hashlib.sha256).hexdigest()
    return f"https://{host}{canon_uri}?{canon_qs}&X-Amz-Signature={sig}"


def main():
    if not (R2_ACCOUNT_ID and R2_ACCESS_KEY_ID and R2_SECRET_KEY):
        print("Fill in R2_ACCOUNT_ID / R2_ACCESS_KEY_ID / R2_SECRET_KEY at the top of this "
              "file first (copy them from Render's env vars on capp-data-server).")
        sys.exit(1)
    if not os.path.isdir(LOGO_DIR):
        print(f"Can't find {LOGO_DIR} — run this from capp-data-server\\ inside T:\\CAPP_FINAL.")
        sys.exit(1)

    files = [f for f in os.listdir(LOGO_DIR) if f.lower().endswith(".png")]
    print(f"Found {len(files)} logo files in {LOGO_DIR}")
    ok, failed = 0, []
    for i, fname in enumerate(sorted(files), 1):
        key = f"_team_logos/{fname}"
        path = os.path.join(LOGO_DIR, fname)
        with open(path, "rb") as fh:
            data = fh.read()
        url = r2_presign_put(key)
        req = urllib.request.Request(url, data=data, method="PUT",
                                     headers={"Content-Type": "image/png"})
        try:
            urllib.request.urlopen(req, timeout=30)
            ok += 1
        except Exception as e:
            failed.append((fname, str(e)))
        if i % 25 == 0 or i == len(files):
            print(f"  {i}/{len(files)} uploaded...")

    print(f"\nDone: {ok} uploaded, {len(failed)} failed.")
    for fname, err in failed:
        print(f"  FAILED {fname}: {err}")


if __name__ == "__main__":
    main()
