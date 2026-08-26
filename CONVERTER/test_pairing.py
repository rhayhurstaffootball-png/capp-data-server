"""Regression tests for converter pairing token precedence.

WHY THIS EXISTS
---------------
`_worker_token()` used to return a saved device_token.json BEFORE looking at
`--pair-token`. That made re-pairing structurally impossible: once a machine had
paired even once, every later "Setup Converter" silently reused the old
credential and never called /converter/register. When the server side of that
pairing was gone (unpaired from the admin panel, or replaced when the coach
paired another machine), the worker sat polling into 401s forever while the
Binder setup screen waited for a device that could never appear -- and
re-downloading the installer could not fix it, because reinstalling is the exact
path that was short-circuited.

Hit for real by a coach between Aug 12 and Aug 26 2026: 8 pairing tokens minted,
every single one left unredeemed (used_at NULL).

These tests drive the REAL functions out of the converter source. The module
cannot simply be imported -- it does its install/mutex/pairing work at module
level -- so the two functions under test are exec'd into a controlled namespace.
That keeps them the live source of truth rather than a copy that can drift.

Run: python CONVERTER/test_pairing.py
"""
import ast
import json
import pathlib
import sys

SRC = pathlib.Path(__file__).with_name("capp_binder_converter.py")
WANTED = {"_register_with_token", "_worker_token"}


def load_funcs(ns):
    """Exec ONLY the two functions under test into `ns`, from the real source."""
    tree = ast.parse(SRC.read_text(encoding="utf-8-sig"))
    picked = [n for n in tree.body
              if isinstance(n, ast.FunctionDef) and n.name in WANTED]
    missing = WANTED - {n.name for n in picked}
    assert not missing, f"functions not found in source: {missing}"
    mod = ast.Module(body=picked, type_ignores=[])
    exec(compile(mod, str(SRC), "exec"), ns)
    return ns


class Harness:
    """Minimal stand-ins for the module globals the two functions touch."""

    def __init__(self, saved=None, register_ok=True):
        self.saved = saved              # what's in device_token.json
        self.register_ok = register_ok  # does /converter/register succeed?
        self.register_calls = []        # tokens actually sent to the server
        self.logs = []
        self.exited = None

    def namespace(self):
        h = self

        def _load_device_token():
            return h.saved or ""

        def _register_with_token_stub(tok, fatal=True):
            raise AssertionError("real _register_with_token must be used")

        def urlopen_ok(tok):
            h.register_calls.append(tok)
            if not h.register_ok:
                raise RuntimeError("boom")
            h.saved = "device-token-for-" + tok
            return h.saved

        class _Resp:
            def __init__(self, tok):
                self.tok = tok

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        # Fake just enough of urllib/json for the real _register_with_token body.
        class _FakeReq:
            def __init__(self, url, data=None, method=None, headers=None):
                self.payload = json.loads(data.decode()) if data else {}

        class _FakeUrlOpen:
            def __call__(self, req, timeout=None):
                tok = req.payload.get("pairing_token")
                h.register_calls.append(tok)
                if not h.register_ok:
                    raise RuntimeError("server said no")
                return _FakeStream("device-token-for-" + tok)

        class _FakeStream:
            def __init__(self, dev):
                self.dev = dev

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        class _FakeJson:
            @staticmethod
            def load(stream):
                return {"worker_token": stream.dev}

            @staticmethod
            def dumps(o):
                return json.dumps(o)

        class _TokenPath:
            def write_text(self, text, encoding=None):
                h.saved = json.loads(text)["worker_token"]

            def exists(self):
                return False

        def _sys_exit(code):
            h.exited = code
            raise SystemExit(code)

        class _FakeSys:
            argv = []
            exit = staticmethod(_sys_exit)

        class _FakeUrllib:
            class request:
                Request = _FakeReq
                urlopen = _FakeUrlOpen()

        return {
            "_load_device_token": _load_device_token,
            "_register_with_pairing_token_file": lambda: None,
            "_DEVICE_TOKEN_PATH": _TokenPath(),
            "_PAIRING_TOKEN_PATH": _TokenPath(),
            "urllib": _FakeUrllib,
            "json": _FakeJson,
            "SERVER": "https://server",
            "WORKER_NAME": "TEST-PC",
            "log": lambda m: h.logs.append(str(m)),
            "sys": _FakeSys,
        }


def run(name, saved, argv, register_ok=True):
    h = Harness(saved=saved, register_ok=register_ok)
    ns = h.namespace()
    load_funcs(ns)
    ns["sys"].argv = argv
    try:
        tok = ns["_worker_token"]()
    except SystemExit as e:
        tok = None
        h.exited = e.code
    return h, tok


FAILS = []


def check(label, cond):
    print(("  PASS  " if cond else "  FAIL  ") + label)
    if not cond:
        FAILS.append(label)


print("1. THE BUG: stale device_token.json + a fresh --pair-token")
h, tok = run("stale", saved="STALE-DEAD-TOKEN",
             argv=["conv.exe", "--pair-token", "NEW123"])
check("re-pair is actually sent to the server", h.register_calls == ["NEW123"])
check("worker ends up on the NEW credential",
      tok == "device-token-for-NEW123")
check("stale token is NOT reused", tok != "STALE-DEAD-TOKEN")

print("2. First-ever pairing (no file on disk) still works")
h, tok = run("first", saved=None, argv=["conv.exe", "--pair-token", "FIRST1"])
check("registers once", h.register_calls == ["FIRST1"])
check("returns the new device token", tok == "device-token-for-FIRST1")

print("3. Normal startup, no --pair-token: uses the saved token, no network")
h, tok = run("normal", saved="GOOD-TOKEN", argv=["conv.exe"])
check("no server call made", h.register_calls == [])
check("returns saved token", tok == "GOOD-TOKEN")

print("4. SAFETY: failed re-pair must NOT kill a working converter")
h, tok = run("failsafe", saved="GOOD-TOKEN",
             argv=["conv.exe", "--pair-token", "BAD1"], register_ok=False)
check("it did attempt the re-pair", h.register_calls == ["BAD1"])
check("falls back to the existing credential", tok == "GOOD-TOKEN")
check("process did NOT exit", h.exited is None)

print("5. Failed FIRST pairing (nothing to fall back on) still exits loudly")
h, tok = run("firstfail", saved=None,
             argv=["conv.exe", "--pair-token", "BAD2"], register_ok=False)
check("exits non-zero", h.exited == 1)
check("says FATAL in the log", any("FATAL" in m for m in h.logs))

print("6. Malformed --pair-token (flag present, value missing)")
h, tok = run("malformed", saved="GOOD-TOKEN", argv=["conv.exe", "--pair-token"])
check("no server call", h.register_calls == [])
check("keeps working on the saved token", tok == "GOOD-TOKEN")

print()
if FAILS:
    print(f"{len(FAILS)} FAILED: " + "; ".join(FAILS))
    sys.exit(1)
print("all pairing tests passed")
