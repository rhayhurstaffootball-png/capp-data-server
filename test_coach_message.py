"""Coach messages (Oct 24 2026): posted from the Game Day board, ride on the game's /plays payload, expire, retract.
Run:  python test_coach_message.py
"""
import os
import sys
import time

os.environ.setdefault("CAPP_CBS", "0")
import main as M

fails = 0


def check(name, got, want):
    global fails
    ok = got == want
    fails += not ok
    print(("PASS " if ok else "FAIL ") + name + ("" if ok else f"   got {got!r}  want {want!r}"))


M._coach_messages.clear()
row = M._coach_message_post("401856835", "  Remove the extra point row under the 10:32 nullified touchdown.\n BYU is 14 until 9:45. ", 20)
check("posted: id, game, whitespace folded", (len(row["id"]), row["game_id"], row["text"]),
      (12, "401856835", "Remove the extra point row under the 10:32 nullified touchdown. BYU is 14 until 9:45."))
check("active for the game", [m["id"] for m in M._coach_messages_active("401856835")], [row["id"]])
check("nothing for another game", M._coach_messages_active("401858270"), [])
check("list route shape", [(m["game_id"], m["id"]) for m in M.admin_gameday_messages("")["messages"]], [("401856835", row["id"])])

row2 = M._coach_message_post("401856835", "second", 5)
check("two active, oldest first", [m["text"] for m in M._coach_messages_active("401856835")], [row["text"], "second"])
check("expired messages drop out", [m["text"] for m in M._coach_messages_active("401856835", now=time.time() + 6 * 60)], [row["text"]])
check("retract", (M._coach_message_retract(row["id"])["id"], [m["text"] for m in M._coach_messages_active("401856835")]), (row["id"], ["second"]))
check("retract unknown -> None", M._coach_message_retract("nope"), None)

for bad, why in (("", "x"), ("401856835", ""), ("abc", "x"), ("401856835", "y" * 601)):
    try:
        M._coach_message_post(bad, why)
        check(f"rejects {bad!r}/{why[:3]!r}", "accepted", "ValueError")
    except ValueError:
        check(f"rejects {bad!r}/{why[:3]!r}", "ValueError", "ValueError")
check("minutes clamped 1..240", (M._coach_message_post("1", "a", 0)["minutes"], M._coach_message_post("1", "a", 9999)["minutes"], M._coach_message_post("1", "a", "zz")["minutes"]), (1, 240, 20))

# the /plays route attaches them without touching the cached payload
cached = {"entries": [{"entry_key": "k1"}], "status": "in"}
M.get_game_plays = lambda gid, league="cfb", force_refresh=False: cached
M.mark_game_active = lambda *a, **k: None
M._record_game_request = lambda *a, **k: None
import ncaa_check
ncaa_check.without_added_rows = lambda p: p
M._coach_messages.clear()
out = M.plays("777", league="cfb", force_refresh=False, ncaa_rows=False)
check("no message -> payload untouched", "coach_messages" in out, False)
r = M._coach_message_post("777", "hello coach")
out = M.plays("777", league="cfb", force_refresh=False, ncaa_rows=False)
check("message rides on the payload", [m["text"] for m in out.get("coach_messages", [])], ["hello coach"])
check("the cached dict was not written into", "coach_messages" in cached, False)

print("\nALL PASS" if not fails else f"\n{fails} FAILED")
sys.exit(1 if fails else 0)
