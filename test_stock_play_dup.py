"""A stock-phrase play has no player to share, so same_snap must decide it on down + distance + spot.

MSST @ SC Q4 (Sep 19 2026): ESPN wrote "(00:37) Kneel down by Mississippi St. at MSU47" - naming the
TEAM - and CBS wrote "K.Taylor kneels at the MSST 47" at 0:21. Same 2nd & 15, same spot, 16 s apart.
_share_player found nothing in common so every branch of same_snap was skipped and the same knee was
added a second time: a duplicate row AND a duplicate board, which shifts every later board in Catapult.

The guard must NOT swallow a real second knee. UNT @ TXST Q4 the same day: ESPN has one at TXST04,
CBS has another at the TXST 1. Three yards apart, so the spot gate rejects the pair before this rule
is reached, and the real play is still added.
"""
import sys

sys.path.insert(0, r"T:\capp-data-server")
import cbs_check as C  # noqa: E402

fails = []


def check(label, got, want):
    ok = got == want
    print(("PASS " if ok else "FAIL ") + f"{label}  (got {got}, want {want})")
    if not ok:
        fails.append(label)


def espn(text, down, dist, fp, clock):
    return {"play_text": text, "down": str(down), "distance": dist,
            "field_position": fp, "clock": clock}


def cbs(text, down, dist, spot, secs):
    return {"kind": "play", "text": text, "down": down, "distance": dist,
            "spot_yard": spot, "clock_secs": secs}


print("--- the duplicate that shipped")
check("MSST @ SC: the same knee, same 2&15, same spot -> one play",
      C.same_snap(espn("(00:37) Kneel down by Mississippi St. at MSU47 for loss of 5 yards", 2, 15, -47, "0:37"),
                  cbs("K.Taylor kneels at the MSST 47.", 2, 15, 47, 21)), True)

print("\n--- the real add that must survive")
check("UNT @ TXST: a SECOND knee 3 yards away -> not the same play",
      C.same_snap(espn("(00:45) Kneel down by #8 B.Jackson at TXST04 for loss of 0 yards", 1, 10, -4, "0:45"),
                  cbs("B.Jackson kneels at the TXST 1.", 2, 13, 1, 32)), False)

print("\n--- the rule is limited to stock phrases and still needs down + distance")
check("a different down at the same spot is a different play",
      C.same_snap(espn("(00:37) Kneel down by Mississippi St. at MSU47", 2, 15, -47, "0:37"),
                  cbs("K.Taylor kneels at the MSST 47.", 3, 15, 47, 21)), False)
check("distance off by more than a typo is a different play",
      C.same_snap(espn("(00:37) Kneel down by Mississippi St. at MSU47", 2, 15, -47, "0:37"),
                  cbs("K.Taylor kneels at the MSST 47.", 2, 20, 47, 21)), False)
check("distance off by ONE is still the same play",
      C.same_snap(espn("(00:37) Kneel down by Mississippi St. at MSU47", 2, 15, -47, "0:37"),
                  cbs("K.Taylor kneels at the MSST 47.", 2, 16, 47, 21)), True)
check("an ordinary run at the same down/distance/spot is NOT covered by this rule",
      C.same_snap(espn("(00:37) #7 J.Smith rush middle for 2 yards to the MSU47", 2, 15, -47, "0:37"),
                  cbs("D.Jones rush up the middle for 2 yards to the MSST 47.", 2, 15, 47, 21)), False)
check("a spike is covered too",
      C.same_snap(espn("(00:04) Spiked the ball to stop the clock at MSU47", 2, 15, -47, "0:04"),
                  cbs("K.Taylor spikes the ball at the MSST 47.", 2, 15, 47, 4)), True)

print()
print("ALL PASS" if not fails else f"{len(fails)} FAILED: " + "; ".join(fails))
sys.exit(1 if fails else 0)
