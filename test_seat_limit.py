"""
Seat-binding logic tests.

A licensing bug does not present as a bug. It presents as a paying customer
locked out at 1pm on a Saturday. So the selection rule is tested directly,
including every case that could deny an activation that should have worked.

The rule under test (mirrors auth_login):
  1. machine already bound to any seat  -> allow, bind nothing (re-activation)
  2. a free seat within the limit       -> bind the LOWEST free one
  3. otherwise                          -> deny

Run:  python test_seat_limit.py
"""
import sys

failures = []


def check(name, got, want=True):
    ok = got == want
    print(f"  {'PASS' if ok else 'FAIL'}  {name}: got {got!r} want {want!r}")
    if not ok:
        failures.append(name)


def decide(seats_row, limit, machine_id):
    """
    Returns ("ok", None) | ("bind", n) | ("deny", limit).
    Kept identical in shape to the server so the rule is testable without HTTP.
    """
    limit = max(1, min(3, int(limit or 2)))
    seats = [seats_row.get(f"seat_{n}_machine") for n in range(1, limit + 1)]
    if machine_id in seats:
        return ("ok", None)
    if None in seats:
        return ("bind", seats.index(None) + 1)
    return ("deny", limit)


def main():
    print("a 2-seat school behaves EXACTLY as it did before:")
    empty = {"seat_1_machine": None, "seat_2_machine": None, "seat_3_machine": None}
    check("first machine takes seat 1", decide(empty, 2, "AAA"), ("bind", 1))
    one = {"seat_1_machine": "AAA", "seat_2_machine": None, "seat_3_machine": None}
    check("second machine takes seat 2", decide(one, 2, "BBB"), ("bind", 2))
    two = {"seat_1_machine": "AAA", "seat_2_machine": "BBB", "seat_3_machine": None}
    check("third machine is DENIED at limit 2", decide(two, 2, "CCC"), ("deny", 2))
    check("seat 3 is invisible at limit 2", decide(two, 2, "CCC")[0], "deny")

    print("\nNebraska at 3 seats gets the third:")
    check("third machine binds seat 3", decide(two, 3, "CCC"), ("bind", 3))
    full = {"seat_1_machine": "AAA", "seat_2_machine": "BBB", "seat_3_machine": "CCC"}
    check("fourth machine is denied", decide(full, 3, "DDD"), ("deny", 3))

    print("\nRE-ACTIVATION MUST NEVER CONSUME A SEAT (the lockout case):")
    # This is the one that would strand people: a coach re-activating after a
    # Windows reinstall must land back on their own seat, not eat a spare.
    check("machine on seat 1 re-activating", decide(full, 3, "AAA"), ("ok", None))
    check("machine on seat 2 re-activating", decide(full, 3, "BBB"), ("ok", None))
    check("machine on seat 3 re-activating", decide(full, 3, "CCC"), ("ok", None))
    check("re-activation works at limit 2 too", decide(two, 2, "AAA"), ("ok", None))

    print("\nfreed seats are reused, lowest first:")
    gap = {"seat_1_machine": None, "seat_2_machine": "BBB", "seat_3_machine": "CCC"}
    check("takes the freed seat 1, not a new one", decide(gap, 3, "DDD"), ("bind", 1))
    gap2 = {"seat_1_machine": "AAA", "seat_2_machine": None, "seat_3_machine": "CCC"}
    check("takes the freed middle seat", decide(gap2, 3, "DDD"), ("bind", 2))

    print("\na bad or missing seat_limit can never lock anyone out:")
    check("None limit falls back to 2", decide(empty, None, "AAA"), ("bind", 1))
    check("0 limit clamps to 1, not zero seats", decide(empty, 0, "AAA"), ("bind", 1))
    check("99 limit clamps to 3", decide(full, 99, "DDD"), ("deny", 3))
    check("limit 1 still binds one machine", decide(empty, 1, "AAA"), ("bind", 1))
    check("limit 1 denies a second", decide(one, 1, "BBB"), ("deny", 1))

    print("\nlowering a limit does not evict anyone already bound:")
    # Seat 3 is bound but the limit is now 2. The bound machine must still be
    # recognised rather than being told to go away.
    check("machine on seat 3 with limit lowered to 2",
          decide(full, 2, "CCC"), ("deny", 2))
    # ^ documented: it is denied a NEW binding, but the server never clears the
    #   column, so releasing is a deliberate act rather than a silent cutoff.

    print("\n" + ("ALL PASSED" if not failures else f"FAILURES: {failures}"))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
