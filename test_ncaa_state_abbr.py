"""NCAA writes team names AP-style; CAPP asks with ESPN's spelling. _fold must bring them together.

Measured Sep 23 2026: 18 of 124 Sep 19 games had no NCAA data, and nearly every failure was a state
abbreviation - confirmed live as 'Western Ky.', 'N.C. A&T', 'Central Conn. St.', 'Ga. Southern'.

Guards both directions: the pairs that must now match, and the words that must NOT be mangled.
"""
import sys

sys.path.insert(0, r"T:\capp-data-server")
from ncaa_live import _fold, _name_score, _name_forms  # noqa: E402

fails = []


def check(label, cond):
    print(("PASS " if cond else "FAIL ") + label)
    if not cond:
        fails.append(label)


# ── the pairs that cost us games: ESPN's spelling vs NCAA's ───────────────────────────────────
PAIRS = [
    ("Georgia Southern", "Ga. Southern"),
    ("Western Kentucky", "Western Ky."),
    ("North Carolina A&T", "N.C. A&T"),
    ("Central Connecticut", "Central Conn. St."),
    ("Southeast Missouri State", "Southeast Mo. St."),
    ("Eastern Kentucky", "Eastern Ky."),
    ("West Georgia", "West Ga."),
    ("Northern Colorado", "Northern Colo."),
    ("North Alabama", "North Ala."),
    ("Central Arkansas", "Central Ark."),
    ("East Tennessee State", "East Tenn. St."),
    ("South Florida", "South Fla."),
    ("Mississippi Valley State", "Mississippi Val. St."),
    ("Arkansas-Pine Bluff", "Ark.-Pine Bluff"),
    ("Southeastern Louisiana", "Southeastern La."),
]
print("--- ESPN spelling must match NCAA spelling")
for ours, ncaa in PAIRS:
    score = _name_score(ours, [_fold(ncaa)])
    check(f"{ours:26} <- {ncaa!r}  (score {score})", score >= 1)

# ── the traps: words that merely CONTAIN an abbreviation must survive ─────────────────────────
print("\n--- must NOT be mangled")
UNTOUCHED = [
    ("La Salle", "la salle"),             # bare 'la' is not 'Louisiana'
    ("Indiana", "indiana"),               # 'ind' inside a word
    ("Mississippi", "mississippi"),       # 'miss' inside a word
    ("Delaware", "delaware"),             # 'del' inside a word
    ("Alabama", "alabama"),
    ("Washington", "washington"),
    ("Valparaiso", "valparaiso"),         # 'val' inside a word
    ("Gardner-Webb", "gardner webb"),     # 'ga' inside a word
    ("Lafayette", "lafayette"),
    ("Montana", "montana"),               # 'mont' inside a word
]
for name, want in UNTOUCHED:
    got = _fold(name)
    check(f"{name:14} folds to {got!r}", got == want)

# ── the ones that already worked must keep working ────────────────────────────────────────────
print("\n--- no regression on names that already resolved")
SAME = [("Air Force", "Air Force"), ("Maryland", "Maryland"), ("Nebraska", "Nebraska"),
        ("San Jose State", "San Jos\u00e9 State"), ("Oregon State", "Oregon St."),
        ("Jacksonville State", "Jacksonville St."), ("Middle Tennessee", "Middle Tenn.")]
for ours, ncaa in SAME:
    score = _name_score(ours, [_fold(ncaa)])
    check(f"{ours:20} <- {ncaa!r}  (score {score})", score >= 1)

print()
print("ALL PASS" if not fails else f"{len(fails)} FAILED: " + "; ".join(fails))
sys.exit(1 if fails else 0)
