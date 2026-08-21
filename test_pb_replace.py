"""Tests for pb_replace_folder's path matching.

The matching is the only part of that script that can do silent damage: an
over-reaching prefix pulls unrelated sections into the replace set, and a
false "NEW" leaves the real doc stale while you think you replaced it. So the
tests are all about `under()`, `key_of()` and `scan_local()`.

    python test_pb_replace.py
"""

import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pb_replace_folder import key_of, norm, scan_local, under


class TestNorm(unittest.TestCase):
    def test_backslashes_become_slashes(self):
        self.assertEqual(norm(r"04 CALLS\03 END OF GAME"), "04 CALLS/03 END OF GAME")

    def test_strips_edge_slashes(self):
        self.assertEqual(norm("/a/b/"), "a/b")

    def test_none_and_empty(self):
        self.assertEqual(norm(None), "")
        self.assertEqual(norm(""), "")


class TestUnder(unittest.TestCase):
    def test_exact_root_is_under_itself(self):
        self.assertTrue(under("2026 AF DEF PLAYBOOK", "2026 AF DEF PLAYBOOK"))

    def test_child_is_under(self):
        self.assertTrue(under("2026 AF DEF PLAYBOOK/04 CALLS", "2026 AF DEF PLAYBOOK"))

    def test_deep_child_is_under(self):
        self.assertTrue(under(
            "2026 AF DEF PLAYBOOK/04 CALLS/01 CONVENTIONAL CALLS/01 COVERAGES",
            "2026 AF DEF PLAYBOOK"))

    def test_sibling_is_not_under(self):
        self.assertFalse(under("2026 AF OFF PLAYBOOK/04 CALLS", "2026 AF DEF PLAYBOOK"))

    def test_prefix_must_match_whole_segments(self):
        # THE bug this function exists to prevent: naive startswith would say
        # yes here and drag a whole unrelated section into the replace set.
        self.assertFalse(under("01 OFFENSE LINE/01 PLAYS", "01 OFFENSE"))
        self.assertFalse(under("01 OFFENSE LINE", "01 OFFENSE"))

    def test_partial_word_not_under(self):
        self.assertFalse(under("2026 AF DEF PLAYBOOK 2", "2026 AF DEF PLAYBOOK"))

    def test_case_insensitive(self):
        self.assertTrue(under("2026 af def playbook/04 calls", "2026 AF DEF PLAYBOOK"))

    def test_backslash_input(self):
        self.assertTrue(under(r"2026 AF DEF PLAYBOOK\04 CALLS", "2026 AF DEF PLAYBOOK"))

    def test_empty_prefix_matches_everything(self):
        self.assertTrue(under("anything/at/all", ""))

    def test_shorter_path_not_under_longer_prefix(self):
        self.assertFalse(under("2026 AF DEF PLAYBOOK", "2026 AF DEF PLAYBOOK/04 CALLS"))


class TestKeyOf(unittest.TestCase):
    def test_same_title_different_folders_are_distinct(self):
        # Five docs share the title "00 COVER PAGES" in different folders.
        # Keys stay unique only because the folder is part of the key.
        a = key_of("BOOK/04 CALLS/01 COVERAGES/00 COVER PAGES", "00 COVER PAGES")
        b = key_of("BOOK/04 CALLS/02 PRESSURES/00 COVER PAGES", "00 COVER PAGES")
        self.assertNotEqual(a, b)

    def test_case_and_slash_insensitive(self):
        self.assertEqual(key_of(r"BOOK\04 Calls", "End Of Game"),
                         key_of("book/04 CALLS", "END OF GAME"))

    def test_title_whitespace_trimmed(self):
        self.assertEqual(key_of("a", "  X  "), key_of("a", "X"))

    def test_cover_page_singular_differs_from_plural(self):
        # Real tree has both "00 COVER PAGE" and "00 COVER PAGES".
        self.assertNotEqual(key_of("a", "00 COVER PAGE"), key_of("a", "00 COVER PAGES"))


class TestScanLocal(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        for rel in ["01 WHITE PAGES/00 PHILOSOPHY.pdf",
                    "01 WHITE PAGES/01 TERMINOLOGY.pdf",
                    "04 CALLS/03 END OF GAME/03 END OF GAME.pdf",
                    "ROOT LEVEL.pdf",
                    "01 WHITE PAGES/Thumbs.db",
                    "01 WHITE PAGES/~$$LOCK.~vsdx"]:
            p = os.path.join(self.tmp, rel.replace("/", os.sep))
            os.makedirs(os.path.dirname(p), exist_ok=True)
            open(p, "wb").write(b"x")

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_only_pdfs(self):
        rows = scan_local(self.tmp, "BOOK")
        titles = {t for _f, t, _p in rows}
        self.assertEqual(len(rows), 4)
        self.assertNotIn("Thumbs", titles)

    def test_root_pdf_lands_at_prefix(self):
        rows = scan_local(self.tmp, "BOOK")
        root = [r for r in rows if r[1] == "ROOT LEVEL"][0]
        self.assertEqual(root[0], "BOOK")

    def test_nested_folder_path(self):
        rows = scan_local(self.tmp, "BOOK")
        eog = [r for r in rows if r[1] == "03 END OF GAME"][0]
        self.assertEqual(eog[0], "BOOK/04 CALLS/03 END OF GAME")

    def test_title_drops_extension_only(self):
        rows = scan_local(self.tmp, "BOOK")
        self.assertIn("00 PHILOSOPHY", {t for _f, t, _p in rows})

    def test_prefix_is_applied_verbatim(self):
        rows = scan_local(self.tmp, "2026 AF DEF PLAYBOOK")
        self.assertTrue(all(f.startswith("2026 AF DEF PLAYBOOK") for f, _t, _p in rows))

    def test_wrong_prefix_produces_no_overlap(self):
        # The Aug 4 failure mode, as a test: source basename != Binder root,
        # so every key misses and everything reads as NEW.
        good = {key_of(f, t) for f, t, _p in scan_local(self.tmp, "2026 AF DEF PLAYBOOK")}
        bad = {key_of(f, t) for f, t, _p in scan_local(self.tmp, "2026 AFA DEFENSIVE PLAYBOOK")}
        self.assertEqual(good & bad, set())

    def test_results_sorted(self):
        rows = scan_local(self.tmp, "BOOK")
        self.assertEqual(rows, sorted(rows, key=lambda r: (r[0], r[1])))


if __name__ == "__main__":
    unittest.main(verbosity=2)
