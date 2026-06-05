import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.build_duckdb_demo import language_hint


class LanguageHintTest(unittest.TestCase):
    def test_arabic_script_overrides_bad_source_language_hint(self):
        text = "Horchata de mango \u0645\u0634\u0631\u0648\u0628 \u0645\u0643\u0633\u064a\u0643\u064a \u0644\u0630\u064a\u0630 Ingredients: 1 cup rice"
        self.assertEqual(language_hint("fr", text), "ar")


if __name__ == "__main__":
    unittest.main()
