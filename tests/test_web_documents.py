from __future__ import annotations

import unittest
from unittest.mock import patch

from service.web_documents import extract_terms


class WebDocumentImportTests(unittest.TestCase):
    def test_legacy_doc_fallback_recovers_ascii_and_utf16_terms(self) -> None:
        raw = (
            b"\xd0\xcf\x11\xe0Microsoft Word\x00word\x00people-to-people\x00"
            + "vocabulary book".encode("utf-16le")
            + b"\x00\xff"
        )

        with patch("service.web_documents.shutil.which", return_value=None):
            terms = extract_terms(raw, "words.doc")

        self.assertIn("word", terms)
        self.assertIn("people-to-people", terms)
        self.assertIn("vocabulary book", terms)
        self.assertNotIn("microsoft word", terms)


if __name__ == "__main__":
    unittest.main()
