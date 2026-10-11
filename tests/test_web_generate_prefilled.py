from __future__ import annotations

import unittest
from unittest.mock import patch

try:
    from service import server
except ModuleNotFoundError:
    server = None


@unittest.skipIf(server is None, "FastAPI service dependencies are not installed")
class WebGeneratePrefilledTests(unittest.TestCase):
    def test_prefetched_entries_skip_server_dictionary_lookup(self) -> None:
        payload = server.PdfRequest(
            terms=["word", "missing"],
            entries=[
                {
                    "word": "word",
                    "requested_term": "word",
                    "definition": "A unit of language.",
                    "definition_zh": "语言单位。",
                }
            ],
        )

        with patch.object(
            server,
            "lookup_entry",
            side_effect=AssertionError("prefetched generation must not query SQLite"),
        ):
            entries, missing = server.resolve_pdf_entries(payload)

        self.assertEqual([entry["word"] for entry in entries], ["word"])
        self.assertEqual(missing, ["missing"])


if __name__ == "__main__":
    unittest.main()
