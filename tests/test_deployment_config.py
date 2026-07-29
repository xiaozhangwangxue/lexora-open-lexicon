from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class DeploymentConfigTest(unittest.TestCase):
    def test_full_shard_service_uses_cloudflare_edge(self) -> None:
        service = (
            ROOT / "deploy" / "lexora-enrich@.service"
        ).read_text(encoding="utf-8")
        self.assertIn(
            "EnvironmentFile=/opt/lexora/.env",
            service,
        )
        self.assertIn(
            "Environment=LEXORA_EDGE_URL="
            "https://dict.12323456.xyz/internal",
            service,
        )
        self.assertIn("--profile auto", service)

    def test_micro_shard_service_runs_core_then_deep(self) -> None:
        service = (
            ROOT / "deploy" / "lexora-enrich-micro@.service"
        ).read_text(encoding="utf-8")
        self.assertIn("--profile auto", service)


if __name__ == "__main__":
    unittest.main()
