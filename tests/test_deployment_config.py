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
        self.assertIn(
            "--workers 8 --delay 16 --translation-delay 16",
            service,
        )

    def test_micro_watchdog_only_restarts_micro_service(self) -> None:
        watch = (
            ROOT / "deploy" / "lexora-enrich-watch@.service"
        ).read_text(encoding="utf-8")
        recover = (
            ROOT / "deploy" / "lexora-enrich-recover@.service"
        ).read_text(encoding="utf-8")
        self.assertIn("After=lexora-enrich-micro@%i.service", watch)
        self.assertIn("OnFailure=lexora-enrich-recover@%i.service", watch)
        self.assertIn(
            "systemctl is-active --quiet lexora-enrich-micro@%i.service",
            watch,
        )
        self.assertIn(
            "systemctl restart lexora-enrich-micro@%i.service",
            recover,
        )

    def test_full_watchdog_only_restarts_full_service(self) -> None:
        watch = (
            ROOT / "deploy" / "lexora-enrich-full-watch@.service"
        ).read_text(encoding="utf-8")
        recover = (
            ROOT / "deploy" / "lexora-enrich-full-recover@.service"
        ).read_text(encoding="utf-8")
        self.assertIn("After=lexora-enrich@%i.service", watch)
        self.assertIn(
            "OnFailure=lexora-enrich-full-recover@%i.service",
            watch,
        )
        self.assertIn(
            "systemctl is-active --quiet lexora-enrich@%i.service",
            watch,
        )
        self.assertIn(
            "systemctl restart lexora-enrich@%i.service",
            recover,
        )
        self.assertNotIn("lexora-enrich-micro@", watch)
        self.assertNotIn("lexora-enrich-micro@", recover)

    def test_full_watchdog_timer_targets_full_watchdog(self) -> None:
        timer = (
            ROOT / "deploy" / "lexora-enrich-full-watch@.timer"
        ).read_text(encoding="utf-8")
        self.assertIn(
            "Unit=lexora-enrich-full-watch@%i.service",
            timer,
        )
        self.assertNotIn("lexora-enrich-micro@", timer)

    def test_progress_snapshot_is_low_priority_and_cached(self) -> None:
        service = (
            ROOT / "deploy" / "lexora-progress-snapshot@.service"
        ).read_text(encoding="utf-8")
        timer = (
            ROOT / "deploy" / "lexora-progress-snapshot@.timer"
        ).read_text(encoding="utf-8")
        self.assertIn("Nice=19", service)
        self.assertIn("IOSchedulingClass=idle", service)
        self.assertIn("progress-shard-%i.json", service)
        self.assertIn("OnUnitActiveSec=5min", timer)
        self.assertIn("AccuracySec=30s", timer)

    def test_top20k_quality_snapshot_is_bounded_and_low_priority(self) -> None:
        service = (
            ROOT / "deploy" / "lexora-top20k-quality-snapshot@.service"
        ).read_text(encoding="utf-8")
        timer = (
            ROOT / "deploy" / "lexora-top20k-quality-snapshot@.timer"
        ).read_text(encoding="utf-8")
        self.assertIn("Nice=15", service)
        self.assertIn("IOSchedulingClass=best-effort", service)
        self.assertIn("IOSchedulingPriority=7", service)
        self.assertIn("TimeoutStartSec=12min", service)
        self.assertIn("top20k-quality-shard-%i.json", service)
        self.assertIn("OnUnitActiveSec=60min", timer)

    def test_progress_deploy_requires_a_current_atomic_quality_snapshot(self) -> None:
        workflow = (
            ROOT / ".github" / "workflows" / "deploy-progress-details.yml"
        ).read_text(encoding="utf-8")
        self.assertIn('if ! snapshot_valid; then', workflow)
        self.assertIn('systemctl start "$quality_unit"', workflow)
        self.assertIn('value["qualityGateVersion"] == 2', workflow)
        self.assertIn('value["candidateDigest"] == digest', workflow)
        self.assertIn('-p ExecMainCode -p ExecMainStatus', workflow)
        self.assertIn('journalctl -u "$quality_unit"', workflow)
        self.assertIn(
            'systemctl enable --now "$quality_timer"',
            workflow,
        )
        self.assertNotIn('systemctl stop "$quality_timer"', workflow)

    def test_edge_smoke_allows_atomic_quality_snapshot_to_finish_later(self) -> None:
        script = (
            ROOT / "deploy" / "deploy-progress-edge.sh"
        ).read_text(encoding="utf-8")
        self.assertIn("for attempt in {1..18}", script)
        self.assertIn('if top["available"]:', script)
        self.assertIn('assert top["total"] == 20_000', script)
        self.assertNotIn('assert top["available"] is True', script)

    def test_fast20k_repair_service_consumes_exact_queue_ids(self) -> None:
        service = (
            ROOT / "deploy" / "lexora-top20k-repair@.service"
        ).read_text(encoding="utf-8")
        self.assertIn(
            "--repair-queue "
            "/opt/lexora/build/lexora-open-oxford-safe-20k.sqlite",
            service,
        )
        self.assertIn("--quality-repair-only", service)
        self.assertNotIn("--max-frequency-rank", service)


if __name__ == "__main__":
    unittest.main()
