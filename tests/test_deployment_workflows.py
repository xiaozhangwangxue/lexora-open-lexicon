from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class DeploymentWorkflowTest(unittest.TestCase):
    def test_preparation_is_non_activating_and_compares_two_snapshots(self) -> None:
        workflow = (
            ROOT / ".github" / "workflows" / "deploy-top20k-repair.yml"
        ).read_text(encoding="utf-8")
        self.assertIn("PREPARE_SAFE_TOP20K", workflow)
        self.assertIn("cancel-in-progress: false", workflow)
        self.assertIn('sqlite_snapshot_manifest.py" backup', workflow)
        self.assertIn("canonical-0.json", workflow)
        self.assertIn("canonical-1.json", workflow)
        self.assertIn("compare \\", workflow)
        self.assertIn("merge-owned", workflow)
        self.assertIn("canonical-shard-1.sqlite", workflow)
        self.assertIn("canonical-owned.sqlite", workflow)
        self.assertIn("scp -3", workflow)
        self.assertIn("fast20k_repair_delta", workflow)
        self.assertIn("candidate_sha256", workflow)
        self.assertIn("structuralReady", workflow)
        self.assertNotIn("systemctl start", workflow)
        self.assertNotIn("systemctl enable", workflow)

    def test_activation_verifies_both_before_switch_and_has_rollback(self) -> None:
        workflow = (
            ROOT / ".github" / "workflows" / "activate-top20k-repair.yml"
        ).read_text(encoding="utf-8")
        self.assertIn("ACTIVATE_SAFE_TOP20K", workflow)
        self.assertIn("cancel-in-progress: false", workflow)
        verify_position = workflow.index("Verify both sealed releases")
        activate_position = workflow.index("Activate both hosts")
        self.assertLess(verify_position, activate_position)
        self.assertIn("rollback_activated", workflow)
        self.assertIn("trap on_error ERR", workflow)
        self.assertLess(
            workflow.index('activated+=("$host:$shard")'),
            workflow.index("bash '$control' activate"),
        )
        self.assertIn("CANDIDATE_SHA", workflow)
        self.assertIn("CANONICAL_SHA", workflow)
        self.assertIn("sha256sum /opt/lexora/deployments/repair/current", workflow)

    def test_progress_deploys_an_immutable_full_dependency_bundle(self) -> None:
        workflow = (
            ROOT / ".github" / "workflows" / "deploy-progress-details.yml"
        ).read_text(encoding="utf-8")
        self.assertIn("find tools service deploy -type f", workflow)
        self.assertIn("prepare-code", workflow)
        self.assertIn("verify-release", workflow)
        self.assertIn("import enrich_oxford_scope,top20k_quality", workflow)
        self.assertIn("lexora-top20k-quality-current.conf", workflow)
        self.assertIn("full_collector_before", workflow)
        self.assertIn("trap - ERR", workflow)
        self.assertNotIn('sudo install -o opc -g opc -m 0755 "/tmp/$file"', workflow)

    def test_repair_release_uses_digest_isolated_state_and_exact_writer_restore(
        self,
    ) -> None:
        dropin = (ROOT / "deploy" / "lexora-top20k-repair-current.conf").read_text(
            encoding="utf-8"
        )
        state = (ROOT / "deploy" / "repair_collector_state.sh").read_text(
            encoding="utf-8"
        )
        self.assertIn("candidate.env", dropin)
        self.assertIn("${LEXORA_CANDIDATE_DIGEST}", dropin)
        self.assertIn("preflight_repair_queue.py", dropin)
        self.assertIn("--success-marker", dropin)
        self.assertIn("--ready-marker", dropin)
        self.assertIn("runtime-ready-shard-%i.json", dropin)
        self.assertIn("${LEXORA_RELEASE_ID}", dropin)
        self.assertLess(
            dropin.index("/usr/bin/rm -f"),
            dropin.index("preflight_repair_queue.py"),
        )
        self.assertNotIn("fast20k-repair-shard-%i.sqlite", dropin)
        self.assertIn("lexora-enrich-micro@", state)
        self.assertIn("lexora-enrich@${shard}.service", state)
        self.assertIn("micro_state", state)
        self.assertIn("full_state", state)
        self.assertIn("lexora-enrich-watch@${shard}.timer", state)
        self.assertIn("lexora-enrich-full-watch@${shard}.timer", state)
        self.assertLess(
            state.index('"$micro_watch_timer" "$full_watch_timer"'),
            state.index('"$micro_collector" "$full_collector"'),
        )

        workflow = (
            ROOT / ".github" / "workflows" / "deploy-top20k-repair.yml"
        ).read_text(encoding="utf-8")
        self.assertIn("preflight_repair_queue.py", workflow)
        self.assertIn('--dataset "$remote_tmp/canonical.sqlite"', workflow)
        self.assertIn('--shard-index "$shard"', workflow)

        quality_dropin = (
            ROOT / "deploy" / "lexora-top20k-quality-current.conf"
        ).read_text(encoding="utf-8")
        self.assertIn("--candidate /opt/lexora/deployments/repair/current/", quality_dropin)

        control = (ROOT / "deploy" / "top20k_release_control.sh").read_text(
            encoding="utf-8"
        )
        self.assertIn("restore_writer_snapshot", control)
        self.assertIn("marker_valid", control)
        self.assertIn("validate_preflight_marker.py", control)
        self.assertIn("--kind preflight", control)
        self.assertIn("--kind runtime", control)
        self.assertIn("ExecMainStartTimestampMonotonic", control)
        self.assertIn('consecutive" -ge 3', control)
        self.assertIn("seq 1 180", control)

    def test_deployment_shell_helpers_have_valid_syntax(self) -> None:
        for path in (
            ROOT / "deploy" / "top20k_release_control.sh",
            ROOT / "deploy" / "repair_collector_state.sh",
        ):
            subprocess.run(["bash", "-n", str(path)], check=True)

    def test_never_started_release_rollback_is_a_strict_noop(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "bin"
            binary.mkdir()
            calls = root / "systemctl-calls"
            fake_systemctl = binary / "systemctl"
            fake_systemctl.write_text(
                "#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" >> \"$CALLS\"\nexit 99\n",
                encoding="utf-8",
            )
            fake_systemctl.chmod(0o755)
            fake_flock = binary / "flock"
            fake_flock.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            fake_flock.chmod(0o755)
            environment = os.environ.copy()
            environment.update(
                {
                    "CALLS": str(calls),
                    "LEXORA_ROOT": str(root / "lexora"),
                    "LEXORA_SYSTEMD_ROOT": str(root / "systemd"),
                    "PATH": str(binary) + os.pathsep + environment["PATH"],
                }
            )
            result = subprocess.run(
                [
                    "bash",
                    str(ROOT / "deploy" / "top20k_release_control.sh"),
                    "rollback",
                    "never-started",
                    "0",
                    "a" * 64,
                    "b" * 64,
                ],
                check=False,
                capture_output=True,
                text=True,
                env=environment,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("rollback_noop=never-started", result.stdout)
            self.assertFalse(calls.exists())

            # A durable backup from an older activation must not be mistaken
            # for work started by this coordinator run once another release
            # is active.
            track = root / "lexora" / "deployments" / "repair"
            backup = track / "systemd-backups" / "never-started"
            backup.mkdir(parents=True)
            (backup / "state.env").write_text(
                "service_active_before=active\n"
                "timer_active_before=active\n"
                "timer_enabled_before=enabled\n"
                "micro_active_before=active\n"
                "full_active_before=inactive\n",
                encoding="utf-8",
            )
            (track / "activation-state.json").write_text(
                '{"format":"lexora-deployment-activation-v1",'
                '"phase":"active","activeRelease":"newer"}',
                encoding="utf-8",
            )
            repeated = subprocess.run(
                [
                    "bash",
                    str(ROOT / "deploy" / "top20k_release_control.sh"),
                    "rollback",
                    "never-started",
                    "0",
                    "a" * 64,
                    "b" * 64,
                ],
                check=False,
                capture_output=True,
                text=True,
                env=environment,
            )
            self.assertEqual(repeated.returncode, 0, repeated.stderr)
            self.assertIn("rollback_noop=never-started", repeated.stdout)
            self.assertFalse(calls.exists())


if __name__ == "__main__":
    unittest.main()
