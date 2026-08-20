from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class DeploymentWorkflowTest(unittest.TestCase):
    def test_legacy_repair_retirement_preserves_data_and_restores_one_writer(
        self,
    ) -> None:
        workflow = (
            ROOT / ".github" / "workflows" / "retire-legacy-top20k-repair.yml"
        ).read_text(encoding="utf-8")
        self.assertIn("RETIRE_LEGACY_TOP20K_SHARD0", workflow)
        self.assertIn("opc@140.245.114.238", workflow)
        self.assertIn('systemctl stop "$repair"', workflow)
        self.assertIn('systemctl start --no-block "$micro"', workflow)
        self.assertIn('systemctl disable --now "$timer"', workflow)
        self.assertNotIn("rm -", workflow)
        self.assertNotIn("134.185.82.7", workflow)

    def test_preparation_is_non_activating_and_merges_fixed_owners(self) -> None:
        workflow = (
            ROOT / ".github" / "workflows" / "deploy-top20k-repair.yml"
        ).read_text(encoding="utf-8")
        self.assertIn("PREPARE_SAFE_TOP20K", workflow)
        self.assertIn("cancel-in-progress: false", workflow)
        self.assertNotIn('sqlite_snapshot_manifest.py" backup', workflow)
        self.assertIn("canonical-0.json", workflow)
        self.assertIn("canonical-1.json", workflow)
        self.assertIn("compare \\", workflow)
        self.assertIn("refresh-owner", workflow)
        self.assertIn("--shard-index 1 --shard-count 2", workflow)
        self.assertIn("candidate-host0.sqlite", workflow)
        self.assertIn("candidate-final.sqlite", workflow)
        self.assertNotIn("scp -3", workflow)
        self.assertIn("fast20k_repair_delta", workflow)
        self.assertIn("candidate_sha256", workflow)
        self.assertIn("structuralReady", workflow)
        self.assertIn('{"words": 16_000, "phrases": 4_000}', workflow)
        self.assertIn("--phrase-target 4000", workflow)
        self.assertGreaterEqual(workflow.count("--skip-canonical-quick-check"), 3)
        self.assertNotIn("systemctl start", workflow)
        self.assertNotIn("systemctl enable", workflow)

    def test_preparation_captures_both_small_identities_in_parallel(self) -> None:
        workflow = (
            ROOT / ".github" / "workflows" / "deploy-top20k-repair.yml"
        ).read_text(encoding="utf-8")
        self.assertIn("timeout-minutes: 120", workflow)
        self.assertIn('prepare_one "$shard" &', workflow)
        self.assertIn('wait "$pid" || status=1', workflow)
        self.assertNotIn("canonical-shard-1.sqlite", workflow)

    def test_activation_verifies_both_before_switch_and_has_rollback(self) -> None:
        workflow = (
            ROOT / ".github" / "workflows" / "activate-top20k-repair.yml"
        ).read_text(encoding="utf-8")
        self.assertIn("ACTIVATE_SAFE_TOP20K", workflow)
        self.assertIn("cancel-in-progress: false", workflow)
        self.assertIn("timeout-minutes: 90", workflow)
        self.assertIn('verify_one "$shard" &', workflow)
        self.assertIn('wait "$pid" || status=1', workflow)
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
        self.assertIn("progress_validation_target", workflow)
        self.assertIn(
            'restore_one "$progress_validation_target" progress_validation.py',
            workflow,
        )
        self.assertIn("import service.server", workflow)
        self.assertIn("full_collector_before", workflow)
        self.assertIn("validate_collection_writer_state.py", workflow)
        self.assertIn('--full "$full_collector_after"', workflow)
        self.assertNotIn('case "$collector_after:$repair_after"', workflow)
        self.assertIn("quality_job_after_reload", workflow)
        self.assertIn("quality_main_pid_after_reload", workflow)
        self.assertIn("inactive:0|activating:0", workflow)
        self.assertIn('sudo systemctl stop "$quality_unit"', workflow)
        self.assertIn("canceling stale queued quality snapshot job", workflow)
        self.assertIn("quality_job_after_cancel", workflow)
        self.assertIn(
            "from service.progress_validation import validate_top20k_quality_snapshot",
            workflow,
        )
        self.assertIn("validate_top20k_quality_snapshot(", workflow)
        self.assertLess(
            workflow.index("sudo systemctl daemon-reload"),
            workflow.index("quality_job_after_reload"),
        )
        self.assertLess(
            workflow.index("quality_job_after_reload"),
            workflow.index('sudo systemctl start "$quality_unit"'),
        )
        self.assertLess(
            workflow.index('sudo systemctl start "$quality_unit"'),
            workflow.index('sudo systemctl enable --now "$quality_timer"'),
        )
        self.assertIn("trap - ERR", workflow)
        self.assertNotIn('sudo install -o opc -g opc -m 0755 "/tmp/$file"', workflow)

    def test_web_parity_deploys_progress_validator_with_server(self) -> None:
        workflow = (
            ROOT / ".github" / "workflows" / "deploy-web-parity-api.yml"
        ).read_text(encoding="utf-8")
        self.assertIn("service/progress_validation.py", workflow)
        self.assertIn(
            "for file in server.py progress_validation.py web_documents.py web_pdf.py",
            workflow,
        )
        self.assertIn("/opt/lexora/service/progress_validation.py", workflow)
        self.assertIn("import service.server", workflow)

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
            dropin.index("repair_collector_state.sh capture"),
            dropin.index("/usr/bin/rm -f"),
        )
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
        self.assertIn(
            "--dataset /opt/lexora/build/lexora-open-oxford-scope.sqlite",
            workflow,
        )
        self.assertIn('--shard-index "$shard"', workflow)

        quality_dropin = (
            ROOT / "deploy" / "lexora-top20k-quality-current.conf"
        ).read_text(encoding="utf-8")
        self.assertIn(
            "--candidate /opt/lexora/deployments/repair/current/", quality_dropin
        )

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
                '#!/usr/bin/env bash\nprintf \'%s\\n\' "$*" >> "$CALLS"\nexit 99\n',
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

    def test_collector_restore_without_a_complete_capture_is_a_strict_noop(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "bin"
            runtime = root / "run"
            binary.mkdir()
            runtime.mkdir()
            calls = root / "systemctl-calls"
            fake_systemctl = binary / "systemctl"
            fake_systemctl.write_text(
                '#!/usr/bin/env bash\nprintf \'%s\\n\' "$*" >> "$CALLS"\nexit 99\n',
                encoding="utf-8",
            )
            fake_systemctl.chmod(0o755)
            environment = os.environ.copy()
            environment.update(
                {
                    "CALLS": str(calls),
                    "LEXORA_RUN_ROOT": str(runtime),
                    "LEXORA_STATE_ROOT": str(root / "state"),
                    "PATH": str(binary) + os.pathsep + environment["PATH"],
                }
            )
            script = ROOT / "deploy" / "repair_collector_state.sh"

            missing = subprocess.run(
                ["bash", str(script), "restore", "0"],
                check=False,
                capture_output=True,
                text=True,
                env=environment,
            )
            self.assertEqual(missing.returncode, 0, missing.stderr)
            self.assertIn("reason=missing-capture-marker", missing.stdout)
            self.assertFalse(calls.exists())

            # Simulate an old marker followed by an early capture failure.
            # Capture must invalidate the old snapshot before validation, and
            # ExecStopPost restore must consequently remain a no-op.
            marker = runtime / "lexora-top20k-repair-0.collector-state"
            marker.write_text("micro=active\n", encoding="utf-8")
            failed_capture = subprocess.run(
                ["bash", str(script), "capture", "0"],
                check=False,
                capture_output=True,
                text=True,
                env=environment,
            )
            self.assertNotEqual(failed_capture.returncode, 0)
            self.assertFalse(marker.exists())
            after_failure = subprocess.run(
                ["bash", str(script), "restore", "0"],
                check=False,
                capture_output=True,
                text=True,
                env=environment,
            )
            self.assertEqual(after_failure.returncode, 0, after_failure.stderr)
            self.assertIn("reason=missing-capture-marker", after_failure.stdout)
            self.assertFalse(calls.exists())

            # With a valid candidate id but unavailable systemd, capture must
            # fail before publishing a marker or stopping anything.  The
            # ensuing ExecStopPost restore makes no additional systemctl call.
            environment["LEXORA_CANDIDATE_DIGEST"] = "d" * 64
            unavailable = subprocess.run(
                ["bash", str(script), "capture", "1"],
                check=False,
                capture_output=True,
                text=True,
                env=environment,
            )
            self.assertNotEqual(unavailable.returncode, 0)
            self.assertIn("unavailable collector unit state", unavailable.stderr)
            shard_one_marker = runtime / "lexora-top20k-repair-1.collector-state"
            self.assertFalse(shard_one_marker.exists())
            calls_before_restore = calls.read_text(encoding="utf-8")
            unavailable_restore = subprocess.run(
                ["bash", str(script), "restore", "1"],
                check=False,
                capture_output=True,
                text=True,
                env=environment,
            )
            self.assertEqual(unavailable_restore.returncode, 0)
            self.assertIn("reason=missing-capture-marker", unavailable_restore.stdout)
            self.assertEqual(calls.read_text(encoding="utf-8"), calls_before_restore)

    def test_interrupted_release_rollback_resumes_after_transaction_switch(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            lexora = root / "lexora"
            systemd = root / "systemd"
            binary = root / "bin"
            release_id = "resume-rollback"
            track = lexora / "deployments" / "repair"
            release = track / "releases" / release_id
            backup = track / "systemd-backups" / release_id
            (release / "deploy").mkdir(parents=True)
            backup.mkdir(parents=True)
            systemd.mkdir()
            binary.mkdir()

            calls = root / "systemctl-calls"
            transaction_calls = root / "transaction-calls"
            fail_once = root / "fail-daemon-reload-once"
            fail_once.touch()
            fake_systemctl = binary / "systemctl"
            fake_systemctl.write_text(
                "#!/usr/bin/env bash\n"
                'printf \'%s\\n\' "$*" >> "$CALLS"\n'
                'if [[ "$*" == daemon-reload && -f "$FAIL_ONCE" ]]; then\n'
                '  rm -f -- "$FAIL_ONCE"\n'
                "  exit 1\n"
                "fi\n"
                "exit 0\n",
                encoding="utf-8",
            )
            fake_systemctl.chmod(0o755)
            fake_sudo = binary / "sudo"
            fake_sudo.write_text('#!/usr/bin/env bash\nexec "$@"\n', encoding="utf-8")
            fake_sudo.chmod(0o755)
            fake_flock = binary / "flock"
            fake_flock.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            fake_flock.chmod(0o755)

            transaction = release / "deploy" / "deployment_transaction.py"
            transaction.write_text(
                "import json, os, sys\n"
                "from pathlib import Path\n"
                "args = sys.argv[1:]\n"
                "if args[-1] != 'rollback': raise SystemExit(2)\n"
                "track = Path(args[args.index('--root') + 1]) / 'deployments' / 'repair'\n"
                "Path(os.environ['TRANSACTION_CALLS']).open('a').write('rollback\\n')\n"
                "(track / 'activation-state.json').write_text(json.dumps({"
                "'format':'lexora-deployment-activation-v1','phase':'active',"
                "'activeRelease':'previous'}))\n",
                encoding="utf-8",
            )
            (backup / "state.env").write_text(
                "service_active_before=inactive\n"
                "timer_active_before=inactive\n"
                "timer_enabled_before=disabled\n"
                "micro_active_before=inactive\n"
                "full_active_before=inactive\n",
                encoding="utf-8",
            )
            for name in ("main.service", "main.timer", "current.conf"):
                (backup / f"{name}.absent").touch()
            (track / "activation-state.json").write_text(
                '{"format":"lexora-deployment-activation-v1",'
                '"phase":"active","activeRelease":"resume-rollback"}',
                encoding="utf-8",
            )
            environment = os.environ.copy()
            environment.update(
                {
                    "CALLS": str(calls),
                    "FAIL_ONCE": str(fail_once),
                    "TRANSACTION_CALLS": str(transaction_calls),
                    "LEXORA_ROOT": str(lexora),
                    "LEXORA_SYSTEMD_ROOT": str(systemd),
                    "PATH": str(binary) + os.pathsep + environment["PATH"],
                }
            )
            command = [
                "bash",
                str(ROOT / "deploy" / "top20k_release_control.sh"),
                "rollback",
                release_id,
                "0",
                "a" * 64,
                "b" * 64,
            ]

            first = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
                env=environment,
            )
            self.assertNotEqual(first.returncode, 0)
            rollback_marker = backup / "rollback-in-progress"
            self.assertTrue(rollback_marker.is_file())
            self.assertEqual(
                transaction_calls.read_text(encoding="utf-8"), "rollback\n"
            )

            second = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
                env=environment,
            )
            self.assertEqual(second.returncode, 0, second.stderr)
            self.assertFalse(rollback_marker.exists())
            self.assertEqual(
                transaction_calls.read_text(encoding="utf-8"), "rollback\n"
            )


if __name__ == "__main__":
    unittest.main()
