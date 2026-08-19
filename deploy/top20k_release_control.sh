#!/usr/bin/env bash
# Activate one already sealed top-20k release, or restore its exact predecessor.
set -euo pipefail

usage() {
  echo "usage: $0 activate|rollback RELEASE_ID SHARD CANDIDATE_SHA CANONICAL_IDENTITY_SHA" >&2
  exit 2
}

[[ $# -eq 5 ]] || usage
action=$1
release_id=$2
shard=$3
candidate_sha=$4
canonical_sha=$5
[[ "$action" == activate || "$action" == rollback ]] || usage
[[ "$release_id" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]] || usage
[[ "$shard" == 0 || "$shard" == 1 ]] || usage
[[ "$candidate_sha" =~ ^[0-9a-f]{64}$ ]] || usage
[[ "$canonical_sha" =~ ^[0-9a-f]{64}$ ]] || usage

root=${LEXORA_ROOT:-/opt/lexora}
systemd_root=${LEXORA_SYSTEMD_ROOT:-/etc/systemd/system}
track_root="$root/deployments/repair"
release="$track_root/releases/$release_id"
transaction="$release/deploy/deployment_transaction.py"
service="lexora-top20k-repair@${shard}.service"
timer="lexora-top20k-repair@${shard}.timer"
micro_collector="lexora-enrich-micro@${shard}.service"
full_collector="lexora-enrich@${shard}.service"
backup="$track_root/systemd-backups/$release_id"
state_file="$backup/state.env"
started_file="$backup/activation-started"
install -d -m 0750 "$track_root"
exec 9>"$track_root/.control.lock"
flock -w 10 9 || { echo "another release control operation is active" >&2; exit 1; }

main_target="$systemd_root/lexora-top20k-repair@.service"
timer_target="$systemd_root/lexora-top20k-repair@.timer"
dropin_dir="$systemd_root/lexora-top20k-repair@.service.d"
dropin_target="$dropin_dir/10-current-release.conf"

run_systemctl() {
  sudo systemctl "$@"
}

save_one() {
  local target=$1 name=$2
  if [[ -e "$target" ]]; then
    sudo cp -a "$target" "$backup/$name"
  else
    : > "$backup/$name.absent"
  fi
}

restore_one() {
  local target=$1 name=$2
  if [[ -e "$backup/$name" ]]; then
    sudo install -m 0644 "$backup/$name" "$target"
  elif [[ -e "$backup/$name.absent" ]]; then
    # This removes only a unit introduced by the failed/reverted deployment.
    sudo rm -f -- "$target"
  else
    echo "missing rollback record for $target" >&2
    return 1
  fi
}

restore_writer_snapshot() {
  local writer_status=0
  if [[ "$micro_active_before" == active || "$micro_active_before" == activating ]]; then
    run_systemctl start --no-block "$micro_collector" || writer_status=1
  else
    run_systemctl stop "$micro_collector" || true
  fi
  if [[ "$full_active_before" == active || "$full_active_before" == activating ]]; then
    run_systemctl start --no-block "$full_collector" || writer_status=1
  else
    run_systemctl stop "$full_collector" || true
  fi
  return "$writer_status"
}

release_activation_phase() {
  python3 - "$track_root/activation-state.json" "$release_id" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
release_id = sys.argv[2]
if not path.is_file():
    print("missing")
    raise SystemExit
try:
    value = json.loads(path.read_text(encoding="utf-8"))
except Exception:
    print("invalid")
    raise SystemExit
if (
    value.get("activeRelease") == release_id
    and value.get("phase") in {"prepared", "active"}
):
    print("requested")
else:
    print("other")
PY
}

rollback_impl() {
  local activation_phase
  activation_phase=$(release_activation_phase)
  if [[ ! -f "$started_file" && "$activation_phase" != requested ]]; then
    if [[ "$activation_phase" == missing || "$activation_phase" == other ]]; then
      # The coordinator records a host before entering SSH so it can recover a
      # lost connection after a switch.  If SSH never began, this release has
      # neither an activation journal nor saved unit state: rollback must be a
      # strict no-op and must not stop the previously active repair service.
      echo "rollback_noop=never-started release_id=$release_id"
      return 0
    fi
    echo "release activation exists without saved system state; refusing mutation" >&2
    return 1
  fi
  if [[ ! -f "$state_file" ]]; then
    echo "release activation exists without saved system state; refusing mutation" >&2
    return 1
  fi
  set +e
  local rollback_status=0
  run_systemctl stop "$service" || rollback_status=1
  if [[ "$activation_phase" == requested ]]; then
    python3 "$transaction" --root "$root" --track repair \
      --release-id "$release_id" rollback || rollback_status=1
  elif [[ "$activation_phase" != missing && "$activation_phase" != other ]]; then
    echo "invalid activation journal; refusing transactional rollback" >&2
    rollback_status=1
  fi
  if [[ -f "$state_file" ]]; then
    # shellcheck disable=SC1090
    source "$state_file"
    restore_one "$main_target" main.service || rollback_status=1
    restore_one "$timer_target" main.timer || rollback_status=1
    restore_one "$dropin_target" current.conf || rollback_status=1
    run_systemctl daemon-reload || rollback_status=1
    if [[ "$timer_enabled_before" == enabled ]]; then
      run_systemctl enable "$timer" || rollback_status=1
    else
      run_systemctl disable "$timer" || rollback_status=1
    fi
    if [[ "$timer_active_before" == active ]]; then
      run_systemctl start "$timer" || rollback_status=1
    else
      run_systemctl stop "$timer" || rollback_status=1
    fi
    if [[ "$service_active_before" == active || "$service_active_before" == activating ]]; then
      run_systemctl start --no-block "$service" || rollback_status=1
    fi
    restore_writer_snapshot || rollback_status=1
  fi
  set -e
  if [[ "$rollback_status" == 0 ]]; then
    rm -f -- "$started_file"
  fi
  return "$rollback_status"
}

if [[ "$action" == rollback ]]; then
  rollback_impl
  exit
fi

[[ -f "$transaction" ]] || { echo "missing transaction tool in release" >&2; exit 1; }
[[ -f "$release/deploy/lexora-top20k-repair@.service" ]] || {
  echo "missing repair service in release" >&2; exit 1;
}
[[ -f "$release/deploy/lexora-top20k-repair@.timer" ]] || {
  echo "missing repair timer in release" >&2; exit 1;
}
[[ -f "$release/deploy/lexora-top20k-repair-current.conf" ]] || {
  echo "missing current-release drop-in" >&2; exit 1;
}

python3 "$transaction" --root "$root" --track repair \
  --release-id "$release_id" verify-release \
  --candidate-sha256 "$candidate_sha" \
  --canonical-identity-sha256 "$canonical_sha"

if [[ -e "$backup" ]]; then
  if [[ -f "$backup/activation-complete" ]] \
    && [[ -L "$track_root/current" ]] \
    && [[ "$(readlink "$track_root/current")" == "releases/$release_id" ]]; then
    echo "release is already active and verified: $release_id"
    exit 0
  fi
  echo "refusing to overwrite activation backup: $backup" >&2
  exit 1
fi
install -d -m 0750 "$backup"
service_active_before=$(systemctl is-active "$service" || true)
timer_active_before=$(systemctl is-active "$timer" || true)
timer_enabled_before=$(systemctl is-enabled "$timer" || true)
micro_active_before=$(systemctl is-active "$micro_collector" || true)
full_active_before=$(systemctl is-active "$full_collector" || true)
printf 'service_active_before=%q\ntimer_active_before=%q\ntimer_enabled_before=%q\nmicro_active_before=%q\nfull_active_before=%q\n' \
  "$service_active_before" "$timer_active_before" "$timer_enabled_before" \
  "$micro_active_before" "$full_active_before" \
  > "$state_file"
: > "$started_file"
save_one "$main_target" main.service
save_one "$timer_target" main.timer
sudo install -d -m 0755 "$dropin_dir"
save_one "$dropin_target" current.conf

set +e
(
  set -euo pipefail
  run_systemctl stop "$service" || true
  python3 "$transaction" --root "$root" --track repair \
    --release-id "$release_id" activate
  sudo install -m 0644 "$release/deploy/lexora-top20k-repair@.service" \
    "$main_target"
  sudo install -m 0644 "$release/deploy/lexora-top20k-repair@.timer" \
    "$timer_target"
  sudo install -m 0644 "$release/deploy/lexora-top20k-repair-current.conf" \
    "$dropin_target"
  run_systemctl daemon-reload
  run_systemctl enable "$timer"
  # Stopping the old repair unit may execute its historical unconditional
  # ExecStopPost.  Reset both possible writers to the exact pre-deploy state
  # before the new unit captures and temporarily stops them.
  restore_writer_snapshot
  run_systemctl start --no-block "$service"
  started=0
  # The fail-closed full-shard preflight reads thousands of rows before the
  # network process becomes ExecStart.  Allow that bounded local validation
  # to finish instead of treating a healthy low-power OCI host as failed.
  for attempt in $(seq 1 180); do
    state=$(systemctl is-active "$service" || true)
    substate=$(systemctl show "$service" -p SubState --value || true)
    main_pid=$(systemctl show "$service" -p MainPID --value || true)
    if [[ "$state" == active ]] \
      || [[ "$state" == activating && "$substate" == start \
        && "${main_pid:-0}" -gt 0 ]]; then
      started=1
      break
    fi
    if [[ "$state" == failed || "$state" == inactive ]]; then
      break
    fi
    sleep 1
  done
  if [[ "$started" != 1 ]]; then
    run_systemctl show "$service" -p ActiveState -p SubState -p Result \
      -p ExecMainStatus -p ExecMainCode || true
    sudo journalctl -u "$service" -n 120 --no-pager || true
    exit 1
  fi
)
status=$?
set -e
if [[ $status -ne 0 ]]; then
  echo "activation failed; restoring previous release and unit state" >&2
  rollback_impl || true
  exit "$status"
fi

printf 'release_id=%s\ncandidate_sha256=%s\ncanonical_identity_sha256=%s\n' \
  "$release_id" "$candidate_sha" "$canonical_sha" \
  > "$backup/activation-complete"
rm -f -- "$started_file"
echo "activated_release=$release_id shard=$shard candidate_sha256=$candidate_sha"
