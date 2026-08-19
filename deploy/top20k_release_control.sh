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
rollback_file="$backup/rollback-in-progress"
candidate_digest=""
if [[ -f "$release/candidate.env" ]]; then
  candidate_digest=$(sed -n 's/^LEXORA_CANDIDATE_DIGEST=//p' \
    "$release/candidate.env" | head -n 1)
fi
if [[ ! "$candidate_digest" =~ ^[0-9a-f]{64}$ ]]; then
  candidate_digest=""
fi
preflight_marker="$root/state/fast20k/${candidate_digest:-invalid}/preflight-shard-$shard.json"
runtime_marker="$root/state/fast20k/${candidate_digest:-invalid}/runtime-ready-shard-$shard.json"
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

write_durable_marker() {
  local target=$1 marker_kind=$2
  python3 - "$target" "$marker_kind" "$release_id" <<'PY'
import json
import os
import sys
import tempfile
from pathlib import Path

target = Path(sys.argv[1])
target.parent.mkdir(parents=True, exist_ok=True)
descriptor, name = tempfile.mkstemp(
    prefix=f".{target.name}.", suffix=".tmp", dir=target.parent
)
temporary = Path(name)
try:
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        json.dump(
            {"format": "lexora-release-control-marker-v1", "kind": sys.argv[2],
             "releaseId": sys.argv[3]},
            stream,
            sort_keys=True,
        )
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, target)
    directory = os.open(target.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
finally:
    temporary.unlink(missing_ok=True)
PY
}

fsync_rollback_material() {
  python3 - "$backup" <<'PY'
import os
import sys
from pathlib import Path

directory = Path(sys.argv[1])
for path in directory.iterdir():
    if path.is_file() and not path.is_symlink():
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
descriptor = os.open(directory, os.O_RDONLY)
try:
    os.fsync(descriptor)
finally:
    os.close(descriptor)
PY
}

marker_valid() {
  python3 "$release/deploy/validate_preflight_marker.py" \
    --marker "$preflight_marker" --release-id "$release_id" \
    --candidate-digest "$candidate_digest" --shard-index "$shard" \
    --shard-count 2 --kind preflight >/dev/null \
    && python3 "$release/deploy/validate_preflight_marker.py" \
      --marker "$runtime_marker" --release-id "$release_id" \
      --candidate-digest "$candidate_digest" --shard-index "$shard" \
      --shard-count 2 --kind runtime >/dev/null
}

service_ready_once() {
  local state substate main_pid exec_started result exec_code exec_status
  marker_valid || return 1
  state=$(systemctl is-active "$service" || true)
  substate=$(systemctl show "$service" -p SubState --value || true)
  main_pid=$(systemctl show "$service" -p MainPID --value || true)
  exec_started=$(systemctl show "$service" \
    -p ExecMainStartTimestampMonotonic --value || true)
  result=$(systemctl show "$service" -p Result --value || true)
  exec_code=$(systemctl show "$service" -p ExecMainCode --value || true)
  exec_status=$(systemctl show "$service" -p ExecMainStatus --value || true)
  [[ "$main_pid" =~ ^[0-9]+$ && "$exec_started" =~ ^[0-9]+$ \
    && "$exec_status" =~ ^-?[0-9]+$ ]] || return 1
  # The runtime marker PID must match the live MainPID.  Type=oneshot may also
  # legitimately finish an empty shard; that path requires an exited/0 result.
  python3 "$release/deploy/validate_preflight_marker.py" \
    --marker "$runtime_marker" --release-id "$release_id" \
    --candidate-digest "$candidate_digest" --shard-index "$shard" \
    --shard-count 2 --kind runtime --active-state "$state" \
    --sub-state "$substate" --main-pid "$main_pid" \
    --exec-started-monotonic "$exec_started" --result "$result" \
    --exec-code "$exec_code" --exec-status "$exec_status" >/dev/null
}

confirm_service_ready() {
  local attempts=${1:-3} consecutive=0
  for _ in $(seq 1 "$attempts"); do
    if service_ready_once; then
      if [[ "$(systemctl is-active "$service" || true)" == inactive ]]; then
        return 0
      fi
      consecutive=$((consecutive + 1))
      if [[ "$consecutive" -ge 3 ]]; then
        return 0
      fi
    else
      consecutive=0
    fi
    sleep 1
  done
  return 1
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
  if [[ ! -f "$started_file" && ! -f "$rollback_file" \
    && "$activation_phase" != requested ]]; then
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
  case "$activation_phase" in
    requested) ;;
    missing|other)
      [[ -f "$rollback_file" || -f "$started_file" ]] || {
        echo "rollback continuation marker is missing; refusing mutation" >&2
        return 1
      }
      ;;
    *)
      echo "invalid activation journal; refusing transactional rollback" >&2
      return 1
      ;;
  esac
  # Persist continuation intent before stopping the repair service or changing
  # the active release.  If a later unit/timer restore fails, a retry can
  # finish it even though the transaction already points at the predecessor.
  if [[ ! -f "$rollback_file" ]]; then
    write_durable_marker "$rollback_file" rollback-in-progress
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
    rm -f -- "$started_file" "$rollback_file" \
      "$preflight_marker" "$runtime_marker"
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
[[ -f "$release/deploy/validate_preflight_marker.py" ]] || {
  echo "missing preflight marker validator in release" >&2; exit 1;
}
[[ "$candidate_digest" =~ ^[0-9a-f]{64}$ ]] || {
  echo "release candidate digest is missing or invalid" >&2; exit 1;
}

python3 "$transaction" --root "$root" --track repair \
  --release-id "$release_id" verify-release \
  --candidate-sha256 "$candidate_sha" \
  --canonical-identity-sha256 "$canonical_sha"

if [[ -e "$backup" ]]; then
  if [[ -f "$backup/activation-complete" ]] \
    && [[ -L "$track_root/current" ]] \
    && [[ "$(readlink "$track_root/current")" == "releases/$release_id" ]]; then
    if confirm_service_ready 5; then
      echo "release is already active and ready: $release_id"
      exit 0
    fi
    echo "active release has no valid stable preflight/main-process evidence" >&2
    exit 1
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
save_one "$main_target" main.service
save_one "$timer_target" main.timer
sudo install -d -m 0755 "$dropin_dir"
save_one "$dropin_target" current.conf
# The marker is published only after the complete rollback material exists,
# but still before the first service stop or release switch.
fsync_rollback_material
write_durable_marker "$started_file" activation-started

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
  rm -f -- "$preflight_marker" "$runtime_marker"
  run_systemctl start --no-block "$service"
  started=0
  consecutive=0
  # The fail-closed full-shard preflight reads thousands of rows before the
  # network process becomes ExecStart.  Allow that bounded local validation
  # to finish instead of treating a healthy low-power OCI host as failed.
  for attempt in $(seq 1 180); do
    if service_ready_once; then
      if [[ "$(systemctl is-active "$service" || true)" == inactive ]]; then
        started=1
        break
      fi
      consecutive=$((consecutive + 1))
      if [[ "$consecutive" -ge 3 ]]; then
        started=1
        break
      fi
    else
      consecutive=0
    fi
    state=$(systemctl is-active "$service" || true)
    if [[ "$state" == failed || "$state" == inactive ]]; then
      break
    fi
    sleep 1
  done
  if [[ "$started" != 1 ]]; then
    run_systemctl show "$service" -p ActiveState -p SubState -p Result \
      -p ExecMainStatus -p ExecMainCode -p ExecMainStartTimestampMonotonic || true
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
