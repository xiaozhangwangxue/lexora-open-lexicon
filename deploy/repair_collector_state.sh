#!/usr/bin/env bash
# Preserve the collector's pre-repair state instead of always starting it.
set -euo pipefail

[[ $# -eq 2 ]] || exit 2
action=$1
shard=$2
[[ "$action" == capture || "$action" == restore ]] || exit 2
[[ "$shard" == 0 || "$shard" == 1 ]] || exit 2

micro_collector="lexora-enrich-micro@${shard}.service"
full_collector="lexora-enrich@${shard}.service"
micro_watch="lexora-enrich-watch@${shard}.service"
micro_watch_timer="lexora-enrich-watch@${shard}.timer"
micro_recover="lexora-enrich-recover@${shard}.service"
full_watch="lexora-enrich-full-watch@${shard}.service"
full_watch_timer="lexora-enrich-full-watch@${shard}.timer"
full_recover="lexora-enrich-full-recover@${shard}.service"
run_root=${LEXORA_RUN_ROOT:-/run}
state_root=${LEXORA_STATE_ROOT:-/opt/lexora/state}
marker="$run_root/lexora-top20k-repair-${shard}.collector-state"

unit_state() {
  systemctl is-active "$1" 2>/dev/null || true
}

restore_unit_state() {
  local unit=$1 state=$2
  if [[ "$state" == active || "$state" == activating ]]; then
    systemctl start --no-block "$unit"
  else
    systemctl stop "$unit" || true
  fi
}

validate_unit_states() {
  local value
  for value in "$@"; do
    case "$value" in
      active|activating|inactive|failed|deactivating|unknown) ;;
      *) echo "invalid or unavailable collector unit state" >&2; return 1 ;;
    esac
  done
}

if [[ "$action" == capture ]]; then
  temporary="${marker}.tmp-$$"
  # A failed capture must never leave an older run's snapshot for
  # ExecStopPost to replay.  Until the new atomic marker exists, restore is a
  # strict no-op.
  rm -f -- "$marker" "$temporary"
  [[ "${LEXORA_CANDIDATE_DIGEST:-}" =~ ^[0-9a-f]{64}$ ]] || {
    echo "candidate digest is missing or invalid" >&2; exit 1;
  }
  micro_state=$(unit_state "$micro_collector")
  full_state=$(unit_state "$full_collector")
  micro_watch_state=$(unit_state "$micro_watch")
  micro_watch_timer_state=$(unit_state "$micro_watch_timer")
  micro_recover_state=$(unit_state "$micro_recover")
  full_watch_state=$(unit_state "$full_watch")
  full_watch_timer_state=$(unit_state "$full_watch_timer")
  full_recover_state=$(unit_state "$full_recover")
  validate_unit_states \
    "$micro_state" "$full_state" \
    "$micro_watch_state" "$micro_watch_timer_state" "$micro_recover_state" \
    "$full_watch_state" "$full_watch_timer_state" "$full_recover_state"
  install -d -m 0750 "$run_root"
  install -d -o opc -g opc -m 0750 \
    "$state_root/fast20k/$LEXORA_CANDIDATE_DIGEST"
  printf '%s\n' \
    "micro=$micro_state" \
    "full=$full_state" \
    "micro_watch=$micro_watch_state" \
    "micro_watch_timer=$micro_watch_timer_state" \
    "micro_recover=$micro_recover_state" \
    "full_watch=$full_watch_state" \
    "full_watch_timer=$full_watch_timer_state" \
    "full_recover=$full_recover_state" > "$temporary"
  mv -f -- "$temporary" "$marker"
  python3 - "$marker" "$run_root" <<'PY'
import os
import sys

file_descriptor = os.open(sys.argv[1], os.O_RDONLY)
try:
    os.fsync(file_descriptor)
finally:
    os.close(file_descriptor)
directory_descriptor = os.open(sys.argv[2], os.O_RDONLY)
try:
    os.fsync(directory_descriptor)
finally:
    os.close(directory_descriptor)
PY
  # Quiesce every known unit that can write or restart a writer.  Timers and
  # watchdogs are stopped first so they cannot race the collector stop.
  for unit in \
    "$micro_watch_timer" "$full_watch_timer" \
    "$micro_watch" "$full_watch" \
    "$micro_recover" "$full_recover" \
    "$micro_collector" "$full_collector"; do
    if systemctl cat "$unit" >/dev/null 2>&1; then
      systemctl stop "$unit"
    fi
  done
  if systemctl is-active --quiet "$micro_collector" \
    || systemctl is-active --quiet "$full_collector"; then
    echo "a canonical writer is still active; refusing repair" >&2
    exit 1
  fi
  exit
fi

if [[ ! -f "$marker" ]]; then
  echo "collector_state_restore=noop shard=$shard reason=missing-capture-marker"
  exit 0
fi
micro_state=
full_state=
micro_watch_state=
micro_watch_timer_state=
micro_recover_state=
full_watch_state=
full_watch_timer_state=
full_recover_state=
while IFS='=' read -r name value; do
  case "$name" in
    micro) micro_state=$value ;;
    full) full_state=$value ;;
    micro_watch) micro_watch_state=$value ;;
    micro_watch_timer) micro_watch_timer_state=$value ;;
    micro_recover) micro_recover_state=$value ;;
    full_watch) full_watch_state=$value ;;
    full_watch_timer) full_watch_timer_state=$value ;;
    full_recover) full_recover_state=$value ;;
    *) echo "invalid collector state key: $name" >&2; exit 1 ;;
  esac
done < "$marker"
validate_unit_states \
  "$micro_state" "$full_state" \
  "$micro_watch_state" "$micro_watch_timer_state" "$micro_recover_state" \
  "$full_watch_state" "$full_watch_timer_state" "$full_recover_state"
# Restore writers first and guardian timers last.  This reproduces the exact
# active/inactive snapshot without letting a watchdog race restoration.
restore_unit_state "$micro_collector" "$micro_state"
restore_unit_state "$full_collector" "$full_state"
restore_unit_state "$micro_recover" "$micro_recover_state"
restore_unit_state "$full_recover" "$full_recover_state"
restore_unit_state "$micro_watch" "$micro_watch_state"
restore_unit_state "$full_watch" "$full_watch_state"
restore_unit_state "$micro_watch_timer" "$micro_watch_timer_state"
restore_unit_state "$full_watch_timer" "$full_watch_timer_state"
rm -f -- "$marker"
