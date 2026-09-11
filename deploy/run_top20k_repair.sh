#!/usr/bin/env bash
# Redirect after the service process has entered its permitted sandbox. This
# avoids systemd's pre-sandbox file-open failure while retaining a compact,
# per-shard error trail for strict repair diagnostics.
set -euo pipefail

shard=${1:?missing shard}
shift
log_file="/opt/lexora/state/top20k-repair-shard-${shard}.log"

exec "$@" >> "$log_file" 2>&1
