#!/usr/bin/env bash
set -euo pipefail

HOST=${1:?usage: $0 <oracle-public-ip>}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
KEY=${LEXORA_SSH_KEY:-"$ROOT/ssh/lexora_oci"}
REMOTE="opc@$HOST"

ssh -i "$KEY" -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new "$REMOTE" \
  'sudo mkdir -p /opt/lexora-lexicon/{build,service} && sudo chown -R opc:opc /opt/lexora-lexicon'
rsync -av --partial --progress -e "ssh -i $KEY -o IdentitiesOnly=yes" \
  "$ROOT/service/" "$REMOTE:/opt/lexora-lexicon/service/"
rsync -av --partial --progress -e "ssh -i $KEY -o IdentitiesOnly=yes" \
  "$ROOT/build/lexora-english-600k.sqlite" \
  "$ROOT/build/lexora-frequency-20k.sqlite" \
  "$ROOT/build/manifest.json" "$REMOTE:/opt/lexora-lexicon/build/"
ssh -i "$KEY" -o IdentitiesOnly=yes "$REMOTE" \
  'sudo chown -R lexora:lexora /opt/lexora-lexicon && sudo systemctl daemon-reload && sudo systemctl enable --now lexora-lexicon'
