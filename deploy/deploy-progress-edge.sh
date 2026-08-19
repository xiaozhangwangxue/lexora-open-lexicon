#!/bin/zsh
set -euo pipefail

SCRIPT_DIR=${0:A:h}
PROJECT_ROOT=${SCRIPT_DIR:h}

cd "$PROJECT_ROOT"
npx --yes wrangler@4.124.0 deploy --config wrangler.dictionary.jsonc

verified=0
for attempt in {1..18}; do
  # A previous Worker response may remain at the custom domain for up to its
  # 60-second cache lifetime after a successful deploy.  Retry the public
  # route instead of reporting a false deployment failure.
  payload=$(curl --fail --silent --show-error --max-time 20 \
    -H 'Cache-Control: no-cache' \
    "https://dict.12323456.xyz/v1/progress?deploy-smoke=$attempt-$(date +%s)")
  if PAYLOAD="$payload" python3 - <<'PY'
import json
import os

value = json.loads(os.environ["PAYLOAD"])
assert value["finished"] <= value["total"]
assert len(value["shards"]) == 2
assert {item["shard"] for item in value["shards"]} == {0, 1}
assert value["remaining"] == value["total"] - value["finished"]
top = value["top20k"]
assert isinstance(top["available"], bool)
assert 0 <= top["complete"] <= top["total"] <= 20_000
assert top["complete"] + top["incomplete"] == top["total"]
if top["available"]:
    assert top["total"] == 20_000
    assert {item["shard"] for item in top["shards"]} == {0, 1}
print(json.dumps({
    "full": f'{value["finished"]}/{value["total"]}',
    "top20k": f'{top["complete"]}/{top["total"]}',
    "top20kAvailable": top["available"],
    "ready": top["ready"],
}, ensure_ascii=False, sort_keys=True))
PY
  then
    verified=1
    break
  fi
  sleep 5
done
test "$verified" = 1
