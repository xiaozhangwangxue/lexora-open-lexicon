#!/bin/zsh
set -euo pipefail

SCRIPT_DIR=${0:A:h}
PROJECT_ROOT=${SCRIPT_DIR:h}

cd "$PROJECT_ROOT"
npx --yes wrangler@4.124.0 deploy --config wrangler.dictionary.jsonc

payload=$(curl --fail --silent --show-error --max-time 20 \
  'https://dict.12323456.xyz/v1/progress')
PAYLOAD="$payload" python3 - <<'PY'
import json
import os

value = json.loads(os.environ["PAYLOAD"])
assert value["finished"] <= value["total"]
assert len(value["shards"]) == 2
assert {item["shard"] for item in value["shards"]} == {0, 1}
top = value["top20k"]
assert top["available"] is True
assert top["total"] == 20_000
assert top["complete"] + top["incomplete"] == top["total"]
print(json.dumps({
    "full": f'{value["finished"]}/{value["total"]}',
    "top20k": f'{top["complete"]}/{top["total"]}',
    "ready": top["ready"],
}, ensure_ascii=False, sort_keys=True))
PY
