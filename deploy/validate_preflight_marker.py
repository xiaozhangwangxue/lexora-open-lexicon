#!/usr/bin/env python3
"""Validate atomic preflight and runtime readiness markers for repair."""
from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Any


FORMATS = {
    "preflight": ("lexora-top20k-preflight-v1", "completedAt"),
    "runtime": ("lexora-top20k-runtime-ready-v1", "readyAt"),
}


def validate_marker(
    path: Path,
    *,
    release_id: str,
    candidate_digest: str,
    shard_index: int,
    shard_count: int,
    kind: str = "preflight",
) -> dict[str, Any]:
    try:
        expected_format, timestamp_key = FORMATS[kind]
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise ValueError("marker must be a JSON object")
        if value.get("format") != expected_format:
            raise ValueError("marker format mismatch")
        if value.get("releaseId") != release_id:
            raise ValueError("marker release mismatch")
        if value.get("candidateDigest") != candidate_digest:
            raise ValueError("marker candidate mismatch")
        if int(value.get("shardIndex", -1)) != shard_index:
            raise ValueError("marker shard mismatch")
        if int(value.get("shardCount", -1)) != shard_count:
            raise ValueError("marker shard count mismatch")
        if int(value.get("selectedOwnerRows", 0)) < 1:
            raise ValueError("marker selected owner coverage is empty")
        if int(value.get("preflightRows", -1)) < 0:
            raise ValueError("marker queue count is invalid")
        if int(value.get("processId", 0)) < 1:
            raise ValueError("marker process id is invalid")
        completed = dt.datetime.fromisoformat(
            str(value.get(timestamp_key, "")).replace("Z", "+00:00")
        )
        if completed.tzinfo is None:
            raise ValueError("marker completion time has no timezone")
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise RuntimeError(f"repair {kind} marker is invalid: {error}") from error
    return value


def validate_service_state(
    runtime_marker: dict[str, Any],
    *,
    active_state: str,
    sub_state: str,
    main_pid: int,
    exec_started_monotonic: int,
    result: str,
    exec_code: str,
    exec_status: int,
) -> None:
    """Require the runtime marker to belong to a live or cleanly finished run."""
    if exec_started_monotonic < 1:
        raise RuntimeError("repair main process never started")
    if active_state == "active" or (
        active_state == "activating" and sub_state == "start"
    ):
        if main_pid < 1:
            raise RuntimeError("repair main process has no live pid")
        if int(runtime_marker.get("processId", 0)) != main_pid:
            raise RuntimeError("runtime marker process id does not match MainPID")
        return
    if (
        active_state == "inactive"
        and result == "success"
        and exec_code == "exited"
        and exec_status == 0
    ):
        return
    raise RuntimeError(
        "repair service is neither running nor cleanly completed: "
        f"active={active_state} sub={sub_state} result={result} "
        f"code={exec_code} status={exec_status}"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--marker", type=Path, required=True)
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--candidate-digest", required=True)
    parser.add_argument("--shard-index", type=int, required=True)
    parser.add_argument("--shard-count", type=int, required=True)
    parser.add_argument("--kind", choices=tuple(FORMATS), default="preflight")
    parser.add_argument("--active-state")
    parser.add_argument("--sub-state", default="")
    parser.add_argument("--main-pid", type=int, default=0)
    parser.add_argument("--exec-started-monotonic", type=int, default=0)
    parser.add_argument("--result", default="")
    parser.add_argument("--exec-code", default="")
    parser.add_argument("--exec-status", type=int, default=-1)
    args = parser.parse_args()
    value = validate_marker(
        args.marker,
        release_id=args.release_id,
        candidate_digest=args.candidate_digest,
        shard_index=args.shard_index,
        shard_count=args.shard_count,
        kind=args.kind,
    )
    if args.active_state is not None:
        if args.kind != "runtime":
            parser.error("service state validation requires --kind runtime")
        validate_service_state(
            value,
            active_state=args.active_state,
            sub_state=args.sub_state,
            main_pid=args.main_pid,
            exec_started_monotonic=args.exec_started_monotonic,
            result=args.result,
            exec_code=args.exec_code,
            exec_status=args.exec_status,
        )
    print(json.dumps(value, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
