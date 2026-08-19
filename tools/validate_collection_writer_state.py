#!/usr/bin/env python3
"""Fail closed unless exactly one collection writer is running.

An all-inactive state is accepted only when a recent cached progress snapshot
proves that the shard has finished.  The validator is intentionally separate
from systemd so its deployment contract can be regression tested locally.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Any

RUNNING_STATES = {"active", "activating"}
KNOWN_STATES = RUNNING_STATES | {"inactive"}


def completion_proof(
    snapshot: Path | None,
    *,
    now: dt.datetime | None = None,
    max_age_seconds: int = 900,
) -> dict[str, Any] | None:
    if snapshot is None or not snapshot.is_file() or max_age_seconds < 1:
        return None
    try:
        value = json.loads(snapshot.read_text(encoding="utf-8"))
        finished = value["finished"]
        total = value["total"]
        remaining = value.get("remaining", total - finished)
        if any(type(item) is not int for item in (finished, total, remaining)):
            return None
        updated_at = dt.datetime.fromisoformat(
            str(value["updatedAt"]).replace("Z", "+00:00")
        )
        if updated_at.tzinfo is None:
            updated_at = updated_at.replace(tzinfo=dt.timezone.utc)
        checked_at = now or dt.datetime.now(dt.timezone.utc)
        age = (checked_at.astimezone(dt.timezone.utc) - updated_at).total_seconds()
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
        return None
    if (
        total <= 0
        or finished != total
        or remaining != 0
        or age < 0
        or age > max_age_seconds
    ):
        return None
    return {
        "finished": finished,
        "total": total,
        "updatedAt": value["updatedAt"],
        "ageSeconds": round(age, 3),
    }


def validate_writer_states(
    *,
    micro: str,
    full: str,
    repair: str,
    progress_snapshot: Path | None = None,
    now: dt.datetime | None = None,
    max_age_seconds: int = 900,
) -> dict[str, Any]:
    states = {"micro": micro, "full": full, "repair": repair}
    failed = [name for name, state in states.items() if state == "failed"]
    if failed:
        raise ValueError("failed collection writer: " + ",".join(failed))
    unknown = {
        name: state
        for name, state in states.items()
        if state not in KNOWN_STATES and state != "failed"
    }
    if unknown:
        raise ValueError(
            "unknown collection writer state: "
            + ",".join(f"{name}={state}" for name, state in unknown.items())
        )
    running = [name for name, state in states.items() if state in RUNNING_STATES]
    if len(running) > 1:
        raise ValueError(
            "multiple collection writers are running: " + ",".join(running)
        )
    if len(running) == 1:
        return {"writer": running[0], "states": states, "complete": False}
    proof = completion_proof(
        progress_snapshot,
        now=now,
        max_age_seconds=max_age_seconds,
    )
    if proof is None:
        raise ValueError("all collection writers are inactive without completion proof")
    return {"writer": None, "states": states, "complete": True, "proof": proof}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--micro", required=True)
    parser.add_argument("--full", required=True)
    parser.add_argument("--repair", required=True)
    parser.add_argument("--progress-snapshot", type=Path)
    parser.add_argument("--max-age-seconds", type=int, default=900)
    args = parser.parse_args()
    try:
        result = validate_writer_states(
            micro=args.micro,
            full=args.full,
            repair=args.repair,
            progress_snapshot=args.progress_snapshot,
            max_age_seconds=args.max_age_seconds,
        )
    except ValueError as error:
        parser.error(str(error))
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
