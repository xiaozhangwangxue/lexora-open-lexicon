# Safe top-20k deployment

The repair candidate is deployed in two deliberately separate manual
workflows.  Neither workflow writes to or replaces the canonical database.

## 1. Prepare

Run **Prepare top-20k repair deployment** and type
`PREPARE_SAFE_TOP20K`.  The workflow:

1. archives the complete `tools/`, `service/` and deployment dependency set;
2. stages that archive in an immutable release directory on both OCI hosts and
   performs real Python imports from the staged directory;
3. streams only the immutable corpus identity from both live WAL databases;
4. requires the two immutable identity digests and schemas to match;
5. selects exactly 16,000 common words plus 4,000 reliable phrases, then
   refreshes shard 1's fixed-owner rows from its live database without copying
   either multi-gigabyte canonical database;
6. checks the candidate file SHA-256, `PRAGMA quick_check`, v3 candidate digest,
   fixed two-shard contract and provenance count before sealing each release;
7. runs the complete fixed-owner shard preflight against each live host, so a
   mutable baseline mismatch is rejected before activation.

The preparation workflow never calls `systemctl` and never changes a `current`
link.  Its summary prints the release ID, candidate SHA-256 and canonical
identity SHA-256 required by activation.

## 2. Activate

Run **Activate prepared top-20k repair**, paste all three exact values and type
`ACTIVATE_SAFE_TOP20K`.  Before changing either server, the coordinator:

- recomputes the live canonical identity on both hosts;
- re-hashes and opens both sealed candidates;
- performs the staged imports again; and
- requires both candidates to have the same requested SHA-256.

Activation swaps one `current` symlink.  Code, candidate and service drop-ins
therefore change together.  If either host fails, the coordinator calls the
saved rollback transaction on every host already activated.  Unit files,
timer state, the prior release link and the exact pre-deployment micro/full
collector states are restored.

Repair state is stored under
`/opt/lexora/state/fast20k/<candidate_digest>/`.  A new candidate cannot reuse a
non-empty legacy state database, and old state is retained rather than deleted.
The service captures and stops both possible canonical writers plus their
watchdog/recovery services and timers before repair, then restores exactly the
units that were active before the repair.

After activation, deploy the progress release.  Its quality snapshot is bound
to the active repair candidate digest and fixed modulo shard; a missing, stale
or malformed snapshot is regenerated synchronously under the unit's 12-minute
timeout before the deployment can succeed.

## Canonical identity CLI

Create an auditable identity receipt while SQLite is live:

```sh
python3 tools/sqlite_snapshot_manifest.py identity \
  --database canonical.sqlite > canonical.json
```

Compare canonical identity across replicas whose mutable enrichment fields may
legitimately differ:

```sh
python3 tools/sqlite_snapshot_manifest.py compare \
  --manifests host-0.json host-1.json
```

The manifest contains `quick_check`, schema digest, row bounds and a streamed
digest of immutable entry identity fields. Mutable source/scope/enrichment
fields are intentionally excluded from cross-replica identity and are instead
validated from their fixed owner before sealing and again before activation.
