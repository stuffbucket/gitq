"""Ref layout, job encoding, and the claim/complete/fail state machine.

Ref names carry the index. Because reftable stores refs sorted by name and
returns them that way, a name of `<due>-<prio>-<key>` means an unsorted
enumeration already arrives in due order -- no sort, no scan of payloads.

This module owns every ref name in the system; other modules ask it rather
than composing paths of their own.
"""
from __future__ import annotations

import hashlib
import re
import time

from .git import GitError, RefTxn

Q = "refs/jobs/q"
DONE = "refs/jobs/done"
DEAD = "refs/jobs/dead"
OWNER = "refs/jobs/owner"
CRON = "refs/jobs/cron"
# Never pushed: a sighting log is only meaningful on the clock that wrote it,
# so it lives in the reaper's own store. See reaper.py.
SIGHT = "refs/local/reaper"
MIRROR = "refs/mirror"

# Single source of truth for the globs the CLI and tests use to inspect state.
PATTERNS = {
    "pending": Q + "/*/pending/*",
    "claimed": Q + "/*/claimed/*",
    "done": DONE + "/*/*",
    "dead": DEAD + "/*",
    "owner": OWNER + "/*",
    "cron": CRON + "/*/*",
}

_SAFE = re.compile(r"[^A-Za-z0-9._-]+")


def now_or(now=None):
    """Every entry point takes an injectable clock; this is that one idiom."""
    return int(time.time() if now is None else now)


def safe_key(key):
    """Make an arbitrary string usable inside a ref name."""
    k = _SAFE.sub("-", str(key)).strip("-.")
    k = re.sub(r"\.{2,}", ".", k)      # '..' is illegal in a ref name
    if not k or k.endswith(".lock"):
        raise ValueError("unusable job key: {!r}".format(key))
    return k[:120]


def shard_of(key, nshards):
    """Stable across processes -- Python's hash() is salted per run."""
    digest = hashlib.sha1(safe_key(key).encode()).digest()
    return "s{:02d}".format(int.from_bytes(digest[:4], "big") % nshards)


def job_name(due, prio, key):
    return "{:012d}-{:02d}-{}".format(int(due), int(prio), safe_key(key))


class Queue:
    def __init__(self, git, nshards=16):
        self.git = git
        self.nshards = nshards

    # -- ref names ----------------------------------------------------------
    def pending_ref(self, shard, name):
        return "{}/{}/pending/{}".format(Q, shard, name)

    def claimed_ref(self, shard, name):
        return "{}/{}/claimed/{}".format(Q, shard, name)

    def owner_ref(self, shard):
        return "{}/{}".format(OWNER, shard)

    def cron_ref(self, task, when):
        return "{}/{}/{:012d}".format(CRON, task, int(when))

    def sighting_ref(self, reaper_id):
        return "{}/{}".format(SIGHT, reaper_id)

    def shards(self):
        return ["s{:02d}".format(i) for i in range(self.nshards)]

    def refs(self, kind):
        return self.git.remote_refs(PATTERNS[kind])

    # -- enqueue ------------------------------------------------------------
    def prepare(self, task, args=None, key=None, due=None, prio=50, max_attempts=3):
        """Build a job's blob and target ref without pushing.

        Exposed so a caller can bundle a job into a larger atomic push -- the
        cron scheduler needs the period marker and the job itself to land as
        one transaction.
        """
        due = now_or(due)
        key = safe_key(key or "{}-{}".format(task, due))
        payload = {
            "key": key, "task": task, "args": args or {},
            "attempt": 0, "max_attempts": max_attempts, "due": due, "prio": prio,
        }
        oid = self.git.write_json(payload)
        ref = self.pending_ref(shard_of(key, self.nshards),
                               job_name(due, prio, key))
        return ref, oid

    def enqueue(self, task, args=None, key=None, due=None, prio=50, max_attempts=3):
        """Create a pending job. Returns False if this exact job already exists.

        That refusal is the idempotency guarantee: the ref name is the
        idempotency key, and ref creation is compare-and-swap against absence,
        so a duplicate enqueue is a no-op rather than a second job.
        """
        ref, oid = self.prepare(task, args, key, due, prio, max_attempts)
        _, res = self.git.push_atomic(RefTxn().create(ref, oid))
        return res.created(ref)

    def enqueue_batch(self, jobs):
        """One atomic push for many jobs. ~2.5x cheaper per job than one at a time."""
        txn = RefTxn()
        for j in jobs:
            ref, oid = self.prepare(j["task"], j.get("args"), j.get("key"),
                                    j.get("due"), j.get("prio", 50),
                                    j.get("max_attempts", 3))
            if ref in txn.refs():
                continue      # same job twice in one batch is one job, not an error
            txn.create(ref, oid)
        ok, res = self.git.push_atomic(txn)
        return sum(1 for r in txn.refs() if res.created(r)) if ok else 0

    # -- claim --------------------------------------------------------------
    def _mirror_pending(self, shards):
        """Refresh these shards' mirrors in one fetch, tolerating failure.

        A transient failure -- the hub busy, the machine out of process slots
        under load -- must not take a worker down. Falling back to the mirror
        already on disk is safe precisely because every claim is a
        compare-and-swap: a stale pending oid fails its lease, costing a
        rejected push and never a double run.
        """
        try:
            self.git.mirror_many(
                [("{}/{}/pending".format(Q, s), "{}/{}/pending".format(MIRROR, s))
                 for s in shards])
        except GitError:
            pass

    def _due_from_mirror(self, shard, now):
        """Due jobs from a shard's mirror as it stands, earliest first."""
        prefix = "{}/{}/pending/".format(MIRROR, shard)
        out = []
        for ref, oid in self.git.local_refs(prefix.rstrip("/")).items():
            name = ref[len(prefix):]
            try:
                due = int(name.split("-", 1)[0])
            except ValueError:
                continue
            if due <= now:
                out.append((name, oid))
        out.sort()  # cheap: shard-scoped, and names already sort into due order
        return out

    def poll(self, shard, now=None):
        """Mirror a shard's pending refs locally and return due jobs."""
        now = now_or(now)
        self._mirror_pending([shard])
        return self._due_from_mirror(shard, now)

    def poll_many(self, shards, now=None):
        """poll() for several shards, in ONE fetch. Returns {shard: [(name, oid)]}.

        A fetch costs the same whether it carries one refspec or sixteen, so a
        worker holding sixteen shards spent ~412ms per poll doing separately
        what costs ~28ms here. Every shard passed gets its mirror pruned, so
        pass all of them -- one left out is one that silently goes stale.
        """
        now = now_or(now)
        shards = list(shards)
        self._mirror_pending(shards)
        return {s: self._due_from_mirror(s, now) for s in shards}

    def _claim_txn(self, staged):
        """The pending->claimed move for one or more jobs, as one transaction.

        Releasing the pending ref and taking the claimed one are the same event;
        splitting them would leak a job or run it twice.
        """
        txn = RefTxn()
        for c in staged:
            txn.delete(c["pending"], c["pending_oid"])
            txn.create(c["ref"], c["oid"])
        return txn

    def claim_batch(self, shard, worker, limit=10, lease_s=300, now=None, due=None):
        """Atomically move up to `limit` pending jobs to claimed. Exactly-once.

        The whole batch is one --atomic push guarded by --force-with-lease on
        every pending ref, so a losing racer creates nothing at all.

        `due` takes an already-polled list, so a caller that mirrored several
        shards in one fetch does not turn around and re-fetch this one.
        """
        now = now_or(now)
        found = (self.poll(shard, now) if due is None else due)[:limit]
        if not found:
            return []
        # One process to read every payload and one to write every claim,
        # rather than two per job. A job whose blob this store somehow lacks is
        # dropped rather than guessed at -- read_blobs omits what it cannot find.
        payloads = self.git.read_json_many([oid for _, oid in found])
        found = [(n, o) for n, o in found if o in payloads]
        jobs = [dict(payloads[oid], worker=worker, claimed_at=now,
                     lease_until=now + lease_s) for _, oid in found]
        oids = self.git.write_json_many(jobs)
        staged = [{"shard": shard, "name": name,
                   "pending": self.pending_ref(shard, name), "pending_oid": pending,
                   "ref": self.claimed_ref(shard, name), "oid": oid, "job": job}
                  for (name, pending), job, oid in zip(found, jobs, oids)]
        if not staged:
            return []
        ok, _ = self.git.push_atomic(self._claim_txn(staged))
        if ok:
            return staged
        # Lost the batch. Retry one at a time so partial progress is still
        # possible when only some refs were taken by someone else.
        return [c for c in staged
                if self.git.push_atomic(self._claim_txn([c]))[0]]

    # -- terminal transitions ----------------------------------------------
    def _move(self, claim, oid, dst):
        """Retire a claim into `dst`, all-or-nothing.

        The claimed ref is released under the value we hold and the destination
        is created only if absent -- the shape every terminal transition needs.
        """
        ok, _ = self.git.push_atomic(
            RefTxn().delete(claim["ref"], claim["oid"]).create(dst, oid))
        return ok

    def complete(self, claim, now=None):
        now = now_or(now)
        day = time.strftime("%Y%m%d", time.gmtime(now))
        # Suffixed with the claim time so each run gets its own record, which
        # keeps this a plain create with no silent overwrite of an earlier one.
        return self._move(claim, claim["oid"], "{}/{}/{}-{}".format(
            DONE, day, claim["job"]["key"], claim["job"].get("claimed_at", now)))

    def fail(self, claim, backoff_s=60, now=None):
        """Retry with backoff, or dead-letter once attempts are exhausted."""
        now = now_or(now)
        job = dict(claim["job"])
        job["attempt"] = job.get("attempt", 0) + 1
        if job["attempt"] >= job.get("max_attempts", 3):
            oid = self.git.write_json(job)
            return self._move(claim, oid,
                              "{}/{}-{}".format(DEAD, job["key"], now)), "dead"
        job["due"] = now + backoff_s * job["attempt"]
        for k in ("worker", "claimed_at", "lease_until"):
            job.pop(k, None)
        oid = self.git.write_json(job)
        dst = self.pending_ref(claim["shard"],
                               job_name(job["due"], job["prio"], job["key"]))
        return self._move(claim, oid, dst), "retry"

    def renew(self, claim, lease_s=300, now=None):
        """Extend a lease mid-run so the reaper does not steal a healthy job."""
        now = now_or(now)
        job = dict(claim["job"], lease_until=now + lease_s)
        oid = self.git.write_json(job)
        ok, _ = self.git.push_atomic(
            RefTxn().update(claim["ref"], oid, claim["oid"]))
        if ok:
            claim["oid"], claim["job"] = oid, job
        return ok
