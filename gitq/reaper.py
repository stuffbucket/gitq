"""Reclaim jobs whose worker died mid-run, without comparing two clocks.

Git has no server clock, so a `lease_until` written by one machine and read by
another is a comparison across two independent clocks: a reaper running ahead
by more than the lease will steal a job that is still executing.

This avoids the comparison entirely. Two quantities are used, and both are
differences measured on a single clock:

  * how long the claimer intended to hold the lease -- `lease_until - claimed_at`,
    both stamped by the claimer, so their difference is skew-free
  * how long *this reaper* has watched the claim sit unchanged -- an interval on
    the reaper's own clock

A claim is reclaimed only when this reaper has seen the identical claim blob for
longer than the claimer's own declared lease duration. Because `renew()` writes
a fresh blob on every heartbeat, an unchanged oid is exactly the signal that the
worker has stopped heartbeating.

Sightings persist under `refs/local/reaper/<id>` in the reaper's own object
store, and are never pushed. A log of "when did I first see this" is only
meaningful on the clock that wrote it, so publishing it to the hub bought
nothing and cost a push and a fetch on every sweep. The `<id>` namespacing
remains, for two reapers sharing one store.
"""
from __future__ import annotations

from .git import GitError
from .queue import MIRROR, Q, now_or, safe_key

DEFAULT_STALE_S = 300


class Reaper:
    """Liveness by observation. `reaper_id` must be stable across restarts."""

    def __init__(self, queue, reaper_id="default"):
        self.q = queue
        self.git = queue.git
        self.id = safe_key(reaper_id)
        self.ref = queue.sighting_ref(self.id)
        self._seen = None      # carried across sweeps; only this reaper writes it
        self._prev_oid = None

    # -- sighting log -------------------------------------------------------
    def _load(self):
        """Read our own log. Timestamps in it are our own clock's."""
        if self._seen is not None:
            return                       # already in hand from the last sweep
        self._seen, self._prev_oid = {}, None
        oid = self.git.read_local_ref(self.ref)
        if not oid:
            return
        try:
            self._seen = self.git.read_json(oid).get("seen", {})
            self._prev_oid = oid
        except Exception:
            self._seen = {}              # unreadable log: start over, watch again

    def _save(self, seen, now):
        try:
            oid = self.git.write_json(
                {"reaper": self.id, "updated_at": now, "seen": seen})
            if self.git.write_local_ref(self.ref, oid, self._prev_oid):
                self._seen, self._prev_oid = seen, oid
                return
        except Exception:
            pass
        self._seen = None      # force a reload next sweep rather than trust memory

    # -- sweep --------------------------------------------------------------
    def _mirror_claims(self, shards):
        """One fetch for the whole sweep, not one per shard."""
        try:
            self.git.mirror_many(
                [("{}/{}/claimed".format(Q, s), "{}/{}/claimed".format(MIRROR, s))
                 for s in shards])
        except GitError:
            pass          # sweep what is already mirrored; the next sweep retries

    def _claims_in(self, shard, limit):
        """Yield (ref, oid, job) for every mirrored claim on one shard.

        Payloads are read in one process. At limit=100 that is the difference
        between ~944ms of forking and ~10ms of reading.
        """
        prefix = "{}/{}/claimed/".format(MIRROR, shard)
        items = [(ref[len(prefix):], oid) for ref, oid
                 in self.git.local_refs(prefix.rstrip("/")).items()][:limit]
        jobs = self.git.read_json_many([oid for _, oid in items])
        for name, oid in items:
            if oid in jobs:
                yield self.q.claimed_ref(shard, name), oid, jobs[oid]

    def sweep(self, now=None, limit=100):
        """One pass. Returns (reclaimed, dead_lettered).

        Reclaims nothing on the first sighting of a claim by design -- staleness
        is an observed interval, so it takes at least two sweeps spanning the
        lease duration to establish it.
        """
        now = now_or(now)
        self._load()
        prev, seen = self._seen, {}
        reclaimed = dead = 0

        shards = self.q.shards()
        self._mirror_claims(shards)
        for shard in shards:
            for ref, oid, job in self._claims_in(shard, limit):
                # The claimer's own intent, as a difference of its own stamps.
                span = int(job.get("lease_until", 0)) - int(job.get("claimed_at", 0))
                stale_after = span if span > 0 else DEFAULT_STALE_S

                before = prev.get(ref)
                # A changed oid means the worker heartbeated since we last looked.
                first_seen = before[1] if (before and before[0] == oid) else now

                ok = outcome = None
                if now - first_seen >= stale_after:
                    ok, outcome = self.q.fail(
                        {"shard": shard, "ref": ref, "oid": oid, "job": job},
                        backoff_s=0, now=now)
                if not ok:
                    seen[ref] = [oid, first_seen]   # keep watching
                elif outcome == "dead":
                    dead += 1
                else:
                    reclaimed += 1

        self._save(seen, now)
        return reclaimed, dead


def reap(queue, now=None, limit=100, reaper_id="default"):
    """Convenience wrapper. State round-trips through the sightings ref, so a
    fresh Reaper each call still accumulates observations correctly."""
    return Reaper(queue, reaper_id).sweep(now=now, limit=limit)
