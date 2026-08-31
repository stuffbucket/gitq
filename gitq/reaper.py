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

Sightings persist under `refs/jobs/reaper/<id>`, namespaced per reaper so a
reaper only ever compares its own earlier readings against its own clock.
"""
from __future__ import annotations

from .queue import MIRROR, Q, SIGHT, now_or, safe_key

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
        try:
            found = self.git.mirror_names(SIGHT, MIRROR + "/reaper", one=self.id)
            oid = found.get(self.id)
            if oid:
                self._seen = self.git.read_json(oid).get("seen", {})
                self._prev_oid = oid
        except Exception:
            pass

    def _save(self, seen, now):
        try:
            oid = self.git.write_json(
                {"reaper": self.id, "updated_at": now, "seen": seen})
            ok, _ = self.git.push_atomic(["{}:{}".format(oid, self.ref)],
                                         {self.ref: self._prev_oid or ""})
            if ok:
                self._seen, self._prev_oid = seen, oid
                return
        except Exception:
            pass
        self._seen = None      # force a reload next sweep rather than trust memory

    # -- sweep --------------------------------------------------------------
    def _claims_in(self, shard, limit):
        """Yield (ref, oid, job) for every claim on one shard."""
        dst = "{}/{}/claimed".format(MIRROR, shard)
        try:
            found = self.git.mirror_names("{}/{}/claimed".format(Q, shard), dst)
        except Exception:
            return
        for name, oid in list(found.items())[:limit]:
            try:
                yield self.q.claimed_ref(shard, name), oid, self.git.read_json(oid)
            except Exception:
                continue

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

        for shard in self.q.shards():
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
