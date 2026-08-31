"""Shard ownership leases.

Sharding is what makes this scale: when every worker races for the same
lexicographically-first job, ~90% of claim round-trips are wasted. When each
worker owns a disjoint set of shards, none are.

Ownership itself is a lease ref, so there is still no coordinator: a dead
worker's shards are simply CAS-stolen by whoever notices the expiry first.
"""
from __future__ import annotations

from .queue import MIRROR, OWNER, now_or

_MIRROR = MIRROR + "/owner"


class ShardLeases:
    def __init__(self, queue, worker, lease_s=60):
        self.q = queue
        self.git = queue.git
        self.worker = worker
        self.lease_s = lease_s
        self.held = {}      # shard -> oid of the lease blob we wrote
        self._leases = {}   # oid -> parsed blob; a lease only changes with its oid

    def _blob(self, shard, now):
        return self.git.write_json(
            {"shard": shard, "owner": self.worker, "lease_until": now + self.lease_s})

    def _current(self):
        """Mirror the owner refs locally so their blobs are readable here.

        ls-remote alone gives oids this store may not hold; reading them would
        fail and -- if that failure were treated as "expired" -- every worker
        would steal every shard.
        """
        return self.git.mirror_names(OWNER, _MIRROR)

    def _lease(self, oid):
        """Parse a lease blob, memoised: an unchanged oid is an unchanged lease."""
        if oid not in self._leases:
            self._leases[oid] = self.git.read_json(oid)
        return self._leases[oid]

    def acquire(self, want=None, now=None):
        """Take unowned or expired shards, up to `want`. Returns held shards."""
        now = now_or(now)
        current = self._current()
        for shard in self.q.shards():
            if want is not None and len(self.held) >= want:
                break
            if shard in self.held:
                continue
            existing = current.get(shard)
            if existing is None:
                expected = ""                      # must-not-exist
            else:
                try:
                    lease = self._lease(existing)
                except Exception:
                    continue                       # unreadable: assume held, never steal
                if lease.get("owner") != self.worker and lease.get("lease_until", 0) > now:
                    continue
                expected = existing                # steal: CAS from the observed value
            ref = self.q.owner_ref(shard)
            # No "+": the lease both authorises the non-fast-forward swap and
            # makes it a compare-and-swap. A "+" would override the lease.
            oid = self._blob(shard, now)
            _, res = self.git.push_atomic(["{}:{}".format(oid, ref)], {ref: expected})
            if res.wrote(ref):
                self.held[shard] = oid
        return sorted(self.held)

    def renew(self, now=None):
        """Refresh held leases; drop any that were stolen out from under us."""
        now = now_or(now)
        for shard in list(self.held):
            ref = self.q.owner_ref(shard)
            oid = self._blob(shard, now)
            ok, _ = self.git.push_atomic(["{}:{}".format(oid, ref)],
                                         {ref: self.held[shard]})
            if ok:
                self.held[shard] = oid
            else:
                del self.held[shard]
        return sorted(self.held)

    def release(self):
        for shard, oid in list(self.held.items()):
            ref = self.q.owner_ref(shard)
            self.git.push_atomic([":" + ref], {ref: oid})
            del self.held[shard]
