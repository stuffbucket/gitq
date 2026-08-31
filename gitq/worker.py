"""The poll loop: shard leases in, jobs out."""
from __future__ import annotations

import threading
import time
import traceback

from .cron import tick
from .queue import now_or
from .reaper import Reaper
from .shards import ShardLeases


class Worker:
    def __init__(self, queue, name, handlers, schedules=None, shards_wanted=None,
                 batch=10, lease_s=300, shard_lease_s=60, poll_s=2.0,
                 reap_every_s=30.0):
        self.q = queue
        self.name = name
        self.handlers = handlers
        self.schedules = schedules or []
        self.shard_lease_s = shard_lease_s
        self.leases = ShardLeases(self.q, name, lease_s=shard_lease_s)
        self.reaper = Reaper(self.q, name)
        # None means "as many as exist"; keeping it concrete lets the loop below
        # short-circuit instead of re-scanning every shard on every poll.
        self.shards_wanted = shards_wanted or self.q.nshards
        self.batch = batch
        self.lease_s = lease_s
        self.poll_s = poll_s
        self.reap_every_s = reap_every_s
        self._last_reap = 0.0
        self._last_renew = 0.0
        self._cron_attempted = set()
        self.stats = {"claimed": 0, "done": 0, "failed": 0, "reclaimed": 0,
                      "fired": 0, "lost": 0}

    def _heartbeat(self, now=None, claim=None):
        """Refresh every lease this worker's liveness rests on.

        A worker holds two kinds: the shards it owns, and the job it is running.
        Both are renewed from here so a long handler cannot starve one of them.
        That was a real hole -- `_execute` blocks the poll loop, so a job
        outliving shard_lease_s used to hand this worker's shards to whoever
        noticed the expiry, and the worker discovered it only on return.

        Returns whether the job claim is still ours (True when there is none).
        """
        now = now_or(now)
        # A 60s lease renewed every 2s is 30x more often than it needs to be,
        # at one push per shard each time.
        if now - self._last_renew >= self.shard_lease_s / 3.0:
            self.leases.renew(now=now)
            self._last_renew = now
        if claim is None:
            return True
        return self.q.renew(claim, lease_s=self.lease_s, now=now)

    def run_once(self, now=None):
        now = now_or(now)
        self._heartbeat(now)
        held = sorted(self.leases.held)
        if len(held) < self.shards_wanted:
            held = self.leases.acquire(want=self.shards_wanted, now=now)

        if self.schedules:
            self.stats["fired"] += len(tick(self.q, self.schedules, now=now,
                                            attempted=self._cron_attempted))

        if now - self._last_reap > self.reap_every_s:
            reclaimed, _ = self.reaper.sweep(now=now)
            self.stats["reclaimed"] += reclaimed
            self._last_reap = now

        worked = 0
        for shard in held:
            for claim in self.q.claim_batch(shard, self.name, limit=self.batch,
                                            lease_s=self.lease_s, now=now):
                self.stats["claimed"] += 1
                worked += 1
                self._execute(claim)
        return worked

    def _execute(self, claim):
        """Run a handler while holding its lease open.

        The handler runs on its own thread so this one can renew the lease
        underneath it. Without that, any job outliving lease_s gets reclaimed
        by the reaper and run a second time -- which would break the
        exactly-once guarantee for exactly the long jobs that most need it.

        A daemon thread, deliberately: when the lease is lost the handler is
        abandoned where it stands. Anything that joins on it -- a thread pool,
        say -- would block here waiting for the very job we no longer own.
        """
        handler = self.handlers.get(claim["job"]["task"])
        if handler is None:
            self.q.fail(claim)
            self.stats["failed"] += 1
            return

        err = []

        def body():
            try:
                handler(claim["job"].get("args", {}))
            except BaseException as exc:                # noqa: BLE001
                err.append(exc)

        t = threading.Thread(target=body, daemon=True)
        t.start()
        # Fast enough for the shorter of the two leases: heartbeating the job
        # on its own schedule would let the shard lease lapse underneath it.
        interval = max(1.0, min(self.lease_s, self.shard_lease_s) / 3.0)
        while True:
            t.join(interval)
            if not t.is_alive():
                break
            if not self._heartbeat(claim=claim):
                self.stats["lost"] += 1            # someone else owns it now
                return

        if err:
            traceback.print_exception(type(err[0]), err[0], err[0].__traceback__)
            self.q.fail(claim)
            self.stats["failed"] += 1
        else:
            self.q.complete(claim)
            self.stats["done"] += 1

    def run(self, until=None):
        try:
            while until is None or time.time() < until:
                if self.run_once() == 0:
                    time.sleep(self.poll_s)
        except KeyboardInterrupt:
            pass
        finally:
            self.leases.release()
        return self.stats
