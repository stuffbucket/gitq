"""Correctness tests. Concurrency is real: each thread drives its own git store,
and every race is resolved inside git itself.

Run: python3 tests/test_gitq.py
"""
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from harness import (SKEW, check, claim_all, cleanup, collect, lab,  # noqa: E402,F401
                     main, parallel)
from gitq import ShardLeases, cron, reap  # noqa: E402

NSHARDS = 8


def test_idempotent_enqueue():
    d, qs = lab(2)
    try:
        a = qs[0].enqueue("greet", key="same-key", due=1)
        b = qs[1].enqueue("greet", key="same-key", due=1)
        refs = qs[0].git.remote_refs("refs/jobs/q/*/pending/*")
        check("duplicate enqueue is a no-op", a and not b and len(refs) == 1,
              "first={} second={} refs={}".format(a, b, len(refs)))
    finally:
        cleanup(d)


def test_concurrent_enqueue_same_key():
    d, qs = lab(10)
    try:
        wins = []
        lock = threading.Lock()

        def go(q):
            ok = q.enqueue("greet", key="racy", due=1)
            with lock:
                wins.append(ok)
        parallel([lambda q=q: go(q) for q in qs])
        refs = qs[0].git.remote_refs("refs/jobs/q/*/pending/*")
        check("10 concurrent enqueues of one key -> 1 job",
              sum(wins) == 1 and len(refs) == 1,
              "wins={} refs={}".format(sum(wins), len(refs)))
    finally:
        cleanup(d)


# --------------------------------------------------------------- exactly-once
def test_exactly_once_claim():
    d, qs = lab(10)
    try:
        qs[0].enqueue("work", key="solo", due=1)
        shard = qs[0].shards()
        claims, lock = [], threading.Lock()

        def go(i, q):
            got = []
            for s in shard:
                got += q.claim_batch(s, "w{}".format(i), limit=5, now=time.time())
            with lock:
                claims.extend(got)
        parallel([lambda i=i, q=q: go(i, q) for i, q in enumerate(qs)])
        check("10 workers, 1 job -> claimed exactly once", len(claims) == 1,
              "claims={}".format(len(claims)))
    finally:
        cleanup(d)


def test_no_loss_no_duplication():
    d, qs = lab(10)
    try:
        n = 40
        qs[0].enqueue_batch([{"task": "work", "key": "j{}".format(i), "due": 1}
                                 for i in range(n)])
        claims, lock = [], threading.Lock()

        def go(i, q):
            got = []
            try:
                for _ in range(6):
                    for s in q.shards():
                        got += q.claim_batch(s, "w{}".format(i), limit=5,
                                             now=time.time())
            finally:
                # Record the tally even if this thread dies. A job that was
                # claimed and then dropped from the count is indistinguishable
                # from a job the queue lost -- this test used to report the
                # second when it was looking at the first.
                with lock:
                    claims.extend(got)
        parallel([lambda i=i, q=q: go(i, q) for i, q in enumerate(qs)])
        keys = [c["job"]["key"] for c in claims]
        left = qs[0].git.remote_refs("refs/jobs/q/*/pending/*")
        check("40 jobs, 10 workers -> no duplicates",
              len(keys) == len(set(keys)), "claims={} unique={}".format(len(keys), len(set(keys))))
        check("40 jobs, 10 workers -> none lost",
              len(keys) + len(left) == n, "claimed={} pending={}".format(len(keys), len(left)))
    finally:
        cleanup(d)


# ----------------------------------------------------------- state transitions
def test_complete_and_retry_and_dead():
    d, qs = lab(1)
    q = qs[0]
    try:
        q.enqueue("work", key="done-me", due=1)
        c = [x for s in q.shards() for x in q.claim_batch(s, "w0", now=time.time())][0]
        ok = q.complete(c)
        done = q.git.remote_refs("refs/jobs/done/*/*")
        claimed = q.git.remote_refs("refs/jobs/q/*/claimed/*")
        check("complete moves claimed -> done", ok and len(done) == 1 and not claimed,
              "ok={} done={} claimed={}".format(ok, len(done), len(claimed)))

        q.enqueue("work", key="retry-me", due=1, max_attempts=2)
        outcomes = []
        for _ in range(3):
            got = [x for s in q.shards() for x in q.claim_batch(s, "w0", now=time.time())]
            if not got:
                break
            outcomes.append(q.fail(got[0], backoff_s=0)[1])
        dead = q.git.remote_refs("refs/jobs/dead/*")
        check("retry then dead-letter at max_attempts",
              outcomes == ["retry", "dead"] and len(dead) == 1,
              "outcomes={} dead={}".format(outcomes, len(dead)))
    finally:
        cleanup(d)


def test_reaper_reclaims_expired_lease():
    d, qs = lab(1)
    q = qs[0]
    try:
        q.enqueue("work", key="orphan", due=1)
        now = time.time()
        got = [x for s in q.shards() for x in q.claim_batch(s, "dead-worker",
                                                            lease_s=5, now=now)]
        check("job is claimed before reaping", len(got) == 1)

        # Staleness is an observed interval, so the first sweep only establishes
        # a baseline. That is what makes it independent of the claimer's clock.
        first, _ = reap(q, now=now)
        check("first sweep only records a sighting", first == 0,
              "reclaimed={} on first sight".format(first))

        reclaimed, _ = reap(q, now=now + 10)
        pending = q.git.remote_refs("refs/jobs/q/*/pending/*")
        claimed = q.git.remote_refs("refs/jobs/q/*/claimed/*")
        check("reaper returns expired claim to pending",
              reclaimed == 1 and len(pending) == 1 and not claimed,
              "reclaimed={} pending={} claimed={}".format(reclaimed, len(pending), len(claimed)))
    finally:
        cleanup(d)


def test_healthy_lease_survives_reaper():
    d, qs = lab(1)
    q = qs[0]
    try:
        q.enqueue("work", key="healthy", due=1)
        [x for s in q.shards() for x in q.claim_batch(s, "w0", lease_s=600,
                                                      now=time.time())]
        reclaimed, _ = reap(q, now=time.time())
        check("reaper leaves live leases alone", reclaimed == 0,
              "reclaimed={}".format(reclaimed))
    finally:
        cleanup(d)


# ------------------------------------------------------------------ scheduling
def test_cron_fires_once_across_workers():
    d, qs = lab(10)
    try:
        sched = [{"task": "beat", "cron": "* * * * *"}]
        fired, lock = [], threading.Lock()
        now = time.time()

        def go(q):
            got = cron.tick(q, sched, now=now, catchup_min=0)
            with lock:
                fired.extend(got)
        parallel([lambda q=q: go(q) for q in qs])
        jobs = qs[0].git.remote_refs("refs/jobs/q/*/pending/*")
        check("10 workers tick the same minute -> 1 job",
              len(fired) == 1 and len(jobs) == 1,
              "fired={} jobs={}".format(len(fired), len(jobs)))
    finally:
        cleanup(d)


# ---------------------------------------------------------------- shard leases
def test_shard_leases_are_disjoint_and_stealable():
    d, qs = lab(3)
    try:
        a = ShardLeases(qs[0], "a", lease_s=60)
        b = ShardLeases(qs[1], "b", lease_s=60)
        now = time.time()
        held_a = a.acquire(want=4, now=now)
        held_b = b.acquire(want=4, now=now)
        check("shard ownership is disjoint", not (set(held_a) & set(held_b)),
              "a={} b={}".format(held_a, held_b))

        # Was vacuous: held_a and held_b are disjoint, so their intersection with
        # anything is always empty and the assertion could never fail.
        c = ShardLeases(qs[2], "c", lease_s=60)
        taken = set(held_a) | set(held_b)
        blocked = c.acquire(want=NSHARDS, now=now)
        check("live leases block a would-be stealer",
              not (set(blocked) & taken),
              "c took {} which overlaps live leases {}".format(
                  sorted(set(blocked) & taken), sorted(taken)))

        c.held.clear()
        stolen = c.acquire(want=NSHARDS, now=now + 3600)   # every lease expired
        check("expired shard leases are stealable", taken <= set(stolen),
              "stolen={} did not cover expired {}".format(sorted(stolen), sorted(taken)))
    finally:
        cleanup(d)


if __name__ == "__main__":
    sys.exit(main(globals()))
