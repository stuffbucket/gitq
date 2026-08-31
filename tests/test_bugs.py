"""Regression tests for two correctness gaps found in audit.

Both are silent in normal operation and only bite on a crash or a slow job,
which is exactly why they need tests rather than inspection.
"""
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from harness import (SKEW, check, claim_all, cleanup, collect, lab,  # noqa: E402,F401
                     main, parallel)
from gitq import cron, reap  # noqa: E402
from gitq.reaper import Reaper  # noqa: E402
from gitq.worker import Worker  # noqa: E402


def test_cron_period_survives_a_crash_between_marker_and_job():
    """The marker and the job must move in a single push.

    The previous version of this test patched `q.enqueue`, which cron stopped
    calling once the fix landed -- so it passed while asserting nothing. It now
    checks the mechanism directly: exactly one push, carrying both refs.
    """
    d, (q,) = lab()
    try:
        sched = [{"task": "beat", "cron": "* * * * *"}]
        now = time.time()
        calls = []
        real = q.git.push_atomic

        def spy(txn):
            calls.append(txn.refs())
            return real(txn)
        q.git.push_atomic = spy
        cron.tick(q, sched, now=now, catchup_min=0)
        q.git.push_atomic = real

        pushes = [c for c in calls if any("refs/jobs/cron/" in r for r in c)]
        both = [c for c in pushes
                if any("refs/jobs/cron/" in r for r in c)
                and any("/pending/" in r for r in c)]
        check("marker and job travel in one push",
              len(pushes) == 1 and len(both) == 1,
              "pushes touching the marker={} carrying both refs={}"
              .format(len(pushes), len(both)))

        jobs = q.git.remote_refs("refs/jobs/q/*/pending/*")
        marks = q.git.remote_refs("refs/jobs/cron/*/*")
        check("a fired period leaves exactly one marker and one job",
              len(marks) == 1 and len(jobs) == 1,
              "markers={} jobs={}".format(len(marks), len(jobs)))
    finally:
        cleanup(d)


def test_cron_does_not_refire_the_same_minute():
    """A repeat tick pushes an identical marker blob, which git reports as
    'up to date' with exit 0. Only the porcelain flag distinguishes that from
    a real creation."""
    d, (q,) = lab()
    try:
        sched = [{"task": "beat", "cron": "* * * * *"}]
        now = time.time()
        first = cron.tick(q, sched, now=now, catchup_min=0)
        second = cron.tick(q, sched, now=now, catchup_min=0)
        jobs = q.git.remote_refs("refs/jobs/q/*/pending/*")
        check("re-ticking the same minute fires nothing",
              len(first) == 1 and len(second) == 0 and len(jobs) == 1,
              "first={} second={} jobs={}".format(len(first), len(second), len(jobs)))
    finally:
        cleanup(d)


def test_cron_fired_count_is_accurate():
    d, qs = lab(6)
    try:
        sched = [{"task": "beat", "cron": "* * * * *"}]
        now = time.time()
        fired, lock = [], threading.Lock()

        def go(q):
            got = cron.tick(q, sched, now=now, catchup_min=0)
            with lock:
                fired.extend(got)
        ts = [threading.Thread(target=go, args=(q,)) for q in qs]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        check("only the winning worker reports firing", len(fired) == 1,
              "fired={} (losers must not report a fire they did not cause)"
              .format(len(fired)))
    finally:
        cleanup(d)


# ---- BUG B: a job running longer than its lease gets reaped and re-run ------
def test_long_job_keeps_its_lease():
    d, (q,) = lab()
    try:
        from gitq.worker import Worker
        q.enqueue("slow", key="slow-one", due=1)
        started = threading.Event()

        def slow(args):
            started.set()
            time.sleep(4)

        w = Worker(q, "w0", {"slow": slow}, lease_s=2, reap_every_s=1e9)
        t = threading.Thread(target=w.run_once, daemon=True)
        t.start()
        started.wait(10)

        # Two sweeps spanning the lease: the first only records a sighting, so
        # a single call could never have reclaimed anything and the earlier
        # one-sweep version of this test proved nothing.
        from gitq.reaper import Reaper
        r = Reaper(q, "watcher")
        r.sweep(now=time.time())
        time.sleep(3)                      # now well past the 2s lease
        reclaimed, _ = r.sweep(now=time.time())
        check("reaper does not steal a job that is still running",
              reclaimed == 0,
              "reclaimed={} -> the job will run twice".format(reclaimed))
        t.join(15)
    finally:
        cleanup(d)




# ---- Clock skew: the reaper must never compare two machines' clocks ---------
SKEW = 300  # B's clock runs 5 minutes ahead of A's


def test_reaper_is_immune_to_clock_skew():
    """B is 5 min ahead. A's lease looks long-expired to B, but B has only
    been watching for 30 seconds, so B must not touch it."""
    d, (A, B) = lab(2)
    try:
        from gitq.reaper import Reaper
        A.enqueue("work", key="live", due=1)
        now = time.time()
        got = [c for s in A.shards() for c in A.claim_batch(s, "A", lease_s=300, now=now)]
        r = Reaper(B, "b")
        first, _ = r.sweep(now=now + SKEW)
        second, _ = r.sweep(now=now + SKEW + 30)
        check("skewed reaper leaves a healthy claim alone",
              len(got) == 1 and first == 0 and second == 0,
              "claimed={} sweeps=({}, {})".format(len(got), first, second))
    finally:
        cleanup(d)


def test_reaper_still_reclaims_a_truly_dead_worker():
    """Same skew, but now B has genuinely watched the claim sit unchanged for
    longer than the lease the claimer declared."""
    d, (A, B) = lab(2)
    try:
        from gitq.reaper import Reaper
        A.enqueue("work", key="orphan", due=1)
        now = time.time()
        [c for s in A.shards() for c in A.claim_batch(s, "A", lease_s=60, now=now)]
        r = Reaper(B, "b")
        r.sweep(now=now + SKEW)                 # baseline, skewed
        reclaimed, _ = r.sweep(now=now + SKEW + 90)   # 90s of B's own time
        check("a dead worker's job is still reclaimed", reclaimed == 1,
              "reclaimed={}".format(reclaimed))
    finally:
        cleanup(d)


def test_heartbeat_resets_staleness():
    """A renewal rewrites the claim blob; the new oid proves liveness and
    restarts the observation window."""
    d, (A, B) = lab(2)
    try:
        from gitq.reaper import Reaper
        A.enqueue("work", key="beating", due=1)
        now = time.time()
        claim = [c for s in A.shards() for c in A.claim_batch(s, "A", lease_s=60, now=now)][0]
        r = Reaper(B, "b")
        r.sweep(now=now + SKEW)
        A.renew(claim, lease_s=60, now=now + 30)      # heartbeat -> new oid
        reclaimed, _ = r.sweep(now=now + SKEW + 90)
        check("heartbeat restarts the observation window", reclaimed == 0,
              "reclaimed={}".format(reclaimed))
    finally:
        cleanup(d)


if __name__ == "__main__":
    sys.exit(main(globals()))
