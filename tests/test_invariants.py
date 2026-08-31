"""Properties the suite was found not to be checking.

Every test here exists because a mutation removed the guarantee it names and
nothing failed. See tools/mutate.py.
"""
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from harness import (SKEW, check, claim_all, cleanup, collect, lab,  # noqa: E402,F401
                     main, parallel)
from gitq import ShardLeases  # noqa: E402
from gitq.git import GitError  # noqa: E402
from gitq.queue import shard_of  # noqa: E402


def test_concurrent_shard_steal_is_exclusive():
    """Stealing an expired lease is a forced push. Only the lease makes it a
    compare-and-swap; without it every racer overwrites and all believe they
    own the shard. The existing disjointness test acquires sequentially, so it
    cannot see this."""
    d, qs = lab(10, nshards=1)
    try:
        old = ShardLeases(qs[0], "old", lease_s=1)
        old.acquire(want=1, now=time.time())          # one shard, now expiring

        winners, lock = [], threading.Lock()
        later = time.time() + 100

        def steal(i, q):
            sl = ShardLeases(q, "thief{}".format(i), lease_s=60)
            got = sl.acquire(want=1, now=later)
            if got:
                with lock:
                    winners.append(i)
        ts = [threading.Thread(target=steal, args=(i, q)) for i, q in enumerate(qs)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        check("only one worker can steal an expired shard lease",
              len(winners) == 1,
              "{} workers all believe they own the shard".format(len(winners)))
    finally:
        cleanup(d)


def test_jobs_are_claimed_in_due_order():
    """The ref name IS the index: zero-padding is what makes lexicographic ref
    order equal due order. Unpadded, '100' sorts before '9'."""
    d, (q,) = lab(1, nshards=1)
    try:
        for due in (100, 9, 20):
            q.enqueue("work", key="j{}".format(due), due=due)
        claims = q.claim_batch("s00", "w0", limit=10, now=1000)
        order = [c["job"]["due"] for c in claims]
        check("jobs come off the queue in due order", order == sorted(order),
              "got {} (expected ascending)".format(order))
    finally:
        cleanup(d)


def test_ref_unsafe_keys_are_sanitized():
    """Keys reach git as ref names. Spaces, ~, ^, :, ? and .. are all illegal
    there, so an unsanitized key silently fails to enqueue."""
    d, (q,) = lab()
    try:
        nasty = "report 2026/Q1~draft^2:final?..x"
        ok = q.enqueue("work", key=nasty, due=1)
        refs = list(q.git.remote_refs("refs/jobs/q/*/pending/*"))
        bad = [r for r in refs if any(ch in r for ch in " ~^:?[\\") or ".." in r]
        check("a ref-hostile key still enqueues", ok, "enqueue returned {}".format(ok))
        check("and produces a legal ref name", refs and not bad,
              "refs={} illegal={}".format(refs, bad))
    finally:
        cleanup(d)


def test_push_refuses_to_leave_the_jobs_namespace():
    """Structural guarantee that this library cannot write a branch, in the
    hub or anywhere else, even if pointed at a source repo."""
    d, (q,) = lab()
    try:
        oid = q.git.write_blob("x")
        for spec in ("{}:refs/heads/main".format(oid),
                     "+{}:refs/heads/main".format(oid),
                     ":refs/heads/main",
                     "{}:refs/tags/v1".format(oid)):
            try:
                q.git.push_atomic([spec])
                check("refuses {}".format(spec[-16:]), False, "push was allowed")
            except GitError as e:
                # Either guard is a correct refusal: force refspecs are rejected
                # before the namespace is even considered.
                check("refuses {}".format(spec[:1] + spec[-16:]),
                      "refusing to push outside" in str(e)
                      or "refusing a force refspec" in str(e), str(e))
        # A force refspec must be refused even when its target is legitimate --
        # the namespace guard would wave this one through.
        try:
            q.git.push_atomic(["+{}:refs/jobs/q/s00/pending/x".format(oid)])
            check("refuses a force refspec inside the namespace", False,
                  "force push was allowed")
        except GitError as e:
            check("refuses a force refspec inside the namespace",
                  "refusing a force refspec" in str(e), str(e))

        try:
            q.git.push_atomic(["{}:refs/jobs/q/s00/pending/x".format(oid)],
                              {"refs/heads/main": ""})
            check("refuses a lease outside the namespace", False, "lease was allowed")
        except GitError as e:
            check("refuses a lease outside the namespace",
                  "refusing to lease outside" in str(e), str(e))
    finally:
        cleanup(d)


def test_completion_records_do_not_overwrite_each_other():
    """Same key completing twice in a day must leave two records, not one."""
    d, (q,) = lab(1, nshards=1)
    try:
        for i in range(2):
            q.enqueue("work", key="repeat", due=1 + i)
            c = q.claim_batch("s00", "w0", now=1000 + i * 10)[0]
            q.complete(c, now=1000 + i * 10)
        done = q.git.remote_refs("refs/jobs/done/*/*")
        check("each completion keeps its own record", len(done) == 2,
              "done refs={} (an overwrite would leave 1)".format(len(done)))
    finally:
        cleanup(d)


def test_keys_distribute_across_shards():
    """Sharding is what removes claim contention (10% -> 100% efficiency).
    A constant shard is not a correctness bug, so only this notices it."""
    d, (q,) = lab(1, nshards=16)
    try:
        from gitq.queue import shard_of
        shards = {shard_of("job-{}".format(i), 16) for i in range(200)}
        check("keys spread over the shard space", len(shards) >= 12,
              "200 keys landed on only {} of 16 shards".format(len(shards)))
    finally:
        cleanup(d)


def test_multi_ref_push_is_all_or_nothing():
    """A partial push would let a claim create its claimed ref without
    releasing the pending one. claim_batch's per-ref fallback masks this, so
    it has to be asserted at the push itself."""
    d, (q,) = lab()
    try:
        # Distinct blobs: pushing the identical object to an existing ref is a
        # no-op git reports as success, which would not exercise the lease.
        old, new = q.git.write_blob("x"), q.git.write_blob("y")
        taken = "refs/jobs/q/s00/pending/000000000001-50-taken"
        fresh = "refs/jobs/q/s00/pending/000000000002-50-fresh"
        q.git.push_atomic(["{}:{}".format(old, taken)], {taken: ""})

        # `taken` exists at `old`, so its must-not-exist lease fails. `fresh`
        # would succeed on its own and must be rolled back with it.
        ok, _ = q.git.push_atomic(
            ["{}:{}".format(new, fresh), "{}:{}".format(new, taken)],
            {fresh: "", taken: ""})
        landed = q.refs("pending")
        check("a push with one failing ref applies none of them",
              not ok and fresh not in landed,
              "ok={} fresh_created={}".format(ok, fresh in landed))
    finally:
        cleanup(d)


if __name__ == "__main__":
    sys.exit(main(globals()))
