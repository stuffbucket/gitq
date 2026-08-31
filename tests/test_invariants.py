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
from gitq import RefTxn, ShardLeases, Worker  # noqa: E402
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


def test_a_transaction_cannot_leave_the_jobs_namespace():
    """Structural guarantee that this library cannot write a branch, in the hub
    or anywhere else, even if pointed at a source repo. Checked when the ref is
    added, so the error names the guilty call site rather than the push."""
    d, (q,) = lab()
    try:
        oid = q.git.write_blob("x")
        for label, build in (
                ("create a branch", lambda t: t.create("refs/heads/main", oid)),
                ("update a branch", lambda t: t.update("refs/heads/main", oid, oid)),
                ("delete a branch", lambda t: t.delete("refs/heads/main", oid)),
                ("create a tag", lambda t: t.create("refs/tags/v1", oid))):
            try:
                build(RefTxn())
                check("refuses to {}".format(label), False, "it was allowed")
            except GitError as e:
                check("refuses to {}".format(label),
                      "outside refs/jobs/" in str(e), str(e))
    finally:
        cleanup(d)


def test_no_write_can_omit_the_value_it_replaces():
    """The blind overwrite has no spelling. Every method that changes an
    existing ref demands the value it expects to find, which is what makes
    "compare-and-swap" a property of the type rather than of each call site."""
    d, (q,) = lab()
    try:
        oid = q.git.write_blob("x")
        job = "refs/jobs/q/s00/pending/000000000001-50-x"
        for label, build in (
                ("update", lambda t: t.update(job, oid, "")),
                ("delete", lambda t: t.delete(job, ""))):
            try:
                build(RefTxn())
                check("{} demands an expectation".format(label), False,
                      "an unguarded {} was allowed".format(label))
            except GitError as e:
                check("{} demands an expectation".format(label),
                      "needs the value" in str(e), str(e))
        try:
            RefTxn().create(job, oid).create(job, oid)
            check("one ref cannot be written twice in a transaction", False,
                  "duplicate was allowed")
        except GitError as e:
            check("one ref cannot be written twice in a transaction",
                  "two updates" in str(e), str(e))
    finally:
        cleanup(d)


def test_every_ref_in_a_push_carries_a_lease():
    """push_atomic's last-line checks, which exist to catch a bug in RefTxn
    rather than a careless caller. Reaching them means forging a refspec --
    exactly what such a bug would do -- so that is how they are tested."""
    d, (q,) = lab()
    try:
        oid = q.git.write_blob("x")
        forged = "refs/jobs/q/s00/pending/000000000001-50-forged"

        unleased = RefTxn().create(
            "refs/jobs/q/s00/pending/000000000002-50-ok", oid)
        unleased._specs.append("{}:{}".format(oid, forged))
        try:
            q.git.push_atomic(unleased)
            check("an unleased ref cannot reach git", False, "push was allowed")
        except GitError as e:
            check("an unleased ref cannot reach git", "unleased" in str(e), str(e))

        forced = RefTxn().create(forged, oid)
        forced._specs[0] = "+" + forced._specs[0]
        try:
            q.git.push_atomic(forced)
            check("a force refspec cannot reach git", False, "push was allowed")
        except GitError as e:
            check("a force refspec cannot reach git",
                  "force refspec" in str(e), str(e))

        try:
            q.git.push_atomic(["{}:{}".format(oid, forged)])
            check("a bare refspec list is not a transaction", False,
                  "the pre-RefTxn calling convention still works")
        except GitError as e:
            check("a bare refspec list is not a transaction",
                  "takes a RefTxn" in str(e), str(e))

        check("no forged push landed", not q.refs("pending"),
              "hub holds {}".format(sorted(q.refs("pending"))))
    finally:
        cleanup(d)


def test_a_long_job_keeps_its_shard_lease_alive():
    """_execute blocks the poll loop, so the shard lease has to be renewed from
    inside it. Otherwise a job outliving shard_lease_s silently hands this
    worker's shards to whoever notices the expiry first."""
    d, (q,) = lab(1, nshards=1)
    try:
        q.enqueue("slow", key="j1", due=1)
        w = Worker(q, "w0", {"slow": lambda args: time.sleep(3)},
                   shard_lease_s=1, lease_s=60, poll_s=0.1)
        start = time.time()
        w.leases.acquire(want=1, now=start)
        w._last_renew = start        # so the poll loop's own renewal is a no-op
        before = dict(w.leases.held)
        w.run_once(now=start)
        check("the job ran", w.stats["done"] == 1,
              "stats={}".format(w.stats))
        check("shard lease was renewed while the job ran",
              bool(w.leases.held) and w.leases.held != before,
              "held {} before, {} after".format(before, w.leases.held))
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
        q.git.push_atomic(RefTxn().create(taken, old))

        # `taken` exists at `old`, so its must-not-exist lease fails. `fresh`
        # would succeed on its own and must be rolled back with it.
        ok, _ = q.git.push_atomic(
            RefTxn().create(fresh, new).create(taken, new))
        landed = q.refs("pending")
        check("a push with one failing ref applies none of them",
              not ok and fresh not in landed,
              "ok={} fresh_created={}".format(ok, fresh in landed))
    finally:
        cleanup(d)


if __name__ == "__main__":
    sys.exit(main(globals()))
