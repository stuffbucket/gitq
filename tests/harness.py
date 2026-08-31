"""Shared scaffolding for the three suites.

Each suite previously carried its own copy of PASS/FAIL, check(), the temp-dir
fixture, the thread helper and the __main__ runner -- about 110 duplicated
lines, so any change to the reporting format had to be made three times.
"""
import shutil
import sys
import tempfile
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from gitq import Queue                                    # noqa: E402
from gitq.setup_repo import init_hub, init_worker         # noqa: E402

SKEW = 300          # a second clock running five minutes fast
PASS, FAIL = [], []


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print("  {} {}{}".format("PASS" if cond else "FAIL", name,
                             "" if cond else "  <- " + detail))


def lab(n=1, nshards=8):
    """A hub plus n worker queues. Returns (dir, queues); caller removes dir."""
    d = Path(tempfile.mkdtemp(prefix="gitq-test-"))
    hub = init_hub(d / "hub.git")
    return d, [Queue(init_worker(d / "w{}.git".format(i), hub), nshards)
               for i in range(n)]


def cleanup(d):
    shutil.rmtree(d, ignore_errors=True)


def parallel(fns):
    """Run thunks concurrently and wait. Contention here is inside git, so
    threads are genuine parallelism for our purposes."""
    ts = [threading.Thread(target=f) for f in fns]
    for t in ts:
        t.start()
    for t in ts:
        t.join()


def collect(fns):
    """parallel(), accumulating each thunk's return value without the caller
    hand-rolling a list and a lock."""
    out, lock = [], threading.Lock()

    def wrap(f):
        def go():
            r = f()
            if r:
                with lock:
                    out.extend(r)
        return go
    parallel([wrap(f) for f in fns])
    return out


def claim_all(q, worker, **kw):
    """Claim across every shard -- the idiom eight call sites repeated."""
    return [c for s in q.shards() for c in q.claim_batch(s, worker, **kw)]


def main(ns):
    """Discover and run test_* in the caller's namespace. Returns an exit code."""
    for t in [v for k, v in sorted(ns.items())
              if k.startswith("test_") and callable(v)]:
        print("{}:".format(t.__name__))
        t()
    print("\n{} passed, {} failed".format(len(PASS), len(FAIL)))
    return 1 if FAIL else 0
