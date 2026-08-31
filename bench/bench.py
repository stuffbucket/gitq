"""Sharded vs unsharded, measured through the real implementation.

The shell harness that motivated this design showed 10% claim efficiency for
deterministic pick vs 100% for sharded. This checks the library reproduces it.

Run: python3 bench/bench.py [nworkers] [njobs]
"""
import shutil
import sys
import tempfile
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from gitq import Queue                                     # noqa: E402
from gitq.setup_repo import init_hub, init_worker          # noqa: E402

NSHARDS = 16


def build(root, nworkers, njobs):
    hub = init_hub(root / "hub.git")
    qs = [Queue(init_worker(root / "w{}.git".format(i), hub), NSHARDS)
          for i in range(nworkers)]
    qs[0].enqueue_batch([{"task": "noop", "key": "j{}".format(i), "due": 1}
                         for i in range(njobs)])
    return qs


class CountingPush:
    """Wraps push_atomic to count calls. A class rather than a closure: this is
    attached to a Queue for the process lifetime, and a closure would pin the
    whole enclosing frame alive with it."""

    __slots__ = ("orig", "counter", "lock")

    def __init__(self, orig, counter, lock):
        self.orig = orig
        self.counter = counter
        self.lock = lock

    def __call__(self, txn):
        with self.lock:
            self.counter[0] += 1
        return self.orig(txn)


def run(mode, qs):
    pushes = [0]
    lock = threading.Lock()
    for q in qs:
        q.git.push_atomic = CountingPush(q.git.push_atomic, pushes, lock)

    claimed, clock = [], threading.Lock()
    shard_list = qs[0].shards()

    def go(i, q):
        mine = shard_list if mode == "unsharded" else \
            [s for n, s in enumerate(shard_list) if n % len(qs) == i]
        got = []
        deadline = time.time() + 20
        while time.time() < deadline:
            round_got = []
            for s in mine:
                round_got += q.claim_batch(s, "w{}".format(i), limit=10, now=time.time())
            got += round_got
            if not round_got:
                break
        with clock:
            claimed.extend(got)

    t0 = time.time()
    ts = [threading.Thread(target=go, args=(i, q)) for i, q in enumerate(qs)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    wall = time.time() - t0
    keys = [c["job"]["key"] for c in claimed]
    return {
        "mode": mode, "wall": wall, "claimed": len(keys),
        "unique": len(set(keys)), "pushes": pushes[0],
        "per_push": len(keys) / max(pushes[0], 1),
        "rate": len(keys) / wall if wall else 0,
    }


def main():
    nworkers = int(sys.argv[1]) if len(sys.argv) > 1 else 8
    njobs = int(sys.argv[2]) if len(sys.argv) > 2 else 200
    print("{} workers, {} jobs, {} shards\n".format(nworkers, njobs, NSHARDS))
    print("{:<11} {:>8} {:>8} {:>7} {:>8} {:>9} {:>10}".format(
        "MODE", "WALL(s)", "CLAIMED", "UNIQUE", "PUSHES", "JOBS/PUSH", "JOBS/s"))
    for mode in ("unsharded", "sharded"):
        root = Path(tempfile.mkdtemp(prefix="gitq-bench-"))
        try:
            r = run(mode, build(root, nworkers, njobs))
            print("{:<11} {:>8.2f} {:>8} {:>7} {:>8} {:>9.2f} {:>10.1f}".format(
                r["mode"], r["wall"], r["claimed"], r["unique"], r["pushes"],
                r["per_push"], r["rate"]))
            if r["claimed"] != r["unique"]:
                print("  !! DUPLICATE CLAIMS -- exactly-once violated")
        finally:
            shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
