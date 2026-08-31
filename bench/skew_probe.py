"""Two workers, clocks 5 minutes apart. Which guarantees survive?"""
import shutil, sys, tempfile, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from gitq import Queue, cron
from gitq.setup_repo import init_hub, init_worker

SKEW = 300  # B's clock runs 5 minutes ahead of A's

def lab(n=2):
    d = Path(tempfile.mkdtemp(prefix="gitq-skew-"))
    hub = init_hub(d / "hub.git")
    return d, [Queue(init_worker(d / "w{}.git".format(i), hub), 8) for i in range(n)]

# --- 1. scheduling identity: is a period ever scheduled twice? ---------------
d, (A, B) = lab()
try:
    sched = [{"task": "beat", "cron": "* * * * *"}]
    base = (int(time.time()) // 60) * 60
    fired = []
    for step in range(11):                       # real time advances 0..600s
        real = base + step * 60
        fired += cron.tick(A, sched, now=real,        catchup_min=0)
        fired += cron.tick(B, sched, now=real + SKEW, catchup_min=0)
    periods = [w for _, w in fired]
    jobs = A.git.remote_refs("refs/jobs/q/*/pending/*")
    marks = A.git.remote_refs("refs/jobs/cron/*/*")
    print("1. CRON PERIOD DEDUP UNDER SKEW")
    print("   fires={} distinct periods={} markers={} jobs={}".format(
        len(periods), len(set(periods)), len(marks), len(jobs)))
    print("   -> {}".format("PRESERVED: no period scheduled twice"
          if len(periods) == len(set(periods)) == len(jobs) else "VIOLATED"))
finally:
    shutil.rmtree(d, ignore_errors=True)

# --- 2. claiming: does CAS still admit exactly one winner? ------------------
d, (A, B) = lab()
try:
    A.enqueue("work", key="solo", due=1)
    now = time.time()
    wins = []
    for s in A.shards():
        wins += A.claim_batch(s, "A", now=now)
        wins += B.claim_batch(s, "B", now=now + SKEW)
    print("\n2. CLAIM EXACTLY-ONCE UNDER SKEW")
    print("   winners={} ({})".format(len(wins), [w["job"]["worker"] for w in wins]))
    print("   -> {}".format("PRESERVED: claiming is pure CAS, no clock read"
          if len(wins) == 1 else "VIOLATED"))
finally:
    shutil.rmtree(d, ignore_errors=True)

# --- 3. lease expiry: staleness is now an OBSERVED interval, not a deadline --
print("\n3. LEASE / REPLAY UNDER SKEW  (A claims, B reaps with a 5-min-fast clock)")
print("   B reclaims only after watching the claim sit unchanged longer than")
print("   the lease duration A itself declared. Neither side reads the other's clock.")
from gitq.reaper import Reaper
for lease_s, watched in ((300, 30), (300, 400), (60, 30), (60, 90)):
    d, (A, B) = lab()
    try:
        A.enqueue("work", key="live", due=1)
        now = time.time()
        got = [c for s in A.shards() for c in A.claim_batch(s, "A", lease_s=lease_s, now=now)]
        r = Reaper(B, "b")
        r.sweep(now=now + SKEW)                       # baseline sighting (skewed)
        reclaimed, _ = r.sweep(now=now + SKEW + watched)
        alive = watched < lease_s
        verdict = ("OK: left alone, worker still heartbeating" if reclaimed == 0 and alive
                   else "OK: reclaimed, worker really is dead" if reclaimed == 1 and not alive
                   else "<-- WRONG")
        print("   lease={:>4}s skew={:>4}s B-watched={:>4}s -> reclaimed={} {}".format(
            lease_s, SKEW, watched, reclaimed, verdict))
    finally:
        shutil.rmtree(d, ignore_errors=True)
