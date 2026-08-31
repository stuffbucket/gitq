"""Mutation testing: break one guarantee at a time, see if a test notices.

Each mutation removes a specific safety property rather than perturbing a random
operator, so a survivor names the exact behaviour nothing is checking.

Run: python3 tools/mutate.py [--only NAME]
"""
import re
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SUITES = ["tests/test_gitq.py", "tests/test_bugs.py",
          "tests/test_invariants.py", "tests/test_hub_hook.py"]

# (name, file, old, new, what guarantee this removes)
MUTATIONS = [
    ("no-atomic", "gitq/git.py",
     'args = ["push", "--atomic", "--porcelain"]',
     'args = ["push", "--porcelain"]',
     "multi-ref pushes stop being all-or-nothing"),

    ("no-cas", "gitq/git.py",
     '        args += ["--force-with-lease={}:{}".format(r, o) for r, o in leases.items()]',
     '        args += []',
     "claims/creates no longer compare-and-swap"),

    ("enqueue-ignores-flag", "gitq/queue.py",
     'return res.created(ref)',
     'return res.created(ref) or res.unchanged(ref)',
     "duplicate enqueue reports success"),

    ("claim-unleased-claimed-ref", "gitq/queue.py",
     '            txn.create(c["ref"], c["oid"])',
     '            txn._specs.append("{}:{}".format(c["oid"], c["ref"]))',
     "the claimed ref is written with no expectation -- now reachable only by "
     "forging a refspec past RefTxn, which push_atomic refuses outright"),

    ("never-dead-letter", "gitq/queue.py",
     'if job["attempt"] >= job.get("max_attempts", 3):',
     'if False:',
     "exhausted jobs retry forever instead of dead-lettering"),

    ("job-name-unpadded", "gitq/queue.py",
     'return "{:012d}-{:02d}-{}".format(int(due), int(prio), safe_key(key))',
     'return "{}-{}-{}".format(int(due), int(prio), safe_key(key))',
     "ref names stop sorting into due order"),

    ("renew-noop", "gitq/queue.py",
     '        job = dict(claim["job"], lease_until=now + lease_s)',
     '        return True\n        job = dict(claim["job"], lease_until=now + lease_s)',
     "lease renewal silently does nothing"),

    ("reaper-uses-deadline", "gitq/reaper.py",
     "                if now - first_seen >= stale_after:",
     "                if int(job.get('lease_until', 0)) <= now:",
     "reaper goes back to comparing two machines' clocks"),

    ("reaper-always-reclaims", "gitq/reaper.py",
     "                ok = outcome = None\n                if now - first_seen >= stale_after:",
     "                ok = outcome = None\n                if True:",
     "reaper reclaims even healthy, heartbeating claims"),

    ("worker-no-heartbeat", "gitq/worker.py",
     "            if not self._heartbeat(claim=claim):",
     "            if False:",
     "long jobs stop renewing their lease"),

    ("heartbeat-skips-shards", "gitq/worker.py",
     "            self.leases.renew(now=now)",
     "            pass",
     "shard leases are never refreshed, so a long job loses its shards"),

    ("heartbeat-ignores-shard-lease", "gitq/worker.py",
     "        interval = max(1.0, min(self.lease_s, self.shard_lease_s) / 3.0)",
     "        interval = max(1.0, self.lease_s / 3.0)",
     "the heartbeat paces itself off the job lease alone, letting the shorter "
     "shard lease lapse under a long job"),

    ("cron-split-push", "gitq/cron.py",
     '            _, res = queue.git.push_atomic(\n'
     '                RefTxn().create(ref, marker).create(job_ref, job_oid))',
     '            _, res = queue.git.push_atomic(RefTxn().create(ref, marker))\n'
     '            if res.created(ref):\n'
     '                queue.git.push_atomic(RefTxn().create(job_ref, job_oid))',
     "cron marker and job become two separate pushes again"),

    ("cron-ignores-flag", "gitq/cron.py",
     'if res.created(ref):',
     'if not res.rejected(ref):',
     "cron losers report firing a period they did not schedule"),

    ("force-past-the-lease", "gitq/git.py",
     '        args += ["origin", *specs]',
     '        args += ["origin", *[s if s.startswith(":") else "+" + s for s in specs]]',
     "a '+' reaches git downstream of the guard, overriding every lease and "
     "turning each compare-and-swap into a blind overwrite"),

    ("no-unleased-guard", "gitq/git.py",
     '            if spec.split(":")[-1] not in leases:',
     '            if False:',
     "a ref can be written by a push that never stated what it expected to find"),

    ("no-guards", "gitq/git.py",
     '            if spec.startswith("+"):',
     '            if False:',
     "force refspecs stop being rejected"),

    ("no-namespace-guard", "gitq/git.py",
     '        if not ref.startswith(NAMESPACE):',
     '        if False:',
     "transactions may leave refs/jobs/ and write branches"),

    ("hub-accepts-anything", "gitq/setup_repo.py",
     '    _install_hook(path)',
     '    pass',
     "the hub stops enforcing its namespace, leaving only the client assertion"),

    ("no-dot-collapse", "gitq/queue.py",
     '    k = re.sub(r"\\.{2,}", ".", k)      # \'..\' is illegal in a ref name',
     '    pass',
     "'..' survives into ref names, which git rejects"),

    ("shard-constant", "gitq/queue.py",
     '    return "s{:02d}".format(int.from_bytes(digest[:4], "big") % nshards)',
     '    return "s00"',
     "all jobs collapse onto one shard"),

    ("safe-key-nosanitize", "gitq/queue.py",
     '    k = _SAFE.sub("-", str(key)).strip("-.")',
     '    k = str(key)',
     "ref-unsafe characters pass through into ref names"),
]


def run_suites(stop_early=True):
    """Returns (all_passed, set_of_failed_check_names)."""
    failed = set()
    ok = True
    for suite in SUITES:
        p = subprocess.run([sys.executable, suite], cwd=ROOT,
                           capture_output=True, text=True, timeout=600)
        if p.returncode != 0:
            ok = False
        for line in (p.stdout + p.stderr).splitlines():
            m = re.match(r"\s*FAIL (.+?)(  <-|$)", line)
            if m:
                failed.add(m.group(1).strip())
        if "Traceback" in p.stderr and p.returncode != 0:
            failed.add("{}: crashed".format(Path(suite).name))
        if not ok and stop_early:
            break          # killed already; the remaining suites add nothing
    return ok, failed


def main():
    only = None
    if "--only" in sys.argv:
        only = sys.argv[sys.argv.index("--only") + 1]

    try:
        print("baseline (unmutated): ", end="", flush=True)
        ok, failed = run_suites(stop_early=False)
        if not ok:
            print("DIRTY -- fix the suite before mutating: {}".format(failed))
            return 2
        print("green\n")

        killed = survived = 0
        rows = []
        for name, rel, old, new, desc in MUTATIONS:
            if only and only != name:
                continue
            target = ROOT / rel
            src = target.read_text()
            if old not in src:
                rows.append((name, "SKIP", "pattern not found -- source drifted", ""))
                continue
            target.write_text(src.replace(old, new, 1))
            t0 = time.time()
            try:
                ok, failed = run_suites()
            except subprocess.TimeoutExpired:
                ok, failed = False, {"timeout"}
            finally:
                target.write_text(src)
            dt = time.time() - t0
            if ok:
                survived += 1
                rows.append((name, "SURVIVED", desc, ""))
            else:
                killed += 1
                caught = sorted(failed)
                rows.append((name, "killed", desc,
                             "{} ({:.0f}s)".format(caught[0] if caught else "?", dt)))
            print("  {:<24} {}".format(name, rows[-1][1]), flush=True)

        print("\n{:<24} {:<9} {}".format("MUTATION", "RESULT", "GUARANTEE REMOVED"))
        print("-" * 96)
        for name, res, desc, extra in rows:
            print("{:<24} {:<9} {}".format(name, res, desc))
            if res == "killed" and extra:
                print("{:<34} caught by: {}".format("", extra))
        print("\nkilled={} survived={}".format(killed, survived))

        print("\nverifying source restored: ", end="", flush=True)
        ok, _ = run_suites()
        print("green" if ok else "DIRTY -- RESTORE FAILED")
        return 0 if survived == 0 else 1
    finally:
        pass


if __name__ == "__main__":
    sys.exit(main())
