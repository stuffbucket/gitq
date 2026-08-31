"""Run every suite, one process each, and report a single verdict.

One process per suite because the harness keeps PASS/FAIL at module scope --
sharing an interpreter would merge the tallies and hide which suite failed.
"""
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SUITES = ["tests/test_gitq.py", "tests/test_bugs.py",
          "tests/test_invariants.py", "tests/test_hub_hook.py"]


def main():
    print(subprocess.run(["git", "--version"], capture_output=True,
                         text=True).stdout.strip())
    failed = []
    for suite in SUITES:
        p = subprocess.run([sys.executable, suite], cwd=ROOT,
                           capture_output=True, text=True)
        last = (p.stdout.strip().splitlines() or ["no output"])[-1]
        print("{:<28} {}".format(suite, last))
        if p.returncode != 0:
            failed.append(suite)
            print(p.stdout, p.stderr, sep="\n")
    print("\n{}".format("FAILED: " + ", ".join(failed) if failed else "all suites passed"))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
