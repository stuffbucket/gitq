"""The hub's pre-receive hook: namespace enforcement, not client etiquette.

push_atomic's own check would refuse every illegal refspec before git saw it,
so these tests deliberately go around it and drive `git push` directly. A test
that went through push_atomic would pass with no hook installed at all.
"""
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from harness import check, cleanup, lab, main                  # noqa: E402
from gitq.git import run_git                                   # noqa: E402
from gitq.setup_repo import init_hub, init_worker              # noqa: E402


def hub_refs(hub):
    """Ask the hub what actually landed. A push's exit code is not evidence."""
    out = run_git("--git-dir", str(hub), "for-each-ref",
                  "--format=%(refname)").stdout
    return set(out.split())


def a_commit(git):
    """refs/heads/* rejects a non-commit on its own, which would let the
    illegal-ref tests pass without any hook. Push a real commit instead."""
    tree = git.run("hash-object", "-w", "-t", "tree", "--stdin",
                   stdin="").stdout.strip()
    return git.run("-c", "user.name=t", "-c", "user.email=t@e",
                   "commit-tree", tree, "-m", "x").stdout.strip()


def test_hook_is_installed_and_executable():
    d, (q,) = lab(1)
    try:
        hook = d / "hub.git" / "hooks" / "pre-receive"
        check("pre-receive exists, not .sample", hook.exists(),
              "missing {}".format(hook))
        check("pre-receive is 0755", hook.exists() and
              (hook.stat().st_mode & 0o777) == 0o755,
              "mode={:o}".format(hook.stat().st_mode & 0o777)
              if hook.exists() else "absent")
    finally:
        cleanup(d)


def test_push_inside_namespace_still_lands():
    """The failure mode worth fearing is a hook that rejects everything."""
    d, (q,) = lab(1)
    try:
        oid = q.git.write_blob("payload")
        p = q.git.run("push", "origin",
                      "{}:refs/jobs/hook/ok".format(oid), check=False)
        refs = hub_refs(d / "hub.git")
        check("legal ref survives the hook",
              p.returncode == 0 and "refs/jobs/hook/ok" in refs,
              "rc={} refs={} err={}".format(p.returncode, sorted(refs),
                                            p.stderr.strip()))
    finally:
        cleanup(d)


def test_hub_rejects_a_ref_outside_the_namespace():
    d, (q,) = lab(1)
    try:
        c = a_commit(q.git)
        p = q.git.run("push", "origin",
                      "{}:refs/heads/main".format(c), check=False)
        refs = hub_refs(d / "hub.git")
        check("refs/heads/main never reaches the hub",
              p.returncode != 0 and "refs/heads/main" not in refs,
              "rc={} refs={}".format(p.returncode, sorted(refs)))
        check("hook names the offending ref",
              "refs/heads/main" in p.stderr and "gitq" in p.stderr,
              "stderr={!r}".format(p.stderr.strip()))
    finally:
        cleanup(d)


def test_mixed_atomic_push_loses_the_legal_ref_too():
    d, (q,) = lab(1)
    try:
        oid, c = q.git.write_blob("payload"), a_commit(q.git)
        p = q.git.run("push", "--atomic", "origin",
                      "{}:refs/jobs/hook/legal".format(oid),
                      "{}:refs/heads/illegal".format(c), check=False)
        refs = hub_refs(d / "hub.git")
        check("one bad ref drops the whole transaction",
              p.returncode != 0 and not refs & {"refs/jobs/hook/legal",
                                                "refs/heads/illegal"},
              "rc={} refs={}".format(p.returncode, sorted(refs)))
    finally:
        cleanup(d)


def test_init_hub_upgrades_an_existing_unguarded_hub():
    """A hub predating the hook must pick it up without losing its refs."""
    d = Path(tempfile.mkdtemp(prefix="gitq-test-"))
    try:
        hub = d / "hub.git"
        run_git("init", "--bare", "--ref-format=reftable", "--quiet", str(hub))
        w = init_worker(d / "w.git", hub)
        c, marker = a_commit(w), w.write_blob("survivor")
        w.run("push", "origin", "{}:refs/jobs/marker".format(marker))
        before = w.run("push", "origin",
                       "{}:refs/heads/main".format(c), check=False)
        check("unguarded hub accepts a branch (the bug being fixed)",
              before.returncode == 0 and
              "refs/heads/main" in hub_refs(hub),
              "rc={}".format(before.returncode))

        init_hub(hub)

        after = w.run("push", "origin",
                      "{}:refs/heads/other".format(c), check=False)
        refs = hub_refs(hub)
        check("re-init installs the hook",
              after.returncode != 0 and "refs/heads/other" not in refs,
              "rc={} refs={}".format(after.returncode, sorted(refs)))
        check("re-init did not re-create the repo",
              "refs/jobs/marker" in refs, "refs={}".format(sorted(refs)))
    finally:
        cleanup(d)


if __name__ == "__main__":
    sys.exit(main(dict(globals())))
