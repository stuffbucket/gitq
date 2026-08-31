"""Thin wrapper over git plumbing.

The queue never creates a commit. A job is a single blob; a ref points straight
at it. That keeps writes to one object per job and makes identical payloads
collapse to the same SHA for free.
"""
from __future__ import annotations

import json
import subprocess

# Every push this library makes must land inside this namespace. Enforced in
# push_atomic so a misconfigured hub -- one pointed at a real source repo --
# cannot be made to write a branch, rather than merely not doing so today.
NAMESPACE = "refs/jobs/"


class GitError(RuntimeError):
    pass


def run_git(*args, stdin=None, check=True):
    """Run one git command. Shared so callers without a Git instance -- repo
    setup, the CLI's gc -- raise the same error from the same place."""
    p = subprocess.run(["git", *args], input=stdin, capture_output=True, text=True)
    if check and p.returncode != 0:
        raise GitError("git {}\n{}".format(" ".join(args), p.stderr.strip()))
    return p


class PushResult:
    """Outcome of one push, per destination ref.

    Callers ask what happened in their own vocabulary; git's porcelain status
    alphabet stays inside this module. Exit status alone cannot answer
    `created`: pushing a ref that already points at the identical object
    succeeds with flag "=", because git skips the update and so never
    evaluates the lease.
    """

    __slots__ = ("ok", "flags")

    def __init__(self, ok, flags):
        self.ok = ok
        self.flags = flags

    def created(self, ref):
        return self.flags.get(ref) == "*"

    def wrote(self, ref):
        return self.flags.get(ref) in ("*", "+", " ")

    def unchanged(self, ref):
        return self.flags.get(ref) == "="

    def rejected(self, ref):
        return self.flags.get(ref) == "!"

    def __bool__(self):
        return self.ok


class Git:
    """Operations against one local object store with an 'origin' remote."""

    def __init__(self, gitdir):
        self.gitdir = str(gitdir)

    def run(self, *args, stdin=None, check=True):
        return run_git("--git-dir", self.gitdir, *args, stdin=stdin, check=check)

    # -- objects ------------------------------------------------------------
    def write_blob(self, data):
        return self.run("hash-object", "-w", "--stdin", stdin=data).stdout.strip()

    def read_blob(self, oid):
        return self.run("cat-file", "blob", oid).stdout

    def write_json(self, obj):
        """sort_keys is load-bearing: it is what makes an identical payload
        collapse to the same SHA, which in turn is the reaper's liveness
        signal. Centralised so no call site can forget it."""
        return self.write_blob(json.dumps(obj, sort_keys=True))

    def read_json(self, oid):
        return json.loads(self.read_blob(oid))

    # -- refs ---------------------------------------------------------------
    def local_refs(self, prefix):
        """Enumerate local refs under a prefix.

        Deliberately no --sort: reftable already stores refs in name order, and
        --sort forces git to read and sort every matching ref before returning
        even one (~100x slower at 100k refs).
        """
        out = self.run("for-each-ref", "--format=%(refname) %(objectname)", prefix).stdout
        refs = {}
        for line in out.splitlines():
            if line.strip():
                name, oid = line.rsplit(" ", 1)
                refs[name] = oid
        return refs

    def remote_refs(self, pattern):
        out = self.run("ls-remote", "origin", pattern).stdout
        refs = {}
        for line in out.splitlines():
            if line.strip():
                oid, name = line.split("\t", 1)
                refs[name] = oid
        return refs

    def mirror(self, src_prefix, dst_prefix, one=None):
        """Fetch a remote ref subtree into a local namespace, pruning what is
        gone. `one` fetches a single ref instead of the whole subtree.

        --no-tags/--no-write-fetch-head: this is the hottest command in the
        system, and neither tag auto-following nor FETCH_HEAD is ever read.
        """
        src, dst = src_prefix.rstrip("/"), dst_prefix.rstrip("/")
        spec = ("+{}/{}:{}/{}".format(src, one, dst, one) if one
                else "+{}/*:{}/*".format(src, dst))
        args = ["fetch", "--prune", "--quiet", "--no-tags",
                "--no-write-fetch-head", "origin", spec]
        self.run(*args)

    def mirror_names(self, src_prefix, dst_prefix, one=None):
        """Mirror a subtree and return {name_below_prefix: oid}."""
        self.mirror(src_prefix, dst_prefix, one=one)
        dst = dst_prefix.rstrip("/") + "/"
        return {ref[len(dst):]: oid
                for ref, oid in self.local_refs(dst_prefix).items()}

    # -- the one write primitive --------------------------------------------
    def push_atomic(self, refspecs, leases=None):
        """All-or-nothing multi-ref update, guarded by compare-and-swap.

        leases maps refname -> expected oid ("" means must-not-exist). Returns
        (ok, PushResult). A False result means someone else won the race, which
        is an ordinary outcome here, not an error.
        """
        leases = leases or {}
        for spec in refspecs:
            if spec.startswith("+"):
                # A "+" silently overrides --force-with-lease, turning every
                # compare-and-swap into a blind overwrite. Measured: 10 racers
                # against one ref yielded ~6 winners with "+", exactly 1 without.
                # The lease alone already authorises a non-fast-forward swap.
                raise GitError("refusing a force refspec: {!r}".format(spec))
            if not spec.split(":")[-1].startswith(NAMESPACE):
                raise GitError(
                    "refusing to push outside {}: {!r}".format(NAMESPACE, spec))
        for ref in leases:
            if not ref.startswith(NAMESPACE):
                raise GitError(
                    "refusing to lease outside {}: {!r}".format(NAMESPACE, ref))

        args = ["push", "--atomic", "--porcelain"]
        args += ["--force-with-lease={}:{}".format(r, o) for r, o in leases.items()]
        args += ["origin", *refspecs]
        p = self.run(*args, check=False)
        flags = {}
        for line in p.stdout.splitlines():
            parts = line.split("\t")
            if len(parts) >= 2 and parts[0][:1] and parts[0][:1] in "* =+-!":
                flags[parts[1].split(":")[-1]] = parts[0][:1]
        return p.returncode == 0, PushResult(p.returncode == 0, flags)
