"""Thin wrapper over git plumbing.

The queue never creates a commit. A job is a single blob; a ref points straight
at it. That keeps writes to one object per job and makes identical payloads
collapse to the same SHA for free.
"""
from __future__ import annotations

import json
import os
import subprocess
import tempfile

# Every ref this library writes must live inside this namespace. Enforced in
# RefTxn so a misconfigured hub -- one pointed at a real source repo -- cannot
# be made to write a branch, rather than merely not doing so today. The hub
# enforces it again on its own side; this half is an assertion, that half is
# the actual boundary.
NAMESPACE = "refs/jobs/"

# The lease value meaning "this ref must not exist". git spells it as an empty
# --force-with-lease expectation; naming it keeps that spelling in one place.
ABSENT = ""


class GitError(RuntimeError):
    pass


def run_git(*args, stdin=None, check=True, text=True):
    """Run one git command. Shared so callers without a Git instance -- repo
    setup, the CLI's gc -- raise the same error from the same place.

    text=False for the batch object commands, whose framing counts bytes.
    """
    p = subprocess.run(["git", *args], input=stdin, capture_output=True, text=text)
    if check and p.returncode != 0:
        err = p.stderr if text else p.stderr.decode("utf-8", "replace")
        raise GitError("git {}\n{}".format(" ".join(args), err.strip()))
    return p


class RefTxn:
    """One all-or-nothing ref update, expressed only as compare-and-swaps.

    Every write in this system is a CAS, and a CAS is two halves that have to
    agree: the refspec naming the new value, and the lease naming the value it
    is allowed to replace. Held apart -- a list of refspecs here, a dict of
    leases there -- nothing stops a call site from updating a ref it never
    leased, which is a blind overwrite wearing the syntax of a compare-and-swap.
    That is not hypothetical: the claim path shipped for a while creating its
    `claimed` ref with no expectation at all.

    Here the two halves cannot be produced separately. Every method demands the
    value it expects to find, so `push_atomic` can assert something structural:
    each ref touched carries a lease, or the transaction does not exist.
    """

    __slots__ = ("_specs", "_leases")

    def __init__(self):
        self._specs = []
        self._leases = {}

    def _add(self, ref, spec, expect):
        if not ref.startswith(NAMESPACE):
            raise GitError(
                "refusing to touch a ref outside {}: {!r}".format(NAMESPACE, ref))
        if ref in self._leases:
            # git rejects the whole push for this ("multiple updates for ref"),
            # so catching it here just moves the error to the guilty call site.
            raise GitError("two updates for {} in one transaction".format(ref))
        self._specs.append(spec)
        self._leases[ref] = expect
        return self

    def create(self, ref, oid):
        """Write a ref that must not already exist -- the idempotency primitive."""
        return self._add(ref, "{}:{}".format(oid, ref), ABSENT)

    def update(self, ref, oid, expect):
        """Replace a ref's value, from the exact value we observed it holding."""
        if not expect:
            raise GitError(
                "update of {} needs the value it replaces; use create()".format(ref))
        return self._add(ref, "{}:{}".format(oid, ref), expect)

    def delete(self, ref, expect):
        """Remove a ref we still hold. An unconditional delete is not offered:
        it would drop a job someone else had already taken."""
        if not expect:
            raise GitError("delete of {} needs the value it removes".format(ref))
        return self._add(ref, ":" + ref, expect)

    def set(self, ref, oid, expect):
        """create() or update(), for callers whose expectation is data --
        a shard lease is taken from absence or stolen from a value, and which
        one is not known until the owner ref has been read."""
        return self.create(ref, oid) if not expect else self.update(ref, oid, expect)

    def specs(self):
        return list(self._specs)

    def leases(self):
        return dict(self._leases)

    def refs(self):
        return list(self._leases)

    def __len__(self):
        return len(self._specs)


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

    def run(self, *args, stdin=None, check=True, text=True):
        return run_git("--git-dir", self.gitdir, *args,
                       stdin=stdin, check=check, text=text)

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

    # -- the same, in bulk ---------------------------------------------------
    # One process per object is one fork per object, and a fork costs roughly a
    # thousand times more than the read itself: measured at ~3us to read a job
    # payload against ~9ms to spawn the git that reads it. Claiming ten jobs
    # went from ~93ms to ~8ms, and a 100-claim reaper sweep from ~944ms to
    # ~10ms, purely by not doing that.
    def read_blobs(self, oids):
        """Read many blobs in one process. Returns {oid: bytes}, omitting any
        object this store does not have."""
        oids = list(oids)
        if not oids:
            return {}
        data = self.run("cat-file", "--batch",
                        stdin=("\n".join(oids) + "\n").encode(), text=False).stdout
        out, pos = {}, 0
        for oid in oids:
            nl = data.index(b"\n", pos)
            header = data[pos:nl].split()
            pos = nl + 1
            # A bad oid is reported on stdout as "<input> missing" and still
            # exits 0. Silently skipping it here is why callers filter on the
            # returned keys rather than assuming one result per request.
            if header[-1] in (b"missing", b"ambiguous"):
                continue
            size = int(header[2])
            out[oid] = data[pos:pos + size]
            pos += size + 1        # the trailing newline is not counted in size
        return out

    def read_json_many(self, oids):
        return {oid: json.loads(blob) for oid, blob in self.read_blobs(oids).items()}

    def write_blobs(self, blobs):
        """Write many blobs in one process, returning their oids in order.

        --stdin-paths takes paths, not contents, so the payloads go to a
        scratch dir first. N file writes and one fork still beats N forks.
        """
        blobs = list(blobs)
        if not blobs:
            return []
        with tempfile.TemporaryDirectory(prefix="gitq-blobs-") as td:
            paths = []
            for i, blob in enumerate(blobs):
                path = os.path.join(td, str(i))
                with open(path, "wb") as fh:
                    fh.write(blob)
                paths.append(path)
            out = self.run("hash-object", "-w", "--stdin-paths",
                           stdin="\n".join(paths) + "\n").stdout
        return out.split()

    def write_json_many(self, objs):
        return self.write_blobs(
            json.dumps(o, sort_keys=True).encode() for o in objs)

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

    def read_local_ref(self, ref):
        """Value of a ref in this store, or None. For refs that never leave it."""
        p = self.run("rev-parse", "--verify", "--quiet", ref, check=False)
        return p.stdout.strip() or None

    def write_local_ref(self, ref, oid, expect):
        """Compare-and-swap a ref in this store only, never pushed.

        Not a RefTxn: nothing here is shared, so there is no transaction to be
        atomic across and no namespace to stay inside. Still a CAS, because two
        processes can share one object store.
        """
        return self.run("update-ref", ref, oid, expect or "",
                        check=False).returncode == 0

    def mirror_many(self, pairs):
        """Fetch remote ref subtrees into local namespaces, pruning what is gone.

        --no-tags/--no-write-fetch-head: this is the hottest command in the
        system, and neither tag auto-following nor FETCH_HEAD is ever read.

        One fetch, however many subtrees. A fetch costs ~26ms of connection and
        ref advertisement no matter how many refspecs it carries, so the
        per-subtree loop this replaced paid that once per subtree: measured
        412ms for 16 shards against 28ms for the same 16 here.

        Pruning stays correct. --prune is scoped to the destinations of the
        refspecs actually passed, so this prunes exactly what the equivalent
        loop pruned -- verified against partial deletion, a fully emptied
        shard, and orphaned local refs, byte-identical either way. The
        corollary is the caller's job: pass every subtree whose mirror should
        be current, because one you leave out is one that silently goes stale.
        """
        specs = ["+{}/*:{}/*".format(src.rstrip("/"), dst.rstrip("/"))
                 for src, dst in pairs]
        if not specs:
            return
        self.run("fetch", "--prune", "--quiet", "--no-tags",
                 "--no-write-fetch-head", "origin", *specs)

    def mirror_names(self, src_prefix, dst_prefix):
        """Mirror one subtree and return {name_below_prefix: oid}."""
        self.mirror_many([(src_prefix, dst_prefix)])
        dst = dst_prefix.rstrip("/") + "/"
        return {ref[len(dst):]: oid
                for ref, oid in self.local_refs(dst_prefix).items()}

    # -- the one write primitive --------------------------------------------
    def push_atomic(self, txn):
        """Apply a RefTxn against origin: all of it lands, or none of it does.

        Returns (ok, PushResult). A False result means someone else won the
        race, which is an ordinary outcome here, not an error.
        """
        if not isinstance(txn, RefTxn):
            raise GitError("push_atomic takes a RefTxn, not {}".format(type(txn).__name__))
        specs, leases = txn.specs(), txn.leases()
        if not specs:
            return True, PushResult(True, {})

        # The last point before the bytes reach git. RefTxn cannot construct a
        # spec that trips either check -- they are here to catch a bug in
        # RefTxn, not a careless caller, so they must stay cheap and total.
        for spec in specs:
            if spec.startswith("+"):
                # A "+" silently overrides --force-with-lease, turning every
                # compare-and-swap into a blind overwrite. Measured: 10 racers
                # against one ref yielded ~6 winners with "+", exactly 1 without.
                raise GitError("refusing a force refspec: {!r}".format(spec))
            if spec.split(":")[-1] not in leases:
                raise GitError("unleased ref in transaction: {!r}".format(spec))

        args = ["push", "--atomic", "--porcelain"]
        args += ["--force-with-lease={}:{}".format(r, o) for r, o in leases.items()]
        args += ["origin", *specs]
        p = self.run(*args, check=False)
        flags = {}
        for line in p.stdout.splitlines():
            parts = line.split("\t")
            if len(parts) >= 2 and parts[0][:1] and parts[0][:1] in "* =+-!":
                flags[parts[1].split(":")[-1]] = parts[0][:1]
        return p.returncode == 0, PushResult(p.returncode == 0, flags)
