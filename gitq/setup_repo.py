"""Create the shared hub repo and per-worker object stores."""
from __future__ import annotations

from pathlib import Path

from .git import NAMESPACE, Git, run_git

# push_atomic's namespace check is an assertion about our own code; it cannot
# bind anything that is not this library. This hook is the enforcement, and it
# lives on the hub because that is the only side an outsider does not control.
# Rejecting exits nonzero, which drops the whole push -- correct here, since
# every write in the system is one atomic transaction.
_HOOK = """#!/bin/sh
# Installed by gitq. Refs outside {ns} are not this queue's business.
while read -r old new ref
do
\tcase "$ref" in
\t{ns}*) ;;
\t*) echo "gitq: hub accepts only {ns}*, got $ref" >&2; exit 1 ;;
\tesac
done
exit 0
""".format(ns=NAMESPACE)


def _install_hook(path):
    """Rewritten on every init_hub, not just at creation: a hub made before
    this existed is otherwise left unguarded forever."""
    hooks = path / "hooks"
    hooks.mkdir(exist_ok=True)
    hook = hooks / "pre-receive"       # the .sample git ships is inert
    hook.write_text(_HOOK)
    hook.chmod(0o755)
    return hook


def init_hub(path):
    """The shared queue. reftable is not the default -- it must be asked for."""
    path = Path(path)
    if not path.exists():
        run_git("init", "--bare", "--ref-format=reftable", "--quiet", str(path))
    _install_hook(path)
    return path


def init_worker(path, hub):
    """A worker needs an object store and a remote, not a working tree."""
    path, hub = Path(path), Path(hub)
    if not path.exists():
        run_git("init", "--bare", "--ref-format=reftable", "--quiet", str(path))
        run_git("--git-dir", str(path), "remote", "add", "origin",
                str(hub.resolve()))
    return Git(path)
