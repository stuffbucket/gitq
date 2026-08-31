"""Create the shared hub repo and per-worker object stores."""
from __future__ import annotations

from pathlib import Path

from .git import Git, run_git


def init_hub(path):
    """The shared queue. reftable is not the default -- it must be asked for."""
    path = Path(path)
    if not path.exists():
        run_git("init", "--bare", "--ref-format=reftable", "--quiet", str(path))
    return path


def init_worker(path, hub):
    """A worker needs an object store and a remote, not a working tree."""
    path, hub = Path(path), Path(hub)
    if not path.exists():
        run_git("init", "--bare", "--ref-format=reftable", "--quiet", str(path))
        run_git("--git-dir", str(path), "remote", "add", "origin",
                str(hub.resolve()))
    return Git(path)
