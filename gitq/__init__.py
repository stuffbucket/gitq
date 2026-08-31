"""gitq -- a sharded, idempotent job queue whose only storage is a git repo.

Correctness rests on two git primitives, both measured under contention:
  * creating a ref is compare-and-swap against absence (idempotent enqueue)
  * `push --atomic --force-with-lease` is a multi-ref transaction (exactly-once claim)

Use a reftable-backed repo: at 100k refs it is ~110x smaller on disk and
returns the next job in ~14ms, versus a linear scan on the files backend.
"""
from .git import Git, GitError
from .queue import PATTERNS, Queue, safe_key, shard_of
from .shards import ShardLeases
from .reaper import Reaper, reap
from .cron import tick, parse, matches
from .worker import Worker

__all__ = ["Git", "GitError", "PATTERNS", "Queue", "ShardLeases", "Worker",
           "Reaper", "reap", "tick", "parse", "matches", "shard_of", "safe_key"]
