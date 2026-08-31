"""Leaderless periodic scheduling.

Every worker evaluates every schedule and races to enqueue. The per-period ref
`refs/jobs/cron/<task>/<minute>` can only be created once, so exactly one
worker wins and the rest no-op. That is the redundancy: no elected scheduler,
no failover, and the schedule keeps firing as long as any one worker is alive.
"""
from __future__ import annotations

import time

from .git import RefTxn
from .queue import now_or

_BOUNDS = [(0, 59), (0, 23), (1, 31), (1, 12), (0, 6)]
_PARSED = {}


def _field(spec, lo, hi):
    """Expand one cron field into a set. Supports *, */n, a-b, a-b/n, and lists."""
    out = set()
    for part in spec.split(","):
        step = 1
        if "/" in part:
            part, s = part.split("/", 1)
            step = int(s)
        if part == "*":
            start, end = lo, hi
        elif "-" in part:
            a, b = part.split("-", 1)
            start, end = int(a), int(b)
        else:
            start = end = int(part)
        if not (lo <= start <= hi and lo <= end <= hi and start <= end):
            raise ValueError("cron field out of range: {}".format(spec))
        out.update(range(start, end + 1, step))
    return out


def parse(expr):
    """Memoised: tick() is called every poll and expressions never change."""
    if expr not in _PARSED:
        fields = expr.split()
        if len(fields) != 5:
            raise ValueError(
                "expected 5 cron fields, got {}: {!r}".format(len(fields), expr))
        _PARSED[expr] = [_field(f, lo, hi) for f, (lo, hi) in zip(fields, _BOUNDS)]
    return _PARSED[expr]


def matches(parsed, when):
    t = time.localtime(when)
    minute, hour, dom, mon, dow = parsed
    # Standard cron: when both day-of-month and day-of-week are restricted,
    # either matching is enough.
    dom_r, dow_r = len(dom) < 31, len(dow) < 7
    dom_ok, dow_ok = t.tm_mday in dom, (t.tm_wday + 1) % 7 in dow
    day_ok = (dom_ok or dow_ok) if (dom_r and dow_r) else (dom_ok and dow_ok)
    return t.tm_min in minute and t.tm_hour in hour and day_ok and t.tm_mon in mon


def tick(queue, schedules, now=None, catchup_min=10, attempted=None):
    """Enqueue any schedule whose minute has come. Safe to call from every worker.

    schedules: list of {"task", "cron", optional "args"/"prio"/"max_attempts"}.
    Looks back `catchup_min` minutes so a brief total outage still fires, and
    silently skips anything already claimed by another worker for that minute.

    `attempted` is a set this caller carries between calls. A minute we have
    already tried is settled either way -- we won it, or someone else did -- so
    re-pushing it every poll is pure waste. Without it a `* * * * *` schedule
    costs 33 git processes per poll to accomplish nothing.
    """
    now = now_or(now)
    fired = []
    for sched in schedules:
        parsed = parse(sched["cron"])
        for back in range(catchup_min, -1, -1):
            when = (now // 60 - back) * 60
            if not matches(parsed, when):
                continue
            mark = (sched["task"], when)
            if attempted is not None and mark in attempted:
                continue
            if attempted is not None:
                attempted.add(mark)
            ref = queue.cron_ref(sched["task"], when)
            marker = queue.git.write_blob("{}@{}\n".format(sched["task"], when))
            job_ref, job_oid = queue.prepare(
                task=sched["task"],
                args=sched.get("args", {}),
                key="{}-{}".format(sched["task"], when),
                due=when,
                prio=sched.get("prio", 50),
                max_attempts=sched.get("max_attempts", 3),
            )
            # One transaction: claiming the period and scheduling the job land
            # together or not at all. Two pushes would let a crash in between
            # leave a marker with no job, silently skipping that period forever.
            _, res = queue.git.push_atomic(
                RefTxn().create(ref, marker).create(job_ref, job_oid))
            if res.created(ref):
                fired.append(mark)
    return fired
