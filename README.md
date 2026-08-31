# gitq

A sharded, idempotent job queue and cron scheduler whose only storage is a git
repository. No database, no daemon, no OS-level cron. Workers are ordinary
processes that share nothing but a bare repo.

Python 3.9+, standard library only. Git 2.45+ (2.50 tested) for `reftable`.

## Why this can work

Two git primitives do the load-bearing work. Both were measured under
contention before any of this was written (see *Measurements*):

- **Creating a ref is compare-and-swap against absence.** A push whose source
  ref does not exist locally sends `old = 0000…`, and the remote enforces it.
  That is the idempotent enqueue and the per-period cron lock, for free.
- **`push --atomic --force-with-lease` is a multi-ref transaction.** Deleting
  `pending/<job>` and creating `claimed/<job>` in one push is all-or-nothing,
  so a losing racer creates nothing. That is exactly-once claiming.

Ten workers racing for one job: one wins. Verified in `tests/`, every run.

## Liveness by observation

Git has no server clock. A `lease_until` written by one machine and read by
another compares two independent clocks: a reaper running 5 minutes fast will
steal a job that is still executing, and the job runs twice.

The reaper therefore never compares clocks. It uses two quantities, each a
difference measured on a *single* clock:

- **how long the claimer meant to hold the lease** -- `lease_until - claimed_at`,
  both stamped by the claimer, so the difference carries no skew
- **how long this reaper has watched the claim sit unchanged** -- an interval on
  the reaper's own clock, persisted per-reaper at `refs/local/reaper/<id>`

A claim is reclaimed only once this reaper has seen the identical claim blob for
longer than the claimer's own declared lease. `renew()` rewrites the blob on
every heartbeat, so an unchanged oid *is* the signal that a worker stopped.

Measured with a reaper 5 minutes fast (`bench/skew_probe.py`):

| Lease | Skew | Reaper watched | Reclaimed | |
|---|---|---|---|---|
| 300 s | 300 s | 30 s | 0 | left alone, still heartbeating |
| 300 s | 300 s | 400 s | 1 | reclaimed, worker really dead |
| 60 s | 300 s | 30 s | 0 | left alone |
| 60 s | 300 s | 90 s | 1 | reclaimed |

Scheduling and claiming were never at risk: a cron period's identity is an
absolute epoch minute in the ref name, and claiming is pure CAS that reads no
clock at all. Both verified under 5-minute skew.

## Safety properties, enforced not asserted

Every write goes through `RefTxn`, which is the only thing in the system that
can build one. It has three methods -- `create(ref, oid)`, `update(ref, oid,
expect)`, `delete(ref, expect)` -- and each produces the refspec and the
`--force-with-lease` expectation *together*. That is the point: a
compare-and-swap is two halves that must agree, and while they were a list of
refspecs over here and a dict of leases over there, nothing stopped a call site
from updating a ref it never leased. Nothing hypothetical about it -- the claim
path shipped for a while creating its `claimed` ref with no expectation at all.

- **No force pushes.** Not one. `RefTxn` has no method that emits a `+`, so
  there is no call site that could. `push_atomic` re-checks anyway, and refuses
  any spec whose destination it does not hold a lease for.
- **No blind overwrites.** `update` and `delete` will not accept an empty
  expectation; a write that does not know what it is replacing cannot be
  spelled.
- **No branch or tag is ever written.** Every ref must sit under `refs/jobs/`.
  `RefTxn` checks it when the ref is added, so the error names the guilty call
  site -- and the hub checks it again in a `pre-receive` hook, which is the only
  side an outsider does not control. Point the hub at a real source repo by
  mistake and it still cannot write `refs/heads/*`.
- **No commits, no merges, no history.** A job is a blob. The hub has no
  branches, so it can never conflict, never need a rebase, and `git log` on it
  is empty. Nothing to squash, nothing to reset.
- **No `reset`, `--hard`, or `filter-branch`** anywhere in the codebase.

Each of these is covered by a test and verified by mutation (`tools/mutate.py`):
removing any one of them makes a named check fail.

## Ref layout

```
refs/jobs/q/<shard>/pending/<due:012d>-<prio:02d>-<key>   -> payload blob
refs/jobs/q/<shard>/claimed/<same-name>                   -> claim blob (worker, lease_until)
refs/jobs/owner/<shard>                                   -> shard lease blob
refs/jobs/cron/<task>/<minute:012d>                       -> per-period lock
refs/jobs/done/<YYYYMMDD>/<key>                           -> completed
refs/jobs/dead/<key>                                      -> attempts exhausted

refs/mirror/...                                           -> local: fetched shard state
refs/local/reaper/<id>                                    -> local: this reaper's sightings
```

The bottom two never leave the machine that writes them. A sighting log in
particular *cannot* usefully be shared: it records when one clock first saw a
claim, and is meaningless against any other.

A job is **one blob**. Nothing commits, so there is no history to walk and
identical payloads collapse to the same SHA.

**The ref name is the index.** `reftable` stores refs sorted by name and
returns them that way, so a `<due>-<prio>-<key>` name means enumeration already
arrives in due order — no sort, no reading payloads to decide what runs next.

## Quickstart

```bash
./gitq-cli init --shards 16
./gitq-cli enqueue shell --key nightly --args '{"cmd":"echo hello"}'
./gitq-cli enqueue shell --key nightly --args '{"cmd":"echo hello"}'  # no-op, exit 3
./gitq-cli work w1 --seconds 30
./gitq-cli ls done
```

Library use:

```python
from gitq import Queue, Worker
from gitq.setup_repo import init_hub, init_worker

hub = init_hub("/srv/queue.git")
q = Queue(init_worker("/var/lib/gitq/w1.git", hub), nshards=16)

q.enqueue("resize", args={"path": "/tmp/a.png"}, key="resize-a")

Worker(q, "w1", handlers={"resize": do_resize},
       schedules=[{"task": "vacuum", "cron": "17 3 * * *"}]).run()
```

Redundancy needs no configuration: start a second worker. Shards are leased,
so it takes half of them; kill either one and the survivor steals the rest
after the lease expires. Every worker also evaluates every cron schedule and
races to enqueue — the per-minute ref means exactly one wins. There is no
elected scheduler to fail over.

## Measurements

All on macOS, git 2.50.1, local bare repo. `bench/bench.py`, and the shell
harnesses that produced the design in `/tmp/gitq-lab*.sh`.

**Sharding is the whole ballgame.** 8 workers, 200 jobs:

| Mode | Wall | Claimed | Unique | Pushes | Jobs/push | Jobs/s |
|---|---|---|---|---|---|---|
| unsharded | 10.00 s | 200 | 200 | 880 | 0.23 | 20.0 |
| **sharded** | **2.02 s** | 200 | 200 | **29** | **6.90** | **99.0** |

5× the throughput on 30× fewer round-trips. Unsharded, every worker picks the
same lexicographically-first job and ~77% of pushes are wasted. Both modes
claimed 200 unique jobs — exactly-once holds either way.

**Use reftable.** At 100k refs, versus the default `files` backend:

| Backend | Next-job scan | Disk |
|---|---|---|
| files | 159–365 ms (linear) | 40,112 KB |
| reftable | **14 ms** | **368 KB** |

**Batching amortizes ~2.5×** and then plateaus: 110 ms/job at K=1, 44 ms at
K=10, flat past that. K=200 was a single 8.7 s push — long tail, holds locks.
Sweet spot 10–50; the default is 10.

## Landmines

Each of these cost a debugging cycle and is worth knowing before you edit.

- **Never pass `--sort` to `for-each-ref`.** It reads and sorts every matching
  ref before returning even one: 1,522 ms vs **14 ms** at 100k refs. `reftable`
  already returns name order.
- **Exit status is not enough to tell "created" from "already there."** Pushing
  a ref that already points at the identical blob succeeds with porcelain flag
  `=`, because git skips the update and so never evaluates the lease. Idempotent
  enqueue checks for `*`. This was a real bug the tests caught.
- **`git clone` does not inherit reftable.** Pass `--ref-format=reftable`
  explicitly, or you silently get the slow backend.
- **Never treat an unreadable lease as expired.** `ls-remote` returns oids this
  store may not hold; reading one fails. Fetch first, and on failure assume the
  lease is held. Getting this backwards made every worker steal every shard —
  also caught by the tests.
- **A `+` in a refspec silently overrides `--force-with-lease`.** This is the
  one that matters most. `+<oid>:<ref>` turns a compare-and-swap into a blind
  overwrite, with no warning and no error. Measured: 10 workers racing for one
  ref produced **~6 winners with `+`, exactly 1 without** -- identical on both
  the `files` and `reftable` backends. The lease alone already authorises a
  non-fast-forward swap, so `+` is never needed alongside one. The same trap
  applies to `git push --force-with-lease --force` at a human keyboard.
- **Objects accumulate.** Every state transition writes a blob. Run
  `./gitq-cli gc` on a schedule -- but never `--prune=now` on a live hub: job
  blobs are reachable only from their refs, so aggressive pruning can delete an
  object a worker has pushed but not yet pointed a ref at. The default grace
  period is the point of it.
- **Two pushes are not a transaction.** Cron once wrote its period marker and
  then enqueued the job separately; a crash in between left a marker with no
  job, silently skipping that period. Anything that must happen together goes
  in one `push --atomic`.
- **A running job must renew its lease.** The handler runs on its own thread
  while `_heartbeat` renews the claim underneath it. Without that, any job
  outliving `lease_s` was reclaimed and run twice -- breaking exactly-once for
  precisely the long jobs that most depend on it.
- **A worker holds two leases, not one.** `_execute` blocks the poll loop, so a
  job outliving `shard_lease_s` used to let this worker's *shards* expire while
  its job lease stayed healthy -- the worker went on working and found out only
  on return. Both are now refreshed from the same `_heartbeat`, paced off the
  shorter of the two.
- **The handler thread is a daemon on purpose.** When the lease is lost the
  handler is abandoned where it stands. A thread pool would be tidier right up
  until `shutdown()` blocked on the future for the job we no longer own.
- **A worker must survive a failed git command.** Not every failure is about
  this queue: the hub can be momentarily busy, and a machine under load can
  simply run out of process slots (`cannot fork() for git-upload-pack`). Dying
  on the first one and never coming back is worse than any of them. `poll`
  degrades to the mirror already on disk -- safe precisely because a claim is a
  compare-and-swap, so a stale oid costs a rejected push and never a double run.
- **A dropped tally looks exactly like a lost job.** The contention test
  collected each thread's claims at the end, so a thread that died mid-round
  took its record of successful claims with it, and the test reported job loss
  the queue had not committed. CI on a 2-core runner found this; nothing on a
  developer machine ever did.
- **A blob pushed to `refs/heads/*` is rejected by git anyway** ("trying to
  write non-commit object to branch"). Testing the namespace guard with a job
  payload therefore proves nothing; `tests/test_hub_hook.py` builds a real
  commit so the hook is what does the rejecting.

## Cost of a poll

Every git operation is a process, and on the numbers below a fork costs about a
thousand times more than the work it wraps -- reading a job payload takes ~3us
inside a `cat-file` that takes ~9ms to start. So the unit that matters is not
milliseconds, it is *how many processes a poll costs*.

| operation | processes before | after |
|---|---|---|
| worker poll + claim, 100 jobs over 16 shards | 248 | **65** |
| reaper sweep, 100 claims | 135 | **36** |

Three changes, all of them "do the same work in one process instead of N":

- `cat-file --batch` reads every payload at once. A 100-claim sweep went from
  ~944ms of forking to ~10ms of reading.
- `hash-object --stdin-paths` writes every claim at once.
- one `fetch` carries every held shard's refspec. A fetch costs ~26ms of
  connection and ref advertisement whether it carries one refspec or sixteen,
  so sixteen shards cost ~412ms separately and ~28ms together.

The last one is only safe because `--prune` stays scoped to the destinations of
the refspecs actually passed, so it prunes exactly what the per-shard loop
pruned. That was verified against partial deletion, a fully emptied shard and
orphaned local refs before it was relied on, and there is a test for it. The
corollary is a real constraint on callers: **pass every shard whose mirror
should be current**, because one left out is one that silently goes stale.

Not done: resolving `git` to an absolute path. On macOS `["git", ...]` finds
the `xcrun` shim, which costs ~5.8ms per call over the real binary -- a large
win, and an entirely macOS-specific one. Not worth platform-specific tuning in
a library whose portability is the thing CI checks.

## What this does not have

Deliberate omissions, and the reason to reach for Postgres instead:

- **No `LISTEN/NOTIFY`.** Workers poll; latency floor is the poll interval.
- **No server-side lease expiry.** `reaper.py` is a client-side substitute and
  only runs while some worker is alive.
- **Network latency is untested.** Every number here is against a local bare
  repo. Over SSH, handshake per push will dominate — batching should help more
  there, but that is an expectation, not a measurement.
- **No fan-out, priority classes, or rate limiting** beyond the `<prio>` sort key.

### Known limitations

Real, understood, and not fixed -- weigh them before trusting this with
anything you cannot afford to run twice.

- **A reaper's own clock can still step.** Staleness is an interval measured
  on one clock (see *Liveness by observation*), so cross-machine skew is a
  non-issue -- but an NTP step on the reaper itself can make an interval look
  longer than it was. Far narrower than the original exposure, not zero.
- **Reclamation is slower than a deadline-based reaper.** A claim must be
  watched, unchanged, for the lease duration starting from *first sighting*,
  not from when it was claimed. A restarted reaper begins that window again.
  It errs toward leaving jobs alone, which is the safe direction.
- **`reaper_id` must be unique per reaper and stable across restarts.** Two
  reapers sharing an id overwrite each other's sightings. `Worker` uses its own
  name; the CLI uses `cli`.
- **`safe_key` truncates at 120 characters.** Two keys sharing a 120-char
  prefix collide, and the second enqueue is silently treated as a duplicate.
  Hash long keys yourself before passing them in.

Past a few thousand jobs/hour, or if you need sub-second dispatch, use
[pg-boss](https://github.com/timgit/pg-boss), [Procrastinate](https://github.com/procrastinate-org/procrastinate),
or [Graphile Worker](https://github.com/graphile/worker). They ship all of the above.

## Layout

```
gitq/git.py       git plumbing; RefTxn builds writes, push_atomic applies them
gitq/queue.py     ref naming, job encoding, claim/complete/fail/renew
gitq/shards.py    shard ownership leases (steal on expiry)
gitq/reaper.py    reclaim dead workers' jobs, by observation not deadline
gitq/cron.py      5-field cron + leaderless per-period dedup
gitq/worker.py    the poll loop; one heartbeat for both leases
gitq/setup_repo.py  hub + worker stores, and the hub's pre-receive hook
tests/test_gitq.py   15 assertions, real thread contention
tests/test_bugs.py   8 regressions: cron atomicity, lease renewal, clock skew
tests/harness.py     shared fixtures for the four suites
tests/test_invariants.py  28 properties mutation testing proved were unchecked
tests/test_hub_hook.py    9 checks that the hub enforces its own namespace
bench/bench.py       sharded vs unsharded
bench/skew_probe.py  which guarantees survive a 5-minute clock skew
tools/mutate.py      removes one guarantee at a time, checks a test notices
tools/run_tests.py   runs all four suites, one process each, one verdict
Dockerfile           the Linux test environment; pins a git that has reftable
```

## Mutation testing

Passing tests are not evidence that the tests test anything. `tools/mutate.py`
removes one guarantee at a time -- drop `--atomic`, strip the leases, revert a
fix -- and reports whether any test notices. A survivor names a behaviour
nothing is checking.

```bash
python3 tools/mutate.py            # 26 mutations, ~12 min
python3 tools/mutate.py --only no-cas
```

The first sweep scored **7 killed, 7 survived**, and three of the survivors
were tests written specifically to catch the bug they failed to catch:

- the crash-recovery test patched `q.enqueue`, which cron stopped calling once
  the fix landed, so it passed while asserting nothing
- the long-job test called the reaper once, and the observation-based reaper
  never reclaims on a first sighting, so it could not fail
- a shard-lease assertion intersected two sets already proven disjoint, so it
  was unfalsifiable

Repairing those and adding `tests/test_invariants.py` brought it to **18 killed,
0 survived**. The concurrent-steal test added in that pass is what exposed the
`+`-overrides-the-lease bug above -- a real defect in shipped code, found
because a mutation showed nothing was racing that path.

The `RefTxn` pass made three of those mutations unrepresentable -- there is no
longer a way to write the code they described -- so they were replaced with
mutations against the new guarantees, plus one that disables the hub hook, two
for surviving transient git failures, and two for the batch object and fetch
paths. The suite now stands at **26 killed, 0 survived**.

Twenty-four of those kill by failing a named check. Two kill by crashing the
suite instead -- `claim-unleased-claimed-ref`, which has to forge a refspec
past `RefTxn` to express the old bug at all and is refused outright when it
does, and `safe-key-nosanitize`, where git rejects the malformed ref name
before any assertion is reached. A crash is a weaker signal than a named
failure, so it is worth knowing which is which.

Run it on a machine with disk headroom. The sweep restores each file after
mutating it, and if that write fails -- a full disk, most plausibly -- every
remaining mutation "passes" instantly against a broken tree and the run reports
kills it did not earn. It says `RESTORE FAILED` when this happens. Believe it;
that line invalidates the whole run, including the total.

## Tests

```bash
python3 tests/test_gitq.py       # 15 passed, 0 failed
python3 tests/test_bugs.py       #  8 passed, 0 failed
python3 tests/test_invariants.py # 28 passed, 0 failed
python3 tests/test_hub_hook.py   #  9 passed, 0 failed
python3 tools/mutate.py          # 26 killed, 0 survived
python3 bench/bench.py 8 200
python3 bench/skew_probe.py
```

Or all four suites at once, with one verdict:

```bash
python3 tools/run_tests.py
```

### On Linux

Development here happens on macOS; CI and the Dockerfile are the check that
nothing has quietly become macOS-specific. The image exists mostly to pin a
git new enough for `reftable` -- `--ref-format=reftable` landed in 2.45, and
distro gits are often older. The build fails outright if the base image ever
drifts below that, rather than halfway through a suite.

```bash
docker build -t gitq-test .
docker run --rm gitq-test                          # all four suites
docker run --rm gitq-test python3 tools/mutate.py  # the full sweep, ~10 min
```

Verified on Alpine, git 2.54.0, Python 3.13, musl: 60 checks, 0 failed.

CI runs exactly that image on every pull request, and the mutation sweep
weekly -- it is too slow per-commit, and "a test stopped testing anything" is
not a per-commit problem.

## Repo identity

This lives under `~/github/stuffbucket/`, so `~/.gitconfig`'s conditional
include supplies the `stuffbucket` identity automatically. There is
deliberately **no local `user.name`/`user.email`** here, matching the sibling
repos -- setting one would defeat the point of the directory-based scheme.

## Working on this repo

**Squash-merge into `main`.** The value of this history is that each change is
tied to a measurement. One commit per logical change, with the number that
motivated it in the body, keeps that legible; `fix test` / `typo` / `wip` does
not. If a branch contains genuinely independent changes, split the PR rather
than squashing them into one misleading commit.

**Rebase feature branches onto `main`; never merge `main` into a branch.** The
bugs in this codebase are concurrency bugs surfaced by stress and mutation
testing -- exactly the class where `git bisect` earns its keep, and bisect wants
a linear history.

**Never force-push `main`.** On your own feature branch after a rebase,
`--force-with-lease` is correct -- but per the landmine above, do not combine it
with `--force` or a `+` refspec, or the lease is silently disabled and you have
an unconditional force push.

**Bar for merge:** all three suites green *and* `tools/mutate.py` reporting zero
survivors. A new guarantee without a mutation that kills it is untested by
default.

Identity comes from `~/.gitconfig`'s conditional include for
`~/github/stuffbucket/`; there is deliberately no local `user.*` here.

## License

MIT. See [LICENSE](LICENSE).
