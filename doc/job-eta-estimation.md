# Job completion-time estimation (design)

Status: **proposal** — not yet implemented.

## Motivation

A producer sometimes enqueues a job that an end user is actively waiting for
("we're preparing your export…"). For those jobs it is useful to answer:

> How long until this job finishes?

For a given job, the answer depends on:

- the estimated duration of the job itself;
- all jobs ahead of it in its tube (equal or lower `pri`, and for equal `pri`,
  a lower id);
- how many workers are watching that tube;
- for each of those workers, which *other* tubes they also watch and how much
  work is queued there (a shared worker's capacity is split by dispatch order,
  not evenly);
- any per-tube concurrency cap set with `limit-tube`;
- jobs currently reserved, and how far through their work they are.

None of this is derivable from existing stats. This document proposes a small
protocol extension: producers may attach a duration estimate to a job at put
time, and any client may ask the server for a job's estimated time to
completion (ETA).

## Why a simulation, not a formula

Beanstalkd's dispatch rule is deterministic and simple (`next_awaited_job` in
`prot.c`): when a worker is waiting, it receives the job with the smallest
`(pri, id)` across the tubes *it* watches, subject to each tube's
`reserve_limit`. Given a snapshot of the queues, the workers, and their watch
sets, the entire future dispatch order is fully determined (up to which
identical worker takes which job — which does not affect any job's ETA).

Closed-form models break down exactly where accuracy matters. A per-tube
formula (`work ahead ÷ workers watching`) is wrong whenever watch sets
overlap: the share of a worker's capacity that a tube receives depends on the
priority-interleaved contents of every other tube that worker watches, and
that allocation shifts over time as tubes drain at different rates. Adding
`reserve_limit` caps makes the coupling worse. Modelling this analytically is
a fixed-point problem over time — i.e. a simulation wearing a disguise.

So the design replays the real dispatch rule directly: a discrete-event
simulation over a snapshot of all ready jobs and all worker connections. One
global pass stamps an ETA on **every** job — it costs no more asymptotically
than estimating a single deep job, and it lets subsequent queries be O(1).
The pass is run on demand and cached; clients state how much staleness they
will tolerate.

## Protocol additions

Every duration in this extension — job estimates, ETAs, snapshot ages, and
the estimate commands' `max-age` argument — is expressed in
**milliseconds**: one unit throughout, no per-field exceptions to remember.
(This differs deliberately from `delay`/`ttr`, which the existing protocol
expresses in seconds: service times are commonly sub-second, and second
granularity would destroy the estimates' usefulness.)

### put-est

Identical to `put`, with one extra argument:

    put-est <pri> <delay> <ttr> <est> <bytes>\r\n
    <data>\r\n

- `<est>` is the producer's estimate of how long the job will take a worker
  to process, in milliseconds (uint32; max ~49.7 days). `0` means "unknown" —
  the server falls back to the tube's learned average (below).

Responses are identical to `put`. Plain `put` remains unchanged and behaves
exactly like `put-est` with `<est>` = 0.

### estimate-job

    estimate-job <id> [<max-age>]\r\n

- `<id>` is a job id.
- `<max-age>` (optional) is the maximum acceptable staleness of the answer,
  in milliseconds. If omitted, a server default applies (5000, configurable
  by flag). `0` demands a fully fresh answer.

Responses:

- `OK <bytes>\r\n<data>\r\n` — `<data>` is a YAML dictionary, following the
  stats commands' reply format:
  - `est` — the job's effective duration estimate in milliseconds: the
    producer-supplied value, or — if the job was put without one — the
    tube's *current* fallback (below), resolved at reply time;
  - `etd-unixtime-ms` — estimated time of dispatch: the epoch-ms Unix
    timestamp at which a worker is expected to start the job. For a
    reserved job this is its *actual* start time (stamped into `etd_at`
    when the job was reserved), typically in the past;
  - `eta-unixtime-ms` — estimated completion time, epoch ms:
    `etd-unixtime-ms + est`, except that an overrunning reserved job (one
    already past its estimate) reports the snapshot time instead — a job
    past due is always "expected momentarily";
  - `queue-position` — the job's place in its tube's dispatch order: `0`
    means running now (reserved); `k ≥ 1` means k-th in line among the
    tube's not-yet-started jobs. Within a tube this is simply the job's
    rank in `(pri, id)` order, so it is exact, not simulated;
  - `age-ms` — staleness of the snapshot behind this answer (≤ `max-age`);
  - `now-unixtime-ms` — the server clock at reply time. Clients derive
    relative times as `…-unixtime-ms − now-unixtime-ms`, which is immune to
    client/server clock skew (a "starts in Xs, done Ys later" display needs
    exactly this plus `etd`/`eta`).

  `etd-unixtime-ms`, `eta-unixtime-ms`, and `queue-position` are all `-1`
  if the job is buried, delayed, or unreachable (no watcher,
  `reserve_limit 0`, indefinite pause).

  Each reply is self-consistent by construction (`eta` is computed from the
  same `est` it reports), but when serving a stale snapshot, a fallback
  job's `est` uses the current EWMA rather than the value the simulation
  used — so *across* replies, adjacent jobs' time windows may slightly
  overlap or gap, and a job's `eta` may differ slightly from its tube's
  frozen aggregates. Accepted deliberately: no consumer schedules against
  cross-job comparisons, and the fresher EWMA is the better estimate.
- `NOT_FOUND\r\n` — no such job.
- `NOT_ESTIMATED\r\n` — the server cannot currently produce an estimate
  (e.g. the ready-job count exceeds the safety cap, below).

### estimate-tube

    estimate-tube <tube> [<max-age>]\r\n

`<max-age>` as above. Responses `NOT_FOUND` (no such tube) and
`NOT_ESTIMATED` as above, or a YAML dictionary:

- `est-work-all-ms` — total expected work currently on the tube: the sum of
  estimates of its ready jobs plus the estimated remaining time of its
  reserved jobs;
- `est-work-urgent-ms` — the same, restricted to urgent jobs (`pri` < 1024,
  matching `current-jobs-urgent`);
- `eta-all-unixtime-ms` — the tube's projected drain time: the epoch-ms
  timestamp at which the last currently-known job of the tube completes;
  `-1` if the tube contains unreachable jobs (the work sums are still
  reported — work is defined regardless of reachability);
- `eta-urgent-unixtime-ms` — the same, over urgent jobs only;
- `age-ms`, `now-unixtime-ms` — as in `estimate-job`.

These per-tube aggregates are intended primarily as inputs to worker
autoscaling. Scaling on ready-job *counts* (the only signal the existing
stats offer) misjudges any tube whose job durations vary: a thousand 10 ms
jobs and ten 100 s jobs look identical to a count but need very different
capacity. The two field families answer different questions:

- The `est-work-*` sums are **capacity-independent** — pure backlog volume
  in milliseconds of work. This is the natural primary scaling signal:
  `target workers ≈ est-work ÷ desired drain time`.
- The `eta-*` timestamps are **capacity-dependent** — they bake in the
  current worker pool, its watch-set overlaps, and `reserve_limit` caps.
  They serve as the SLO check ("will urgent work drain by the deadline at
  current capacity?") that triggers scaling when breached. Note the
  feedback loop: adding workers shortens ETAs on the next simulation, so
  ETA-based rules should compare against a target *time*, not a target
  rate, or they will oscillate.

### Reply format and the freshness contract

Dictionary replies — rather than a positional `ETA <n> <m>` line — follow
the stats idiom and leave room to add fields later (work ahead of a
specific job, confidence bounds) without breaking a single existing
client.

Both commands promise exactly one thing about their implementation: **the
answer is derived from a snapshot of queue state no older than `max-age`
milliseconds at reply time.** Today the server satisfies this by invisibly
running one global simulation and caching its per-job stamps and per-tube
aggregates, which both commands read — so a call costs O(N log N) or O(1)
depending on cache state, and concurrent pollers of different jobs and
tubes share one simulation per freshness window.

Keeping that hidden is the point of the command shape. Because callers ask
about one job or one tube — rather than commanding "simulate" — the
implementation retains latitude to change. With many millions of jobs and a
query about a job near the front of the queue, a future implementation may
terminate the simulation early, as soon as the queried job (or the queried
tube's last job) has been assigned; a partial run then needs coverage
bookkeeping so it only answers queries it actually reached, falling through
to a fuller run otherwise. Incremental or analytical implementations would
be equally conformant. None of these changes would touch the protocol.

### Stats extensions

- `stats-job` gains `est: <ms>` — the stored per-job estimate (0 if none
  was supplied). This is a static attribute of the job, like `ttr`, so it
  belongs with the other stats.
- `stats-tube` gains `avg-service-time: <ms>` — the learned average
  service time (below): likewise a plain attribute, not a simulation
  output.

ETAs and work totals are deliberately **not** exposed through the stats
commands: an ETA is meaningless without a freshness bound, and stats has no
`max-age` argument. `estimate-job` and `estimate-tube` are the single read
path for everything the simulation produces. (The internal clock is
`gettimeofday`-based epoch nanoseconds, so the epoch-ms timestamps in their
replies are pure unit conversions of the stored values.)

## Fallback estimates: learned per-tube service time

Estimates should degrade gracefully when producers don't supply them. The
server maintains, per tube, an exponentially weighted moving average (EWMA)
of *observed* service time: on `delete` of a reserved job, the sample is
`now − etd_at`, where `etd_at` is the job's actual start time, stamped at
reservation. A smoothing factor of 1/8 is proposed.

Every completed job contributes a sample — including jobs that carried a
producer estimate. The EWMA tracks what the tube's job mix *actually* costs,
independent of what producers claim; feeding it only estimate-less jobs would
bias it toward whatever unrepresentative subset lacks estimates. (The
estimate is only ever a per-job override at simulation time; it plays no role
in learning.)

A job whose stored estimate is 0 uses its tube's EWMA in the simulation. If
the tube has no samples yet, a fixed default (1000 ms) applies. This makes
the feature useful immediately — even with no cooperating producers, ETAs are
roughly right for tubes with homogeneous jobs — and self-correcting as the
job mix changes.

Note on `touch`: an earlier draft recovered the start time as
`deadline_at − ttr`, which `touch` corrupts, forcing touched jobs out of
the sample set — a severe bias for workloads that touch routinely. Stamping
the start into `etd_at` at reservation removes the problem: every completed
job contributes a correct sample regardless of `touch`, and the reserved
jobs' remaining-time computation in the simulation is `touch`-proof too.

## The simulation

### Snapshot

Taken at simulation start (time T0), entirely from in-memory state:

1. **Per tube: an eagerly sorted copy of the ready set.** Copy the tube's
   `ready.data` pointer array (`memcpy`, O(n)) and sort it by `(pri, id)`
   with `qsort(3)`. Consumption during the simulation is then a bare integer
   cursor per tube: peek = `jobs[cursor]`, consume = `cursor++`.

   Two implementation traps here:

   - **Never pop the live heaps.** `heapremove` invokes `setpos`, which
     writes `heap_index` back into the live `Job` structs and would corrupt
     the real queue's bookkeeping. Work only on the copied pointer array.
   - **Don't reuse `job_pri_less` as the qsort comparator.** qsort hands the
     comparator pointers to the array *slots* (`Job **`), not the `Job *`
     values; a fresh comparator must dereference one extra level.

   Why eager sort rather than lazily popping the copied heap: the simulation
   provably visits every job, so the total comparison work is unavoidable
   either way, and heap order gives a comparison sort essentially no head
   start (it fixes only Θ(n) bits of the ~n·log n needed; a valid heap can
   still contain Θ(n²) inversions). n pop-mins is heapsort's selection phase
   at ~2·n·log n comparisons with two cold `Job` dereferences each; qsort
   averages ~1.4·n·log n comparisons, one side of each being the cached
   pivot, with sequential array access. Eager qsort is both faster and less
   code.

2. **Per tube: bookkeeping.** Simulated in-flight count seeded from
   `stat.reserved_ct` (for `reserve_limit` enforcement); an availability
   floor of `unpause_at` if the tube is paused.

3. **The worker pool.** Every connection with `CONN_TYPE_WORKER`, with its
   watch set. Its free-at time is T0 if it is waiting or idle. If it holds
   reservations, each reserved job contributes remaining time
   `max(0, est − (T0 − etd_at))`, where `etd_at` is its actual start time
   stamped at reservation (`touch`-proof, unlike deriving it from
   `deadline_at − ttr`); the worker's free-at is T0 plus the sum.

Buried jobs are ignored. Delayed jobs are ignored in v1 (see Limitations).

### Event loop

Workers sit in a min-heap keyed by free-at time. Repeat until every tube's
cursor is exhausted or no remaining job is reachable:

1. Pop the earliest-free worker (time t).
2. Scan its watched tubes' cursors: the candidate is the smallest `(pri, id)`
   head among tubes that are not blocked (in-flight < `reserve_limit`, and
   `unpause_at` ≤ t). This mirrors `next_awaited_job` exactly.
3. If a candidate exists: stamp `job.etd_at = t` (absolute time) and
   `job.queue_pos` = the tube's cursor position before advancing + 1
   (i.e. its 1-based rank among the tube's not-yet-started jobs); advance
   that tube's cursor, increment its in-flight count, and push the worker
   back with free-at `t + est(job)`, where `est(job)` is the stored
   estimate or the tube fallback. Neither the completion time nor the
   effective estimate is stored: `estimate-job` re-resolves the estimate at
   reply time and derives `eta = max(etd_at + est, T0)` (the max only
   binds for overrunning reserved jobs). Only `etd_at` is genuinely
   dispatch-time state (simulated here; actual, stamped at reservation, for
   reserved jobs); everything else is derivable, and a stored copy could
   only ever agree or be a bug. Any state transition through `enqueue_job`
   (put, release, timeout, kick) resets `etd_at`/`queue_pos` to `-1`, so a
   stale stamp can never masquerade as current. (For fallback jobs served
   from a stale snapshot the re-resolved EWMA may differ from the one the
   pass used — accepted; see the reply-format notes.)
4. When a simulated job completes (i.e. whenever a worker is popped whose
   previous job was in tube T), decrement T's in-flight count. If T was
   blocked at its `reserve_limit` and has parked workers (step 5), wake them.
5. If the worker found no candidate (all watched tubes empty or blocked),
   park it on each blocked-but-nonempty tube it watches. Parked workers are
   re-offered work only when one of those tubes unblocks — this keeps the
   loop O((N + W) · (log W + T)) instead of re-scanning all parked workers
   on every event.

Jobs never reached (unwatched tube, `reserve_limit 0`, indefinite pause) keep
`etd_at = -1` and `queue_pos = -1`, which `estimate-job` reports as-is
(with `eta-unixtime-ms` also `-1`).

During the pass the loop also accumulates the per-tube aggregates served by
`estimate-tube` — running maxima of completion time (`etd_at + est(job)`,
using the estimates in effect during the pass) over all jobs and over
urgent jobs, and total expected work for both groups — at O(1) per
assignment (reserved jobs' remaining times, computed during the snapshot,
contribute too).

Which physical worker receives a given job is deliberately not modelled
beyond watch sets (the live server picks among a tube's waiting connections
in `ms_take` rotation order); it has no effect on job completion times when
workers are interchangeable within a watch set.

### Caching

The simulation stamps every job's `etd_at` and `queue_pos`, refreshes the
per-tube aggregates, and records a global snapshot time `sim_done_at`
(which is also the T0 in the ETA derivation). An `estimate-job` or
`estimate-tube` whose `max-age` is satisfied by `now − sim_done_at` answers
from the stamps at O(1); otherwise it re-runs the simulation first. Both
commands share the one cache, so mixed pollers cost one simulation per
freshness window. Because every reported ETA is an absolute timestamp,
cached results age gracefully — a value read 3 s later is simply 3 s
closer — rather than going stale abruptly.

### Cost

For N ready jobs, W worker connections, and T tubes:

- sorting: Σ n_t log n_t pointer comparisons, sequential access;
- event loop: N assignments × (O(log W) heap ops + O(T) cursor scan);
- transient memory: N copied pointers + W heap entries;
- permanent memory: 4 bytes per job (`est`, fits in the existing `Job`
  padding) + 12 bytes per job (`etd_at`, `queue_pos`; alignment will
  likely pad this to 16) + a few words per tube.

At N = 100 000 this is a few milliseconds — but it runs on the single event
loop and blocks all other traffic while it runs. Two guardrails:

- the freshness cache bounds how often it can run (a client-supplied
  `max-age` of 0 still forces it, but a hostile client could equally spam
  `stats`);
- a configurable cap on total ready jobs (default 1 000 000, `-e` flag;
  `-e0` disables the cap for operators who prefer a stall to a refusal),
  above which the estimate commands return `NOT_ESTIMATED` rather than
  stall the server. (Early termination, if implemented later, would raise
  the practical ceiling for near-front queries — another reason the
  commands don't promise a global pass.)

## Persistence

`est` is **not** written to the WAL in v1, following the precedent of
`reserve_limit` (also not persisted). After a restart, recovered jobs have
`est = 0` and use the tube EWMA fallback (which also restarts empty and
re-learns). If restart survival is later wanted, `Jobrec` gains a field via
the documented `Walver` bump workflow in `dat.h`.

## Assumptions and limitations

The estimate answers: *if nothing new arrives and nothing fails, when does
this job finish?* Specifically it assumes:

- **No future arrivals.** Jobs put after the snapshot with lower `(pri, id)`
  will push real completion later than estimated. Clients are expected to
  re-poll; each poll re-syncs with reality, so the number self-corrects
  rather than drifting. Display as "about N minutes", not a countdown.
- **No failures.** TTR expiries, `release`, `bury`, and worker crashes are
  not modelled; every job is assumed to succeed in its estimated time.
- **A static worker pool.** Workers present at the snapshot are assumed to
  keep reserving until the horizon; workers that join later make estimates
  pessimistic, workers that leave make them optimistic.
- **Sticky worker classification.** `CONN_TYPE_WORKER` is set by the first
  `reserve` and never cleared, so a connection that reserved once and then
  became a pure producer still counts as capacity. Dedicated worker
  connections (the common deployment) are unaffected.
- **Delayed jobs are invisible (v1).** A delayed job that will mature with a
  low `(pri, id)` mid-horizon would jump the simulated queue; modelling this
  needs a per-tube maturation event stream merged into the loop. Deferred:
  delay is dominated by retry/backoff traffic, and the added machinery isn't
  justified yet. Delayed jobs themselves report `eta-unixtime-ms: -1`.
- **Estimate quality is the producer's problem.** Garbage in, garbage out;
  the EWMA fallback bounds how wrong "unknown" jobs can be.

## Implementation notes

- New command constants and dispatch in `prot.c`; the `limit-tube` /
  `unlimit-tube` commit is the template for the plumbing. Note that
  `estimate-tube <200-char name> <max-age>` is longer than the current
  longest command line, so `LINE_BUF_SIZE` must grow (its defining comment
  in `dat.h` names the longest line; update both).
- `Job` gains `uint32 est_ms` (fits in the existing 6-byte pad) plus
  `int64 etd_at` and `int32 queue_pos` (in-memory only). `est_ms` is what
  the producer said (0 = unknown, reported raw by `stats-job`); the
  effective estimate is resolved from it (or the tube EWMA) on demand,
  never stored. Note that when a call triggers a fresh simulation, the
  reply's resolution is guaranteed identical to the pass's — the server is
  single-threaded, so the EWMA cannot move within one event-loop tick.
- Internal times are nanoseconds (`nanoseconds()`); all protocol
  conversions happen at the parse/reply boundary. Ranges: uint32 ms ≈ 49.7
  days ≪ int64 ns overflow; no truncation hazards.
- Tests in `testserv.c`: `put-est` round-trip and the new stats fields;
  EWMA learning via reserve/delete cycles; simulation scenarios with
  hand-computed ETAs read back via `estimate-job` / `estimate-tube` —
  single tube/single worker FIFO; priority interleave within a tube; two
  tubes sharing one worker (the overlap case); a tube at its `reserve_limit`
  with idle workers parked; reserved-job remaining time (including its
  past-tense `etd`); the `eta = etd + est` invariant, including the overrun
  clamp to snapshot time; a fallback job's `est` tracking the current EWMA
  when served from a stale snapshot; dense, correctly ordered
  `queue-position` values within a tube; per-tube drain
  and work aggregates including the urgent split; cache freshness (`max-age`
  honoured, omitted `max-age` uses the server default, `0` forces a
  re-run); `NOT_FOUND` and `NOT_ESTIMATED` replies.

## Alternatives considered

- **Incremental analytical model** (per-tube Fenwick trees over priority
  buckets, `ETA ≈ max(tube work ÷ tube cap, global work ÷ total workers)`):
  O(log P) always-fresh queries with no simulation pause, but wrong under
  overlapping watch sets and interleaved priorities — precisely the hard
  cases — and it puts machinery on the hot put/delete path. Rejected as the
  starting point; remains the fallback if the simulation pause ever proves
  problematic.
- **Client-side estimation** (mirror the queue in an external store, model
  there): keeps the server untouched, but the mirror drifts from truth on
  every timeout, bury, kick, and priority-changing release — the server
  already holds the exact queue and can answer honestly. Rejected.
- **On-demand partial simulation** (simulate only until the queried job or
  tube is reached): saves nothing in the worst case and needs coverage
  bookkeeping, so v1 always runs the global pass — it is simpler and
  amortises across all waiting clients. Not rejected, though: the command
  interface deliberately avoids promising a global pass, reserving early
  termination as a future optimisation for very large backlogs (see the
  freshness contract).
