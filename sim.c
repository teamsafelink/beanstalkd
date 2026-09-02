// Job completion-time estimation. See doc/job-eta-estimation.md.
//
// This file owns everything behind the estimate-job/estimate-tube
// commands except the protocol plumbing itself (which lives in prot.c):
// the registry of worker connections, the per-tube learned service-time
// average, the resolution of a job's effective duration estimate, the
// freshness cache, and the discrete-event simulation that replays the
// dispatch rule over a snapshot of the ready queues.

#include "dat.h"
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

size_t sim_max_ready_jobs = 1000000;
uint64 sim_default_max_age_ms = 5000;

// When the last simulation ran (epoch ns); 0 = never.
static int64 sim_done_at = 0;

// Every connection that has ever issued a reserve command. Maintained by
// connsetworker/connclose. This is the simulated worker pool.
static struct Ms workers;

void
sim_init(void)
{
    ms_init(&workers, NULL, NULL);
    sim_done_at = 0;
}

void
sim_register_worker(Conn *c)
{
    ms_append(&workers, c);
}

void
sim_forget_worker(Conn *c)
{
    ms_remove(&workers, c);
}

int64
sim_last_run(void)
{
    return sim_done_at;
}

// Record one observed service time in the tube's EWMA (alpha = 1/8).
// Every completed job contributes a sample, whether or not it carried a
// producer estimate; the average tracks what the tube's job mix actually
// costs.
void
sim_observe_service_time(Tube *t, int64 service_ns)
{
    if (service_ns <= 0)
        return;
    if (t->avg_service_ns)
        t->avg_service_ns = (t->avg_service_ns * 7 + service_ns) / 8;
    else
        t->avg_service_ns = service_ns;
}

// The effective duration estimate for a job, in ms: the producer's value,
// else the tube's learned average, else a fixed default. Never stored;
// resolved on demand (both during a pass and at reply time).
uint32
sim_effective_est_ms(Job *j)
{
    if (j->est_ms)
        return j->est_ms;
    if (j->tube && j->tube->avg_service_ns) {
        int64 ms = j->tube->avg_service_ns / 1000000;
        if (ms < 1)
            ms = 1;
        if (ms > UINT32_MAX)
            ms = UINT32_MAX;
        return (uint32)ms;
    }
    return SIM_DEFAULT_EST_MS;
}


typedef struct SimWorker SimWorker;
typedef struct SimTube SimTube;

struct SimWorker {
    Conn  *conn;
    int64 free_at;
    Tube  *last_tube;  // tube of the job this worker just finished
    byte  initial;     // still completing its pre-existing reservations
    byte  parked;      // waiting for a reserve_limit slot; not in the heap
    size_t heappos;
};

struct SimTube {
    Job    **jobs;     // ready jobs, sorted by (pri, id)
    size_t len;
    size_t cursor;     // next job to dispatch; consumed jobs precede it
    int64  inflight;   // simulated reservation count (reserve_limit)
    int64  avail_at;   // pause floor: no dispatch from this tube before it
    SimWorker **parked;
    size_t nparked;
    size_t capparked;
};

static int
simworker_less(void *wa, void *wb)
{
    return ((SimWorker *)wa)->free_at < ((SimWorker *)wb)->free_at;
}

static void
simworker_setpos(void *w, size_t i)
{
    ((SimWorker *)w)->heappos = i;
}

// qsort comparator over an array of Job* — note the extra indirection:
// qsort hands us pointers to the array slots, not the Job* values.
static int
simjob_cmp(const void *a, const void *b)
{
    const Job *ja = *(Job * const *)a;
    const Job *jb = *(Job * const *)b;

    if (ja->r.pri != jb->r.pri)
        return ja->r.pri < jb->r.pri ? -1 : 1;
    if (ja->r.id != jb->r.id)
        return ja->r.id < jb->r.id ? -1 : 1;
    return 0;
}

static int
park(SimTube *s, SimWorker *w)
{
    if (s->nparked == s->capparked) {
        size_t ncap = (s->nparked + 1) * 2;
        SimWorker **np = realloc(s->parked, ncap * sizeof(SimWorker *));
        if (!np)
            return 0;
        s->parked = np;
        s->capparked = ncap;
    }
    s->parked[s->nparked++] = w;
    return 1;
}

// One reservation slot opened up on this tube at time now: wake at most
// one parked worker. Workers parked on several blocked tubes appear in
// several parked lists; the parked flag makes extra entries harmless.
static int
wake_one_parked(SimTube *s, Heap *wheap, int64 now)
{
    while (s->nparked) {
        SimWorker *w = s->parked[--s->nparked];
        if (!w->parked)
            continue;
        w->parked = 0;
        w->free_at = now;
        return heapinsert(wheap, w);
    }
    return 1;
}

// A simulated job of tube t finished at time now.
static int
sim_complete(SimTube *st, Heap *wheap, Tube *t, int64 now)
{
    SimTube *s = &st[t->sim_index];

    s->inflight--;
    if (s->cursor < s->len &&
        (t->reserve_limit < 0 || s->inflight < t->reserve_limit))
        return wake_one_parked(s, wheap, now);
    return 1;
}

// Run the global simulation over a snapshot taken at t0, stamping every
// reachable job's etd_at/queue_pos and every tube's aggregates.
// Returns 1 on success; 0 if the server refused (too many ready jobs)
// or ran out of memory. On 0, sim_done_at is left unchanged.
static int
sim_run(int64 t0)
{
    size_t i, k;
    int ok = 0;
    size_t ntubes = tubes.len;
    size_t nworkers = workers.len;
    SimTube *st = NULL;
    SimWorker *sw = NULL;
    Heap wheap = {0};

    size_t nready = 0;
    for (i = 0; i < ntubes; i++)
        nready += ((Tube *)tubes.items[i])->ready.len;
    if (nready > sim_max_ready_jobs)
        return 0;

    st = calloc(ntubes, sizeof(SimTube));
    sw = calloc(nworkers ? nworkers : 1, sizeof(SimWorker));
    if (!st || !sw)
        goto done;
    wheap.less = simworker_less;
    wheap.setpos = simworker_setpos;

    // Snapshot each tube: copy and sort its ready jobs, reset stamps,
    // and total up the expected work (which, per the design doc, counts
    // every job regardless of reachability).
    for (i = 0; i < ntubes; i++) {
        Tube *t = tubes.items[i];
        SimTube *s = &st[i];

        t->sim_index = i;
        s->len = t->ready.len;
        s->inflight = (int64)t->stat.reserved_ct;
        s->avail_at = t0;
        if (t->pause && t->unpause_at > t0)
            s->avail_at = t->unpause_at;

        t->est_work_all_ms = 0;
        t->est_work_urgent_ms = 0;
        t->eta_all_at = t0;
        t->eta_urgent_at = t0;

        if (s->len) {
            // Never pop the live heap: heapremove calls setpos, which
            // writes heap_index back into the live jobs. Sort a copy of
            // the pointer array and consume it with a cursor instead.
            s->jobs = malloc(s->len * sizeof(Job *));
            if (!s->jobs)
                goto done;
            memcpy(s->jobs, t->ready.data, s->len * sizeof(Job *));
            qsort(s->jobs, s->len, sizeof(Job *), simjob_cmp);
        }
        for (k = 0; k < s->len; k++) {
            Job *j = s->jobs[k];
            int64 est = sim_effective_est_ms(j);

            j->etd_at = -1;
            j->queue_pos = -1;
            t->est_work_all_ms += est;
            if (j->r.pri < URGENT_THRESHOLD)
                t->est_work_urgent_ms += est;
        }
    }

    // Build the worker pool. A worker holding reservations frees up once
    // its jobs' estimated remaining time elapses. Each reserved job's
    // actual start time was stamped in etd_at at reservation, so this is
    // accurate even for jobs that have used touch.
    for (i = 0; i < nworkers; i++) {
        Conn *c = workers.items[i];
        SimWorker *w = &sw[i];
        Job *j;

        w->conn = c;
        w->free_at = t0;
        for (j = c->reserved_jobs.next; j != &c->reserved_jobs; j = j->next) {
            int64 est_ns = (int64)sim_effective_est_ms(j) * 1000000;
            int64 remaining = est_ns - (t0 - j->etd_at);
            Tube *jt = j->tube;

            if (j->etd_at < 0 || remaining < 0)
                remaining = 0;
            w->free_at += remaining;
            w->initial = 1;

            jt->est_work_all_ms += remaining / 1000000;
            if (t0 + remaining > jt->eta_all_at)
                jt->eta_all_at = t0 + remaining;
            if (j->r.pri < URGENT_THRESHOLD) {
                jt->est_work_urgent_ms += remaining / 1000000;
                if (t0 + remaining > jt->eta_urgent_at)
                    jt->eta_urgent_at = t0 + remaining;
            }
        }
        if (!heapinsert(&wheap, w))
            goto done;
    }

    // The event loop. Pop the earliest-free worker; it takes the smallest
    // (pri, id) job across the tubes it watches, skipping paused tubes and
    // tubes at their reserve_limit — a replay of next_awaited_job.
    while (wheap.len) {
        SimWorker *w = heapremove(&wheap, 0);
        int64 t = w->free_at;
        Job *best = NULL;
        SimTube *bestst = NULL;
        int64 soonest_avail = 0;
        int any_blocked = 0;

        // Complete whatever this worker just finished.
        if (w->initial) {
            Job *j;
            w->initial = 0;
            for (j = w->conn->reserved_jobs.next;
                 j != &w->conn->reserved_jobs; j = j->next) {
                if (!sim_complete(st, &wheap, j->tube, t))
                    goto done;
            }
        } else if (w->last_tube) {
            if (!sim_complete(st, &wheap, w->last_tube, t))
                goto done;
            w->last_tube = NULL;
        }

        for (k = 0; k < w->conn->watch.len; k++) {
            Tube *wt = w->conn->watch.items[k];
            SimTube *s = &st[wt->sim_index];

            if (s->cursor >= s->len)
                continue;
            if (s->avail_at > t) {
                if (!soonest_avail || s->avail_at < soonest_avail)
                    soonest_avail = s->avail_at;
                continue;
            }
            if (wt->reserve_limit >= 0 && s->inflight >= wt->reserve_limit) {
                any_blocked = 1;
                continue;
            }
            if (!best || job_pri_less(s->jobs[s->cursor], best)) {
                best = s->jobs[s->cursor];
                bestst = s;
            }
        }

        if (best) {
            int64 est_ns = (int64)sim_effective_est_ms(best) * 1000000;
            Tube *bt = best->tube;

            best->etd_at = t;
            best->queue_pos = (int32)(bestst->cursor + 1);
            bestst->cursor++;
            bestst->inflight++;

            if (t + est_ns > bt->eta_all_at)
                bt->eta_all_at = t + est_ns;
            if (best->r.pri < URGENT_THRESHOLD &&
                t + est_ns > bt->eta_urgent_at)
                bt->eta_urgent_at = t + est_ns;

            w->free_at = t + est_ns;
            w->last_tube = bt;
            if (!heapinsert(&wheap, w))
                goto done;
            continue;
        }

        if (soonest_avail) {
            // Only paused tubes have work for this worker: wait it out.
            w->free_at = soonest_avail;
            if (!heapinsert(&wheap, w))
                goto done;
            continue;
        }

        if (any_blocked) {
            // Park on every watched tube that is blocked at its limit but
            // still has jobs; a completion there wakes this worker. A tube
            // limited to 0 never completes, so parking there is futile.
            w->parked = 1;
            for (k = 0; k < w->conn->watch.len; k++) {
                Tube *wt = w->conn->watch.items[k];
                SimTube *s = &st[wt->sim_index];

                if (s->cursor < s->len && wt->reserve_limit > 0 &&
                    s->inflight >= wt->reserve_limit) {
                    if (!park(s, w))
                        goto done;
                }
            }
            continue;
        }

        // No watched tube will ever have work again: the worker retires.
    }

    // Jobs never reached keep etd_at = -1; a tube containing any makes its
    // drain time (but not its work sums) unknowable.
    for (i = 0; i < ntubes; i++) {
        Tube *t = tubes.items[i];
        SimTube *s = &st[i];

        for (k = s->cursor; k < s->len; k++) {
            t->eta_all_at = -1;
            if (s->jobs[k]->r.pri < URGENT_THRESHOLD) {
                t->eta_urgent_at = -1;
                break;
            }
        }
    }

    sim_done_at = t0;
    ok = 1;

done:
    if (st) {
        for (i = 0; i < ntubes; i++) {
            free(st[i].jobs);
            free(st[i].parked);
        }
        free(st);
    }
    free(sw);
    free(wheap.data);
    return ok;
}

// Ensure an estimate no staler than max_age_ms is available, running the
// simulation if needed. max_age_ms of 0 forces a fresh run. Returns 1 on
// success, 0 if the simulation was refused.
int
sim_fresh(int64 now, uint64 max_age_ms)
{
    if (sim_done_at && max_age_ms > 0 &&
        now - sim_done_at <= (int64)max_age_ms * 1000000)
        return 1;
    return sim_run(now);
}
