/* ---------------------------------------------------------------------------
 *
 * (c) The GHC Team, 2013
 *
 * Execution Replay implementation
 *
 * --------------------------------------------------------------------------*/

#include "PosixSource.h"
#include "Rts.h"

#include "eventlog/EventLog.h"
#include "Capability.h"
#include "Event.h"
#include "Hash.h"
#include "Messages.h"
#include "RtsFlags.h"
#include "RtsUtils.h"
#include "Schedule.h"
#include "Task.h"
#include "Trace.h"
#include "Threads.h"
#include "Printer.h"
#ifdef DEBUG
#include "sm/Storage.h"
#endif
#include "Replay.h"

#include <math.h>
#include <string.h>

rtsBool replay_enabled = rtsFalse;

#ifdef REPLAY_LAZY
#error replay_lazy
rtsBool eager_blackholing = rtsFalse;
#else
rtsBool eager_blackholing = rtsTrue;
#endif


HsBool
rtsReplayEnabled(void)
{
#ifdef REPLAY
    if (TRACE_spark_full) {
        return HS_BOOL_TRUE;
    }
#endif
    return HS_BOOL_FALSE;
}

#if defined(REPLAY)
#define START(bd)       ((bd)->start - 1)
#define START_FREE(bd)  ((bd)->free - 1) // last written word
#define END(bd)         (START(bd) + (bd)->blocks * BLOCK_SIZE_W) // last writable word

#define SIZE(bd)    ((bd)->blocks * BLOCK_SIZE_W)
#define FILLED(bd)  (START_FREE(bd) - START(bd))
#define FREE(bd)    (END(bd) - START_FREE(bd))

#define FULL(bd)    (FILLED(bd) == BLOCK_SIZE_W)
#define EMPTY(bd)   (FILLED(bd) == 0)

#define HP_IN_BD(bd,p) (((p) >= START(bd)) && ((p) <= END(bd)))

StgTSO **thread_from_id;
static int thread_from_id_idx;
static int thread_from_id_size;

static HashTable *spark_user;

#ifdef THREADED_RTS
rtsBool emit_dups = rtsFalse;

OSThreadId  replay_init_thread;
int         replay_main_task = -1;
static int  replay_main_gc_task = -1;
static rtsBool replay_main_gc_task_finished = rtsFalse;

static Semaphore replay_sched;
static Semaphore no_task;
static Semaphore **task_replay;

static Task **running_tasks; // owner task for each capability

static HashTable *spark_thunks;
HashTable *spark_ptrs; // map original ptr -> replay ptr
static HashTable *spark_ids;
static HashTable *spark_owners;
static HashTable *spark_dups;

static HashTable *gc_spark_thunks;
static HashTable *gc_spark_ptrs;
static HashTable *gc_spark_ids;

static nat gc_threads_finished = 0;

static Task *findTask(nat no);

static void replayLoop(void);
#endif

void
initReplay(void)
{
    replay_enabled = RtsFlags.ReplayFlags.replay == rtsTrue;

    if (replay_enabled) {
#ifdef THREADED_RTS
        OSThreadId tid;
        int r;

        emit_dups      = RtsFlags.ReplayFlags.dup == rtsTrue;
#endif

        initEventLoggingReplay();

#ifdef THREADED_RTS
        // spawn replay scheduler
        initSemaphore(&replay_sched);
        initSemaphore(&no_task);

        r = createOSThread(&tid, (OSThreadProc*)replayLoop, NULL);
        if (r != 0) {
            sysErrorBelch("failed to create OS thread");
            stg_exit(EXIT_FAILURE);
        }

        // wait for replayLoop to get ready
        waitSemaphore(&no_task);
#else
        // Get the next event ready for the next replayEvent() call
        nextEvent();
#endif

        // flag setup
        initRtsFlagsDefaults();
        RtsFlags.MiscFlags.tickInterval = 0;

        // fake this events so that flags and env are copied
        replayEvent(NULL, createEvent(EVENT_PROGRAM_ARGS));
        replayEvent(NULL, createEvent(EVENT_PROGRAM_ENV));

#ifdef THREADED_RTS
        running_tasks = stgCallocBytes(RtsFlags.ParFlags.nNodes, sizeof(Task *), "initReplay");

        for (r = 0; r < (int)RtsFlags.ParFlags.nNodes; r++) {
            running_tasks[r] = NULL;
        }

        spark_thunks = allocHashTable();
        spark_ptrs = allocHashTable();
        spark_ids = allocHashTable();
        spark_owners = allocHashTable();
        if (emit_dups) {
            spark_dups = allocHashTable();
        }
#endif
        spark_user = allocHashTable();
    }

    thread_from_id = stgMallocBytes(sizeof(StgTSO *), "initReplay");
    thread_from_id_idx = 0;
    thread_from_id_size = 1;
}

void
endReplay(void)
{
    nat i USED_IF_THREADS;

    if (replay_enabled) {
        endEventLoggingReplay();

#ifdef THREADED_RTS
        for (i = 0; i < taskCount; i++) {
            closeSemaphore(task_replay[i]);
            stgFree(task_replay[i]);
        }
        stgFree(task_replay);

        closeSemaphore(&no_task);
        closeSemaphore(&replay_sched);

        stgFree(running_tasks);

        freeHashTable(spark_thunks, stgFree);
        freeHashTable(spark_ptrs, NULL);
        freeHashTable(spark_ids, NULL);
        freeHashTable(spark_owners, NULL);
        if (emit_dups) {
            freeHashTable(spark_dups, NULL);
        }
#endif
        freeHashTable(spark_user, NULL);
    }

    stgFree(thread_from_id);
}

void
debugReplay(char *s USED_IF_DEBUG, ...)
{
#if defined(DEBUG)
    if (TRACE_spark_full) {
        va_list ap;

        va_start(ap, s);
        vfprintf(stderr, s, ap);
        va_end(ap);
    }
#endif
}

void GNU_ATTRIBUTE(__noreturn__)
replayError(char *s, ...)
{
#define MSG_SIZE 64
    char msg[MSG_SIZE];
    va_list ap;

    va_start(ap,s);
    vsnprintf(msg, MSG_SIZE, s, ap);
    va_end(ap);
    barf("replay: %s", msg);
}

#if defined(DEBUG)
static rtsBool
nurseryReaches(bdescr *oldBd, bdescr *bd)
{
    ASSERT(oldBd);

    if (oldBd != bd) {
        bdescr *bd_ = oldBd->link;
        while (bd_ != bd) {
            if (bd_ == NULL)
                return rtsFalse;
            bd_ = bd_->link;
        }
    }
    return rtsTrue;
}

void
replayCheckGCGeneric(StgPtr Hp, Capability *cap, StgPtr HpLim STG_UNUSED, bdescr *CurrentNursery, StgPtr Sp STG_UNUSED, StgPtr SpLim STG_UNUSED)
{
    ASSERT(HP_IN_BD(CurrentNursery, Hp));
    if (cap->replay.last_bd) {
        ASSERT(nurseryReaches(cap->replay.last_bd, CurrentNursery));
    }
    //debugReplay("cap %d: check GC\nHp = %p\nHpLim = %p\nCurrentNursery = %p\nEnd = %p\nSp = %p\nSpLim = %p\n",
    //            cap->no, Hp, HpLim, CurrentNursery, END(CurrentNursery), Sp, SpLim);
}
#endif

// Saves current block and heap pointer. It is meant to be called just
// before running Haskell code (StgRun, resumeThread).
void
replaySaveHp(Capability *cap)
{
#ifdef DEBUG
    debugReplay("Saving Hp...\n");
    debugReplay("  hp    = %p\n", START_FREE(cap->r.rCurrentNursery));
    debugReplay("  alloc = %ld\n\n", cap->replay.alloc);
#endif

    ASSERT(cap->replay.last_bd == NULL);
    ASSERT(cap->replay.last_hp == NULL);

    cap->replay.last_bd = cap->r.rCurrentNursery;
    cap->replay.last_hp = START_FREE(cap->r.rCurrentNursery);
    ASSERT(HP_IN_BD(cap->replay.last_bd, cap->replay.last_hp));
}

// Saves the amount of allocation performed in a Haskell thread. It is
// meant to be called just after running Haskell code (StgRun,
// suspendThread).
void
replaySaveAlloc(Capability *cap)
{
    bdescr *oldBd, *bd, *bd_;
    StgPtr oldHp, hp;

    ASSERT(cap->replay.last_bd != NULL);
    ASSERT(cap->replay.last_hp != NULL);

    oldBd = cap->replay.last_bd;
    oldHp = cap->replay.last_hp;

    bd = cap->r.rCurrentNursery;
    hp = START_FREE(bd);

#ifdef DEBUG
    debugReplay("cap %d: task %d: yielding %p (%p)\n",
                cap->no, cap->running_task->no, bd, hp);
#endif

    // when replaying and only if the thread yielded
    if (cap->replay.hp) {
        ASSERT(cap->r.rRet == ThreadYielding);
        ASSERT(hp == cap->replay.hp);

        cap->replay.bd = NULL;
        cap->replay.hp = NULL;
        cap->replay.hp_alloc = 0;
    }

    W_ alloc = 0, n_blocks = 0;

    ASSERT(START_FREE(oldBd) >= oldHp);

    bd_ = oldBd;
    while (bd_ != bd) {
        ASSERT(FILLED(bd_) > 0);
        alloc += SIZE(bd_);
        cap->replay.real_alloc += FILLED(bd_);
        n_blocks += bd_->blocks;
        bd_ = bd_->link;
        ASSERT(bd_ != NULL);
    }

    W_ last_alloc = hp - START(bd_);
    alloc += last_alloc;
    cap->replay.real_alloc += last_alloc;

    // substract the previouslly allocated memory from the first block, so the
    // loop above can count full blocks in every iteration, including the
    // first one
    ASSERT(alloc >= (W_)(oldHp - START(oldBd)));
    ASSERT(cap->replay.real_alloc >= (W_)(oldHp - START(oldBd)));
    alloc -= oldHp - START(oldBd);
    cap->replay.real_alloc -= oldHp - START(oldBd);

    cap->replay.alloc += alloc;
    cap->replay.blocks += n_blocks;

    traceCapAlloc(cap, cap->replay.alloc, cap->replay.blocks, cap->r.rHpAlloc);

    cap->replay.last_bd = NULL;
    cap->replay.last_hp = NULL;
}

static void GNU_ATTRIBUTE(__noreturn__)
failedMatch(Capability *cap, Event *ev, Event *read)
{
    debugReplay("Got:\n  ");
    printEvent(cap, ev);
    debugReplay("Expected:\n  ");
    printEvent(cap, read);
    barf("replay: '%s' events did not match", EventDesc[ev->header.tag]);
}

static void
setupNextEvent(CapEvent **cep)
{
    CapEvent *ce = *cep;

    switch(ce->ev->header.tag) {
    case EVENT_CAP_ALLOC:
    {
        Capability *cap = capabilities[ce->capno];
        CapEvent *tmp = searchEventCap(cap->no, EVENT_STOP_THREAD);
        ASSERT(tmp != NULL);

        // if yielded, setup the nursery to force the thread to stop
        if (((EventStopThread *)tmp->ev)->status == ThreadYielding) {
            bdescr *bd;
            StgPtr hp;
            StgWord64 alloc, blocks;
            EventCapAlloc *ev;

            ev = (EventCapAlloc *)ce->ev;

            alloc = ev->alloc - cap->replay.alloc;
            blocks = ev->blocks - cap->replay.blocks;

            bd = cap->replay.last_bd;
            hp = cap->replay.last_hp;

            ASSERT(bd != NULL);
            ASSERT(hp != NULL);

#ifdef DEBUG
            //debugReplay("cap %d: setupNextEvent(): yield after:\n", cap->no);
            //debugReplay("  blocks = %" FMT_Word64 " (%" FMT_Word64 ")\n", blocks, ev->blocks);
            //debugReplay("  alloc = %" FMT_Word64 " (%" FMT_Word64 ")\n", alloc, ev->alloc);
            //debugReplay("\n");
#endif

            // add the allocated memory from the first block, so the loop
            // counts full blocks in every iteration, including the first one
            alloc += hp - START(bd);
            while (blocks > 0) {
                ASSERT(alloc >= SIZE(bd));
                alloc -= SIZE(bd);
                ASSERT(blocks >= bd->blocks);
                blocks -= bd->blocks;
                bd = bd->link;
                ASSERT(bd != NULL);
            }

            cap->replay.bd = bd;
            cap->replay.hp = START(bd) + alloc;
            cap->replay.hp_alloc = ev->hp_alloc;

#ifdef DEBUG
            debugReplay("cap %d: setupNextEvent(): yield at bd (hp) = %p (%p)\n\n",
                        cap->no, bd, cap->replay.hp);
#endif

            // if we have to yield in the current block, set HpLim now
            if (cap->r.rCurrentNursery == bd) {
                cap->r.rHpLim = cap->replay.hp;
            }
        }
        break;
    }
#ifdef THREADED_RTS
    // ignore GC events
    case EVENT_GC_WORK:
    case EVENT_GC_IDLE:
    case EVENT_GC_DONE:
    {
        ce = nextEvent();
        setupNextEvent(&ce);
        *cep = ce;
        break;
    }
#endif
    default:
        ; // do nothing
    }
}

//extern char **environ;

#ifdef THREADED_RTS
// task needs the task running in cap to perform some computation before being
// able to continue
static void
replaySync(Capability *cap, Task *task)
{
    debugReplay("cap %d: task %d: replaySync with cap %d\n",
                task->cap->no, task->no, cap->no);
    ASSERT(cap->running_task != NULL);
    ASSERT(cap->replay.sync_task == NULL);
    cap->replay.sync_task = task;
    signalSemaphore(task_replay[cap->running_task->no]);
    waitSemaphore(task_replay[task->no]);
    cap->replay.sync_task = NULL;
}

static void
replayCont(Capability *cap, Task *task)
{
    if (cap->replay.sync_task != NULL) {
        debugReplay("cap %d: task %d: replayCont\n", cap->no, task->no);
        signalSemaphore(task_replay[cap->replay.sync_task->no]);
        waitSemaphore(task_replay[task->no]);
    }
}

static void
replaySyncThunk(Capability *owner, StgClosure *p)
{
    debugReplay("cap %d: task %d: replaySyncThunk with thunk %p -> %p\n",
                myTask()->cap->no, myTask()->no, p, ((StgInd *)p)->indirectee);
    ASSERT(owner->replay.sync_thunk == NULL);
    owner->replay.sync_thunk = p;
    replaySync(owner, myTask());
    owner->replay.sync_thunk = NULL;
}
#endif

void
replayEvent(Capability *cap, Event *ev)
{
    ASSERT(ev);

    CapEvent *ce;
    Event *read;
    nat tag;

    tag = ev->header.tag;
    switch (tag) {
#ifdef THREADED_RTS
    // ignore GC events
    case EVENT_GC_WORK:
    case EVENT_GC_IDLE:
    case EVENT_GC_DONE:
        ASSERT((int)cap->running_task->no == replay_main_gc_task);
        stgFree(ev);
        return;
    case EVENT_CAP_VALUE:
        if (((EventCapValue *)ev)->tag == GC_END) {
            ASSERT(!replay_main_gc_task_finished);
            replay_main_gc_task_finished = rtsTrue;
        }
#endif
    default:
        ;
    }

#ifdef THREADED_RTS
    if (cap != NULL) {
        // blackhole again a restored thunk
        ASSERT(cap->replay.restored_bh == NULL);
        if (cap->replay.restored_bh != NULL) {
            SET_INFO(cap->replay.restored_bh, &stg_BLACKHOLE_info);
            cap->replay.restored_bh = NULL;
        }

        // we may have reached here to synchronise with another task,
        // in that case, return the control to that task
        if (cap->running_task != NULL) {
            replayCont(cap, cap->running_task);
        }
    }
#endif

    ce = readEvent();
    ASSERTM(ce != NULL && ce->ev != NULL, "replayEvent: event could not be read");
    read = ce->ev;

    if (cap != NULL) {
        ASSERTM(cap->no == ce->capno,
                "replay: got event 'cap %d: %s' instead of event 'cap %d: %s'",
                cap->no, EventDesc[tag], ce->capno, EventDesc[read->header.tag]);
    }

    if (tag != read->header.tag) {
        switch (tag) {
#ifdef THREADED_RTS
        // performed a major GC, instead of a minor one, or vice versa
        case EVENT_HEAP_LIVE:
            if (read->header.tag == EVENT_SPARK_COUNTERS) {
                stgFree(ev);
                return;
            }
        case EVENT_SPARK_COUNTERS:
            if (read->header.tag == EVENT_HEAP_LIVE) {
                nextEvent();
                replayEvent(cap, ev);
                return;
            }
#endif
        default:
            ;
        }
        failedMatch(cap, ev, read);
    }

    switch (tag) {
    // set program arguments and environment
    case EVENT_PROGRAM_ARGS:
    case EVENT_PROGRAM_ENV:
    {
        EventCapsetMsg *ecm_read;
        int argc, arg, i, strsize;
        char **argv;

        ecm_read = (EventCapsetMsg *)read;
        strsize = ecm_read->size - sizeof(EventCapsetID);
        for (i = 0, argc = 0; i < strsize; i++) {
            if (ecm_read->msg[i] == '\0') {
                argc++;
            }
        }
        ASSERT(argc > 0);

        argv = stgMallocBytes((argc + 1) * sizeof(char *), "replayEvent");
        for (i = 0, arg = 0; arg < argc; i++, arg++) {
            ASSERT(i < strsize);
            argv[arg] = (char *)&ecm_read->msg[i];
            i = strchr(argv[arg], '\0') - (char *)ecm_read->msg;
        }
        argv[arg] = NULL;

        if (tag == EVENT_PROGRAM_ARGS) {
            nat n_caps USED_IF_THREADS;

            freeRtsArgs();

            setFullProgArgv(argc, argv);
            setupRtsFlags(&argc, argv, RtsOptsAll, NULL, rtsTrue);

#ifdef THREADED_RTS
            n_caps = RtsFlags.ParFlags.nNodes;
            if (n_caps > 1) {
                // done here so that replaying EVENT_PROGRAM_ENV does not fail
                // because the replay buffer for the rest of the capabilities
                // are not initialised
                moreCapEventBufsReplay(1, n_caps);
            }
#endif
        } else {
            //int r;

            //environ = NULL;

            //// putenv does not copy the string
            //for (arg = 0; arg < argc; arg++) {
            //    char *new_arg = stgMallocBytes(strlen(argv[arg]) + 1, "replayEvent");
            //    strcpy(new_arg, argv[arg]);
            //    r = putenv(new_arg);
            //    if (r != 0) {
            //        barf("Could not set environment variable '%s'. Execution Replay may fail\n", new_arg);
            //    }
            //}
        }
        stgFree(argv);
        break;
    }
#ifdef DEBUG
    // allow debug way when replaying
    case EVENT_RTS_IDENTIFIER:
    {
        EventCapsetMsg *ecm, *ecm_read;
        int size, strsize, strsize_read;
        const char *msg, *msg_read, *c, *c_read;

        ecm = (EventCapsetMsg *)ev;
        ecm_read = (EventCapsetMsg *)read;

        if (ecm->capset != ecm_read->capset) {
            failedMatch(cap, ev, read);
        }

        strsize = ecm->size - sizeof(EventCapsetID);
        strsize_read = ecm_read->size - sizeof(EventCapsetID);

        msg = (const char *)ecm->msg;
        msg_read = (const char *)ecm_read->msg;
        if (strncmp(msg, msg_read, strsize) == 0) {
            break;
        }

        // check ProjectVersion
        c = strchr((const char *)ecm->msg, ' ');
        size = c - msg; // ProjectVersion size
        if (strsize_read < size ||
            strncmp(msg, msg_read, size) != 0) {
            failedMatch(cap, ev, read);
        }

        // check RtsWay
        c = (const char *)ecm->msg + size;
        c_read = (const char *)ecm_read->msg + size;
        while (c != msg + strsize &&
               c_read != msg_read + strsize_read &&
               *c == *c_read) {
            c++;
            c_read++;
        }
        // ignore debug_
        size = strsize - (c-msg);
        if (size >= 5 && strncmp(c, "debug", 5) == 0) {
            c += 5;
            if (size >=6 && *c == '_') {
                c++;
            }
        }
        // debug implies l, ignore in read
        size = strsize_read - (c_read-msg_read);
        if (size >= 1 && *c_read == 'l') {
            c_read++;
            size--;
            if (size >= 1 && *c_read == '_') {
                c_read++;
                size--;
            }
        }
        // check the leftover
        if (strsize - (c-msg) != size ||
            strncmp(c, c_read, size) != 0) {
            failedMatch(cap, ev, read);
        }

        break;
    }
#endif
    case EVENT_OSPROCESS_PID:
    case EVENT_OSPROCESS_PPID:
    {
        EventCapsetPid *ecp, *ecp_read;

        ecp = (EventCapsetPid *)ev;
        ecp_read = (EventCapsetPid *)read;
        // ignore pid
        if (ecp->capset != ecp_read->capset) {
            failedMatch(cap, ev, read);
        }
        break;
    }
    case EVENT_WALL_CLOCK_TIME:
    {
        EventWallClockTime *ewct, *ewct_read;

        ewct = (EventWallClockTime *)ev;
        ewct_read = (EventWallClockTime *)read;
        // ignore time
        if (ewct->capset != ewct_read->capset) {
            failedMatch(cap, ev, read);
        }
        break;
    }
#ifdef THREADED_RTS
    // do no compare GC events
    case EVENT_HEAP_SIZE:
        // last event from main GC task
        ASSERT(replay_main_gc_task != -1);
        replay_main_gc_task = -1;
    case EVENT_GC_STATS_GHC:
    case EVENT_HEAP_LIVE:
    case EVENT_HEAP_ALLOCATED:
        break;
    // remaining sparks may not match when called after GC if other thread
    // steals the spark before the event
    case EVENT_SPARK_COUNTERS:
        break;
    case EVENT_STOP_THREAD:
    {
        EventStopThread *est, *est_read;

        est = (EventStopThread *)ev;
        est_read = (EventStopThread *)read;
        // sometimes, when blocking, the blackhole is updated before emitting
        // the event, and the event registers thread id 0
        if (est->thread != est_read->thread ||
            est->status != est_read->status ||
            (est_read->blocked_on != 0 &&
             est->blocked_on != est_read->blocked_on)) {
            failedMatch(cap, ev, read);
        }
        break;
    }
#endif
    case EVENT_TASK_CREATE:
    {
        EventTaskCreate *etc, *etc_read;
        Task *task USED_IF_THREADS;

        etc = (EventTaskCreate *)ev;
        etc_read = (EventTaskCreate *)read;
        // ignore tid
        if (etc->task != etc_read->task || etc->capno != etc_read->capno) {
            failedMatch(cap, ev, read);
        }

#ifdef THREADED_RTS
        task = findTask(etc->task);
        ASSERT(task != NULL);

        // record the task running the main thread
        if (task->id == replay_init_thread) {
            replay_main_task = task->no;
        }
#endif
        break;
    }
#ifdef THREADED_RTS
    case EVENT_TASK_DELETE:
    {
        Task *task USED_IF_THREADS;

        task = findTask(((EventTaskDelete *)ev)->task);
        ASSERT(task != NULL);
        // if worker, let it die
        // boundTaskExiting() ends the incall before emitting 'task delete'
        if (task->incall != NULL && !isBoundTask(task)) {
            // unlink task, check Task.c:workerTaskStop()
            if (task->all_prev) {
                task->all_prev->all_next = task->all_next;
            } else {
                all_tasks = task->all_next;
            }
            if (task->all_next) {
                task->all_next->all_prev = task->all_prev;
            }

#ifdef DEBUG
            printEvent(cap, ev);
#endif
            // ce is freed by replayLoop
            stgFree(ev);
            signalSemaphore(&replay_sched);
            return;
        }
        goto check_events;
    }
#endif
    // set capability variable
    case EVENT_CAP_VALUE:
    {
        EventCapValue *ecv, *ecv_read;
        W_ id USED_IF_THREADS = 0;

        ecv = (EventCapValue *)ev;
        ecv_read = (EventCapValue *)read;

        // set/check the correct value
        switch (ecv->tag) {
        case CTXT_SWITCH:
            ASSERT(ecv_read->value == 0 || ecv_read->value == 1);
            cap->r.rHpLim = (StgPtr)(W_)!ecv_read->value;
            cap->context_switch = ecv_read->value;

            ecv->value = ecv_read->value;
            goto check_events;
        case SCHED_LOOP:
            ASSERT(ecv_read->value <= SCHED_SHUTTING_DOWN);
            if (sched_state < ecv_read->value) {
                sched_state = ecv_read->value;
            }

            ecv->value = ecv_read->value;
            goto check_events;
#ifdef THREADED_RTS
        case SPARK_CREATE:
        case SPARK_RUN:
        case SPARK_STEAL:
        case SPARK_DUD:
        case SPARK_OVERFLOW:
            // always id
            id = ecv_read->value;
            break;
        case SPARK_GC:
        case SPARK_FIZZLE:
        case SUSPEND_COMPUTATION:
        case BH_THUNK:
        case BH_WHNF:
        case THUNK_UPDATE:
        case COLLISION_WHNF:
        case COLLISION_OTHER:
            // always pointer
            id = (W_)lookupHashTable(spark_ptrs, ecv_read->value);
            break;
        //case MSG_BLACKHOLE:
        //    // either id or pointer
        //    if (REPLAY_ATOM(ecv_read->value) == REPLAY_ID_ATOM) {
        //        id = ecv_read->value;
        //    } else {
        //        id = (W_)lookupHashTable(spark_ptrs, ecv_read->value);
        //    }
        //    break;
        case PRUNE_SPARK_QUEUE:
            gc_threads_finished++;
            if (gc_threads_finished == n_capabilities) {
                replayEndGC(cap);
                ASSERT(replay_main_gc_task_finished);
                replay_main_gc_task_finished = rtsFalse;
                gc_threads_finished = 0;
            }
            goto check_events;
#endif
        default:
            goto check_events;
        }
        if (ecv->tag != ecv_read->tag || ecv->value != id) {
            failedMatch(cap, ev, read);
        }
        break;
    }
#ifdef THREADED_RTS
    case EVENT_TASK_ACQUIRE_CAP:
    case EVENT_TASK_RELEASE_CAP:
    {
        Task *task;

        task = findTask(((EventTaskCap *)ev)->task);
        ASSERT(task != NULL);

        // set current task on capability
        if (tag == EVENT_TASK_ACQUIRE_CAP) {
            ASSERT(running_tasks[cap->no] == NULL);
            running_tasks[cap->no] = task;
        } else {
            ASSERT(running_tasks[cap->no] == task);
            running_tasks[cap->no] = NULL;
        }
        goto check_events;
    }
    case EVENT_ENTER_THUNK:
    {
        EventEnterThunk *eet, *eet_read;

        eet = (EventEnterThunk *)ev;
        eet_read = (EventEnterThunk *)read;

        ASSERT(eet->id != 0);
        if (eet->id != eet_read->id) {
            failedMatch(cap, ev, read);
        }

        insertHashTable(spark_ptrs, eet_read->ptr, (void *)eet->ptr);
        break;
    }
    case EVENT_POINTER_MOVE:
    {
        EventPtrMove *epm, *epm_read;
        W_ id;

        epm = (EventPtrMove *)ev;
        epm_read = (EventPtrMove *)read;

        if (lookupHashTable(spark_ptrs, epm_read->ptr) != (void *)epm->ptr) {
            failedMatch(cap, ev, read);
        }

        insertHashTable(gc_spark_ptrs, epm_read->new_ptr, (void *)epm->new_ptr);

        id = (W_)lookupHashTable(spark_ids, epm->ptr);
        ASSERT(id > 0);
        insertHashTable(gc_spark_ids, epm->new_ptr, (void *)id);
        break;
    }
    case EVENT_MSG_BLACKHOLE:
    {
        EventMsgBlackHole *embh, *embh_read;

        embh = (EventMsgBlackHole *)ev;
        embh_read = (EventMsgBlackHole *)read;

        void *p = lookupHashTable(spark_ptrs, embh_read->ptr);
        if (embh->id != embh_read->id ||
            (p != NULL && (W_)p != embh->ptr)) {
            failedMatch(cap, ev, read);
        }

        // only the first time, where id != 0
        if (p == NULL) {
            ASSERT(embh->id != 0);
            // XXX: insert id?
            insertHashTable(spark_ptrs, embh_read->ptr, (void *)embh->ptr);
        }
        break;
    }
    case EVENT_REQUEST_SEQ_GC:
    case EVENT_REQUEST_PAR_GC:
        replayStartGC(cap);
        goto check_events;
#endif
    default:
check_events:
        if (!equalEvents(ev, read)) {
            failedMatch(cap, ev, read);
        }
        break;
    }

#ifdef DEBUG
    ev->header.time = read->header.time;
    printEvent(cap, ev);
#endif

#ifdef THREADED_RTS
    // wait for the replay scheduler signal
    signalSemaphore(&replay_sched);

    if (all_tasks != NULL && myTask() != NULL) {
        waitSemaphore(task_replay[myTask()->no]);
    } else {
        if (sched_state < SCHED_SHUTTING_DOWN) {
            ASSERT(replay_main_task == -1);
            waitSemaphore(&no_task);
        } else {
            ASSERT(replay_main_task != -1);
            waitSemaphore(task_replay[replay_main_task]);
        }
    }
#else
    // Get the next event ready for the next replayEvent() call and
    // setupNextEvent(). It needs to be called before freeing the cap event
    ce = nextEvent();
    setupNextEvent(&ce);
#endif

    freeEvent(ce);
    stgFree(ev);
}

void
replayTraceCapTag(Capability *cap, int tag)
{
    replayTraceCapValue(cap, tag, 0);
}

void
replayTraceCapValue(Capability *cap, int tag, W_ value)
{
    traceCapValue(cap, tag, value);
    replayCapValue(cap, tag, value);
}

W_
replayCapTag(Capability *cap, int tag)
{
    W_ v = 0;
    CapEvent *ce = readEvent();

    if (isEventCapValue(ce, tag)) {
        v = ((EventCapValue *)ce->ev)->value;
        replayCapValue(cap, tag, v);
    }
    return v;
}

void
replayCapValue(Capability *cap, int tag, W_ value)
{
    if (replay_enabled) {
        replayEvent(cap, createCapValueEvent(tag, value));
    }
#ifdef DEBUG
    else if (TRACE_spark_full) {
        printEvent(cap, createCapValueEvent(tag, value));
    }
#endif
}


void
replayNewThread(StgTSO *tso)
{
    ASSERT((int)tso->id-1 == thread_from_id_idx);
    thread_from_id[thread_from_id_idx] = tso;
    thread_from_id_idx++;

    if (thread_from_id_idx == thread_from_id_size) {
        StgTSO **tmp, **new = stgMallocBytes(thread_from_id_size * 2 * sizeof(StgTSO *), "replayNewThread");
        int t;
        for (t = 0; t < thread_from_id_idx; t++) {
            new[t] = thread_from_id[t];
        }
        tmp = thread_from_id;
        thread_from_id = new;
        stgFree(tmp);
        thread_from_id_size *= 2;
    }
}

StgTSO *
findThread(StgThreadID id)
{
    if (id == 0 || (int)id-1 >= thread_from_id_size) {
        barf("findThread: thread %d not created yet", id);
    }
    if (thread_from_id[id-1] == NULL) {
        barf("thread %d is dead", id);
    }
    return thread_from_id[id-1];
}

#ifdef THREADED_RTS
// Task.c
void
replayNewTask(Task *task)
{
    if (replay_enabled) {
        nat n;
        Semaphore **old_task_replay = task_replay;
        task_replay = stgMallocBytes((task->no+1) * sizeof(Semaphore *), "replayNewTask");

        for (n = 0; n < task->no; n++) {
            task_replay[n] = old_task_replay[n];
        }
        stgFree(old_task_replay);

        task_replay[task->no] = stgMallocBytes(sizeof(Semaphore), "replayNewTask");
        initSemaphore(task_replay[task->no]);
    }
}

static Task *
findTask(nat no)
{
    Task *task = all_tasks;

    while (task != NULL) {
        if (task->no == no) {
            return task;
        }
        task = task->all_next;
    }
    return NULL;
}

void
replayWorkerStart(Capability *cap, Task *task)
{
    if (replay_enabled) {
        debugReplay("new task %d\n", task->no);
        waitSemaphore(task_replay[task->no]);
    } else if (TRACE_spark_full) {
        // 'release capability' is cap local and is emitted always after
        // releaseCapability_ which can be the creator of this worker, which will be
        // running and emitting events without the capability lock, so make
        // sure that event is finished before scheduling any work
        ACQUIRE_LOCK(&cap->lock);
        RELEASE_LOCK(&cap->lock);
    }

    // cap has been released by task's creator
    traceTaskAcquireCap(cap, task);
}

void
replayStartWorkerTask(Capability *from, Task *task, Capability *cap)
{
    if (replay_enabled) {
        RELEASE_LOCK(&task->lock);
        traceTaskCreate(from, task, cap);
    } else {
        traceTaskCreate(from, task, cap);
        RELEASE_LOCK(&task->lock);
    }
}

// MVars
void
replayMVar(Capability *cap, StgClosure *p, const StgInfoTable *info, int tag, int value) {
    if (replay_enabled) {
        unlockClosure(p, info);
        replayTraceCapValue(cap, tag, value);
    } else {
        if (TRACE_spark_full) {
            traceCapValue(cap, tag, value);
#ifdef DEBUG
            Event *ev = createCapValueEvent(tag, value);
            printEvent(cap, ev);
#endif
        }
        unlockClosure(p, info);
    }
}
#endif

#ifdef THREADED_RTS
static W_
thunkId(StgClosure *p)
{
    W_ id = 0;

    ASSERT(replay_enabled);

    if (REPLAY_IND_ATOM(p) != 0 && REPLAY_IND_ATOM(p) != REPLAY_PTR_ATOM) {
        id = REPLAY_IND_ID(p);
    } else {
        StgClosure *q = lookupHashTable(spark_thunks, (W_)p);
        if (q != NULL) {
            ASSERT(REPLAY_IND_IS_THUNK(q));
            id = thunkId(q);
        } else {
            id = (W_)lookupHashTable(spark_ids, (W_)p);
        }
    }
    return id;
}

// Capability.c
#define swap(x,y)           \
    do {                    \
        typeof(x) _x = x;   \
        typeof(y) _y = y;   \
        x = _y;             \
        y = _x;             \
    } while(0)

static void
prepareSpark(Capability *cap, W_ id)
{
    SparkPool *pool;
    StgClosure *local;
    W_ local_id;
    int i, size;

    pool = cap->sparks;
    if (sparkPoolSize(pool) == 0) {
        replaySync(cap, myTask());
    }

    // find the right spark
    size = sparkPoolSize(pool);
    ASSERT(size > 0);
    i = local_id = 0;
    while (local_id != id && i < size) {
        local = pool->elements[(pool->top + i) & pool->moduloSize];
        local_id = thunkId(local);
        i++;
    }
    ASSERT(local_id == id);
    i--;

    // swap sparks
    while (i > 0) {
        int idx1 = (pool->top + i) & pool->moduloSize;
        int idx0 = (pool->top + i-1) & pool->moduloSize;
        swap(pool->elements[idx0], pool->elements[idx1]);
        i--;
    }
}

static void
doSave(HashTable *thunks, StgClosure *p)
{
    StgClosure *q;
    nat size;

    ASSERT(REPLAY_IND_IS_THUNK(p));

    size = closure_sizeW(p) * sizeof(W_);
    q = stgMallocBytes(size, "doSave");
    memcpy(q, p, size);

    ASSERT(lookupHashTable(thunks, (W_)p) == NULL);
    insertHashTable(thunks, (W_)p, q);

    //// Prevent an accidental evaluation of the thunk by the thread
    //// that is going to block or find it already updated. It will be
    //// dealt with in replayBlackHole
    //SET_INFO(p, &stg_BLACKHOLE_info);
}

static void
saveSparkGC(StgClosure *spark USED_IF_DEBUG, StgClosure *new)
{
    if (GET_INFO(new) == &stg_BLACKHOLE_info) {
        ASSERT(spark == new);
        // recover thunk info table
        replayRestoreSpark(new);
    }
    doSave(gc_spark_thunks, new);
}

static void
saveSpark(StgClosure *spark)
{
    doSave(spark_thunks, spark);
}

static void markShared(StgClosure *spark_);

static void
sharedPAP(StgClosure *fun, StgClosure **payload, nat n_args)
{
    nat i, j, b;
    W_ bitmap;
    StgLargeBitmap *lbitmap;
    StgFunInfoTable *fun_info;

    markShared(fun);

    fun_info = get_fun_itbl(UNTAG_CLOSURE(fun));
    switch (fun_info->f.fun_type) {
    case ARG_GEN:
	bitmap = BITMAP_BITS(fun_info->f.b.bitmap);
	goto small_bitmap;
    case ARG_GEN_BIG:
	lbitmap = GET_FUN_LARGE_BITMAP(fun_info);
	goto large_bitmap;
    case ARG_BCO:
        lbitmap = BCO_BITMAP(fun);
        goto large_bitmap;
    default:
	bitmap = BITMAP_BITS(stg_arg_bitmaps[fun_info->f.fun_type]);
    small_bitmap:
        for (i = 0; i < n_args; i++, bitmap >>= 1) {
            if ((bitmap & 1) == 0) {
                markShared(payload[i]);
            }
        }
	break;
    large_bitmap:
        for (i = 0, b = 0; i < n_args; b++) {
            bitmap = lbitmap->bitmap[b];
            j = 0;
            for(; i < n_args && j < BITS_IN(W_); j++, i++, bitmap >>= 1 ) {
                if ((bitmap & 1) == 0) {
                    markShared(payload[i]);
                }
            }
        }
    }
}

static void
markShared(StgClosure *q)
{
    nat i;
    StgClosure *p, *spark;
    const StgInfoTable *info;

    spark = UNTAG_CLOSURE(q);
    info = get_itbl(spark);

    if (closure_THUNK(spark) && info->type != THUNK_STATIC) {
        saveSpark(spark);
    } else if (GET_INFO(spark) == &stg_BLACKHOLE_info &&
               REPLAY_IND_IS_THUNK(spark)) {
        return;
    }

    switch (info->type) {
    case FUN:
    case FUN_1_0: case FUN_0_1:
    case FUN_1_1: case FUN_0_2: case FUN_2_0:
    case CONSTR:
    case CONSTR_1_0: case CONSTR_0_1:
    case CONSTR_1_1: case CONSTR_0_2: case CONSTR_2_0:
    case CONSTR_STATIC: case CONSTR_NOCAF_STATIC:
    case FUN_STATIC:
        for (i = 0; i < info->layout.payload.ptrs; i++) {
            p = spark->payload[i];
            markShared(p);
        }
        break;
    case THUNK:
    case THUNK_1_0: case THUNK_0_1:
    case THUNK_1_1: case THUNK_0_2: case THUNK_2_0:
    case THUNK_STATIC:
        for (i = 0; i < info->layout.payload.ptrs; i++) {
            p = ((StgThunk *)spark)->payload[i];
            markShared(p);
        }
        break;
    case THUNK_SELECTOR:
    {
        StgSelector *selector = (StgSelector *)spark;
        markShared(selector->selectee);
        break;
    }
    case AP:
    {
        StgAP *ap = (StgAP *)spark;
        sharedPAP(ap->fun, ap->payload, ap->n_args);
        break;
    }
    case PAP:
    {
        StgPAP *pap = (StgPAP *)spark;
        sharedPAP(pap->fun, pap->payload, pap->n_args);
        break;
    }
    case AP_STACK:
    {
        StgAP_STACK *ap = (StgAP_STACK *)spark;
        markShared(ap->fun);
        //for (i = 0; i < ap->size; i++) {
        //    p = ap->payload[i];
        //    ASSERT(p != NULL);
        //    replayShared(cap, p, 0);
        //}
        break;
    }
    case IND:
    case IND_PERM:
    case IND_STATIC:
        markShared(((StgInd *)spark)->indirectee);
        break;
    case BLACKHOLE:
        if (REPLAY_IND_IS_TSO(spark)) {
            p = (StgClosure *)findThread(REPLAY_IND_TSO(spark));
        } else {
            p = REPLAY_IND_PTR(spark);
        }
        markShared(p);
        break;
    // ignore
    case TSO:
    case ARR_WORDS:
    case MVAR_CLEAN:
    case MVAR_DIRTY:
    case MUT_VAR_CLEAN:
    case MUT_VAR_DIRTY:
        break;
    default:
        barf("markShared: unexpected closure type %s", info_type(spark));
    }
}

StgClosure *
replayNewSpark(StgClosure *p)
{
    if (TRACE_spark_full) {
        //if (replay_enabled) {
        //    //markShared(p);
        //    //saveSpark(p);
        //}
        StgClosure *ind = ((StgInd *)p)->indirectee;
        if (REPLAY_ATOM(ind) == REPLAY_ID_ATOM) {
            ((StgInd *)p)->indirectee = REPLAY_SET_SPARK(REPLAY_PTR(ind));
            return ind;
        }
    }
    return NULL;
}

void
replayStealSpark(Capability *cap, StgClosure *spark)
{
    cap->replay.saved_sparks[cap->replay.saved_sparks_idx++] = spark;
    if (cap->replay.saved_sparks_idx == cap->replay.saved_sparks_size) {
        cap->replay.saved_sparks_size *= 2;
        cap->replay.saved_sparks =
            stgReallocBytes(cap->replay.saved_sparks,
                            cap->replay.saved_sparks_size *
                            sizeof(StgClosure *), "replayStealSpark");
    }
}

void
replayRestoreSpark(StgClosure *bh)
{
    StgClosure *p = lookupHashTable(spark_thunks, (W_)bh);
    ASSERT(p != NULL);
    memcpy(bh, p, closure_sizeW(p) * sizeof(W_));
    //((StgInd *)bh)->indirectee = indirectee;
}

static StgClosure *
restoreSpark(Capability *cap, StgClosure *bh) {
    debugReplay("cap %d: task %d: restoring spark %p\n",
                cap->no, cap->running_task->no, bh);
    ASSERT(cap->replay.restored_bh == NULL);
    ASSERT(cap->replay.restored_ind == NULL);
    cap->replay.restored_bh = bh;
    cap->replay.restored_ind = ((StgInd *)bh)->indirectee;
    cap->replay.restored_info = (StgInfoTable *)GET_INFO(bh);
    replayRestoreSpark(bh);
    return bh;
}

// find thunk id when it has already been blackholed
static W_
blackHoleId(StgClosure *bh)
{
    ASSERT(REPLAY_IND_IS_BH(bh));

    StgClosure *p;

    p = lookupHashTable(spark_thunks, (W_)bh);
    ASSERT(p != NULL);
    ASSERT(REPLAY_IND_IS_THUNK(p));

    return REPLAY_IND_ID(p);
}

// find thunk tso when it has already been blackholed
static W_
blackHoleTso(StgClosure *bh)
{
    ASSERT(REPLAY_IND_IS_BH(bh));

    StgClosure *p;

    p = lookupHashTable(spark_thunks, (W_)bh);
    ASSERT(p != NULL);
    ASSERT(REPLAY_IND_IS_THUNK(p));

    return REPLAY_IND_TSO(p);
}

// find thunk id from event
static W_
eventId(CapEvent *ce)
{
    W_ id;
    void *p;

    if (ce->ev->header.tag == EVENT_MSG_BLACKHOLE) {
        EventMsgBlackHole *ev = (EventMsgBlackHole *)ce->ev;
        if (ev->id != 0) {
            id = ev->id;
        } else {
            p = lookupHashTable(spark_ptrs, ev->ptr);
            ASSERT(p != NULL);
            id = thunkId(p);
        }
    } else if (isEventCapValue(ce, BH_WHNF) ||
               isEventCapValue(ce, BH_THUNK) ||
               isEventCapValue(ce, SPARK_FIZZLE)) {
        p = lookupHashTable(spark_ptrs, ((EventCapValue *)ce->ev)->value);
        ASSERT(p != NULL);
        id = thunkId(p);
    } else {
        barf("eventId: unknown event '%s'",
             EventDesc[ce->ev->header.tag]);
    }

    ASSERT(id != 0);
    return id;
}

StgClosure *
replayFindSpark(Capability *cap)
{
    CapEvent *ce;
    Capability *owner;
    StgClosure *spark;
    int tag;
    W_ id;

    ce = readEvent();
    while (isEventCapValue(ce, SPARK_FIZZLE)) {
        // may be ours
        id = eventId(ce);
        owner = capThunkId(id);
        ASSERT(owner != NULL);

        prepareSpark(owner, id);
        spark = tryStealSpark(owner->sparks);

        // !fizzledSpark(spark)
        if (REPLAY_IND_IS_THUNK(spark)) {
            debugReplay("cap %d: task %d: spark %d (%p) should be fizzled, syncing\n",
                        cap->no, cap->running_task->no, id, spark);
            ce = searchThunkUpdated(id, spark);
            ASSERT(ce != NULL);
            owner = capabilities[ce->capno];
            ASSERT(owner != cap);
            replaySyncThunk(owner, spark);
        }
        // fizzledSpark(spark)
        ASSERT(!REPLAY_IND_IS_THUNK(spark));

        cap->spark_stats.fizzled++;
        debugReplay("cap %d: task %d: spark %p fizzled\n",
                    cap->no, cap->running_task->no, spark);
        replayStealSpark(cap, spark);
        replayTraceCapValue(cap, SPARK_FIZZLE, (W_)spark);
        ce = readEvent();
    }

    if (isEventCapValue(ce, SPARK_RUN) ||
        isEventCapValue(ce, SPARK_STEAL)) {
        id = ((EventCapValue *)ce->ev)->value;
        tag = ((EventCapValue *)ce->ev)->tag;
        owner = capThunkId(id);
        if (tag == SPARK_RUN) {
            ASSERT(owner == cap);
        } else {
            ASSERT(owner != NULL);
        }
        prepareSpark(owner, id);
        spark = tryStealSpark(owner->sparks);

        cap->spark_stats.converted++;

#ifdef DEBUG
        if (tag == SPARK_RUN) {
            debugReplay("cap %d: task %d: run spark %d (%p)\n",
                        cap->no, cap->running_task->no, id, spark);
        } else {
            debugReplay("cap %d: task %d: spark %d (%p) stolen from cap %d\n",
                        cap->no, cap->running_task->no, id, spark, owner->no);
        }
#endif

        replayStealSpark(cap, spark);

        id = thunkId(spark);
        ASSERT(id != 0);
        replayTraceCapValue(cap, tag, id);

        return spark;
    }

    return NULL;
}

void
replayReleaseCapability (Capability *from, Capability* cap)
{
    CapEvent *ce;
    Task *task;

    task = cap->running_task;

    debugReplay("cap %d: task %d: releaseCapability_\n", from->no, task->no);

    cap->running_task = NULL;

    ce = readEvent();
    ASSERT(ce != NULL);
    if (ce->ev->header.tag == EVENT_TASK_CREATE) {
        // startWorkerTask() asserts lock held
        ACQUIRE_LOCK(&cap->lock);
	startWorkerTask(from, cap);
        RELEASE_LOCK(&cap->lock);
    }

    traceTaskReleaseCap(cap, task);
}

static void
taskAssignCap(Task *task, Capability **pCap)
{
    CapEvent *ce;
    nat capno;

    if (!replay_enabled) return;

    ce = readEvent();
    ASSERT(ce != NULL);
    switch (ce->ev->header.tag) {
    case EVENT_TASK_ACQUIRE_CAP:
        capno = ce->capno;
        break;
    case EVENT_TASK_RETURN_CAP:
        capno = ((EventTaskReturnCap *)ce->ev)->capno;
        break;
    default:
        replayError("Unexpected event in taskAssignCap: '%s'",
                    EventDesc[ce->ev->header.tag]);
    }
    *pCap = capabilities[capno];
    task->cap = *pCap;
}

void
replayWaitForReturnCapability(Capability **pCap, Task *task)
{
    Capability *cap;
    CapEvent *ce;

    cap = *pCap;

    if (cap == NULL) {
        taskAssignCap(task, &cap);
    }

    // worker tasks should not migrate
    ASSERT(task->incall->tso != NULL || cap == task->cap);

    ce = readEvent();
    ASSERT(ce != NULL);
    switch (ce->ev->header.tag) {
    case EVENT_TASK_ACQUIRE_CAP:
        break;
    case EVENT_TASK_RETURN_CAP:
        // newReturningTask(cap, task);
        if (cap->returning_tasks_hd) {
            ASSERT(cap->returning_tasks_tl->next == NULL);
            cap->returning_tasks_tl->next = task;
        } else {
            cap->returning_tasks_hd = task;
        }
        cap->returning_tasks_tl = task;

        traceTaskReturnCap(task, cap);

        taskAssignCap(task, &cap);

        // popReturningTask(cap);
        ASSERT(task == cap->returning_tasks_hd);
        cap->returning_tasks_hd = task->next;
        if (!cap->returning_tasks_hd) {
            cap->returning_tasks_tl = NULL;
        }
        task->next = NULL;
        break;
    default:
        replayError("Unexpected event in waitForReturnCapability: '%s'",
                    EventDesc[ce->ev->header.tag]);
    }

    cap->running_task = task;
    traceTaskAcquireCap(cap, task);

    *pCap = cap;
}

static void replayGcWorkerThread(Capability *cap);

static void
yieldCapability_(Capability **pCap, Task *task)
{
    Capability *cap;
    CapEvent *ce;

    cap = *pCap;

    debugReplay("cap %d: task %d: yieldCapability\n", cap->no, task->no);

    ce = readEvent();
    ASSERT(ce != NULL);
    if (ce->ev->header.tag == EVENT_GC_START) {
        traceEventGcStart(cap);
        replayGcWorkerThread(cap);
        traceEventGcEnd(cap);
        traceSparkCounters(cap);
        if (task->cap == cap) {
            return;
        }
    }

    // releaseCapabilityAndQueueWorker()
    debugReplay("cap %d: task %d: releaseCapabilityAndQueueWorker\n", cap->no, task->no);
    ASSERT(!task->stopped);
    if (!isBoundTask(task)) {
        if (cap->n_spare_workers < MAX_SPARE_WORKERS) {
            task->next = cap->spare_workers;
            cap->spare_workers = task;
            cap->n_spare_workers++;
        } else {
            replayReleaseCapability(cap, cap);
            workerTaskStop(cap, task);
            shutdownThread();
        }
    }
    replayReleaseCapability(cap, cap);

    // continue yieldCapability()
    taskAssignCap(task, &cap);
    // worker tasks should not migrate
    ASSERT(task->incall->tso != NULL || *pCap == task->cap);

    // remove worker from spare_workers
    if (task->incall->tso == NULL) {
        ASSERT(cap->spare_workers == task);
        cap->spare_workers = task->next;
        task->next = NULL;
        cap->n_spare_workers--;
    }

    cap->running_task = task;
    traceTaskAcquireCap(cap, task);

    *pCap = cap;
}

static rtsBool
replayProdCapability(Capability *cap, Task *task)
{
    CapEvent *ce;

    ce = readEvent();
    ASSERT(ce != NULL);
    if (ce->ev->header.tag == EVENT_TASK_ACQUIRE_CAP &&
        ce->capno == cap->no) {
        cap->running_task = task;
        traceTaskAcquireCap(cap, task);
        replayReleaseCapability(task->cap, cap);
        return rtsTrue;
    }
    return rtsFalse;
}

rtsBool
replayTryGrabCapability(Capability *cap, Task *task)
{
    CapEvent *ce;

    ce = readEvent();
    ASSERT(ce != NULL);
    if (ce->ev->header.tag == EVENT_TASK_ACQUIRE_CAP &&
        ce->capno == cap->no) {
        debugReplay("cap %d: task %d: tryGrabCapability %d\n",
                    task->cap->no, task->no, cap->no);

        task->cap = cap;
        cap->running_task = task;
        traceTaskAcquireCap(cap, task);
        return rtsTrue;
    }
    return rtsFalse;
}

void
replayShutdownCapability(Capability *cap, Task *task)
{
    CapEvent *ce;

    while (1) {
        cap->running_task = task;
        traceTaskAcquireCap(cap, task);

        ce = readEvent();
        ASSERT(ce != NULL);
        if (ce->ev->header.tag != EVENT_TASK_RELEASE_CAP) {
            break;
        }
        replayReleaseCapability(cap, cap);
    }
    traceSparkCounters(cap);
}

// Schedule.c
void
replayProcessInbox(Capability **pCap)
{
    Capability *cap;

    cap = *pCap;

    while (1) {
        // before the first run there is no nursery nor g0
        if (cap->r.rCurrentNursery && g0) {
            if (cap->r.rCurrentNursery->link == NULL ||
                g0->n_new_large_words >= large_alloc_lim) {
                if (isEventCapValue(readEvent(), GC)) {
                    scheduleDoGC (&cap, cap->running_task, rtsFalse);
                    *pCap = cap;
                }
            }
        }

        if (isEventCapValue(readEvent(), PROCESS_INBOX)) {
            Message *m, *next;
            int r;

            m = cap->inbox;
            cap->inbox = (Message *)END_TSO_QUEUE;

            next = m;
            r = 0;
            while (next != (Message *)END_TSO_QUEUE) {
                next = next->link;
                r++;
            }
            // if (r < ev->value) waitForMessages(cap);
            replayTraceCapValue(cap, PROCESS_INBOX, r);

            while (m != (Message *)END_TSO_QUEUE) {
                MessageBlackHole *msg = (MessageBlackHole *)m;
                next = m->link;
                if (GET_INFO((StgClosure *)m) == &stg_MSG_BLACKHOLE_info &&
                    REPLAY_IND_IS_THUNK(msg->bh)) {
                    // overwritten with result
                    barf("replayProcessInbox");
                    tryWakeupThread(cap, msg->tso);
                } else {
                    executeMessage(cap, m);
                }
                m = next;
            }
        } else {
            break;
        }
    }
}

void
replayActivateSpark(Capability *cap)
{
    CapEvent *ce;

    ce = readEvent();
    ASSERT(ce != NULL);
    if (ce->ev->header.tag == EVENT_CREATE_THREAD) {
        debugReplay("cap %d: task %d: scheduleActivateSpark\n",
                    cap->no, cap->running_task->no);
        createSparkThread(cap);
    }
}

void
replayPushWork(Capability *cap, Task *task)
{
    Capability *free_caps[n_capabilities], *cap0;
    nat i, n_free_caps;
    CapEvent *ce;

    for (i = 0, n_free_caps = 0; i < n_capabilities; i++) {
        cap0 = capabilities[i];
        if (cap != cap0 && !cap0->disabled && tryGrabCapability(cap0, task)) {
            ce = readEvent();
            ASSERT(ce != NULL);
            if (ce->capno == cap0->no && ce->ev->header.tag == EVENT_TASK_RELEASE_CAP) {
                releaseCapability(cap, cap0);
            } else {
                free_caps[n_free_caps++] = cap0;
            }
        }
    }

    if (n_free_caps > 0) {
        StgTSO *prev, *t, *next;

        i = 0;

        if (cap->run_queue_hd != END_TSO_QUEUE) {
            prev = cap->run_queue_hd;
            t = prev->_link;
            prev->_link = END_TSO_QUEUE;
            for (; t != END_TSO_QUEUE; t = next) {
                next = t->_link;
                t->_link = END_TSO_QUEUE;
                if (t->bound == task->incall || tsoLocked(t)) {
                    setTSOLink(cap, prev, t);
                    setTSOPrev(cap, t, prev);
                    prev = t;
                } else if (i == n_free_caps) {
                    i = 0;
                    setTSOLink(cap, prev, t);
                    setTSOPrev(cap, t, prev);
                    prev = t;
                } else {
                    appendToRunQueue(free_caps[i],t);

                    traceEventMigrateThread (cap, t, free_caps[i]->no);

                    if (t->bound) { t->bound->task->cap = free_caps[i]; }
                    t->cap = free_caps[i];
                    i++;
                }
            }
            cap->run_queue_tl = prev;
        }

        // release the capabilities
        for (i = 0; i < n_free_caps; i++) {
            task->cap = free_caps[i];
            releaseAndWakeupCapability(cap, free_caps[i]);
        }
    }
    task->cap = cap; // reset to point to our Capability.
}

void
replayDetectDeadlock(Capability **pCap, Task *task)
{
    CapEvent *ce;

    ce = readEvent();
    if (isEventCapValue(ce, GC)) {
        // majorGC
        if (((EventCapValue *)ce->ev)->value) {
            debugReplay("cap %d: task %d: scheduleDetectDeadlock\n", (*pCap)->no, task->no);
            scheduleDoGC (pCap, task, rtsTrue);
        }
    }
}

void
replayYield(Capability **pCap, Task *task)
{
    CapEvent *ce;
    Capability *cap;

    cap = *pCap;

    // yieldCapability ->
    //     EVENT_GC_START (yieldCapability)                                             ||
    //     EVENT_TASK_CREATE (yieldCapability => releaseCapability_)                    ||
    //     EVENT_TASK_RELEASE_CAP (yieldCapability => releaseCapabilityAndQueueWorker)  ||
    // !yieldCapability ->
    //     EVENT_RUN_THREAD ||
    //     EVENT_CAP_VALUE (SCHED_LOOP) (continue scheduler loop, or deleteThread)
    while (1) {
        // if the task migrates, it must be updated
        ASSERT(cap == task->cap);

        ce = readEvent();
        ASSERT(ce != NULL);
        switch (ce->ev->header.tag) {
        case EVENT_GC_START:
        case EVENT_TASK_CREATE:
        case EVENT_TASK_RELEASE_CAP:
            yieldCapability_(&cap, task);
            break;
        // !yieldCapability ->
        case EVENT_CAP_VALUE:
            ASSERT(((EventCapValue *)ce->ev)->tag == SCHED_LOOP);
        case EVENT_RUN_THREAD:
            *pCap = cap;
            return;
        default:
            debugReplay("Imposible event after scheduleYield():\n");
            printEvent(cap, ce->ev);
            abort();
            stg_exit(EXIT_INTERNAL_ERROR);
        }
    }
}

// Returns 0 for the task initiating the GC
nat
replayRequestSync(Capability **pCap, Task *task, nat sync_type)
{
    nat prev_pending_sync;
    CapEvent *ce;

    debugReplay("cap %d: task %d: requestSync\n", (*pCap)->no, task->no);

    ce = readEvent();
    ASSERT(ce != NULL);
    // yieldCapability ->
    //     EVENT_GC_START (yieldCapability)                                             ||
    //     EVENT_TASK_CREATE (yieldCapability => releaseCapability_)                    ||
    //     EVENT_TASK_RELEASE_CAP (yieldCapability => releaseCapabilityAndQueueWorker)  ||
    // !yieldCapability ->
    //     EVENT_REQUEST_SEQ_GC ||
    //     EVENT_REQUEST_PAR_GC
    switch (ce->ev->header.tag) {
    case EVENT_GC_START:
    case EVENT_TASK_CREATE:
    case EVENT_TASK_RELEASE_CAP:
        do {
            // if we do SCHED_END immediately after, that means we are in
            // exitScheduler() and need to exit to release the capability
            // there
            if (isEventCapValue(peekEventCap(1, ce->capno), SCHED_END)) {
                break;
            }

            yieldCapability_(pCap, task);
            ce = readEvent();
            ASSERT(ce != NULL);
        } while (ce->ev->header.tag == EVENT_GC_START ||
                 ce->ev->header.tag == EVENT_TASK_CREATE ||
                 ce->ev->header.tag == EVENT_TASK_RELEASE_CAP);
        prev_pending_sync = sync_type;
        break;
    case EVENT_REQUEST_SEQ_GC:
    case EVENT_REQUEST_PAR_GC:
        pending_sync = sync_type;
        prev_pending_sync = 0;
        break;
    default:
        debugReplay("Imposible event after requestSync():\n");
        printEvent(*pCap, ce->ev);
        abort();
        stg_exit(EXIT_INTERNAL_ERROR);
    }

    if (prev_pending_sync == 0) {
        ASSERT(replay_main_gc_task == -1);
        replay_main_gc_task = task->no;
    }
    return prev_pending_sync;
}

void
replayExitScheduler(Task *task)
{
    CapEvent *ce;

    ce = searchEventCapTagValueBefore(task->cap->no, SCHED_END, 0, EVENT_CAPSET_REMOVE_CAP);
    //ce = searchEventTagValueBefore(SCHED_END, 0, EVENT_CAPSET_REMOVE_CAP);
    if (ce != NULL) {
        // sched_state < SCHED_SHUTTING_DOWN
        if (sched_state < SCHED_INTERRUPTING) {
            sched_state = SCHED_INTERRUPTING;
        }
        debugReplay("task %d: SCHED_INTERRUPTING\n", task->no);

        Capability *cap = task->cap;
        waitForReturnCapability(&cap,task);
        scheduleDoGC(&cap,task,rtsTrue);
        ASSERT(task->incall->tso == NULL);
        replayTraceCapTag(cap, SCHED_END);
        releaseCapability(cap, cap);
    }

    sched_state = SCHED_SHUTTING_DOWN;
    debugReplay("task %d: SCHED_SHUTTING_DOWN\n", task->no);

    shutdownCapabilities(task, rtsFalse);

    boundTaskExiting(task);
}

// RtsAPI.c
void
replayRtsUnlock(Capability *cap, Task *task)
{
    replayReleaseCapability(cap, cap);

    boundTaskExiting(task);

    if (task->incall == NULL) {
        traceTaskDelete(cap, task);
    }
}

// ThreadPaused.c

// Returns whether we are going to suspend the current computation
rtsBool
replayThreadPaused(Capability *cap, StgClosure *p)
{
    CapEvent *ce;

    ce = readEvent();
    if (cap->replay.sync_thunk == NULL &&
        isEventCapValue(ce, SUSPEND_COMPUTATION) &&
        lookupHashTable(spark_ptrs, ((EventCapValue *)ce->ev)->value) ==
            p) {
        ASSERT(GET_INFO(p) == &stg_BLACKHOLE_info);
        return rtsTrue;
    }

    if (GET_INFO(p) == &stg_BLACKHOLE_info) {
        replayRestoreSpark(p);
    }
    return rtsFalse;
}

// StgMiscClosures.cmm

static Capability *
findBHOwner(Capability *cap, StgClosure *bh)
{
    Capability *owner;
    StgTSO *tso;

    if (REPLAY_IND_ATOM(bh) == 0 &&
        GET_INFO(((StgInd *)bh)->indirectee) == &stg_TSO_info) {
        owner = ((StgTSO *)((StgInd *)bh)->indirectee)->cap;
    } else if (REPLAY_IND_IS_TSO(bh)) {
        tso = findThread(REPLAY_IND_TSO(bh));
        if (tso == cap->r.rCurrentTSO) {
            tso = lookupHashTable(spark_owners, (W_)bh);
        }
        owner = tso->cap;
    // BQ | REPLAY_PTR_ATOM
    } else {
        ASSERT(get_itbl(REPLAY_IND_PTR(bh))->type == BLOCKING_QUEUE);
        StgBlockingQueue *bq = (StgBlockingQueue *)REPLAY_IND_PTR(bh);
        owner = bq->owner->cap;
    }
    return owner;
}

// returns a closure to enter or NULL to evaluate the blackhole
StgClosure *
replayBlackHole(Capability *cap, StgClosure *bh, StgPtr Hp)
{
    CapEvent *ce;
    StgClosure *p = NULL, *ind;
    W_ id;

    ASSERT(cap->replay.restored_bh == NULL);
    if (cap->replay.restored_bh != NULL) {
        SET_INFO(cap->replay.restored_bh, &stg_BLACKHOLE_info);
        cap->replay.restored_bh = NULL;
    }

    ind = ((StgInd *)bh)->indirectee;

    id = thunkId(bh);
    if (id == 0) {
        if (GET_CLOSURE_TAG(ind) == 0) {
            Capability *owner = findBHOwner(cap, bh);
            ASSERT(owner != cap);
            debugReplay("replaySyncThunk Hp = %p\n", Hp);
            replaySyncThunk(owner, bh);
        }
        return NULL;
    }

    ce = readEvent();

    // enter the thunk
    if (cap->replay.sync_thunk == bh ||
        (ce->ev->header.tag == EVENT_ENTER_THUNK &&
         ((EventEnterThunk *)ce->ev)->id == id)) {
        p = restoreSpark(cap, bh);

    // enter blackhole
    } else if (// explicit event
               ((isEventCapValue(ce, BH_WHNF) ||
                 ce->ev->header.tag == EVENT_MSG_BLACKHOLE ||
                 isEventCapValue(ce, BH_THUNK)) &&
                eventId(ce) == id) ||
               // untagged BH
               REPLAY_ATOM(ind) == 0) {
        if (REPLAY_ATOM(ind) != 0 &&
            ((isEventCapValue(ce, BH_WHNF) &&            // needs to point to WHNF
              (REPLAY_ATOM(ind) != REPLAY_PTR_ATOM ||
               GET_CLOSURE_TAG(ind) == 0)) ||
             (isEventCapValue(ce, BH_THUNK) &&           // needs to be updated
              (REPLAY_ATOM(ind) != REPLAY_PTR_ATOM ||
               GET_INFO(REPLAY_PTR(ind)) == &stg_TSO_info ||
               get_itbl(REPLAY_PTR(ind))->type == BLOCKING_QUEUE)))) {
            Capability *owner = findBHOwner(cap, bh);
            ASSERT(owner != cap);
            debugReplay("replaySyncThunk Hp = %p\n", Hp);
            replaySyncThunk(owner, bh);
        }

    // no event, continue
    } else if (REPLAY_IS_SHARED(ind)) {
        // there would be an event otherwise
        p = restoreSpark(cap, bh);
    } else if (REPLAY_ATOM(ind) != REPLAY_PTR_ATOM ||
               GET_CLOSURE_TAG(ind) == 0) {
        Capability *owner = findBHOwner(cap, bh);
        ASSERT(owner != cap);
        replaySyncThunk(owner, bh);
    }

    return p;
}

MessageBlackHole *
replayMessageBlackHole(Capability *cap, StgClosure *p)
{
    MessageBlackHole *msg;
    StgClosure *ind;
    W_ r, id = 0;

    ind = ((StgInd *)p)->indirectee;
    if (TRACE_spark_full) {
        if (REPLAY_IS_TSO(ind)) {
            id = REPLAY_ID(ind);
        } else if (GET_CLOSURE_TAG(ind) != 0) {
            // XXX: check for replay enabled
            return NULL;
        } else {
            // BQ
            if (!REPLAY_ATOM(ind) == 0 ||
                !get_itbl(ind)->type == BLOCKING_QUEUE) {
                barf("replayMessageBlackHole: %p -> %p is not BQ", p, ind);
            }
            ASSERT(REPLAY_ATOM(ind) == 0);
            ASSERT(get_itbl(ind)->type == BLOCKING_QUEUE);
        }
        traceMsgBlackHole(cap, (StgPtr)p, id);
    }

    msg = (MessageBlackHole *)allocate(cap, sizeofW(MessageBlackHole));
    SET_HDR(msg, &stg_MSG_BLACKHOLE_info, CCS_SYSTEM);
    msg->tso = cap->r.rCurrentTSO;
    msg->bh = p;

    if (replay_enabled) {
        CapEvent *ce = readEvent();
        if (REPLAY_ATOM(ind) != 0 &&
            !isEventCapValue(ce, BLOCKED_ON_TSO) &&
            !isEventCapValue(ce, BLOCKED_ON_BQ)) {
            debugReplay("cap %d: messageBlackHole: thread %d blocking on blackhole %p -> %p\n",
                        cap->no, cap->r.rCurrentTSO->id, p, ind);
            return NULL;
        }
    }

    r = messageBlackHole(cap, msg);
    if (r) {
        return msg;
    } else {
        return NULL;
    }
}

// Updates
W_
replayEnter(Capability *cap, StgClosure *p)
{
    StgClosure *ind;
    StgThreadID tso;
    W_ atom, id, shared;

    ind = ((StgInd *)p)->indirectee;
    atom = REPLAY_ATOM(ind);
    tso = REPLAY_TSO(ind);
    id = REPLAY_ID(ind);

    if (replay_enabled) {
        CapEvent *ce;
        rtsBool restored = rtsFalse;

        ASSERT(atom != 0);
        if (atom == 0) barf("replayEnter: atom %p %s", p, info_type(p));

        if (cap->replay.restored_bh != NULL) {
            ASSERT(cap->replay.restored_ind != NULL);
            ASSERT(cap->replay.restored_info != NULL);
            ((StgInd *)cap->replay.restored_bh)->indirectee = cap->replay.restored_ind;
            SET_INFO(cap->replay.restored_bh, cap->replay.restored_info);
            cap->replay.restored_bh = NULL;
            cap->replay.restored_ind = NULL;
            cap->replay.restored_info = NULL;
            restored = rtsTrue;
        }

        if (cap->replay.sync_thunk != p) {
            if (!REPLAY_IS_THUNK(ind)) {
                tso = blackHoleTso(p);
                id = blackHoleId(p);
            }
            ASSERT(tso != 0);
            ASSERT(id != 0);

            ce = peekEventCap(0, cap->no);
            // enter blackhole
            if (// blackhole event
                ((isEventCapValue(ce, BH_WHNF) ||
                  ce->ev->header.tag == EVENT_MSG_BLACKHOLE ||
                  isEventCapValue(ce, BH_THUNK)) &&
                 eventId(ce) == id) ||
                // no enter event when it should have been one
                //(tso != cap->r.rCurrentTSO->id &&
                (capThunkId(id) != cap &&
                 (ce->ev->header.tag != EVENT_ENTER_THUNK ||
                  ((EventEnterThunk *)ce->ev)->id != id))) {
                Capability *owner = findThread(tso)->cap;
                if (owner == cap) { barf("replayEnter: sync with same cap"); }
                ASSERT(owner != cap);
                replaySyncThunk(owner, p);
                ASSERT(get_itbl(p)->type == BLACKHOLE);
                return 1;
            }

            W_ te;
            if ((te = (W_)lookupHashTable(spark_user, (W_)p)) != 0) {
                fprintf(stderr, "entering tracked expression %p %" FMT_Word64 "\n", p, te);
            }

            // first thread entering
            if (!restored) {
                ASSERT(atom == REPLAY_ID_ATOM || atom == REPLAY_SPARK_ATOM);
                insertHashTable(spark_owners, (W_)p, findThread(tso)); // XXX: move to saveSpark
                saveSpark(p);
                ASSERT(!REPLAY_IS_BH(ind));
                if (REPLAY_IS_BH(ind)) { barf("replayEnter: IS_BH %p %p", p, ind); }

            } else {
                debugReplay("cap %d: tso %d: replayEnter %p -> %p\n",
                            cap->no, cap->r.rCurrentTSO->id, p, ind);
                // enter event
                if (ce->ev->header.tag == EVENT_ENTER_THUNK &&
                    ((EventEnterThunk *)ce->ev)->id == id) {
                    traceEnterThunk(cap, id, (StgPtr)p);
                }
                return 0;
            }
        }
    }

#ifndef REPLAY_LAZY
    // already updated
    if (TRACE_spark_full && !replay_enabled && REPLAY_IS_BH(ind)) {
        debugReplay("cap %d: tso %d: replayEnter %p -> %p already updated\n",
                    cap->no, cap->r.rCurrentTSO->id, p, ((StgInd *)p)->indirectee);
        return 1;
    }
#endif

    ASSERT(atom != 0);
    if (atom == 0) barf("replayEnter: atom %p %s", p, info_type(p));
    debugReplay("cap %d: tso %d: replayEnter %p\n",
                cap->no, cap->r.rCurrentTSO->id, p);

    shared = atom == REPLAY_ID_ATOM && capThunkId(id) != cap;
    ASSERT(capThunkId(id) != cap || atom == REPLAY_SPARK_ATOM || replay_enabled);
    if (capThunkId(id) == cap && atom != REPLAY_SPARK_ATOM && !replay_enabled) barf("replayEnter: cap %p", p);
    if (eager_blackholing) {
        W_ tso_atom = shared ? REPLAY_SHARED_TSO : REPLAY_TSO_ATOM;
        ((StgInd *)p)->indirectee = REPLAY_SET_TSO(id, tso_atom, cap->r.rCurrentTSO);
        write_barrier();
        SET_INFO(p, &__stg_EAGER_BLACKHOLE_info);
        if (replay_enabled && cap->replay.sync_thunk == p) {
            replayCont(cap, cap->running_task);
        }
    }
    if (shared || atom == REPLAY_SPARK_ATOM) {
        traceEnterThunk(cap, id, (StgPtr)p);
    }

    return 0;
}

StgClosure *
replayMarkedUpdFrame(Capability *cap, StgClosure *updatee, StgClosure *ret, StgPtr Hp)
{
    CapEvent *ce;
    StgTSO *tso;
    StgClosure *ind;
    W_ id;

    tso = cap->r.rCurrentTSO;
    ind = ((StgInd *)updatee)->indirectee;
    //ASSERT(REPLAY_ATOM(ind) != 0);
    id = thunkId(updatee);

    ce = readEvent();
    ASSERT(ce != NULL);
    if (cap->replay.sync_thunk != updatee &&
        isEventCapValue(ce, COLLISION_WHNF) &&
        lookupHashTable(spark_ptrs, ((EventCapValue *)ce->ev)->value) ==
            updatee) {
        ASSERT(REPLAY_ATOM(ind) != 0);
        StgClosure *v = REPLAY_PTR(ind);
        if (REPLAY_ATOM(ind) != REPLAY_PTR_ATOM || GET_CLOSURE_TAG(v) == 0) {
            Capability *owner = findBHOwner(cap, updatee);
            ASSERT(owner != cap);
            replaySyncThunk(owner, updatee);
            if (REPLAY_IND_IS_TSO(updatee)) {
                replaySyncThunk(owner, updatee);
            }
            ASSERT(!REPLAY_IND_IS_TSO(updatee));
            v = REPLAY_IND_PTR(updatee);
            ASSERT(GET_CLOSURE_TAG(v) != 0);
        } else {
            v = REPLAY_PTR(ind);
        }
        debugReplay("cap %d: tso %d: collision: %p -> %p already updated with %p [Hp = %p]\n",
                    cap->no, tso->id, updatee, ret, v, Hp);
        if (emit_dups) {
            traceCapValue(cap, DUP_SPARK, id);
        }
        replayTraceCapValue(cap, COLLISION_WHNF, (W_)updatee);
        checkBlockingQueues(cap, tso);
        return v;

    } else if (isEventCapValue(ce, COLLISION_OTHER) &&
             lookupHashTable(spark_ptrs, ((EventCapValue *)ce->ev)->value) ==
                 updatee) {
        debugReplay("cap %d: tso %d: marked_upd_frame: other case updateThunk(%p, %p) [Hp = %p]\n",
                    cap->no, tso->id, updatee, ret, Hp);
        debugReplay("cap %d: updateThunk: updating %p\n", cap->no, updatee);
        updateWithIndirection(cap, updatee, ret);
        replayTraceCapValue(cap, COLLISION_OTHER, (W_)updatee);
        checkBlockingQueues(cap, tso);

    // no event, our TSO
    } else if (REPLAY_IS_TSO(ind) ||
               (REPLAY_ATOM(ind) == 0 && GET_INFO(ind) == &stg_TSO_info)) {
        ASSERT(REPLAY_ATOM(ind) != REPLAY_PTR_ATOM);
        //if (REPLAY_ATOM(ind) == REPLAY_SHARED_TSO) {
            debugReplay("cap %d: tso %d: marked_upd_frame: update our BH %p -> %p with %p [Hp = %p]\n",
                        cap->no, tso->id, updatee, ind, ret, Hp);
        //}
        updateWithIndirection(cap, updatee, ret);

    // no event, our BQ
    } else {
        debugReplay("cap %d: tso %d: marked_upd_frame: other case updateThunk(%p, %p) [Hp = %p]\n",
                    cap->no, tso->id, updatee, ret, Hp);
        ASSERT(!eager_blackholing || REPLAY_ATOM(ind) == 0 || REPLAY_ATOM(ind) == REPLAY_PTR_ATOM);
        ASSERT(get_itbl(REPLAY_PTR(ind))->type == BLOCKING_QUEUE);
        updateThunk (cap, tso, updatee, ret);
    }
    return ret;
}

void
replayUpdateWithIndirection(Capability *cap, StgClosure *p, StgClosure *ind)
{
    //W_ id;

    debugReplay("cap %d: tso %d: updateWithIndirection: update BH %p with %p (was %p)\n",
                cap->no, cap->r.rCurrentTSO->id, p, ((StgInd *)p)->indirectee, ind);

    if (replay_enabled) {
        if (REPLAY_IND_ATOM(p) != 0 && REPLAY_IND_ATOM(p) != REPLAY_PTR_ATOM) {
            ASSERT(lookupHashTable(spark_ids, (W_)p) == NULL);
            insertHashTable(spark_ids, (W_)p, (void *)REPLAY_ID(ind));
        }
        //// save id, needed for sparks if not saved on enter
        //if (REPLAY_IND_ATOM(p) == REPLAY_PTR_ATOM) {
        //    ASSERT(REPLAY_ATOM(ind) != 0 && REPLAY_ATOM(ind) != REPLAY_PTR_ATOM);
        //    if (lookupHashTable(spark_ids, (W_)p) == NULL) {
        //        insertHashTable(spark_ids, (W_)p, (void *)REPLAY_ID(ind));
        //    }
        //}
        W_ te;
        if ((te = (W_)lookupHashTable(spark_user, (W_)p)) != 0 &&
            !REPLAY_IND_IS_TSO(p) &&
            !(REPLAY_IND_ATOM(p) == 0 &&
              (get_itbl(UNTAG_CLOSURE(((StgInd *)p)->indirectee))->type == TSO ||
               get_itbl(UNTAG_CLOSURE(((StgInd *)p)->indirectee))->type == BLOCKING_QUEUE))) {
            fprintf(stderr, "updating tracked expression %p %" FMT_Word64 "\n", p, te);
        }
        if (cap->replay.sync_thunk == p) {
//#ifdef DEBUG
//            CapEvent *ce = peekEventCap(0, cap->no);
//            ASSERT(isEventCapValue(ce, THUNK_UPDATE) &&
//                   lookupHashTable(spark_ptrs, ((EventCapValue *)ce->ev)->value)
//                       == p);
//#endif
            replayCont(cap, cap->running_task);
        }
        //if (emit_dups) {
        //    // XXX: just check indirectee is WHNF
        //    StgTSO *tso = lookupHashTable(spark_dups, (W_)p);
        //    id = (W_)lookupHashTable(spark_ids, (W_)p);
        //    ASSERT(id != 0);
        //    if (tso == NULL) {
        //        insertHashTable(spark_dups, (W_)p, cap->r.rCurrentTSO);
        //    } else if (tso != cap->r.rCurrentTSO &&
        //               sched_state != SCHED_INTERRUPTING) {
        //        if (REPLAY_ATOM(ind) == REPLAY_PTR_ATOM && GET_CLOSURE_TAG(ind) == 0) {
        //            if (GET_INFO(REPLAY_PTR(ind)) != &stg_BLACKHOLE_info &&
        //                GET_INFO(REPLAY_PTR(ind)) != &stg_TSO_info &&
        //                GET_INFO(REPLAY_PTR(ind)) != &stg_BLOCKING_QUEUE_CLEAN_info &&
        //                GET_INFO(REPLAY_PTR(ind)) != &stg_BLOCKING_QUEUE_DIRTY_info) {
        //                barf("replayUpdateWithIndirection: overwriting %s", info_type(REPLAY_PTR(ind)));
        //            }
        //        }
        //        traceCapValue(cap, DUP_SPARK, id);
        //    }
        //    if (GET_CLOSURE_TAG(REPLAY_IND_PTR(p)) != 0 ||
        //        sched_state == SCHED_INTERRUPTING) {
        //        if (sched_state == SCHED_INTERRUPTING) {
        //            ASSERT(lookupHashTable(spark_thunks, (W_)REPLAY_IND_PTR(p)) &&
        //                   GET_INFO((StgClosure *)lookupHashTable(spark_thunks, (W_)REPLAY_IND_PTR(p))) == &stg_AP_STACK_info);
        //        }
        //        traceCapValue(cap, WHNF_SPARK, id);
        //    }
        //}
    }

    //id = (W_)p;
    //replayStealSpark(cap, p);
 
    //replayTraceCapValue(cap, THUNK_UPDATE, id);
}

StgClosure *
replayUpdateThunk (Capability *cap, StgTSO *tso, StgClosure *thunk, StgClosure *val)
{
    CapEvent *ce = readEvent();
    if (isEventCapValue(ce, COLLISION_OTHER)) {
        replayTraceCapValue(cap, COLLISION_OTHER, (W_)thunk);
        checkBlockingQueues(cap, tso);
        // TODO: make sure it is already updated
        return thunk;
    } else {
        StgClosure *v;
        StgTSO *owner USED_IF_DEBUG;
        const StgInfoTable *i;
        i = thunk->header.info;
        if (i != &stg_BLACKHOLE_info &&
            i != &stg_CAF_BLACKHOLE_info &&
            i != &__stg_EAGER_BLACKHOLE_info &&
            i != &stg_WHITEHOLE_info) {
            debugReplay("cap %d: updateThunk: %p was not updated\n", cap->no, thunk);
            updateWithIndirection(cap, thunk, val);
            return NULL;
        }

        v = ((StgInd*)thunk)->indirectee;
        if (REPLAY_ATOM(v) != 0) {
            ASSERT(REPLAY_IS_BH(v));
            if (REPLAY_ATOM(v) == REPLAY_PTR_ATOM) {
                v = REPLAY_PTR(v);
            // REPLAY_TSO_ATOM || REPLAY_SHARED_TSO
            } else {
                v = (StgClosure *)findThread(REPLAY_TSO(v));
            }
        }
        ASSERT(REPLAY_ATOM(v) == 0);

        debugReplay("cap %d: updateThunk: updating %p\n", cap->no, thunk);
        updateWithIndirection(cap, thunk, val);

        if ((StgTSO*)v != tso) {
            i = v->header.info;
            ASSERT(INFO_PTR_TO_STRUCT(i)->type == BLOCKING_QUEUE);
            owner = ((StgBlockingQueue*)v)->owner;
            ASSERT(owner == tso);
            wakeBlockingQueue(cap, (StgBlockingQueue*)v);
        }
    }
    return NULL;
}

void
replayLockCAF(Capability *cap STG_UNUSED, W_ id)
{
    CapEvent *ce;
    Capability *owner;
    nat n = 0;

    // XXX: for -debug
    //do {
    //    ce = peekEventCap(n++, cap->no);
    //} while (isEventCapValue(ce, STEAL_BLOCK));
    //ASSERT(ce != NULL);
    //if (ce->ev->header.tag != EVENT_ENTER_THUNK ||
    //    ((EventEnterThunk *)ce->ev)->id != id) {
    //    ce = searchThunkUpdated(id, NULL);
    //    owner = capabilities[ce->capno];
    //    ASSERT(owner != cap);
    //    replaySync(owner, myTask());
    //}
}

// GC
void
replayStartGC(Capability *cap)
{
    if (replay_enabled) {
        ASSERT(gc_spark_thunks == NULL);
        gc_spark_thunks = allocHashTable();

        ASSERT(gc_spark_ptrs == NULL);
        gc_spark_ptrs = allocHashTable();

        ASSERT(gc_spark_ids == NULL);
        gc_spark_ids = allocHashTable();

        freeHashTable(spark_owners, NULL);
        spark_owners = allocHashTable();
    } else {
        ASSERT(replay_main_gc_task == -1);
        replay_main_gc_task = cap->running_task->no;
    }
}

// Returns the updated spark pointer if needed
static StgClosure *
resetSpark(Capability *cap, StgClosure *p, rtsBool steal)
{
    const StgInfoTable *info;
    rtsBool reset = rtsFalse;
    StgClosure *bh = NULL;

    // either blackhole or thunk
    ASSERT(GET_CLOSURE_TAG(p) == 0);

    info = GET_INFO(p);
    if (IS_FORWARDING_PTR(info)) {
        reset = rtsTrue;
        bh = (StgClosure *)UN_FORWARDING_PTR(info);
    } else if (HEAP_ALLOCED(p)) {
        if ((Bdescr((P_)p)->flags & BF_EVACUATED)) {
            bh = p;
        } else {
            reset = rtsTrue;
        }
    } else {
        ASSERT(get_itbl(p)->type == THUNK_STATIC || get_itbl(p)->type == IND_STATIC);
        if (*STATIC_LINK(INFO_PTR_TO_STRUCT(info), p) != NULL) {
            bh = p;
        } else {
            reset = rtsTrue;
        }
    }

    if (reset) {
        ASSERT(bh == NULL || bh != p);
    }
    if (reset && REPLAY_IND_ATOM(p) != 0) {
        debugReplay("cap %d: resetting %p\n", cap->no, p);
        REPLAY_RESET(p);
    }
    if (replay_enabled) {
        W_ v = (W_)lookupHashTable(spark_user, (W_)p);
        if (v != 0) {
            if (bh == NULL ||
                ((REPLAY_IND_ATOM(bh) == 0 || REPLAY_IND_ATOM(bh) == REPLAY_PTR_ATOM) &&
                 GET_CLOSURE_TAG(REPLAY_IND_PTR(bh)) != 0)) {
                removeHashTable(spark_user, (W_)p, NULL);
            } else if (bh != p) {
                removeHashTable(spark_user, (W_)p, NULL);
                insertHashTable(spark_user, (W_)bh, (void *)v);
            }
        }
    }
    if (bh != NULL) {
        debugReplay("cap %d: %p now is %p\n", cap->no, p, bh);
        if (REPLAY_IND_IS_THUNK(bh)) {
            //// not updated yet
            //if (replay_enabled && HEAP_ALLOCED(p)) {
            //    saveSparkGC(p, bh);
            //}
        } else  {
            StgClosure *ind = ((StgInd *)bh)->indirectee;
            ASSERT((REPLAY_ATOM(ind) != 0 &&
                    REPLAY_ATOM(ind) != REPLAY_PTR_ATOM) ||
                   GET_INFO(UNTAG_CLOSURE(REPLAY_PTR(ind))) != &stg_TSO_info);
            if ((REPLAY_ATOM(ind) == 0 ||
                 REPLAY_ATOM(ind) == REPLAY_PTR_ATOM) &&
                GET_INFO(UNTAG_CLOSURE(REPLAY_PTR(ind))) == &stg_TSO_info) {
                barf("resetSpark: found TSO");
            }
            if (REPLAY_IS_TSO(ind) ||
                get_itbl(UNTAG_CLOSURE(REPLAY_PTR(ind)))->type == BLOCKING_QUEUE) {
                if (!REPLAY_IS_TSO(ind)) {
                    tracePtrMove(cap, (StgPtr)p, (StgPtr)bh);
                }
                if (steal) {
                    replayStealSpark(cap, bh);
                }
            } else {
                REPLAY_RESET(bh);
                return NULL;
            }
        }
        return bh;
    }
    return NULL;
}

StgClosure *
replayResetSpark(Capability *cap, StgClosure *p)
{
    return resetSpark(cap, p, rtsTrue);
}

void
replayResetSparks(Capability *cap)
{
    int i, free;
    StgClosure *spark, *new;

    // update thread pointers, only from the main GC task, which has just
    // marked unreachable threads
    ASSERT(replay_main_gc_task != -1);
    if (sched_state < SCHED_SHUTTING_DOWN &&
        (int)cap->running_task->no == replay_main_gc_task) {
        nat g;
        StgTSO *t;

        // clear unreachable threads
        for (g = 0; g <= N; g++) {
            for (t = generations[g].old_threads; t != END_TSO_QUEUE; t = t->global_link) {
                ASSERT(t == findThread(t->id));
                thread_from_id[t->id-1] = NULL;
            }
        }

        // update thread pointers
        for (i = 0; i < thread_from_id_idx; i++) {
            t = thread_from_id[i];
            if (t != NULL && IS_FORWARDING_PTR(GET_INFO((StgClosure *)t))) {
                thread_from_id[i] = (StgTSO *)UN_FORWARDING_PTR(GET_INFO((StgClosure *)t));
            }
        }
    }

    // reset overwritten sparks stored in saved_sparks
    for (i = 0, free = 0; i < cap->replay.saved_sparks_idx; i++) {
        spark = cap->replay.saved_sparks[i];
        new = resetSpark(cap, spark, rtsFalse);
        if (new != NULL) {
            // save new spark pointer
            cap->replay.saved_sparks[free] = new;
            free++;
        }
    }
    ASSERT(free <= cap->replay.saved_sparks_idx);
    cap->replay.saved_sparks_idx = free;
}

void
replayEndGC(Capability *cap STG_UNUSED)
{
    if (replay_enabled) {
#ifdef DEBUG
        ASSERT(checkSparkCountInvariant());
#endif

        freeHashTable(spark_thunks, stgFree);
        spark_thunks = gc_spark_thunks;
        gc_spark_thunks = NULL;

        freeHashTable(spark_ptrs, NULL);
        spark_ptrs = gc_spark_ptrs;
        gc_spark_ptrs = NULL;

        freeHashTable(spark_ids, NULL);
        spark_ids = gc_spark_ids;
        gc_spark_ids = NULL;

        if (emit_dups) {
            freeHashTable(spark_dups, NULL);
            spark_dups = allocHashTable();
        }
    } else {
        // See replayResetSparks()
        ASSERT(replay_main_gc_task != -1);
        replay_main_gc_task = -1;
    }
}

static void
replayGcWorkerThread(Capability *cap)
{
    CapEvent *ce;
    nat tag;

    ce = readEvent();
    ASSERT(ce != NULL);
    tag = ce->ev->header.tag;
    switch (tag) {
    case EVENT_GC_DONE:
    case EVENT_GC_WORK:
    case EVENT_GC_IDLE:
        barf("replay: GC event not expected in worker GC %d", cap->no);
        break;
    default:
        if (!replay_main_gc_task_finished) {
            ASSERT(replay_main_gc_task != -1);
            replaySync(findTask(replay_main_gc_task)->cap, myTask());
        }
        pruneSparkQueue(cap);
    }
}

void
replayWaitForGcThreads(Capability *cap)
{
    nat i;
    rtsBool retry;

    do {
        retry = rtsFalse;
        for (i = 0; i < n_capabilities; i++) {
            if (i == cap->no) {
                continue;
            }
            if (replayProdCapability(capabilities[i], cap->running_task)) {
                retry = rtsTrue;
            }
        }
    } while (retry);
}

void
replayTraceExpression(Capability *cap, StgClosure *p, W_ id)
{
    ASSERT(replay_enabled);
    insertHashTable(spark_user, (W_)p, (void *)id);
    replayStealSpark(cap, p);
}

static int
eventTask(nat capno, Event *ev)
{
    int taskid = -1;

    switch(ev->header.tag) {
    // always emitted by main GC task
    case EVENT_HEAP_ALLOCATED:
        // HEAP_ALLOCATED events are emitted by the main GC task in each cap,
        // except in exitStorage(), where there has been no GC, and the main
        // task emits it as last stats
        if (replay_main_gc_task != -1) {
            taskid = replay_main_gc_task;
        }
        break;
    // task is not running on its capability
    case EVENT_TASK_DELETE:
        taskid = ((EventTaskDelete *)ev)->task;
        break;
    // task is not running on its capability
    case EVENT_TASK_ACQUIRE_CAP:
        taskid = ((EventTaskAcquireCap *)ev)->task;
        if (findTask(taskid) == NULL) {
            // bound main task
            //  'acquire capability' is emitted before 'task create' (see
            //  rts_lock())
            ASSERT(replay_main_task == -1);
            taskid = -1;
        }
        break;
    case EVENT_TASK_RETURN_CAP:
        taskid = ((EventTaskReturnCap *)ev)->task;
        break;

    default:
        if (capno != (EventCapNo)-1 &&
            running_tasks[capno] != NULL) {
            taskid = (int)running_tasks[capno]->no;
        }
    }

    if (taskid == -1) {
        if (replay_main_task == -1) {
            // starting up
        } else if (hs_init_count == 0) {
            // shutting down
            ASSERT(replay_main_task != -1);
            taskid = replay_main_task;
        } else if (replay_main_gc_task != -1) {
            // GC
            taskid = replay_main_gc_task;
        } else {
            barf("eventTask: cannot find a task to run next");
        }
    }
    return taskid;
}

static void
replayLoop(void)
{
    CapEvent *ce = NULL;
    int taskid;

    while (1) {
        if (ce != NULL && ce->ev->header.tag == EVENT_TASK_DELETE &&
            findTask(((EventTaskDelete *)ce->ev)->task) == NULL) {
            CapEvent *ce_ = ce;
            ce = nextEvent();
            freeEvent(ce_);
        } else {
            ce = nextEvent();
        }
        ASSERT(ce != NULL);

        debugReplay("replayLoop: next event is '%s'\n", EventDesc[ce->ev->header.tag]);

        setupNextEvent(&ce);
        taskid = eventTask(ce->capno, ce->ev);

        if (taskid == -1) {
            signalSemaphore(&no_task);
        } else {
            signalSemaphore(task_replay[taskid]);
        }
        waitSemaphore(&replay_sched);

        // finished
        if (ce->ev->header.tag == EVENT_STARTUP &&
            ((EventStartup *)ce->ev)->capno == 0) {
            ce = nextEvent();
            ASSERT(ce == NULL);
            break;
        }
    }

    ASSERT(replay_main_task != -1);
    signalSemaphore(task_replay[replay_main_task]);
}
#endif // THREADED_RTS
#else
void initReplay(void) {}
void endReplay(void) {}
void debugReplay(char *s STG_UNUSED, ...) {}
void replayError(char *s STG_UNUSED, ...) {}
void replaySaveHp(Capability *cap STG_UNUSED) {}
void replaySaveAlloc(Capability *cap STG_UNUSED) {}
void replayEvent(Capability *cap STG_UNUSED, Event *ev STG_UNUSED) {}
#endif // REPLAY

#if !defined(REPLAY) || !defined(THREADED_RTS)
W_ replayEnter(Capability *cap STG_UNUSED, StgClosure *p STG_UNUSED) { return 0; }
#endif
