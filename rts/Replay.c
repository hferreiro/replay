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
#include "Replay.h"

#include <math.h>
#include <string.h>

rtsBool replay_enabled = rtsFalse;

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


#ifdef THREADED_RTS
OSThreadId  replay_init_thread;
int         replay_main_task = -1;

static Semaphore replay_sched;
static Semaphore no_task;
static Semaphore **task_replay;

static Task **running_tasks; // owner task for each capability

static HashTable *spark_thunks;
static HashTable *spark_owners;

static HashTable *gc_spark_thunks;
static HashTable *gc_spark_owners;

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
        spark_owners = allocHashTable();
#endif
    }
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
        freeHashTable(spark_owners, NULL);
#endif
    }
}

void
replayPrint(char *s USED_IF_DEBUG, ...)
{
#if defined(DEBUG)
    if (replay_enabled) {
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
nurseryReaches(bdescr *oldBd, bdescr *bd, StgPtr hp)
{
    ASSERT(oldBd);
    ASSERT(HP_IN_BD(bd, hp));

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
replayCheckGCGeneric(StgPtr Hp, Capability *cap, StgPtr HpLim STG_UNUSED, bdescr *CurrentNursery)
{
    if (cap->replay.last_bd) {
        ASSERT(nurseryReaches(cap->replay.last_bd, CurrentNursery, Hp));
    }
}
#endif

// Saves current block and heap pointer. It is meant to be called just
// before running Haskell code (StgRun, resumeThread).
void
replaySaveHp(Capability *cap)
{
    replayPrint("Saving Hp...\n");
    replayPrint("  hp    = %p\n", START_FREE(cap->r.rCurrentNursery));
    replayPrint("  alloc = %ld\n\n", cap->replay.alloc);

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

    oldBd = cap->replay.last_bd;
    oldHp = cap->replay.last_hp;

    bd = cap->r.rCurrentNursery;
    hp = START_FREE(bd);

#ifdef DEBUG
    debugBelch("Yielded at %p\n", hp);
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
    debugBelch("Got:\n  ");
    printEvent(cap, ev);
    debugBelch("Expected:\n  ");
    printEvent(cap, read);
    barf("replay: '%s' events did not match", EventDesc[ev->header.tag]);
}

static void
setupNextEvent(void)
{
    CapEvent *ce;
    int next = 1;

    ce = readEvent();
    if (ce == NULL) {
        return;
    }

#ifdef THREADED_RTS
    // these events happen once the thread has already stopped, but before
    // 'stop thread'
    while (isEventCapValue(ce, SUSPEND_COMPUTATION)) {
        ce = peekEventCap(next++, ce->capno);
        ASSERT(ce != NULL);
        while (isEventCapValue(ce, STEAL_BLOCK)) {
            ce = peekEventCap(next++, ce->capno);
            ASSERT(ce != NULL);
        }
    }
#endif

    switch(ce->ev->header.tag) {
    case EVENT_STOP_THREAD:
    {
        // if yielded, setup the nursery to force the thread to stop
        if (((EventStopThread *)ce->ev)->status == ThreadYielding) {
            bdescr *bd;
            StgPtr hp;
            StgWord64 alloc, blocks;
            Capability *cap;
            EventCapAlloc *ev;

            ce = peekEventCap(next, ce->capno);
            ASSERT(ce != NULL);
            ev = (EventCapAlloc *)ce->ev;
            ASSERT(ev->header.tag == EVENT_CAP_ALLOC);

            ASSERT(ce->capno != (EventCapNo)-1);
            cap = capabilities[ce->capno];

            alloc = ev->alloc - cap->replay.alloc;
            blocks = ev->blocks - cap->replay.blocks;

            bd = cap->replay.last_bd;
            hp = cap->replay.last_hp;

            ASSERT(bd != NULL);
            ASSERT(hp != NULL);

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

            // if we have to yield in the current block, set HpLim now
            if (cap->r.rCurrentNursery == bd) {
                cap->r.rHpLim = cap->replay.hp;
            }
        }
        break;
    }
    default:
        ; // do nothing
    }
}

//extern char **environ;

#ifdef THREADED_RTS
// task needs the task running in cap to perform some computation before being
// able to continue
static void
replaySync_(Capability *cap, Task *task)
{
    ASSERT(cap->running_task != NULL);
    ASSERT(cap->replay.sync_task == NULL);
    cap->replay.sync_task = task;
    signalSemaphore(task_replay[cap->running_task->no]);
    waitSemaphore(task_replay[task->no]);
    cap->replay.sync_task = NULL;
}

static void
replayCont_(Capability *cap, Task *task)
{
    if (cap->replay.sync_task != NULL) {
        signalSemaphore(task_replay[cap->replay.sync_task->no]);
        waitSemaphore(task_replay[task->no]);
    }
}
#endif

void
replayEvent(Capability *cap, Event *ev)
{
    ASSERT(ev);

    CapEvent *ce;
    Event *read;
    nat tag;

#ifdef THREADED_RTS
    // we may have reached here to synchronise with another task,
    // in that case, return the control to that task
    if (cap != NULL && cap->running_task != NULL) {
        replayCont_(cap, cap->running_task);
    }
#endif

    ce = readEvent();
    ASSERTM(ce != NULL && ce->ev != NULL, "replayEvent: event could not be read");
    read = ce->ev;
    tag = ev->header.tag;

    if (tag != read->header.tag) {
        failedMatch(cap, ev, read);
    }

    if (cap != NULL) {
        ASSERTM(cap->no == ce->capno,
                "replay: %s event: got event from capability "
                "%d instead of %d",
                EventDesc[tag], cap->no, ce->capno);
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
    case EVENT_TASK_DELETE:
    {
        Task *task USED_IF_THREADS;

        if (!equalEvents(ev, read)) {
            failedMatch(cap, ev, read);
        }

#ifdef THREADED_RTS
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
#endif
        break;
    }
    // set capability variable
    case EVENT_CAP_VALUE:
    {
        EventCapValue *ecv, *ecv_read;

        ecv = (EventCapValue *)ev;
        ecv_read = (EventCapValue *)read;

        // set/check the correct value
        switch (ecv->tag) {
        case CTXT_SWITCH:
            ASSERT(ecv_read->value == 0 || ecv_read->value == 1);
            cap->r.rHpLim = (StgPtr)(W_)!ecv_read->value;
            cap->context_switch = ecv_read->value;

            ecv->value = ecv_read->value;
            break;
        case SCHED_LOOP:
            ASSERT(ecv_read->value <= SCHED_SHUTTING_DOWN);
            if (sched_state < ecv_read->value) {
                sched_state = ecv_read->value;
            }

            ecv->value = ecv_read->value;
            break;
        case SPARK_CREATE:
        case SPARK_DUD:
        case SPARK_OVERFLOW:
        case SPARK_RUN:
        case SPARK_STEAL:
        case SPARK_FIZZLE:
        case SPARK_GC:
        case SUSPEND_COMPUTATION:
        case MSG_BLACKHOLE:
        case THUNK_WHNF:
        case THUNK_UPDATED:
            if (ecv->value != ecv_read->value) {
                failedMatch(cap, ev, read);
            }
            goto out;
        default:
            ;
        }

        if (!equalEvents(ev, read)) {
            failedMatch(cap, ev, read);
        }
out:
        break;
    }
#ifdef THREADED_RTS
    case EVENT_TASK_ACQUIRE_CAP:
    case EVENT_TASK_RELEASE_CAP:
    {
        Task *task;

        if (!equalEvents(ev, read)) {
            failedMatch(cap, ev, read);
        }

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
        break;
    }
#endif
    default:
        if (!equalEvents(ev, read)) {
            failedMatch(cap, ev, read);
        }
        break;
    }

#ifdef DEBUG
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
    nextEvent();
    setupNextEvent();
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
        replayCapValue(cap, tag, 0);
    }
    return v;
}

void
replayCapValue(Capability *cap, int tag, W_ value)
{
    if (replay_enabled) {
        replayEvent(cap, createCapValueEvent(tag, value));
    }
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
        waitSemaphore(task_replay[task->no]);
    } else {
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
        traceCapValue(cap, tag, value);
#ifdef DEBUG
        Event *ev = createCapValueEvent(tag, value);
        printEvent(cap, ev);
#endif
        unlockClosure(p, info);
    }
}
#endif

// Capability.c
#ifdef THREADED_RTS
static Capability *
sparkOwner(W_ spark)
{
    Capability *owner = lookupHashTable(spark_owners, spark);
    if (owner == NULL) {
        CapEvent *ce = searchEventTagValueBefore(SPARK_CREATE, spark, EVENT_GC_START);
        if (ce != NULL) {
            owner = capabilities[ce->capno];
        }
    }
    return owner;
}

// finds the capability in conflict with cap
static Capability *
sparkOwner_(Capability *cap, W_ spark)
{
    Capability *owner = sparkOwner(spark);
    if (owner == cap) {
        removeHashTable(spark_owners, spark, cap);
    }
    return sparkOwner(spark);
}

#define swap(x,y)           \
    do {                    \
        typeof(x) _x = x;   \
        typeof(y) _y = y;   \
        x = _y;             \
        y = _x;             \
    } while(0)

static void
prepareSpark(Capability *cap, W_ spark)
{
    SparkPool *pool;

    pool = cap->sparks;
    if (sparkPoolSize(pool) == 0) {
#ifdef DEBUG
        // TODO: may need to replay a few events to get to SPARK_CREATE
        CapEvent *ce = peekEventCap(0, cap->no);
        ASSERT(isEventCapValue(ce, SPARK_CREATE));
#endif
        replaySync_(cap, myTask());
    }
    ASSERT(sparkPoolSize(pool) > 0);

    if ((W_)pool->elements[pool->top & pool->moduloSize] != spark) {
        int idx, idx1, size;

        ASSERT(sparkPoolSize(pool) > 1);

        // find the right spark
        size = sparkPoolSize(pool);
        do {
            idx = (pool->top + size-1) & pool->moduloSize;
            size--;
        } while ((W_)pool->elements[idx] != spark && size > 1);
        ASSERT((W_)pool->elements[idx] == spark);

        // swap sparks
        while (size > 0) {
            idx1 = idx;
            idx = (pool->top + size-1) & pool->moduloSize;
            swap(pool->elements[idx], pool->elements[idx1]);
            size--;
        }
    }

}

static void
storeSpark(HashTable *thunks, StgClosure *spark)
{
    StgClosure *p;
    nat size;

    ASSERT(lookupHashTable(thunks, (W_)spark) == NULL);
    size = closure_sizeW(spark) * sizeof(W_);
    p = stgMallocBytes(size, "replaySaveSpark");
    memcpy(p, spark, size);
    insertHashTable(thunks, (W_)spark, p);

    // Prevent an accidental evaluation of the thunk by the thread
    // that is going to block or find it already updated. It will be
    // dealt with in replayBlackHole
    SET_INFO(spark, &stg_BLACKHOLE_info);
}

static void
saveSpark(Capability *cap, StgClosure *spark, rtsBool isGC)
{
    HashTable *thunks, *owners;

    if (isGC) {
        thunks = gc_spark_thunks;
        owners = gc_spark_owners;
    } else {
        thunks = spark_thunks;
        owners = spark_owners;
    }

    if (lookupHashTable(thunks, (W_)spark) != NULL) {
        // needed for sparkOwner_
        //ASSERT(lookupHashTable(owners, (W_)spark) != NULL);
        //removeHashTable(owners, (W_)spark, NULL);

        // may have been restored
        if (GET_INFO(spark) != &stg_BLACKHOLE_info) {
            SET_INFO(spark, &stg_BLACKHOLE_info);
        }
    } else {
        // check if it is going to be used
        if (existsBlackHoleEventBeforeGC((W_)spark)) {
            storeSpark(thunks, spark);
        }
    }

    if (cap != NULL) {
        insertHashTable(owners, (W_)spark, cap);
    }
}

void
replaySaveSpark(Capability *cap, StgClosure *spark)
{
    saveSpark(cap, spark, rtsFalse);
}

void
replayRestoreSpark(StgClosure *bh)
{
    StgClosure *p;

    p = lookupHashTable(spark_thunks, (W_)bh);
    ASSERT(p != NULL);
    memcpy(bh, p, closure_sizeW(p) * sizeof(W_));
}

StgClosure *
replayFindSpark(Capability *cap)
{
    CapEvent *ce;
    Capability *robbed;
    StgClosure *spark;
    W_ id;
    int tag;

    ce = readEvent();
    while (isEventCapValue(ce, SPARK_FIZZLE)) {
        // may be ours
        id = ((EventCapValue *)ce->ev)->value;
        robbed = sparkOwner(id);
        ASSERT(robbed != NULL);
        prepareSpark(robbed, id);
        spark = tryStealSpark(robbed->sparks);
        ASSERT((W_)spark == id);

        removeHashTable(spark_owners, id, NULL);

        // if (!fizzledSpark(spark)) {
        //     // TODO: sync until replayUpdateWithIndirection(spark)
        //     replaySync_(robbed, myTask());
        // }
        // ASSERT(fizzledSpark(spark));

        cap->spark_stats.fizzled++;
        replayTraceCapValue(cap, SPARK_FIZZLE, (W_)spark);
        ce = readEvent();
    }

    if (isEventCapValue(ce, SPARK_RUN) ||
        isEventCapValue(ce, SPARK_STEAL)) {
        tag = ((EventCapValue *)ce->ev)->tag;
        id = ((EventCapValue *)ce->ev)->value;
        robbed = sparkOwner(id);
        if (tag == SPARK_RUN) {
            ASSERT(robbed == cap);
        } else {
            ASSERT(robbed != NULL);
        }
        prepareSpark(robbed, id);
        spark = tryStealSpark(robbed->sparks);
        ASSERT((W_)spark == id);

        // it may have not been saved in newSpark if not used then, also it
        // needs blackholing
        replaySaveSpark(cap, spark);

        cap->spark_stats.converted++;
        replayTraceCapValue(cap, tag, (W_)spark);

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
        replayError("Unexpected event in waitForReturnCapability: '%s'",
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

static void
yieldCapability_(Capability **pCap, Task *task)
{
    Capability *cap;
    CapEvent *ce;

    cap = *pCap;

    ce = readEvent();
    ASSERT(ce != NULL);
    if (ce->ev->header.tag == EVENT_GC_START) {
        traceEventGcStart(cap);
        gcWorkerThread(cap);
        traceEventGcEnd(cap);
        traceSparkCounters(cap);
        if (task->cap == cap) {
            return;
        }
    }

    // releaseCapabilityAndQueueWorker()
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

rtsBool
replayTryGrabCapability(Capability *cap, Task *task)
{
    CapEvent *ce;

    ce = readEvent();
    ASSERT(ce != NULL);
    if (ce->ev->header.tag == EVENT_TASK_ACQUIRE_CAP &&
        ce->capno == cap->no) {
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
                next = m->link;
                executeMessage(cap, m);
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
            debugBelch("Imposible event after scheduleYield():\n");
            printEvent(cap, ce->ev);
            abort();
            stg_exit(EXIT_INTERNAL_ERROR);
        }
    }
}

// Returns true for the task initiating the GC
nat
replayRequestSync(Capability **pCap, Task *task, nat sync_type)
{
    nat prev_pending_sync;
    CapEvent *ce;

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
        debugBelch("Imposible event after requestSync():\n");
        printEvent(*pCap, ce->ev);
        abort();
        stg_exit(EXIT_INTERNAL_ERROR);
    }

    return prev_pending_sync;
}

void
replayExitScheduler(Task *task)
{
    CapEvent *ce;

    ce = searchEventTagValueBefore(SCHED_END, 0, EVENT_CAPSET_REMOVE_CAP);
    if (ce != NULL) {
        // sched_state < SCHED_SHUTTING_DOWN
        if (sched_state < SCHED_INTERRUPTING) {
            sched_state = SCHED_INTERRUPTING;
        }

        Capability *cap = task->cap;
        waitForReturnCapability(&cap,task);
        scheduleDoGC(&cap,task,rtsTrue);
        ASSERT(task->incall->tso == NULL);
        replayTraceCapTag(cap, SCHED_END);
        releaseCapability(cap, cap);
    }

    sched_state = SCHED_SHUTTING_DOWN;

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
replayThreadPaused(Capability *cap, StgTSO *tso, StgClosure *bh)
{
    CapEvent *ce;

    ce = readEvent();
    if (isEventCapValue(ce, SUSPEND_COMPUTATION) &&
        ((EventCapValue *)ce->ev)->value == (W_)bh) {
        // The thunk may have been restored for tso to carry on with
        // the computation. We need to blackhole it to suspend the computation
        if (GET_INFO(bh) != &stg_BLACKHOLE_info) {
            SET_INFO(bh, &stg_BLACKHOLE_info);
        }
        return rtsTrue;
    }

    // if suspendComputation happened beforehand, we need to recover the thunk
    if (GET_INFO(bh) == &stg_BLACKHOLE_info) {
        replayRestoreSpark(bh);
    }

    return rtsFalse;
}

// StgMiscClosures.cmm

// returns a closure to evaluate or NULL to block
StgClosure *
replayBlackHole(StgTSO *tso, StgClosure *bh)
{
    CapEvent *ce;
    rtsBool whnf, updated;
    StgClosure *p;
    Capability *cap, *owner;
    const StgInfoTable *info = NULL;

    p = ((StgInd *)bh)->indirectee;
    // may be an unboxed value
    //info = GET_INFO(p);

    ce = readEvent();

    // first thread entering the closure, recover the thunk
    if (p == NULL) {
        // restore the thunk
        replayRestoreSpark(bh);

        if (!ce->ev->header.tag == EVENT_CAP_VALUE ||
            ((EventCapValue *)ce->ev)->value != (W_)bh) {
            return bh;
        }
        p = ((StgInd *)bh)->indirectee;
    }

    cap = tso->cap;

    // if the blackhole has to be updated
    whnf = isEventCapValue(ce, THUNK_WHNF);
    updated = isEventCapValue(ce, THUNK_UPDATED);
    if ((whnf || updated) &&
        ((EventCapValue *)ce->ev)->value == (W_)bh) {
        // return if already updated
        if (whnf && GET_CLOSURE_TAG(p) != 0) {
            return (void *)1;
        }
        if (p != NULL) {
            info = GET_INFO(p);
            if (updated && info != &stg_TSO_info &&
                info != &stg_BLOCKING_QUEUE_CLEAN_info &&
                info != &stg_BLOCKING_QUEUE_DIRTY_info) {
                ASSERT(GET_CLOSURE_TAG(p) == 0);
                return p;
            }
        }

        // otherwise, find the cap in which the thunk is evaluated
        owner = sparkOwner_(cap, (W_)bh);
        ASSERT(owner != NULL);

        // let owner update the thunk
        ASSERT(owner->replay.sync_thunk == NULL);
        owner->replay.sync_thunk = bh;
        replaySync_(owner, myTask());
        owner->replay.sync_thunk = NULL;

        // reload the indirection
        p = ((StgInd *)bh)->indirectee;
        info = GET_INFO(p);

        if (isEventCapValue(ce, THUNK_WHNF)) {
            ASSERT(GET_CLOSURE_TAG(p) != 0);
            return (void *)1;
        } else {
            ASSERT(GET_CLOSURE_TAG(p) == 0);
            ASSERT(info != &stg_TSO_info &&
                   info != &stg_BLOCKING_QUEUE_CLEAN_info &&
                   info != &stg_BLOCKING_QUEUE_DIRTY_info);
            return p;
        }
    }

    // if we have to block
    if (isEventCapValue(ce, MSG_BLACKHOLE) &&
        ((EventCapValue *)ce->ev)->value == (W_)bh) {

        // if the blackhole was updated just before blocking (see
        // messageBlackHole returning 0)
        ce = peekEventCap(1, cap->no);
        if (isEventCapValue(ce, THUNK_UPDATED)) {
            ASSERT(((EventCapValue *)ce->ev)->value == (W_)bh);
            return NULL;
        }

        // if it has not been yet blackholed, do not sync, just find the
        // thread we are going to block on, an do it manually
        if (p == NULL ||
            ((info = GET_INFO(p)) != &stg_TSO_info &&
             info != &stg_BLOCKING_QUEUE_CLEAN_info &&
             info != &stg_BLOCKING_QUEUE_DIRTY_info)) {
            // must be the second thread entering the thunk before it was
            // blackholed, so we restored it above
            ASSERT(GET_INFO(bh) != &stg_BLACKHOLE_info);
            ce = searchEventCap(cap->no, EVENT_STOP_THREAD);
            ASSERT(ce != NULL);
            ASSERT(((EventStopThread *)ce->ev)->status ==
                   6 + BlockedOnBlackHole);

            // blackhole it
            SET_INFO(bh, &stg_BLACKHOLE_info);
            ((StgInd *)bh)->indirectee = (StgClosure *)
                findThread(((EventStopThread *)ce->ev)->blocked_on);
        }

        return NULL;
    }

    // otherwise, both threads enter the thunk, restore it
    p = ((StgInd *)bh)->indirectee;
    replayRestoreSpark(bh);
    // save the result or the tso pointer for later use if needed
    ((StgInd *)bh)->indirectee = p;

    return bh;
}

void
replayThunkUpdated(StgTSO *tso, StgClosure *bh, rtsBool isWHNF)
{
    if (replay_enabled && !isWHNF) {
        // AP_STACK works as a blackhole too, we need to save it
        storeSpark(spark_thunks, ((StgInd *)bh)->indirectee);
    }

    replayTraceCapValue(tso->cap, isWHNF ? THUNK_WHNF : THUNK_UPDATED, (W_)bh);
}

MessageBlackHole *
replayMessageBlackHole(StgTSO *tso, StgClosure *bh)
{
    int r;
    MessageBlackHole *msg;

    replayTraceCapValue(tso->cap, MSG_BLACKHOLE, (W_)bh);

    msg = (MessageBlackHole *)allocate(tso->cap, sizeofW(MessageBlackHole));
    SET_HDR(msg, &stg_MSG_BLACKHOLE_info, CCS_SYSTEM);
    msg->tso = tso;
    msg->bh = bh;

    if (replay_enabled) {
        CapEvent *ce = readEvent();

        if (isEventCapValue(ce, THUNK_WHNF) ||
            isEventCapValue(ce, THUNK_UPDATED)) {
            return NULL;
        }
    }

    r = messageBlackHole(tso->cap, msg);
    if (r) {
        return msg;
    } else {
        return NULL;
    }
}

// Updates.h
void
replayUpdateWithIndirection(Capability *cap,
                            StgClosure *p1,
                            StgClosure *p2 STG_UNUSED)
{
    if (cap->replay.sync_thunk == p1) {
        replayCont_(cap, cap->running_task);
    }
}

// GC.c
void
replayStartGC(void)
{
    if (replay_enabled) {
        ASSERT(gc_spark_thunks == NULL);
        gc_spark_thunks = allocHashTable();
        ASSERT(gc_spark_owners == NULL);
        gc_spark_owners = allocHashTable();
    }
}

void
replayPromoteSpark(StgClosure *spark, StgClosure *old)
{
    Capability *cap;

    cap = lookupHashTable(spark_owners, (W_)old);
    ASSERT(cap != NULL);
    saveSpark(cap, spark, rtsTrue);
}

void
replayEndGC(void)
{
    if (replay_enabled) {
        freeHashTable(spark_thunks, stgFree);
        spark_thunks = gc_spark_thunks;
        gc_spark_thunks = NULL;

        freeHashTable(spark_owners, NULL);
        spark_owners = gc_spark_owners;
        gc_spark_owners = NULL;
    }
}

rtsBool
replayGCContinue(void)
{
    CapEvent *ce = readEvent();

    if (ce->ev->header.tag == EVENT_GC_WORK) {
        return rtsTrue;
    } else {
        ASSERT(ce->ev->header.tag == EVENT_GC_DONE);
        return rtsFalse;
    }
}

static int
eventTask(nat capno, Event *ev)
{
    int taskid;

    taskid = -1;
    switch(ev->header.tag) {
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
    replayPrint("=> replayLoop\n");

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

        replayPrint("replayLoop: next event is '%s'\n", EventDesc[ce->ev->header.tag]);

        setupNextEvent();
        taskid = eventTask(ce->capno, ce->ev);

        replayPrint("waking up %d\n", taskid);
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
void replayPrint(char *s STG_UNUSED, ...) {}
void replayError(char *s STG_UNUSED, ...) {}
void replaySaveHp(Capability *cap STG_UNUSED) {}
void replaySaveAlloc(Capability *cap STG_UNUSED) {}
void replayEvent(Capability *cap STG_UNUSED, Event *ev STG_UNUSED) {}
#endif // REPLAY
