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
#include "Printer.h"
#include "Replay.h"

#include <math.h>
#include <string.h>

#define _ptr(p) ((void *)((StgWord)(p) & 0x0fffff))

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
Task       *replay_main_task;

static Semaphore replay_sched;
static Semaphore no_task;
static Semaphore **task_replay;

static Task **running_tasks; // owner task for each capability

// When replaying, store an id -> StgThunk mapping for every spark. Additionally,
// it stores a pointer -> id mapping which is updated during GC (TODO)
HashTable *spark_ids;
HashTable *gc_spark_ids;

static Task *findTask(nat no);

static void replayLoop(void);
#endif

static rtsBool isEventCapValue(CapEvent *ce, int tag);

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

        spark_ids = allocHashTable();
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

        freeHashTable(spark_ids, stgFree);
#endif
    }
}

void
debugReplay(char *s USED_IF_DEBUG, ...)
{
#if defined(DEBUG)
//#if 0
    if (replay_enabled || TRACE_spark_full) {
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
replayCheckGCGeneric(StgPtr Hp, Capability *cap, StgPtr HpLim STG_UNUSED, bdescr *CurrentNursery)
{
    ASSERT(HP_IN_BD(CurrentNursery, Hp));
    if (cap->replay.last_bd) {
        ASSERT(nurseryReaches(cap->replay.last_bd, CurrentNursery));
    }
}
#endif

// Saves current block and heap pointer. It is meant to be called just
// before running Haskell code (StgRun, resumeThread).
void
replaySaveHp(Capability *cap)
{
#ifdef DEBUG
    debugReplay("Saving Hp...\n");
    debugReplay("  hp    = %p\n", _ptr(START_FREE(cap->r.rCurrentNursery)));
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

    oldBd = cap->replay.last_bd;
    oldHp = cap->replay.last_hp;

    bd = cap->r.rCurrentNursery;
    hp = START_FREE(bd);

#ifdef DEBUG
    debugReplay("cap %d: task %d: yielding %p (%p)\n",
                cap->no, cap->running_task->no, _ptr(bd), _ptr(hp));
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
                        cap->no, _ptr(bd), _ptr(cap->replay.hp));
#endif

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
    debugReplay("cap %d: task %d: replaySync_ with cap %d\n",
                task->cap->no, task->no, cap->no);
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
        debugReplay("cap %d: task %d: replayCont_\n", cap->no, task->no);
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
        while (c != msg + (strsize-1) &&
               c_read != msg_read + (strsize_read-1) &&
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
        if (etc->tid == replay_init_thread) {
            replay_main_task = task;
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
            freeEvent(ce);
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
        default:
            ;
        }

        if (!equalEvents(ev, read)) {
            failedMatch(cap, ev, read);
        }

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
    ev->header.time = read->header.time;
    printEvent(cap, ev);
#endif

#ifdef THREADED_RTS
    // wait for the replay scheduler signal
    signalSemaphore(&replay_sched);

    if (myTask() != NULL) {
        //debugReplay("task %d waiting on %p\n", myTask()->no, task_replay[myTask()->no]);
        waitSemaphore(task_replay[myTask()->no]);
    } else {
        if (sched_state < SCHED_SHUTTING_DOWN) {
            ASSERT(replay_main_task == NULL);
            //debugReplay("waiting on no task\n");
            waitSemaphore(&no_task);
        } else {
            ASSERT(replay_main_task != NULL);
            waitSemaphore(task_replay[replay_main_task->no]);
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

static void
replayCapValue(Capability *cap, int tag, int value)
{
    if (replay_enabled) {
        replayEvent(cap, createCapValueEvent(tag, value));
    }
#ifdef DEBUG
    else {
        printEvent(cap, createCapValueEvent(tag, value));
    }
#endif
}

void
replayTraceCapTag(Capability *cap, int tag)
{
    replayTraceCapValue(cap, tag, 0);
}

void
replayTraceCapValue(Capability *cap, int tag, int value)
{
    traceCapValue(cap, tag, value);
    replayCapValue(cap, tag, value);
}

int
replayCapTag(Capability *cap, int tag)
{
    int v = 0;
    CapEvent *ce = readEvent();

    if (isEventCapValue(ce, tag)) {
        v = (int)((EventCapValue *)ce->ev)->value;
        replayCapValue(cap, tag, 0);
    }
    return v;
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

// Capability.c
static rtsBool
isEventCapValue(CapEvent *ce, int tag)
{
    ASSERT(ce != NULL);
    return (ce->ev->header.tag == EVENT_CAP_VALUE &&
            ((EventCapValue *)ce->ev)->tag == tag);
}

#ifdef THREADED_RTS
static Capability *
eventSparkCap(CapEvent *ce)
{
    EventCapValue *ev;
    nat capno;

    ASSERT(ce != NULL);
    ev = (EventCapValue *)ce->ev;
    ASSERT(ev->header.tag == EVENT_CAP_VALUE);
    ASSERT(ev->tag == SPARK_FIZZLE || ev->tag == SPARK_STEAL ||
           ev->tag == SUSPEND_COMPUTATION);

    capno = capSparkId(ev->value);
    ASSERT(capno < n_capabilities);
    return capabilities[capno];
}

#define swap(x,y)           \
    do {                    \
        typeof(x) _x = x;   \
        typeof(y) _y = y;   \
        x = _y;             \
        y = _x;             \
    } while(0)

static StgClosure *
replayStealSpark_(Capability *cap, int id)
{
    CapEvent *ce USED_IF_DEBUG;
    int id_;
    SparkPool *pool;
    StgClosure *spark;

    pool = cap->sparks;
    if (sparkPoolSize(pool) == 0) {
#ifdef DEBUG
        // TODO: may need to replay a few events to get to SPARK_CREATE
        ce = peekEventCap(0, cap->no);
        ASSERT(isEventCapValue(ce, SPARK_CREATE));
        debugReplay("cap %d: task %d: sync with cap %d to create spark %d\n",
                    myTask()->cap->no, myTask()->no, cap->no, id);
#endif
        replaySync_(cap, myTask());
    }
    ASSERT(sparkPoolSize(pool) > 0);

    if (pool->ids[pool->top & pool->moduloSize] != id) {
        int idx, idx1, size;

        ASSERT(sparkPoolSize(pool) > 1);

        // find the right spark
        size = sparkPoolSize(pool);
        do {
            idx = (pool->top + size-1) & pool->moduloSize;
            size--;
        } while (pool->ids[idx] != id && size > 1);
        ASSERT(pool->ids[idx] == id);

        // swap sparks
        while (size > 0) {
            idx1 = idx;
            idx = (pool->top + size-1) & pool->moduloSize;
            swap(pool->elements[idx], pool->elements[idx1]);
            swap(pool->ids[idx], pool->ids[idx1]);
            size--;
        }
    }

    spark = tryStealSpark(cap->sparks, &id_);
    ASSERT(id_ == id);
    return spark;
}

// save the spark's id in its payload given that it has already been blackholed
void
replaySaveSparkId(StgClosure *bh, int id)
{
    if (!replay_enabled) {
        ASSERTM(GET_INFO(bh) == &stg_BLACKHOLE_info, "bh is %s", info_type(bh));
        ((StgThunk *)bh)->payload[0] = (void *)(W_)id;
    }
}

// Sparks cannot be identified from their spark pool so we need to save them
// elsewhere to obtain its id. When replaying we also need to copy the closure,
// to be able to resolve conflicts in spark evaluation.
static rtsBool
saveSpark(HashTable *table, StgTSO *tso, StgClosure *spark, int id)
{
    StgClosure *p;

    if (replay_enabled) {
        // check if it is going to be used
        if (existsBlackHoleEventBeforeGC(id)) {
            // when replaying, we may use tso -> id in replayBlackHole to find
            // the thread responsible of the spark evaluation, additionally we
            // blackhole the spark again, to prevent unintended evaluation
            if (tso != NULL) {
                tso->spark_id = id;
            } else {
                // id -> spark
                ASSERT(lookupHashTable(table, id) == NULL);
                nat size = closure_sizeW(spark) * sizeof(W_);
                p = stgMallocBytes(size, "replaySaveSpark:spark");
                memcpy(p, spark, size);
                insertHashTable(table, id, p);

                // spark -> id
                ASSERT(lookupHashTable(table, (W_)spark) == NULL);
                void *m = stgMallocBytes(sizeof(int), "replaySaveSpark:id");
                memcpy(m, &id, sizeof(int));
                insertHashTable(table, (W_)spark, m);
            }
        } else {
            return rtsFalse;
        }
    } else {
        if (tso->spark_p != NULL) {
            debugReplay("cap %d: tso %d: saving closure, updating last one %p id %d\n",
                        tso->cap->no, tso->id, _ptr(tso->spark_p), tso->spark_id);
            replaySaveSparkId(tso->spark_p, tso->spark_id);
        }
        tso->spark_p = spark;
        tso->spark_id = id;
    }
    return rtsTrue;
}

void
replaySaveSpark(StgTSO *tso, StgClosure *spark, int id)
{
    rtsBool r;

    r = saveSpark(spark_ids, tso, spark, id);
    if (replay_enabled && r) {
        // Prevent an accidental evaluation of the thunk by the thread
        // that is going to block or find it already updated. It will be
        // dealt with in replayBlackHole
        SET_INFO(spark, &stg_BLACKHOLE_info);
        // XXX: delete
        if (tso == NULL && ((StgInd *)spark)->indirectee != NULL) {
            ((StgInd *)spark)->indirectee = NULL;
            //barf("replaySaveSpark: indirectee is not NULL");
        }
    }
}

static int
findSparkId(StgClosure *bh)
{
    ASSERT(replay_enabled);

    int id = 0, *id_p;

    id_p = lookupHashTable(spark_ids, (W_)bh);
    if (id_p != NULL) {
        id = *id_p;
    }
    return id;
}

int
replayRestoreSpark(StgClosure *bh, int id_)
{
    int id;
    StgClosure *p;

    if (id_ == 0) {
        id = findSparkId(bh);
    } else {
        id = id_;
    }
    ASSERT(id > 0);

    p = lookupHashTable(spark_ids, id);
    ASSERT(p != NULL);
    memcpy(bh, p, closure_sizeW(p) * sizeof(W_));

    return id;
}

StgClosure *
replayFindSpark(Capability *cap)
{
    CapEvent *ce;
    Capability *robbed;
    StgClosure *spark;
    int tag, id;

    ce = readEvent();
    while (isEventCapValue(ce, SPARK_FIZZLE)) {
        // may be ours
        robbed = eventSparkCap(ce);
        id = ((EventCapValue *)ce->ev)->value;
        spark = replayStealSpark_(robbed, id);
        ASSERT(spark != NULL);

        // if (!fizzledSpark(spark)) {
        //     // TODO: sync until replayUpdateWithIndirection(spark)
        //     debugReplay("cap %d: task %d: sync with cap %d to fizzle spark %d %p\n",
        //                 cap->no, cap->running_task->no, robbed->no, id,
        //                 (void *)((W_)spark & 0x0fffff));
        //     replaySync_(robbed, myTask());
        // }
        // ASSERT(fizzledSpark(spark));

        if (!fizzledSpark(spark)) {
            debugReplay("cap %d: task %d: spark %d %p should be fizzled, ignoring\n",
                        cap->no, cap->running_task->no, robbed->no, id, _ptr(spark));
        }

        cap->spark_stats.fizzled++;
        debugReplay("cap %d: task %d: spark %d fizzled %p\n",
                    cap->no, cap->running_task->no, id, _ptr(spark));
        replayTraceCapValue(cap, SPARK_FIZZLE, id);
        ce = readEvent();
    }

    if (isEventCapValue(ce, SPARK_RUN) ||
        isEventCapValue(ce, SPARK_STEAL)) {
        tag = ((EventCapValue *)ce->ev)->tag;
        id = ((EventCapValue *)ce->ev)->value;
        if (tag == SPARK_RUN) {
            robbed = cap;
        } else {
            robbed = eventSparkCap(ce);
        }
        spark = replayStealSpark_(robbed, id);
        ASSERT(spark != NULL);

        // first saved in newSpark where we do not know which thread stole it,
        // update that information now
        replaySaveSpark(cap->r.rCurrentTSO, spark, id);

        cap->spark_stats.converted++;
        replayTraceCapValue(cap, tag, id);

        if (tag == SPARK_RUN) {
            debugReplay("cap %d: task %d: run spark %d: %p\n",
                        cap->no, cap->running_task->no, id, _ptr(spark));
        } else {
            debugReplay("cap %d: task %d: spark %d stolen: %p\n",
                        cap->no, cap->running_task->no, id, _ptr(spark));
        }

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

    debugReplay("cap %d: task %d: yieldCapability\n", cap->no, task->no);

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
    //     EVENT_CAP_VALUE (SCHED_LOOP) (continue scheduler loop)
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

// Returns true for the task initiating the GC
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
static StgTSO *
replayFindTSOSpark(StgTSO *tso, StgClosure *bh)
{
    StgClosure *p;

    // the thread has the spark if it stole it
    if (tso->spark_p == bh) {
        return tso;
    }

    // if it has been blackholed, it may be stored in the thread that evaluated
    // it
    if (GET_INFO(bh) == &stg_BLACKHOLE_info) {
        p = ((StgInd *)bh)->indirectee;
        ASSERT(replay_enabled || p != NULL);
        if (p != NULL) {
            if (GET_INFO(UNTAG_CLOSURE(p)) == &stg_TSO_info) {
                tso = (StgTSO *)p;
                ASSERTM(replay_enabled || tso->spark_p == bh,
                        "cap %d: task %d: blackhole %p points to tso %d but it stores %d",
                        tso->cap->no, tso->cap->running_task->no, _ptr(bh),
                        tso->id, tso->spark_id);
                return tso;
            } else if (GET_INFO(UNTAG_CLOSURE(p)) == &stg_BLOCKING_QUEUE_CLEAN_info ||
                       GET_INFO(UNTAG_CLOSURE(p)) == &stg_BLOCKING_QUEUE_DIRTY_info) {
                tso = ((StgBlockingQueue*)p)->owner;
            } else {
                debugReplay("bh indirectee %p is %s\n", _ptr(p), info_type(UNTAG_CLOSURE(p)));
            }
        }
    }

    return NULL;
}

int
replayFindSparkId(StgTSO *tso, StgClosure *bh)
{
    int id = 0;
    StgTSO *t;

    if (replay_enabled) {
        id = findSparkId(bh);
    }

    if (id == 0) {
retry:
        t = replayFindTSOSpark(tso, bh);
        if (t == NULL) {
            if (GET_INFO(bh) == &stg_WHITEHOLE_info) {
                goto retry;
            }

            ASSERT(GET_INFO(bh) == &stg_BLACKHOLE_info);
            id = (W_)((StgThunk *)bh)->payload[0];
            if (!isValidSparkId(id)) {
                id = 0;
            } else {
                debugReplay("cap %d: task %d: id %d from payload\n",
                            tso->cap->no, tso->cap->running_task->no, id);
            }
        } else {
            id = t->spark_id;
            ASSERT((replay_enabled && id == 0) || isValidSparkId(id));
            debugReplay("cap %d: task %d: id %d from thread %d\n",
                        tso->cap->no, tso->cap->running_task->no, id, t->id);
        }
    }

    return id;
}

// Returns whether we are going to suspend the current computation
rtsBool
replayThreadPaused(Capability *cap, StgTSO *tso, StgClosure **bh)
{
    CapEvent *ce;
    int id;

    id = findSparkId(*bh);
    if (id == 0) {
        return rtsFalse;
    }

    ce = readEvent();
    if (isEventCapValue(ce, SUSPEND_COMPUTATION) &&
        ((EventCapValue *)ce->ev)->value == (W_)id) {
        // The thunk may have been restored for tso to carry on with
        // the computation. We need to blackhole it to suspend the computation
        if (GET_INFO(*bh) != &stg_BLACKHOLE_info) {
            debugReplay("cap %d: task %d: tso %d: replayThreadPaused: blackholing spark %d\n",
                        cap->no, cap->running_task->no, tso->id, id);
            SET_INFO(*bh, &stg_BLACKHOLE_info);
        }
        return rtsTrue;
    }

    // if suspendComputation happened beforehand, we need to recover the thunk
    if (GET_INFO(*bh) == &stg_BLACKHOLE_info) {
        replayRestoreSpark(*bh, id);
    }

    return rtsFalse;
}

//// Threads.c
//
//// thunk has just been updated, we need to find and save its id
//void
//replayUpdateThunk(Capability *cap, StgClosure *thunk, StgClosure *indirectee)
//{
//    int id;
//    StgTSO *owner;
//    const StgInfoTable *i;
//
//    i = GET_INFO(indirectee);
//    if (i == &stg_TSO_info) {
//        owner = (StgTSO *)indirectee;
//    } else if (i == &stg_BLOCKING_QUEUE_CLEAN_info ||
//               i == &stg_BLOCKING_QUEUE_DIRTY_info) {
//        owner = ((StgBlockingQueue*)indirectee)->owner;
//    } else {
//        barf("%p indirectee was %s\n", _ptr(indirectee), info_type(UNTAG_CLOSURE(indirectee)));
//    }
//    ASSERTM(owner->spark_p == thunk, "tso %d is not owner", owner->id);
//    id = owner->spark_id;
//
//    debugReplay("cap %d: task %d: updateThunk: spark %p saving id %d in payload\n",
//                cap->no, cap->running_task->no, _ptr(thunk), id);
//    replaySaveSparkId(thunk, id);
//}

// StgMiscClosures.cmm

static Capability *
findCapSpark(int id)
{
    StgTSO *tso;
    nat g;

    for (g = 0; g < RtsFlags.GcFlags.generations; g++) {
        for (tso = generations[g].threads; tso != END_TSO_QUEUE;
             tso = tso->global_link) {
            if (tso->spark_id == id) {
                return tso->cap;
            }
        }
    }
    return NULL;
}

// returns a closure to evaluate or NULL to block
StgClosure *
replayBlackHole(StgTSO *tso, StgClosure *bh)
{
    CapEvent *ce;
    int id;
    rtsBool whnf, updated;
    StgClosure *p;
    Capability *cap, *owner;
    const StgInfoTable *info = NULL;

    p = ((StgInd *)bh)->indirectee;
    // may be an unboxed value
    //info = GET_INFO(p);

    // do not use findSparkId, replayThunkUpdated may have logged a small
    // integer in ->payload[0] that falls in the id range
    id = replayFindSparkId(tso, bh);
    if (id == 0) {
        ASSERT(GET_CLOSURE_TAG(p) != 0);
        return (void *)1;
    }

    ce = readEvent();

    // first thread entering the closure, recover the thunk
    if (p == NULL) {
        // restore the thunk
        replayRestoreSpark(bh, id);

        if (!ce->ev->header.tag == EVENT_CAP_VALUE ||
            (((EventCapValue *)ce->ev)->value != (W_)id &&
             // see replayThunkUpdated
             ((EventCapValue *)ce->ev)->value != (W_)((StgThunk *)bh)->payload[0])) {
            return bh;
        }
        p = ((StgInd *)bh)->indirectee;
    }

    cap = tso->cap;

    // if the blackhole has to be updated
    whnf = isEventCapValue(ce, THUNK_WHNF);
    updated = isEventCapValue(ce, THUNK_UPDATED);
    if ((whnf || updated) &&
            (((EventCapValue *)ce->ev)->value == (W_)id ||
             // see replayThunkUpdated
             ((EventCapValue *)ce->ev)->value == (W_)((StgThunk *)bh)->payload[0])) {
        // return if already updated
        if (whnf && GET_CLOSURE_TAG(p) != 0) {
            return (void *)1;
        }
        info = GET_INFO(p);
        if (updated && p != NULL && info != &stg_TSO_info &&
            info != &stg_BLOCKING_QUEUE_CLEAN_info &&
            info != &stg_BLOCKING_QUEUE_DIRTY_info) {
            ASSERT(GET_CLOSURE_TAG(p) == 0);
            return p;
        }

        // otherwise, find the cap in which the thunk is evaluated
        // try with the spark's original capability
        nat capno = capSparkId(id);
        ASSERT(capno < n_capabilities);
        if (capno == cap->no) {
            // if it is ours, find the capability from the thread that stole
            // the spark
            owner = findCapSpark(id);
            ASSERT(owner != NULL);
        } else {
            owner = capabilities[capno];
        }
        //upd = searchEventTagValueBefore(SPARK_STEAL, id, EVENT_GC_START);
        //if (upd == NULL) {
        //    upd = searchEventTagValueBefore(SPARK_RUN, id, EVENT_GC_START);
        //}
        //if (upd == NULL) {
        //    barf("replayBlackHole: cannot find %d spark %p evaluator", id, _ptr(bh));
        //}
        //owner = capabilities[upd->capno];
        //ASSERT(peekEventCap(0, upd->capno) == upd);

        // let owner update the thunk
        debugReplay("cap %d: task %d: sync with cap %d to evaluate spark %d\n",
                    cap->no, cap->running_task->no, owner->no, id);

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
        ((EventCapValue *)ce->ev)->value == (W_)id) {
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

            debugReplay("cap %d: task %d: manually blackholing stolen "
                        "spark %d with TSO %d\n",
                        cap->no, cap->running_task->no, id,
                        ((EventStopThread *)ce->ev)->blocked_on);
        }

        return NULL;
    }

    // otherwise, both threads enter the thunk, restore it
    debugReplay("cap %d: task %d: replayBlackHole: %d spark %p should not "
                "be blackholed, restoring\n",
                cap->no, cap->running_task->no, id, _ptr(bh));
    p = ((StgInd *)bh)->indirectee;
    replayRestoreSpark(bh, id);
    // save the result or the tso pointer for later use if needed
    ((StgInd *)bh)->indirectee = p;

    return bh;
}

void
replayThunkUpdated(StgTSO *tso, StgClosure *bh, rtsBool isWHNF)
{
    if (!TRACE_spark_full) return;

    debugReplay("cap %d: task %d: replayThunkUpdated: blackhole %p already evaluated to %p\n",
                tso->cap->no, tso->cap->running_task->no,
                _ptr(bh), _ptr(((StgInd *)bh)->indirectee));

    CapEvent *ce;
    int id = replayFindSparkId(tso, bh);
    if (id == 0) {
        return;
    }

    // XXX: this is because we may have lost the id (a thunk updated event
    // just after it was updated). Alternatively, we can make
    // replayFindSparkId search for the id in every thread. If it cannot be
    // found like that, then the value from the payload must be the right one
    if (replay_enabled) {
        ce = readEvent();
        ASSERT(isEventCapValue(ce, THUNK_WHNF) ||
               isEventCapValue(ce, THUNK_UPDATED));
        if (((EventCapValue *)ce->ev)->value == (W_)((StgThunk *)bh)->payload[0]) {
            id = (int)(W_)((StgThunk *)bh)->payload[0];
        }
    }

    if (isWHNF) {
        debugReplay("cap %d: task %d: THUNK_WHNF %d\n",
                    tso->cap->no, tso->cap->running_task->no, id);
    } else {
        debugReplay("cap %d: task %d: THUNK_UPDATED %d\n",
                    tso->cap->no, tso->cap->running_task->no, id);
    }
    replayTraceCapValue(tso->cap, isWHNF ? THUNK_WHNF : THUNK_UPDATED, id);
}

MessageBlackHole *
replayMessageBlackHole(StgTSO *tso, StgClosure *bh)
{
    int id, r;
    MessageBlackHole *msg;

    if (TRACE_spark_full) {
        id = replayFindSparkId(tso, bh);
        ASSERT(id > 0);

        replayTraceCapValue(tso->cap, MSG_BLACKHOLE, id);

        // save id in the blackhole, it may be used later in replayThunkUpdated
        if (!replay_enabled) {
            replaySaveSparkId(bh, id);
        }
    }

    msg = (MessageBlackHole *)allocate(tso->cap, sizeofW(MessageBlackHole));
    SET_HDR(msg, &stg_MSG_BLACKHOLE_info, CCS_SYSTEM);
    msg->tso = tso;
    msg->bh = bh;

    if (replay_enabled) {
        CapEvent *ce = readEvent();

        if (isEventCapValue(ce, THUNK_WHNF) ||
            isEventCapValue(ce, THUNK_UPDATED)) {
            return 0;
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
        ASSERT(gc_spark_ids == NULL);
        gc_spark_ids = allocHashTable();
    } else if (TRACE_spark_full) {
        StgTSO *tso;
        nat g;

        for (g = 0; g < RtsFlags.GcFlags.generations; g++) {
            for (tso = generations[g].threads; tso != END_TSO_QUEUE;
                 tso = tso->global_link) {
                if (tso->spark_p) {
                    tso->spark_p = NULL;
                    tso->spark_id = 0;
                }
            }
        }
    }

}

static StgClosure **promoted_sparks;
static int promoted_sparks_idx;
static int promoted_sparks_size;

void
replayPromoteSpark(StgClosure *spark, int id)
{
    if (saveSpark(gc_spark_ids, NULL, spark, id)) {
        if (promoted_sparks_idx == promoted_sparks_size) {
            promoted_sparks_size += 10;
            promoted_sparks = stgReallocBytes(promoted_sparks, promoted_sparks_size,
                                              "replayPromoteSpark");
        }
        promoted_sparks[promoted_sparks_idx++] = spark;
    }
}

void
replayEndGC(void)
{
    int i;

    if (replay_enabled) {
        freeHashTable(spark_ids, stgFree);
        spark_ids = gc_spark_ids;
        gc_spark_ids = NULL;

        for (i = 0; i < promoted_sparks_idx; i++) {
            SET_INFO(promoted_sparks[i], &stg_BLACKHOLE_info);
        }

        promoted_sparks_idx = promoted_sparks_size = 0;
        stgFree(promoted_sparks);
        promoted_sparks = NULL;
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
            ASSERT(replay_main_task == NULL);
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
        if (replay_main_task == NULL) {
            // starting up
        } else if (hs_init_count == 0) {
            // shutting down
            ASSERT(replay_main_task != NULL);
            taskid = replay_main_task->no;
        } else {
            barf("eventTask: cannot find a task to run next");
        }
    }
    return taskid;
}

static void
replayLoop(void)
{
    CapEvent *ce;
    int taskid;

    while (1) {
        ce = nextEvent();
        ASSERT(ce != NULL); // fails if there are threads doing foreign calls
                            // and freeCapabilities() is not called

        //debugReplay("replayLoop: next event is '%s'\n", EventDesc[ce->ev->header.tag]);

        setupNextEvent();
        taskid = eventTask(ce->capno, ce->ev);

        if (taskid == -1) {
            //debugReplay("waking up initial thread\n");
            signalSemaphore(&no_task);
        } else {
            //debugReplay("waking up %d [%p]\n", taskid, task_replay[taskid]);
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

    ASSERT(replay_main_task != NULL);
    signalSemaphore(task_replay[replay_main_task->no]);
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
