/* ---------------------------------------------------------------------------
 *
 * (c) The GHC Team, 2014
 *
 * Events definition and utility functions
 *
 * --------------------------------------------------------------------------*/

#include "PosixSource.h"
#include "Rts.h"

#ifdef REPLAY

#include "eventlog/EventLog.h"
#include "rts/EventLogFormat.h"

#include "Event.h"
#include "RtsUtils.h"
#include "Sparks.h"
#include "Trace.h"

#include <string.h>

static int event_struct_size[NUM_GHC_EVENT_TAGS] = {
    sizeof(EventCreateThread),
    sizeof(EventRunThread),
    sizeof(EventStopThread),
    sizeof(EventThreadRunnable),
    sizeof(EventMigrateThread),
    0, 0, 0,                        // 5, 6, 7 deprecated
    sizeof(EventThreadWakeup),
    sizeof(EventGCStart),
    sizeof(EventGCEnd),
    sizeof(EventRequestSeqGC),
    sizeof(EventRequestParGC),
    0, 0,                           // 13, 14 deprecated
    sizeof(EventCreateSparkThread),
    sizeof(EventLogMsg),
    sizeof(EventStartup),
    0,                              // 18 EVENT_BLOCK_MARKER
    sizeof(EventUserMsg),
    sizeof(EventGCIdle),
    sizeof(EventGCWork),
    sizeof(EventGCDone),
    0, 0,                           // 23, 24 used by eden
    sizeof(EventCapsetCreate),
    sizeof(EventCapsetDelete),
    sizeof(EventCapsetAssignCap),
    sizeof(EventCapsetRemoveCap),
    sizeof(EventRtsIdentifier),
    sizeof(EventProgramArgs),
    sizeof(EventProgramEnv),
    sizeof(EventOSProcessPid),
    sizeof(EventOSProcessPPid),
    sizeof(EventSparkCounters),
    sizeof(EventSparkCreate),
    sizeof(EventSparkDud),
    sizeof(EventSparkOverflow),
    sizeof(EventSparkRun),
    sizeof(EventSparkSteal),
    sizeof(EventSparkFizzle),
    sizeof(EventSparkGc),
    0,                              // 42 not used by GHC
    sizeof(EventWallClockTime),
    sizeof(EventThreadLabel),
    sizeof(EventCapCreate),
    sizeof(EventCapDelete),
    sizeof(EventCapDisable),
    sizeof(EventCapEnable),
    sizeof(EventHeapAllocated),
    sizeof(EventHeapSize),
    sizeof(EventHeapLive),
    sizeof(EventHeapInfoGHC),
    sizeof(EventGcStatsGHC),
    sizeof(EventGcSync),
    sizeof(EventTaskCreate),
    sizeof(EventTaskMigrate),
    sizeof(EventTaskDelete),
    sizeof(EventUserMarker),
    0,                              // 59 EVENT_HACK_BUG_T9003
    sizeof(EventCapAlloc),
    sizeof(EventCapValue),
    sizeof(EventTaskAcquireCap),
    sizeof(EventTaskReleaseCap),
    sizeof(EventTaskReturnCap),
};

rtsBool
isVariableSizeEvent(EventTypeNum tag)
{
    rtsBool r = rtsFalse;

    switch (tag) {
    case EVENT_LOG_MSG:
    case EVENT_USER_MSG:
    case EVENT_RTS_IDENTIFIER:
    case EVENT_PROGRAM_ARGS:
    case EVENT_PROGRAM_ENV:
    case EVENT_THREAD_LABEL:
    case EVENT_USER_MARKER:
        r = rtsTrue;
    default:
        ;
    }

    return r;
}

int
eventSize(Event *ev) {
    ASSERT(ev);

    EventTypeNum tag;
    int size;

    tag = ev->header.tag;
    switch (tag) {
    case EVENT_LOG_MSG:
    case EVENT_USER_MSG:
    case EVENT_USER_MARKER:
        size = sizeof(EventMsg) + ((EventMsg *)ev)->size;
        break;
    case EVENT_RTS_IDENTIFIER:
    case EVENT_PROGRAM_ARGS:
    case EVENT_PROGRAM_ENV:
        size = sizeof(EventCapsetMsg) + ((EventCapsetMsg *)ev)->size - sizeof(EventCapsetID);
        break;
    case EVENT_THREAD_LABEL:
        size = sizeof(EventThreadLabel) + ((EventThreadLabel *)ev)->size - sizeof(EventThreadID);
        break;
    default:
        size = event_struct_size[ev->header.tag];
    }

    return size;
}

#ifdef DEBUG
static StgTSO *
findThread(StgThreadID id)
{
    StgTSO *t;
    nat g;

    for (g = 0; g < RtsFlags.GcFlags.generations; g++) {
        for (t = generations[g].threads; t != END_TSO_QUEUE; t = t->global_link) {
            if (t->id == id) {
                return t;
            }
        }
    }
    barf("findThread: thread not found");
}
#endif

void
printEvent(Capability *cap USED_IF_DEBUG, Event *ev)
{
    nat tag;

    tag = ev->header.tag;
#ifdef DEBUG
    switch (tag) {
    case EVENT_CREATE_THREAD:
    case EVENT_RUN_THREAD:
    case EVENT_THREAD_RUNNABLE:
    {
        EventThread *et = (EventThread *)ev;
        traceSchedEvent_stderr(cap, tag, findThread(et->thread), 0, 0);
        break;
    }
    case EVENT_STOP_THREAD:
    {
        EventStopThread *est = (EventStopThread *)ev;
        traceSchedEvent_stderr(cap, tag, findThread(est->thread),
                               est->status, est->blocked_on);
        break;
    }
    case EVENT_MIGRATE_THREAD:
    case EVENT_THREAD_WAKEUP:
    {
        EventThreadCap *etc = (EventThreadCap *)ev;
        traceSchedEvent_stderr(cap, tag, findThread(etc->thread), etc->capno, 0);
        break;
    }
    case EVENT_GC_START:
    case EVENT_GC_END:
    case EVENT_REQUEST_SEQ_GC:
    case EVENT_REQUEST_PAR_GC:
    case EVENT_GC_IDLE:
    case EVENT_GC_WORK:
    case EVENT_GC_DONE:
    case EVENT_GC_GLOBAL_SYNC:
    {
        traceGcEvent_stderr(cap, tag);
        break;
    }
    case EVENT_CREATE_SPARK_THREAD:
    {
        EventThread *et = (EventThread *)ev;
        traceSparkEvent_stderr(cap, tag, et->thread);
        break;
    }
    case EVENT_LOG_MSG:
    {
        EventLogMsg *elm = (EventLogMsg *)ev;
        char *msg = stgMallocBytes(elm->size + 1, "printEvent");

        snprintf(msg, elm->size + 1, "%s", elm->msg);
        trace_stderr(msg);
        break;
    }
    case EVENT_USER_MSG:
    {
        EventUserMsg *eum = (EventUserMsg *)ev;
        char *msg = stgMallocBytes(eum->size + 1, "printEvent");

        snprintf(msg, eum->size + 1, "%s", eum->msg);
        traceCap_(cap, msg);
        break;
    }
    case EVENT_STARTUP:
    {
        EventStartup *es = (EventStartup *)ev;
        traceEventStartup_stderr(es->capno);
        break;
    }
    case EVENT_BLOCK_MARKER:
    {
        barf("Cannot print a Block marker event");
    }
    case EVENT_CAPSET_CREATE:
    {
        EventCapsetCreate *ecc = (EventCapsetCreate *)ev;
        traceCapsetEvent_stderr(tag, ecc->capset, ecc->type);
        break;
    }
    case EVENT_CAPSET_DELETE:
    {
        EventCapsetDelete *ecd = (EventCapsetDelete *)ev;
        traceCapsetEvent_stderr(tag, ecd->capset, 0);
        break;
    }
    case EVENT_CAPSET_ASSIGN_CAP:
    case EVENT_CAPSET_REMOVE_CAP:
    {
        EventCapsetCapNo *eccn = (EventCapsetCapNo *)ev;
        traceCapsetEvent_stderr(tag, eccn->capset, eccn->capno);
        break;
    }
    case EVENT_RTS_IDENTIFIER:
    case EVENT_PROGRAM_ARGS:
    case EVENT_PROGRAM_ENV:
    case EVENT_OSPROCESS_PID:
    case EVENT_OSPROCESS_PPID:
    case EVENT_SPARK_COUNTERS:
    case EVENT_WALL_CLOCK_TIME:
    case EVENT_HEAP_ALLOCATED:
    case EVENT_HEAP_SIZE:
    case EVENT_HEAP_LIVE:
    case EVENT_HEAP_INFO_GHC:
    case EVENT_GC_STATS_GHC:
    {
        // TODO: unimplemented
        debugBelch("\n");
        break;
    }
    case EVENT_SPARK_CREATE:
    case EVENT_SPARK_DUD:
    case EVENT_SPARK_OVERFLOW:
    case EVENT_SPARK_RUN:
    case EVENT_SPARK_FIZZLE:
    case EVENT_SPARK_GC:
    {
        traceSparkEvent_stderr(cap, tag, 0);
        break;
    }
    case EVENT_SPARK_STEAL:
    {
        struct _EventCapNo *ec = (struct _EventCapNo *)ev;
        traceSparkEvent_stderr(cap, tag, ec->capno);
        break;
    }
    case EVENT_INTERN_STRING:
    {
        barf("Cannot print a Internal string event");
    }
    case EVENT_THREAD_LABEL:
    {
        EventThreadLabel *etl = (EventThreadLabel *)ev;
        char *buf = stgMallocBytes(etl->size + 1, "printEvent");

        snprintf(buf, etl->size - sizeof(EventThreadID) + 1, "%s", etl->label);
        traceThreadLabel_stderr(cap, findThread(etl->thread), buf);
        break;
    }
    case EVENT_CAP_CREATE:
    case EVENT_CAP_DELETE:
    case EVENT_CAP_ENABLE:
    case EVENT_CAP_DISABLE:
    {
        struct _EventCapNo *ecn = (struct _EventCapNo *)ev;
        traceCapEvent_stderr(capabilities[ecn->capno], tag);
        break;
    }
    case EVENT_TASK_CREATE:
    {
        EventTaskCreate *etc = (EventTaskCreate *)ev;
        traceTaskEvent_stderr(tag, etc->task, etc->capno, 0);
        break;
    }
    case EVENT_TASK_MIGRATE:
    {
        EventTaskMigrate *etm = (EventTaskMigrate *)ev;
        traceTaskEvent_stderr(tag, etm->task, etm->capno, etm->new_capno);
        break;
    }
    case EVENT_TASK_DELETE:
    {
        EventTaskDelete *etd = (EventTaskDelete *)ev;
        traceTaskEvent_stderr(tag, etd->task, 0, 0);
        break;
    }
    case EVENT_USER_MARKER:
    {
        EventUserMarker *eum = (EventUserMarker *)ev;
        char *markername = stgMallocBytes(eum->size + 1, "printEvent");

        snprintf(markername, eum->size + 1, "%s", eum->msg);
        traceCap_stderr(cap, "User marker: %s", markername);
        break;
    }
    case EVENT_HACK_BUG_T9003:
    {
        barf("Cannot print Hack bug T9003 event");
    }
    case EVENT_CAP_ALLOC:
    {
        EventCapAlloc *eca = (EventCapAlloc *)ev;
        traceCapAlloc_stderr(cap, eca->alloc, eca->blocks, eca->hp_alloc);
        break;
    }
    case EVENT_CAP_VALUE:
    {
        EventCapValue *ecv = (EventCapValue *)ev;
        traceCapValue_stderr(cap, ecv->tag, ecv->value);
        break;
    }
    case EVENT_TASK_ACQUIRE_CAP:
    case EVENT_TASK_RELEASE_CAP:
    {
        EventTaskCap *etc = (EventTaskCap *)ev;
        traceTaskCap_stderr(cap, tag, etc->task);
        break;
    }
    case EVENT_TASK_RETURN_CAP:
    {
        EventTaskReturnCap *etrc = (EventTaskReturnCap *)ev;
        traceTaskCap_stderr(capabilities[etrc->capno], tag, etrc->task);
        break;
    }
    default:
        barf("printEvent: unknown event tag %d", tag);
    }
#else
    debugBelch("%s\n", EventDesc[tag]);
#endif
}


#define BUF 512

Event *
createEvent(EventTypeNum tag)
{
    Event *ev;

    ev = stgCallocBytes(1, event_struct_size[tag], "createEvent");
    ev->header.tag = tag;
    ev->header.time = time_ns();

    return ev;
}

Event *
createSchedEvent(EventTypeNum tag,
                 StgTSO      *tso,
                 StgWord      info1,
                 StgWord      info2)
{
    Event *ev;

    ev = createEvent(tag);
    switch (tag) {
    case EVENT_CREATE_THREAD:
    case EVENT_RUN_THREAD:
    case EVENT_THREAD_RUNNABLE:
    {
        EventThread *et = (EventThread *)ev;
        et->thread = tso->id;
        break;
    }
    case EVENT_STOP_THREAD:
    {
        EventStopThread *est = (EventStopThread *)ev;
        est->thread = tso->id;
        est->status = info1;
        est->blocked_on = info2;

        ev = (Event *)est;
        break;
    }
    case EVENT_MIGRATE_THREAD:
    case EVENT_THREAD_WAKEUP:
    {
        EventThreadCap *etc = (EventThreadCap *)ev;
        etc->thread = tso->id;
        etc->capno = info1;
        break;
    }
    default:
        barf ("createSchedEvent: unknown event tag %d", tag);
    }

    return ev;
}

Event *
createStartupEvent(EventCapNo nocaps)
{
    EventStartup *es;

    es = (EventStartup *)createEvent(EVENT_STARTUP);
    es->capno = nocaps;
    return (Event *)es;
}

Event *
createGcEvent(EventTypeNum tag)
{
    Event *ev;

    ev = createEvent(tag);
    switch (tag) {
    case EVENT_GC_START:
    case EVENT_GC_END:
    case EVENT_REQUEST_SEQ_GC:
    case EVENT_REQUEST_PAR_GC:
    case EVENT_GC_IDLE:
    case EVENT_GC_WORK:
    case EVENT_GC_DONE:
    case EVENT_SPARK_CREATE:
    case EVENT_SPARK_DUD:
    case EVENT_SPARK_OVERFLOW:
    case EVENT_SPARK_RUN:
    case EVENT_SPARK_FIZZLE:
    case EVENT_SPARK_GC:
    case EVENT_GC_GLOBAL_SYNC:
        /* Empty events */
        break;
    default:
        barf ("createGcEvent: unknown event tag %d", tag);
    }

    return ev;
}

Event *
createHeapEvent(EventTypeNum tag,
                CapsetID     heap_capset,
                W_           info1)
{
    Event *ev;

    ev = createEvent(tag);
    switch (tag) {
    case EVENT_HEAP_ALLOCATED:
    case EVENT_HEAP_SIZE:
    case EVENT_HEAP_LIVE:
    {
        EventCapsetBytes *ecb = (EventCapsetBytes *)ev;
        ecb->capset = heap_capset;
        ecb->bytes = info1;
        break;
    }
    default:
        barf ("createHeapEvent: unknown event tag %d", tag);
    }

    return ev;
}

Event *
createHeapInfoEvent(CapsetID heap_capset,
                    nat      gens,
                    W_       maxHeapSize,
                    W_       allocAreaSize,
                    W_       mblockSize,
                    W_       blockSize)
{
    EventHeapInfoGHC *ev;

    ev = (EventHeapInfoGHC *)createEvent(EVENT_HEAP_INFO_GHC);
    ev->capset = heap_capset;
    ev->gens = gens;
    ev->maxHeapSize = maxHeapSize;
    ev->allocAreaSize = allocAreaSize;
    ev->mblockSize = mblockSize;
    ev->blockSize = blockSize;

    return (Event *)ev;
}

Event *
createGcStatsEvent(CapsetID heap_capset,
                   nat      gen,
                   W_       copied,
                   W_       slop,
                   W_       fragmentation,
                   nat      par_n_threads,
                   W_       par_max_copied,
                   W_       par_tot_copied)
{
    EventGcStatsGHC *ev;

    ev = (EventGcStatsGHC *)createEvent(EVENT_GC_STATS_GHC);
    ev->capset = heap_capset;
    ev->gen = gen;
    ev->copied = copied;
    ev->slop = slop;
    ev->fragmentation = fragmentation;
    ev->par_n_threads = par_n_threads;
    ev->par_max_copied = par_max_copied;
    ev->par_tot_copied = par_tot_copied;

    return (Event *)ev;
}

Event *
createCapEvent(EventTypeNum tag, EventCapNo capno)
{
    Event *ev;

    ev = createEvent(tag);
    switch (tag) {
    case EVENT_CAP_CREATE:
    case EVENT_CAP_DELETE:
    case EVENT_CAP_DISABLE:
    case EVENT_CAP_ENABLE:
    {
        struct _EventCapNo *ecn = (struct _EventCapNo *)ev;
        ecn->capno = capno;
        break;
    }
    default:
        barf ("createCapEvent: unknown event tag %d", tag);
    }

    return ev;
}

Event *
createCapsetEvent(EventTypeNum tag, CapsetID capset, StgWord info)
{
    Event *ev;

    ev = createEvent(tag);
    switch (tag) {
    case EVENT_CAPSET_CREATE:
    {
        EventCapsetCreate *ecc = (EventCapsetCreate *)ev;
        ecc->capset = capset;
        ecc->type = info;
        break;
    }
    case EVENT_CAPSET_DELETE:
    {
        EventCapsetDelete *ecd = (EventCapsetDelete *)ev;
        ecd->capset = capset;
        break;
    }
    case EVENT_CAPSET_ASSIGN_CAP:
    case EVENT_CAPSET_REMOVE_CAP:
    {
        EventCapsetCapNo *eccn = (EventCapsetCapNo *)ev;
        eccn->capset = capset;
        eccn->capno = info;
        break;
    }
    case EVENT_OSPROCESS_PID:
    case EVENT_OSPROCESS_PPID:
    {
        EventCapsetPid *ecp = (EventCapsetPid *)ev;
        ecp->capset = capset;
        ecp->pid = info;
        break;
    }
    default:
        barf ("createCapsetEvent: unknown event tag %d", tag);
    }

    return ev;
}

Event *
createWallClockTimeEvent(EventCapsetID capset)
{
    EventWallClockTime *ev;
    StgWord64 sec;
    StgWord32 nsec;

    ev = (EventWallClockTime *)createEvent(EVENT_WALL_CLOCK_TIME);
    ev->capset = capset;
    getUnixEpochTime(&sec, &nsec);
    ev->sec = sec;
    ev->nsec = nsec;

    return (Event *)ev;
}

Event *
createCapsetStrEvent(EventTypeNum tag, EventCapsetID capset, const char *msg)
{
    Event *ev;

    switch (tag) {
    case EVENT_RTS_IDENTIFIER:
    {
        EventPayloadSize strsize;

        strsize = strlen(msg);

        EventCapsetMsg *ecm = stgCallocBytes(1, sizeof(EventCapsetMsg) + strsize, "createCapsetStrEvent");
        ecm->size = sizeof(EventCapsetID) + strsize;
        ecm->capset = capset;
        memcpy(ecm->msg, msg, strsize);

        ev = (Event *)ecm;
        break;
    }
    default:
        barf ("createCapsetStrEvent: unknown event tag %d", tag);
    }

    ev->header.tag = tag;
    ev->header.time = time_ns();
    return ev;
}

Event *
createCapsetVecEvent (EventTypeNum  tag,
                      EventCapsetID capset,
                      int           argc,
                      const char   *argv[])
{
    Event *ev;

    switch (tag) {
    case EVENT_PROGRAM_ARGS:
    case EVENT_PROGRAM_ENV:
    {
        int i;
        StgWord8 *p;
        EventPayloadSize size = 0;

        for (i = 0; i < argc; i++) {
            size += 1 + strlen(argv[i]);
        }

        EventCapsetMsg *ecm = stgCallocBytes(1, sizeof(EventCapsetMsg) + size, "createCapsetVecEvent");
        ecm->size = sizeof(EventCapsetID) + size;
        ecm->capset = capset;
        p = ecm->msg;
        for (i = 0; i < argc; i++) {
            size = 1 + strlen(argv[i]);
            memcpy(p, (StgWord8 *)argv[i], size);
            p += size;
        }

        ev = (Event *)ecm;
        break;
    }
    default:
        barf ("createCapsetVecEvent: unknown event tag %d", tag);
    }

    ev->header.tag = tag;
    ev->header.time = time_ns();
    return ev;
}

Event *
createSparkEvent(EventTypeNum tag, StgWord info1)
{
    Event *ev;

    ev = createEvent(tag);
    switch (tag) {
    case EVENT_SPARK_STEAL:         // (cap, victim_cap)
    {
        struct _EventCapNo *ecn = (struct _EventCapNo *)ev;
        ecn->capno = info1;
        break;
    }
    case EVENT_CREATE_SPARK_THREAD: // (cap, thread)
    {
        EventThread *et = (EventThread *)ev;
        et->thread = info1;
        break;
    }
    case EVENT_SPARK_CREATE:
    case EVENT_SPARK_DUD:
    case EVENT_SPARK_OVERFLOW:
    case EVENT_SPARK_RUN:
    case EVENT_SPARK_FIZZLE:
    case EVENT_SPARK_GC:
    {
        break;
    }
    default:
        barf ("createSparkEvent: unknown event tag %d", tag);
    }

    return ev;
}

Event *
createSparkCountersEvent(SparkCounters counters, StgWord remaining)
{
    EventSparkCounters *ev;

    ev = (EventSparkCounters *)createEvent(EVENT_SPARK_COUNTERS);
    ev->created = counters.created;
    ev->dud = counters.dud;
    ev->overflowed = counters.overflowed;
    ev->converted = counters.converted;
    ev->gcd = counters.gcd;
    ev->fizzled = counters.fizzled;
    ev->remaining = remaining;

    return (Event *)ev;
}

Event *
createMsgEvent(EventTypeNum tag, const char *msg, va_list ap)
{
    Event *ev;

    switch (tag) {
    case EVENT_LOG_MSG:
    case EVENT_USER_MSG:
    {
        char buf[BUF];
        int size;

        size = vsnprintf(buf, BUF, msg, ap);
        // XXX: possible bug, size >= BUF?
        if (size > BUF) {
            buf[BUF-1] = '\0';
            size = BUF;
        }

        EventMsg *em = stgCallocBytes(1, sizeof(EventMsg) + size, "createMsgEvent");
        em->size = size;
        memcpy(em->msg, buf, size);

        ev = (Event *)em;
        break;
    }
    default:
        barf ("createMsgEvent: unknown event tag %d", tag);
    }

    ev->header.tag = tag;
    ev->header.time = time_ns();
    return ev;
}

Event *
createThreadLabelEvent(StgTSO *tso, const char *label)
{
    EventThreadLabel *ev;
    EventPayloadSize strsize;

    strsize = strlen(label);

    ev = stgCallocBytes(1, sizeof(EventThreadLabel) + strsize, "createThreadLabelEvent");
    ev->size = sizeof(EventThreadID) + strsize;
    ev->thread = tso->id;
    memcpy(ev->label, label, strsize);

    ev->header.tag = EVENT_THREAD_LABEL;
    ev->header.time = time_ns();
    return (Event *)ev;
}

Event *
createTaskCreateEvent(EventTaskId taskId, EventCapNo capno)
{
    EventTaskCreate *ev;

    ev = (EventTaskCreate *)createEvent(EVENT_TASK_CREATE);
    ev->task = taskId;
    ev->capno = capno;

    return (Event *)ev;
}

Event *
createTaskMigrateEvent(EventTaskId taskId, EventCapNo capno,
                       EventCapNo new_capno)
{
    EventTaskMigrate *ev;

    ev = (EventTaskMigrate *)createEvent(EVENT_TASK_MIGRATE);
    ev->task = taskId;
    ev->capno = capno;
    ev->new_capno = new_capno;

    return (Event *)ev;
}

Event *
createTaskDeleteEvent(EventTaskId taskId)
{
    EventTaskDelete *ev;

    ev = (EventTaskDelete *)createEvent(EVENT_TASK_DELETE);
    ev->task = taskId;

    return (Event *)ev;
}

Event *
createUserMarkerEvent(const char *markername)
{
    EventUserMarker *ev;
    int size;

    size = strlen(markername);

    ev = stgCallocBytes(1, sizeof(EventUserMarker) + size, "createUserMarkerEvent");
    ev->size = size;
    memcpy(ev->msg, markername, size);

    ev->header.tag = EVENT_USER_MARKER;
    ev->header.time = time_ns();
    return (Event *)ev;
}

Event *
createCapAllocEvent(W_ alloc, W_ blocks, W_ hp_alloc)
{
    EventCapAlloc *ev;

    ev = (EventCapAlloc *)createEvent(EVENT_CAP_ALLOC);
    ev->alloc = alloc;
    ev->blocks = blocks;
    ev->hp_alloc = hp_alloc;

    return (Event *)ev;
}

Event *
createCapValueEvent(nat tag, W_ value)
{
    EventCapValue *ev;

    ev = (EventCapValue *)createEvent(EVENT_CAP_VALUE);
    ev->tag = tag;
    ev->value = value;

    return (Event *)ev;
}

Event *
createTaskAcquireCapEvent(EventTaskId taskId)
{
    EventTaskAcquireCap *ev;

    ev = (EventTaskAcquireCap *)createEvent(EVENT_TASK_ACQUIRE_CAP);
    ev->task = taskId;

    return (Event *)ev;
}

Event *
createTaskReleaseCapEvent(EventTaskId taskId)
{
    EventTaskReleaseCap *ev;

    ev = (EventTaskReleaseCap *)createEvent(EVENT_TASK_RELEASE_CAP);
    ev->task = taskId;

    return (Event *)ev;
}

Event *
createTaskReturnCapEvent(EventTaskId taskId, EventCapNo capno)
{
    EventTaskReturnCap *ev;

    ev = (EventTaskReturnCap *)createEvent(EVENT_TASK_RETURN_CAP);
    ev->task = taskId;
    ev->capno = capno;

    return (Event *)ev;
}

#endif // REPLAY
