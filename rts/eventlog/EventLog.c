/* -----------------------------------------------------------------------------
 *
 * (c) The GHC Team, 2008-2009
 *
 * Support for fast binary event logging.
 *
 * ---------------------------------------------------------------------------*/

#include "PosixSource.h"
#include "Rts.h"

#ifdef TRACING

#include "Trace.h"
#include "Capability.h"
#include "RtsUtils.h"
#include "EventLog.h"
#include "Event.h"

#include <string.h>
#include <stdio.h>
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

// PID of the process that writes to event_log_filename (#4512)
static pid_t event_log_pid = -1;

static char *event_log_filename = NULL;

// File for logging events
FILE *event_log_file = NULL;

#define EVENT_LOG_SIZE 2 * (1024 * 1024) // 2MB

static int flushCount;

// Struct for record keeping of buffer to store event types and events.
typedef struct _EventsBuf {
  StgInt8 *begin;
  StgInt8 *pos;
  StgInt8 *marker;
  StgWord64 size;
  EventCapNo capno; // which capability this buffer belongs to, or -1
} EventsBuf;

EventsBuf *capEventBuf; // one EventsBuf for each Capability

EventsBuf eventBuf; // an EventsBuf not associated with any Capability
#ifdef THREADED_RTS
Mutex eventBufMutex; // protected by this mutex
#endif

// Replay definitions
static char *eventLogFilenameReplay = NULL;

FILE  *fileReplay = NULL;
int fileReplayStartSeek; // record where events start

typedef struct _ReplayBuf {
    EventsBuf eb;   // pos points to the next byte to read
                    // marker points to the first empty position
    FILE *f;        // each capability reads in different positions
    CapEvent *ce;   // cached linked events, capno is unset
    nat ceSize;     // cached events list size
} ReplayBuf;

static ReplayBuf *capReplayBuf;
static ReplayBuf  replayBuf;

static CapEvent *eventList = NULL;
static nat eventListSize = 0;

const char *EventDesc[] = {
  [EVENT_CREATE_THREAD]       = "Create thread",
  [EVENT_RUN_THREAD]          = "Run thread",
  [EVENT_STOP_THREAD]         = "Stop thread",
  [EVENT_THREAD_RUNNABLE]     = "Thread runnable",
  [EVENT_MIGRATE_THREAD]      = "Migrate thread",
  [EVENT_THREAD_WAKEUP]       = "Wakeup thread",
  [EVENT_THREAD_LABEL]        = "Thread label",
  [EVENT_STARTUP]             = "Create capabilities",
  [EVENT_CAP_CREATE]          = "Create capability",
  [EVENT_CAP_DELETE]          = "Delete capability",
  [EVENT_CAP_DISABLE]         = "Disable capability",
  [EVENT_CAP_ENABLE]          = "Enable capability",
  [EVENT_GC_START]            = "Starting GC",
  [EVENT_GC_END]              = "Finished GC",
  [EVENT_REQUEST_SEQ_GC]      = "Request sequential GC",
  [EVENT_REQUEST_PAR_GC]      = "Request parallel GC",
  [EVENT_GC_GLOBAL_SYNC]      = "Synchronise stop-the-world GC",
  [EVENT_GC_STATS_GHC]        = "GC statistics",
  [EVENT_HEAP_INFO_GHC]       = "Heap static parameters",
  [EVENT_HEAP_ALLOCATED]      = "Total heap mem ever allocated",
  [EVENT_HEAP_SIZE]           = "Current heap size",
  [EVENT_HEAP_LIVE]           = "Current heap live data",
  [EVENT_CREATE_SPARK_THREAD] = "Create spark thread",
  [EVENT_LOG_MSG]             = "Log message",
  [EVENT_USER_MSG]            = "User message",
  [EVENT_USER_MARKER]         = "User marker",
  [EVENT_GC_IDLE]             = "GC idle",
  [EVENT_GC_WORK]             = "GC working",
  [EVENT_GC_DONE]             = "GC done",
  [EVENT_BLOCK_MARKER]        = "Block marker",
  [EVENT_CAPSET_CREATE]       = "Create capability set",
  [EVENT_CAPSET_DELETE]       = "Delete capability set",
  [EVENT_CAPSET_ASSIGN_CAP]   = "Add capability to capability set",
  [EVENT_CAPSET_REMOVE_CAP]   = "Remove capability from capability set",
  [EVENT_RTS_IDENTIFIER]      = "RTS name and version",
  [EVENT_PROGRAM_ARGS]        = "Program arguments",
  [EVENT_PROGRAM_ENV]         = "Program environment variables",
  [EVENT_OSPROCESS_PID]       = "Process ID",
  [EVENT_OSPROCESS_PPID]      = "Parent process ID",
  [EVENT_WALL_CLOCK_TIME]     = "Wall clock time",
  [EVENT_SPARK_COUNTERS]      = "Spark counters",
  [EVENT_SPARK_CREATE]        = "Spark create",
  [EVENT_SPARK_DUD]           = "Spark dud",
  [EVENT_SPARK_OVERFLOW]      = "Spark overflow",
  [EVENT_SPARK_RUN]           = "Spark run",
  [EVENT_SPARK_STEAL]         = "Spark steal",
  [EVENT_SPARK_FIZZLE]        = "Spark fizzle",
  [EVENT_SPARK_GC]            = "Spark GC",
  [EVENT_TASK_CREATE]         = "Task create",
  [EVENT_TASK_MIGRATE]        = "Task migrate",
  [EVENT_TASK_DELETE]         = "Task delete",
  [EVENT_HACK_BUG_T9003]      = "Empty event for bug #9003",
  [EVENT_CAP_ALLOC]           = "Capability allocation",
  [EVENT_CAP_VALUE]           = "Capability setting value",
  [EVENT_TASK_ACQUIRE_CAP]    = "Task acquiring capability",
  [EVENT_TASK_RELEASE_CAP]    = "Task releasing capability",
  [EVENT_TASK_RETURN_CAP]     = "Task returning to capability",
};

// Event type. 

typedef struct _EventType {
  EventTypeNum etNum;  // Event Type number.
  nat size;            // size of the payload in bytes
  const char *desc;    // Description
} EventType;

EventType eventTypes[NUM_GHC_EVENT_TAGS];

static StgBool initEventType(StgWord8 t);

static void initEventsBuf(EventsBuf* eb, StgWord64 size, EventCapNo capno);
static void resetEventsBuf(EventsBuf* eb);
static void printAndClearEventBuf (EventsBuf *eventsBuf);

static void postEventType(EventsBuf *eb, EventType *et);

static void postLogMsg(EventsBuf *eb, EventTypeNum type, const char *msg, va_list ap);

static void postBlockMarker(EventsBuf *eb);
static void closeBlockMarker(EventsBuf *ebuf);

static StgBool hasRoomForEvent(EventsBuf *eb, EventTypeNum eNum);
static StgBool hasRoomForVariableEvent(EventsBuf *eb, nat payload_bytes);

static inline void postWord8(EventsBuf *eb, StgWord8 i)
{
    *(eb->pos++) = i; 
}

static inline void postWord16(EventsBuf *eb, StgWord16 i)
{
    postWord8(eb, (StgWord8)(i >> 8));
    postWord8(eb, (StgWord8)i);
}

static inline void postWord32(EventsBuf *eb, StgWord32 i)
{
    postWord16(eb, (StgWord16)(i >> 16));
    postWord16(eb, (StgWord16)i);
}

static inline void postWord64(EventsBuf *eb, StgWord64 i)
{
    postWord32(eb, (StgWord32)(i >> 32));
    postWord32(eb, (StgWord32)i);
}

static inline void postBuf(EventsBuf *eb, StgWord8 *buf, nat size)
{
    memcpy(eb->pos, buf, size);
    eb->pos += size;
}

static inline void postEventTypeNum(EventsBuf *eb, EventTypeNum etNum)
{ postWord16(eb, etNum); }

static inline void postTimestamp(EventsBuf *eb)
{ postWord64(eb, time_ns()); }

static inline void postThreadID(EventsBuf *eb, EventThreadID id)
{ postWord32(eb,id); }

static inline void postCapNo(EventsBuf *eb, EventCapNo no)
{ postWord16(eb,no); }

static inline void postCapsetID(EventsBuf *eb, EventCapsetID id)
{ postWord32(eb,id); }

static inline void postCapsetType(EventsBuf *eb, EventCapsetType type)
{ postWord16(eb,type); }

static inline void postOSProcessId(EventsBuf *eb, pid_t pid)
{ postWord32(eb, pid); }

static inline void postKernelThreadId(EventsBuf *eb, EventKernelThreadId tid)
{ postWord64(eb, tid); }

static inline void postTaskId(EventsBuf *eb, EventTaskId tUniq)
{ postWord64(eb, tUniq); }

static inline void postPayloadSize(EventsBuf *eb, EventPayloadSize size)
{ postWord16(eb,size); }

static inline void postEventHeader(EventsBuf *eb, EventTypeNum type)
{
    postEventTypeNum(eb, type);
    postTimestamp(eb);
}

static inline void postInt8(EventsBuf *eb, StgInt8 i)
{ postWord8(eb, (StgWord8)i); }

static inline void postInt32(EventsBuf *eb, StgInt32 i)
{ postWord32(eb, (StgWord32)i); }

#ifdef REPLAY
static inline int readWord8(ReplayBuf *rb, StgWord8 *i)
{
    FILE *f;
    size_t r;

    if (rb != NULL) {
        f = rb->f;
    } else {
        f = fileReplay;
    }
    r = fread (i, 1, 1, f);

    ASSERT(r == (size_t)1);
    return (int)r;
}

static inline int readWord16(ReplayBuf *rb, StgWord16 *i)
{
    int r;

    r = readWord8(rb, (StgWord8 *)i);
    *i = *i << 8;
    r += readWord8(rb, (StgWord8 *)i);

    ASSERT(r == 2);
    return r;
}

static inline int readWord32(ReplayBuf *rb, StgWord32 *i)
{
    int r;

    r = readWord16(rb, (StgWord16 *)i);
    *i = *i << 16;
    r += readWord16(rb, (StgWord16 *)i);

    ASSERT(r == 4);
    return r;
}

static inline int readBuf(ReplayBuf *rb, StgWord8 *p, nat size)
{
    FILE *f;
    size_t r;

    if (rb != NULL) {
        f = rb->f;
    } else {
        f = fileReplay;
    }
    r = fread(p, 1, size, f);

    ASSERT(r == (size_t)size);
    return (int)r;
}

static inline void readEventTypeNum(ReplayBuf *rb, EventTypeNum *etNum)
{ readWord16(rb, etNum); }

static inline void readCapNo(ReplayBuf *rb, EventCapNo *no)
{ readWord16(rb, no); }

static inline void readInt8(ReplayBuf *rb, StgInt8 *i)
{ readWord8(rb, (StgWord8 *)i); }

static inline void readInt32(ReplayBuf *rb, StgInt32 *i)
{ readWord32(rb, (StgWord32 *)i); }

static inline int skipSize(ReplayBuf *rb, long size)
{

    FILE *f;
    int r;

    if (rb != NULL) {
        f = rb->f;
    } else {
        f = fileReplay;
    }
    r = fseek(f, size, SEEK_CUR);

    ASSERT(r == 0);
    return r;
}

static inline rtsBool readEof(ReplayBuf *rb)
{
    FILE *f;

    if (rb != NULL) {
        f = rb->f;
    } else {
        f = fileReplay;
    }

    return (feof(f) == 0);
}

static inline void getWord8(ReplayBuf *rb, StgWord8 *i)
{
    ASSERT(rb->eb.pos < rb->eb.marker);
    *i = *rb->eb.pos;
    rb->eb.pos++;
}

static inline void getWord16(ReplayBuf *rb, StgWord16 *i)
{
    getWord8(rb, (StgWord8 *)i);
    *i = *i << 8;
    getWord8(rb, (StgWord8 *)i);
}

static inline void getWord32(ReplayBuf *rb, StgWord32 *i)
{
    getWord16(rb, (StgWord16 *)i);
    *i = *i << 16;
    getWord16(rb, (StgWord16 *)i);
}

static inline void getWord64(ReplayBuf *rb, StgWord64 *i)
{
    getWord32(rb, (StgWord32 *)i);
    *i = *i << 32;
    getWord32(rb, (StgWord32 *)i);
}

static inline void getBuf(ReplayBuf *rb, StgWord8 *buf, nat size)
{
    ASSERT(rb->eb.pos <= rb->eb.marker - size);
    memcpy(buf, rb->eb.pos, size);
    rb->eb.pos += size;
}

static inline void getEventTypeNum(ReplayBuf *rb, EventTypeNum *etNum)
{ getWord16(rb, etNum); }

static inline void getTimestamp(ReplayBuf *rb, EventTimestamp *ts)
{ getWord64(rb, ts); }

static inline void getThreadID(ReplayBuf *rb, EventThreadID *id)
{ getWord32(rb, id); }

static inline void getThreadStatus(ReplayBuf *rb, EventThreadStatus *status)
{ getWord16(rb, status); }

static inline void getCapNo(ReplayBuf *rb, EventCapNo *no)
{ getWord16(rb, no); }

static inline void getPayloadSize(ReplayBuf *rb, EventPayloadSize *size)
{ getWord16(rb, size); }

static inline void getCapsetID(ReplayBuf *rb, EventCapsetID *id)
{ getWord32(rb, id); }

static inline void getCapsetType(ReplayBuf *rb, EventCapsetType *type)
{ getWord16(rb, type); }

static inline void getTaskId(ReplayBuf *rb, EventTaskId *id)
{ getWord64(rb, id); }

static inline void getKernelThreadId(ReplayBuf *rb, EventKernelThreadId *tid)
{ getWord64(rb, tid); }

static void initReplayBuf(ReplayBuf *rb, EventCapNo capno, FILE *f);
static void resetReplayBuf(ReplayBuf *rb);
static void fillReplayBuf(ReplayBuf *rb);

static void readEventType(void);
static Event *getEvent(ReplayBuf *rb);

static CapEvent *replayBufCurrent(ReplayBuf *rb);
static CapEvent *replayBufN(ReplayBuf *rb, nat n);
static CapEvent *replayBufNext(ReplayBuf *rb);
static CapEvent *replayBufForward(ReplayBuf *rb);

static CapEvent *eventListN(nat n);
static CapEvent *eventListNew(CapEvent *ce);
static CapEvent *eventListNext(void);
static void eventListForward(void);
#endif

static StgBool
initEventType(StgWord8 t)
{
    eventTypes[t].etNum = t;
    eventTypes[t].desc = EventDesc[t];

    switch (t) {
    case EVENT_CREATE_THREAD:   // (cap, thread)
    case EVENT_RUN_THREAD:      // (cap, thread)
    case EVENT_THREAD_RUNNABLE: // (cap, thread)
    case EVENT_CREATE_SPARK_THREAD: // (cap, spark_thread)
        eventTypes[t].size = sizeof(EventThreadID);
        break;

    case EVENT_MIGRATE_THREAD:  // (cap, thread, new_cap)
    case EVENT_THREAD_WAKEUP:   // (cap, thread, other_cap)
        eventTypes[t].size = sizeof(EventThreadID) + sizeof(EventCapNo);
        break;

    case EVENT_STOP_THREAD:     // (cap, thread, status)
        eventTypes[t].size =
            sizeof(EventThreadID) + sizeof(StgWord16) + sizeof(EventThreadID);
        break;

    case EVENT_STARTUP:         // (cap count)
    case EVENT_CAP_CREATE:      // (cap)
    case EVENT_CAP_DELETE:      // (cap)
    case EVENT_CAP_ENABLE:      // (cap)
    case EVENT_CAP_DISABLE:     // (cap)
        eventTypes[t].size = sizeof(EventCapNo);
        break;

    case EVENT_CAPSET_CREATE:   // (capset, capset_type)
        eventTypes[t].size =
            sizeof(EventCapsetID) + sizeof(EventCapsetType);
        break;

    case EVENT_CAPSET_DELETE:   // (capset)
        eventTypes[t].size = sizeof(EventCapsetID);
        break;

    case EVENT_CAPSET_ASSIGN_CAP:  // (capset, cap)
    case EVENT_CAPSET_REMOVE_CAP:
        eventTypes[t].size =
            sizeof(EventCapsetID) + sizeof(EventCapNo);
        break;

    case EVENT_OSPROCESS_PID:   // (cap, pid)
    case EVENT_OSPROCESS_PPID:
        eventTypes[t].size =
            sizeof(EventCapsetID) + sizeof(StgWord32);
        break;

    case EVENT_WALL_CLOCK_TIME: // (capset, unix_epoch_seconds, nanoseconds)
        eventTypes[t].size =
            sizeof(EventCapsetID) + sizeof(StgWord64) + sizeof(StgWord32);
        break;

    case EVENT_SPARK_STEAL:     // (cap, victim_cap)
        eventTypes[t].size =
            sizeof(EventCapNo);
        break;

    case EVENT_REQUEST_SEQ_GC:  // (cap)
    case EVENT_REQUEST_PAR_GC:  // (cap)
    case EVENT_GC_START:        // (cap)
    case EVENT_GC_END:          // (cap)
    case EVENT_GC_IDLE:
    case EVENT_GC_WORK:
    case EVENT_GC_DONE:
    case EVENT_GC_GLOBAL_SYNC:  // (cap)
    case EVENT_SPARK_CREATE:    // (cap)
    case EVENT_SPARK_DUD:       // (cap)
    case EVENT_SPARK_OVERFLOW:  // (cap)
    case EVENT_SPARK_RUN:       // (cap)
    case EVENT_SPARK_FIZZLE:    // (cap)
    case EVENT_SPARK_GC:        // (cap)
        eventTypes[t].size = 0;
        break;

    case EVENT_LOG_MSG:          // (msg)
    case EVENT_USER_MSG:         // (msg)
    case EVENT_USER_MARKER:      // (markername)
    case EVENT_RTS_IDENTIFIER:   // (capset, str)
    case EVENT_PROGRAM_ARGS:     // (capset, strvec)
    case EVENT_PROGRAM_ENV:      // (capset, strvec)
    case EVENT_THREAD_LABEL:     // (thread, str)
        eventTypes[t].size = 0xffff;
        break;

    case EVENT_SPARK_COUNTERS:   // (cap, 7*counter)
        eventTypes[t].size = 7 * sizeof(StgWord64);
        break;

    case EVENT_HEAP_ALLOCATED:    // (heap_capset, alloc_bytes)
    case EVENT_HEAP_SIZE:         // (heap_capset, size_bytes)
    case EVENT_HEAP_LIVE:         // (heap_capset, live_bytes)
        eventTypes[t].size = sizeof(EventCapsetID) + sizeof(StgWord64);
        break;

    case EVENT_HEAP_INFO_GHC:     // (heap_capset, n_generations,
                                  //  max_heap_size, alloc_area_size,
                                  //  mblock_size, block_size)
        eventTypes[t].size = sizeof(EventCapsetID)
                           + sizeof(StgWord16)
                           + sizeof(StgWord64) * 4;
        break;

    case EVENT_GC_STATS_GHC:      // (heap_capset, generation,
                                  //  copied_bytes, slop_bytes, frag_bytes,
                                  //  par_n_threads,
                                  //  par_max_copied, par_tot_copied)
        eventTypes[t].size = sizeof(EventCapsetID)
                           + sizeof(StgWord16)
                           + sizeof(StgWord64) * 3
                           + sizeof(StgWord32)
                           + sizeof(StgWord64) * 2;
        break;

    case EVENT_TASK_CREATE:   // (taskId, cap, tid)
        eventTypes[t].size =
            sizeof(EventTaskId) + sizeof(EventCapNo) + sizeof(EventKernelThreadId);
        break;

    case EVENT_TASK_MIGRATE:   // (taskId, cap, new_cap)
        eventTypes[t].size =
            sizeof(EventTaskId) + sizeof(EventCapNo) * 2;
        break;

    case EVENT_TASK_DELETE:   // (taskId)
        eventTypes[t].size = sizeof(EventTaskId);
        break;

    case EVENT_BLOCK_MARKER:
        eventTypes[t].size = sizeof(StgWord32) + sizeof(EventTimestamp) +
            sizeof(EventCapNo);
        break;

    case EVENT_HACK_BUG_T9003:
        eventTypes[t].size = 0;
        break;

    case EVENT_CAP_ALLOC:
        eventTypes[t].size = sizeof(StgWord64) * 3;
        break;

    case EVENT_CAP_VALUE:
        eventTypes[t].size = sizeof(StgWord8) + sizeof(StgWord64);
        break;

    case EVENT_TASK_ACQUIRE_CAP:
    case EVENT_TASK_RELEASE_CAP:
        eventTypes[t].size = sizeof(EventTaskId);
        break;

    case EVENT_TASK_RETURN_CAP:
        eventTypes[t].size = sizeof(EventTaskId) + sizeof(EventCapNo);
        break;

    default:
        return 0; /* ignore deprecated events */
    }

    return 1;
}

#define BUF 512

void
initEventLogging(void)
{
    StgWord8 t, c;
    nat n_caps;
    char *prog;

    prog = stgMallocBytes(strlen(prog_name) + 1, "initEventLogging");
    strcpy(prog, prog_name);
#ifdef mingw32_HOST_OS
    // on Windows, drop the .exe suffix if there is one
    {
        char *suff;
        suff = strrchr(prog,'.');
        if (suff != NULL && !strcmp(suff,".exe")) {
            *suff = '\0';
        }
    }
#endif

    event_log_filename = stgMallocBytes(strlen(prog)
                                        + 10 /* .%d */
                                        + 10 /* .eventlog */,
                                        "initEventLogging");

    if (sizeof(EventDesc) / sizeof(char*) != NUM_GHC_EVENT_TAGS) {
        barf("EventDesc array has the wrong number of elements");
    }

    if (event_log_pid == -1) { // #4512
        // Single process
        sprintf(event_log_filename, "%s.eventlog", prog);
        event_log_pid = getpid();
    } else {
        // Forked process, eventlog already started by the parent
        // before fork
        event_log_pid = getpid();
        // We don't have a FMT* symbol for pid_t, so we go via Word64
        // to be sure of not losing range. It would be nicer to have a
        // FMT* symbol or similar, though.
        sprintf(event_log_filename, "%s.%" FMT_Word64 ".eventlog", prog, (StgWord64)event_log_pid);
    }
    stgFree(prog);

    /* Open event log file for writing. */
    if ((event_log_file = fopen(event_log_filename, "wb")) == NULL) {
        sysErrorBelch("initEventLogging: can't open %s", event_log_filename);
        stg_exit(EXIT_FAILURE);
    }

    /*
     * Allocate buffer(s) to store events.
     * Create buffer large enough for the header begin marker, all event
     * types, and header end marker to prevent checking if buffer has room
     * for each of these steps, and remove the need to flush the buffer to
     * disk during initialization.
     *
     * Use a single buffer to store the header with event types, then flush
     * the buffer so all buffers are empty for writing events.
     */
#ifdef THREADED_RTS
    // XXX n_capabilities hasn't been initislised yet
    n_caps = RtsFlags.ParFlags.nNodes;
#else
    n_caps = 1;
#endif
    moreCapEventBufs(0,n_caps);

    initEventsBuf(&eventBuf, EVENT_LOG_SIZE, (EventCapNo)(-1));

    // Write in buffer: the header begin marker.
    postInt32(&eventBuf, EVENT_HEADER_BEGIN);

    // Mark beginning of event types in the header.
    postInt32(&eventBuf, EVENT_HET_BEGIN);
    for (t = 0; t < NUM_GHC_EVENT_TAGS; ++t) {
        if (initEventType(t) == 0)
            continue;;

        // Write in buffer: the start event type.
        postEventType(&eventBuf, &eventTypes[t]);
    }

    // Mark end of event types in the header.
    postInt32(&eventBuf, EVENT_HET_END);
    
    // Write in buffer: the header end marker.
    postInt32(&eventBuf, EVENT_HEADER_END);
    
    // Prepare event buffer for events (data).
    postInt32(&eventBuf, EVENT_DATA_BEGIN);

    // Flush capEventBuf with header.
    /*
     * Flush header and data begin marker to the file, thus preparing the
     * file to have events written to it.
     */
    printAndClearEventBuf(&eventBuf);

    for (c = 0; c < n_caps; ++c) {
        postBlockMarker(&capEventBuf[c]);
    }

#ifdef THREADED_RTS
    initMutex(&eventBufMutex);
#endif
}

void
endEventLogging(void)
{
    nat c;

    // Flush all events remaining in the buffers.
    for (c = 0; c < n_capabilities; ++c) {
        printAndClearEventBuf(&capEventBuf[c]);
    }
    printAndClearEventBuf(&eventBuf);
    resetEventsBuf(&eventBuf); // we don't want the block marker

    // Mark end of events (data).
    postEventTypeNum(&eventBuf, EVENT_DATA_END);

    // Flush the end of data marker.
    printAndClearEventBuf(&eventBuf);

    if (event_log_file != NULL) {
        fclose(event_log_file);
    }
}

void
moreCapEventBufs (nat from, nat to)
{
    nat c;

    if (from > 0) {
        capEventBuf = stgReallocBytes(capEventBuf, to * sizeof(EventsBuf),
                                      "moreCapEventBufs");
    } else {
        capEventBuf = stgMallocBytes(to * sizeof(EventsBuf),
                                     "moreCapEventBufs");
    }

    for (c = from; c < to; ++c) {
        initEventsBuf(&capEventBuf[c], EVENT_LOG_SIZE, c);
    }
}


void
freeEventLogging(void)
{
    StgWord8 c;
    
    // Free events buffer.
    if (eventBuf.begin != NULL) {
        stgFree(eventBuf.begin);
    }
    for (c = 0; c < n_capabilities; ++c) {
        if (capEventBuf[c].begin != NULL) 
            stgFree(capEventBuf[c].begin);
    }
    if (capEventBuf != NULL)  {
        stgFree(capEventBuf);
    }
    if (event_log_filename != NULL) {
        stgFree(event_log_filename);
    }
}

void 
flushEventLog(void)
{
    if (event_log_file != NULL) {
        fflush(event_log_file);
    }
}

void 
abortEventLogging(void)
{
    freeEventLogging();
    if (event_log_file != NULL) {
        fclose(event_log_file);
    }
}
/*
 * Post an event message to the capability's eventlog buffer.
 * If the buffer is full, prints out the buffer and clears it.
 */
void
postSchedEvent (Capability *cap, 
                EventTypeNum tag, 
                StgThreadID thread, 
                StgWord info1,
                StgWord info2)
{
    EventsBuf *eb;

    eb = &capEventBuf[cap->no];

    if (!hasRoomForEvent(eb, tag)) {
        // Flush event buffer to make room for new event.
        printAndClearEventBuf(eb);
    }
    
    postEventHeader(eb, tag);

    switch (tag) {
    case EVENT_CREATE_THREAD:   // (cap, thread)
    case EVENT_RUN_THREAD:      // (cap, thread)
    case EVENT_THREAD_RUNNABLE: // (cap, thread)
    {
        postThreadID(eb,thread);
        break;
    }

    case EVENT_CREATE_SPARK_THREAD: // (cap, spark_thread)
    {
        postThreadID(eb,info1 /* spark_thread */);
        break;
    }

    case EVENT_MIGRATE_THREAD:  // (cap, thread, new_cap)
    case EVENT_THREAD_WAKEUP:   // (cap, thread, other_cap)
    {
        postThreadID(eb,thread);
        postCapNo(eb,info1 /* new_cap | victim_cap | other_cap */);
        break;
   }

    case EVENT_STOP_THREAD:     // (cap, thread, status)
    {
        postThreadID(eb,thread);
        postWord16(eb,info1 /* status */);
        postThreadID(eb,info2 /* blocked on thread */);
        break;
    }

    default:
        barf("postSchedEvent: unknown event tag %d", tag);
    }
}

void
postSparkEvent (Capability *cap,
                EventTypeNum tag,
                StgWord info1)
{
    EventsBuf *eb;

    eb = &capEventBuf[cap->no];

    if (!hasRoomForEvent(eb, tag)) {
        // Flush event buffer to make room for new event.
        printAndClearEventBuf(eb);
    }

    postEventHeader(eb, tag);

    switch (tag) {
    case EVENT_CREATE_SPARK_THREAD: // (cap, spark_thread)
    {
        postThreadID(eb,info1 /* spark_thread */);
        break;
    }

    case EVENT_SPARK_STEAL:         // (cap, victim_cap)
    {
        postCapNo(eb,info1 /* victim_cap */);
        break;
   }

    case EVENT_SPARK_CREATE:        // (cap)
    case EVENT_SPARK_DUD:           // (cap)
    case EVENT_SPARK_OVERFLOW:      // (cap)
    case EVENT_SPARK_RUN:           // (cap)
    case EVENT_SPARK_FIZZLE:        // (cap)
    case EVENT_SPARK_GC:            // (cap)
    {
        break;
    }

    default:
        barf("postSparkEvent: unknown event tag %d", tag);
    }
}

void
postSparkCountersEvent (Capability *cap, 
                        SparkCounters counters,
                        StgWord remaining)
{
    EventsBuf *eb;

    eb = &capEventBuf[cap->no];

    if (!hasRoomForEvent(eb, EVENT_SPARK_COUNTERS)) {
        // Flush event buffer to make room for new event.
        printAndClearEventBuf(eb);
    }
    
    postEventHeader(eb, EVENT_SPARK_COUNTERS);
    /* EVENT_SPARK_COUNTERS (crt,dud,ovf,cnv,gcd,fiz,rem) */
    postWord64(eb,counters.created);
    postWord64(eb,counters.dud);
    postWord64(eb,counters.overflowed);
    postWord64(eb,counters.converted);
    postWord64(eb,counters.gcd);
    postWord64(eb,counters.fizzled);
    postWord64(eb,remaining);
}

void
postCapEvent (EventTypeNum  tag,
              EventCapNo    capno)
{
    ACQUIRE_LOCK(&eventBufMutex);

    if (!hasRoomForEvent(&eventBuf, tag)) {
        // Flush event buffer to make room for new event.
        printAndClearEventBuf(&eventBuf);
    }
    
    postEventHeader(&eventBuf, tag);

    switch (tag) {
    case EVENT_CAP_CREATE:   // (cap)
    case EVENT_CAP_DELETE:   // (cap)
    case EVENT_CAP_ENABLE:   // (cap)
    case EVENT_CAP_DISABLE:  // (cap)
    {
        postCapNo(&eventBuf,capno);
        break;
    }

    default:
        barf("postCapEvent: unknown event tag %d", tag);
    }

    RELEASE_LOCK(&eventBufMutex);
}

void postCapsetEvent (EventTypeNum tag,
                      EventCapsetID capset,
                      StgWord info)
{
    ACQUIRE_LOCK(&eventBufMutex);

    if (!hasRoomForEvent(&eventBuf, tag)) {
        // Flush event buffer to make room for new event.
        printAndClearEventBuf(&eventBuf);
    }

    postEventHeader(&eventBuf, tag);
    postCapsetID(&eventBuf, capset);

    switch (tag) {
    case EVENT_CAPSET_CREATE:   // (capset, capset_type)
    {
        postCapsetType(&eventBuf, info /* capset_type */);
        break;
    }

    case EVENT_CAPSET_DELETE:   // (capset)
    {
        break;
    }

    case EVENT_CAPSET_ASSIGN_CAP:  // (capset, capno)
    case EVENT_CAPSET_REMOVE_CAP:  // (capset, capno)
    {
        postCapNo(&eventBuf, info /* capno */);
        break;
    }
    case EVENT_OSPROCESS_PID:   // (capset, pid)
    case EVENT_OSPROCESS_PPID:  // (capset, parent_pid)
    {
        postOSProcessId(&eventBuf, info);
        break;
    }
    default:
        barf("postCapsetEvent: unknown event tag %d", tag);
    }

    RELEASE_LOCK(&eventBufMutex);
}

void postCapsetStrEvent (EventTypeNum tag,
                         EventCapsetID capset,
                         const char *msg)
{
    int strsize = strlen(msg);
    int size = strsize + sizeof(EventCapsetID);

    ACQUIRE_LOCK(&eventBufMutex);

    if (!hasRoomForVariableEvent(&eventBuf, size)){
        printAndClearEventBuf(&eventBuf);

        if (!hasRoomForVariableEvent(&eventBuf, size)){
            // Event size exceeds buffer size, bail out:
            RELEASE_LOCK(&eventBufMutex);
            return;
        }
    }

    postEventHeader(&eventBuf, tag);
    postPayloadSize(&eventBuf, size);
    postCapsetID(&eventBuf, capset);

    postBuf(&eventBuf, (StgWord8*) msg, strsize);

    RELEASE_LOCK(&eventBufMutex);
}

void postCapsetVecEvent (EventTypeNum tag,
                         EventCapsetID capset,
                         int argc,
                         const char *argv[])
{
    int i, size = sizeof(EventCapsetID);

    for (i = 0; i < argc; i++) {
        // 1 + strlen to account for the trailing \0, used as separator
        size += 1 + strlen(argv[i]);
    }

    ACQUIRE_LOCK(&eventBufMutex);

    if (!hasRoomForVariableEvent(&eventBuf, size)){
        printAndClearEventBuf(&eventBuf);

        if(!hasRoomForVariableEvent(&eventBuf, size)){
            // Event size exceeds buffer size, bail out:
            RELEASE_LOCK(&eventBufMutex);
            return;
        }
    }

    postEventHeader(&eventBuf, tag);
    postPayloadSize(&eventBuf, size);
    postCapsetID(&eventBuf, capset);

    for( i = 0; i < argc; i++ ) {
        // again, 1 + to account for \0
        postBuf(&eventBuf, (StgWord8*) argv[i], 1 + strlen(argv[i]));
    }

    RELEASE_LOCK(&eventBufMutex);
}

void postWallClockTime (EventCapsetID capset)
{
    StgWord64 ts;
    StgWord64 sec;
    StgWord32 nsec;

    ACQUIRE_LOCK(&eventBufMutex);
    
    /* The EVENT_WALL_CLOCK_TIME event is intended to allow programs
       reading the eventlog to match up the event timestamps with wall
       clock time. The normal event timestamps measure time since the
       start of the program. To align eventlogs from concurrent
       processes we need to be able to match up the timestamps. One way
       to do this is if we know how the timestamps and wall clock time
       match up (and of course if both processes have sufficiently
       synchronised clocks).

       So we want to make sure that the timestamp that we generate for
       this event matches up very closely with the wall clock time.
       Unfortunately we currently have to use two different APIs to get
       the elapsed time vs the wall clock time. So to minimise the
       difference we just call them very close together.
     */
    
    getUnixEpochTime(&sec, &nsec);  /* Get the wall clock time */
    ts = time_ns();                 /* Get the eventlog timestamp */

    if (!hasRoomForEvent(&eventBuf, EVENT_WALL_CLOCK_TIME)) {
        // Flush event buffer to make room for new event.
        printAndClearEventBuf(&eventBuf);
    }

    /* Normally we'd call postEventHeader(), but that generates its own
       timestamp, so we go one level lower so we can write out the
       timestamp we already generated above. */
    postEventTypeNum(&eventBuf, EVENT_WALL_CLOCK_TIME);
    postWord64(&eventBuf, ts);
    
    /* EVENT_WALL_CLOCK_TIME (capset, unix_epoch_seconds, nanoseconds) */
    postCapsetID(&eventBuf, capset);
    postWord64(&eventBuf, sec);
    postWord32(&eventBuf, nsec);

    RELEASE_LOCK(&eventBufMutex);
}

/*
 * Various GC and heap events
 */
void postHeapEvent (Capability    *cap,
                    EventTypeNum   tag,
                    EventCapsetID  heap_capset,
                    W_           info1)
{
    EventsBuf *eb;

    eb = &capEventBuf[cap->no];

    if (!hasRoomForEvent(eb, tag)) {
        // Flush event buffer to make room for new event.
        printAndClearEventBuf(eb);
    }
    
    postEventHeader(eb, tag);

    switch (tag) {
    case EVENT_HEAP_ALLOCATED:     // (heap_capset, alloc_bytes)
    case EVENT_HEAP_SIZE:          // (heap_capset, size_bytes)
    case EVENT_HEAP_LIVE:          // (heap_capset, live_bytes)
    {
        postCapsetID(eb, heap_capset);
        postWord64(eb, info1 /* alloc/size/live_bytes */);
        break;
    }

    default:
        barf("postHeapEvent: unknown event tag %d", tag);
    }
}

void postEventHeapInfo (EventCapsetID heap_capset,
                        nat           gens,
                        W_          maxHeapSize,
                        W_          allocAreaSize,
                        W_          mblockSize,
                        W_          blockSize)
{
    ACQUIRE_LOCK(&eventBufMutex);

    if (!hasRoomForEvent(&eventBuf, EVENT_HEAP_INFO_GHC)) {
        // Flush event buffer to make room for new event.
        printAndClearEventBuf(&eventBuf);
    }

    postEventHeader(&eventBuf, EVENT_HEAP_INFO_GHC);
    /* EVENT_HEAP_INFO_GHC (heap_capset, n_generations,
                            max_heap_size, alloc_area_size,
                            mblock_size, block_size) */
    postCapsetID(&eventBuf, heap_capset);
    postWord16(&eventBuf, gens);
    postWord64(&eventBuf, maxHeapSize);
    postWord64(&eventBuf, allocAreaSize);
    postWord64(&eventBuf, mblockSize);
    postWord64(&eventBuf, blockSize);

    RELEASE_LOCK(&eventBufMutex);
}

void postEventGcStats  (Capability    *cap,
                        EventCapsetID  heap_capset,
                        nat            gen,
                        W_           copied,
                        W_           slop,
                        W_           fragmentation,
                        nat            par_n_threads,
                        W_           par_max_copied,
                        W_           par_tot_copied)
{
    EventsBuf *eb;

    eb = &capEventBuf[cap->no];

    if (!hasRoomForEvent(eb, EVENT_GC_STATS_GHC)) {
        // Flush event buffer to make room for new event.
        printAndClearEventBuf(eb);
    }
    
    postEventHeader(eb, EVENT_GC_STATS_GHC);
    /* EVENT_GC_STATS_GHC (heap_capset, generation,
                           copied_bytes, slop_bytes, frag_bytes,
                           par_n_threads, par_max_copied, par_tot_copied) */
    postCapsetID(eb, heap_capset);
    postWord16(eb, gen);
    postWord64(eb, copied);
    postWord64(eb, slop);
    postWord64(eb, fragmentation);
    postWord32(eb, par_n_threads);
    postWord64(eb, par_max_copied);
    postWord64(eb, par_tot_copied);
}

void postTaskCreateEvent (Capability *cap,
                          EventTaskId taskId,
                          EventCapNo capno,
                          EventKernelThreadId tid)
{
    EventsBuf *eb;

    eb = &capEventBuf[cap->no];

    if (!hasRoomForEvent(eb, EVENT_TASK_CREATE)) {
        // Flush event buffer to make room for new event.
        printAndClearEventBuf(eb);
    }

    postEventHeader(eb, EVENT_TASK_CREATE);
    /* EVENT_TASK_CREATE (taskID, cap, tid) */
    postTaskId(eb, taskId);
    postCapNo(eb, capno);
    postKernelThreadId(eb, tid);
}

void postTaskMigrateEvent (EventTaskId taskId,
                           EventCapNo capno,
                           EventCapNo new_capno)
{
    ACQUIRE_LOCK(&eventBufMutex);

    if (!hasRoomForEvent(&eventBuf, EVENT_TASK_MIGRATE)) {
        // Flush event buffer to make room for new event.
        printAndClearEventBuf(&eventBuf);
    }

    postEventHeader(&eventBuf, EVENT_TASK_MIGRATE);
    /* EVENT_TASK_MIGRATE (taskID, cap, new_cap) */
    postTaskId(&eventBuf, taskId);
    postCapNo(&eventBuf, capno);
    postCapNo(&eventBuf, new_capno);

    RELEASE_LOCK(&eventBufMutex);
}

void postTaskDeleteEvent (Capability *cap, EventTaskId taskId)
{
    EventsBuf *eb;

    eb = &capEventBuf[cap->no];

    if (!hasRoomForEvent(eb, EVENT_TASK_DELETE)) {
        // Flush event buffer to make room for new event.
        printAndClearEventBuf(eb);
    }

    postEventHeader(eb, EVENT_TASK_DELETE);
    /* EVENT_TASK_DELETE (taskID) */
    postTaskId(eb, taskId);
}

void
postEvent (Capability *cap, EventTypeNum tag)
{
    EventsBuf *eb;

    eb = &capEventBuf[cap->no];

    if (!hasRoomForEvent(eb, tag)) {
        // Flush event buffer to make room for new event.
        printAndClearEventBuf(eb);
    }

    postEventHeader(eb, tag);
}

void
postEventAtTimestamp (Capability *cap, EventTimestamp ts, EventTypeNum tag)
{
    EventsBuf *eb;

    eb = &capEventBuf[cap->no];

    if (!hasRoomForEvent(eb, tag)) {
        // Flush event buffer to make room for new event.
        printAndClearEventBuf(eb);
    }

    /* Normally we'd call postEventHeader(), but that generates its own
       timestamp, so we go one level lower so we can write out
       the timestamp we received as an argument. */
    postEventTypeNum(eb, tag);
    postWord64(eb, ts);
}

void postLogMsg(EventsBuf *eb, EventTypeNum type, const char *msg, va_list ap)
{
    char buf[BUF];
    nat size;

    size = vsnprintf(buf,BUF,msg,ap);
    // FIXME-H: possible bug, size >= BUF?, should not overwrite BUF-1 anyway
    if (size > BUF) {
        buf[BUF-1] = '\0';
        size = BUF;
    }

    if (!hasRoomForVariableEvent(eb, size)) {
        // Flush event buffer to make room for new event.
        printAndClearEventBuf(eb);
    }

    postEventHeader(eb, type);
    postPayloadSize(eb, size);
    postBuf(eb,(StgWord8*)buf,size);
}

void postMsg(const char *msg, va_list ap)
{
    ACQUIRE_LOCK(&eventBufMutex);
    postLogMsg(&eventBuf, EVENT_LOG_MSG, msg, ap);
    RELEASE_LOCK(&eventBufMutex);
}

void postCapMsg(Capability *cap, const char *msg, va_list ap)
{
    postLogMsg(&capEventBuf[cap->no], EVENT_LOG_MSG, msg, ap);
}

void postUserMsg(Capability *cap, const char *msg, va_list ap)
{
    postLogMsg(&capEventBuf[cap->no], EVENT_USER_MSG, msg, ap);
}    

void postEventStartup(EventCapNo n_caps)
{
    ACQUIRE_LOCK(&eventBufMutex);

    if (!hasRoomForEvent(&eventBuf, EVENT_STARTUP)) {
        // Flush event buffer to make room for new event.
        printAndClearEventBuf(&eventBuf);
    }

    // Post a STARTUP event with the number of capabilities
    postEventHeader(&eventBuf, EVENT_STARTUP);
    postCapNo(&eventBuf, n_caps);

    RELEASE_LOCK(&eventBufMutex);
}

void postUserMarker(Capability *cap, const char *markername)
{
    EventsBuf *eb;
    int size = strlen(markername);

    eb = &capEventBuf[cap->no];

    if (!hasRoomForVariableEvent(eb, size)){
        printAndClearEventBuf(eb);

        if (!hasRoomForVariableEvent(eb, size)){
            // Event size exceeds buffer size, bail out:
            return;
        }
    }

    postEventHeader(eb, EVENT_USER_MARKER);
    postPayloadSize(eb, size);
    postBuf(eb, (StgWord8*) markername, size);
}

void postThreadLabel(Capability    *cap,
                     EventThreadID  id,
                     const char    *label)
{
    EventsBuf *eb;
    int strsize = strlen(label);
    int size = strsize + sizeof(EventCapsetID);

    eb = &capEventBuf[cap->no];

    if (!hasRoomForVariableEvent(eb, size)){
        printAndClearEventBuf(eb);

        if (!hasRoomForVariableEvent(eb, size)){
            // Event size exceeds buffer size, bail out:
            return;
        }
    }

    postEventHeader(eb, EVENT_THREAD_LABEL);
    postPayloadSize(eb, size);
    postThreadID(eb, id);
    postBuf(eb, (StgWord8*) label, strsize);
}

void postCapAllocEvent(Capability *cap,
                       W_          alloc,
                       W_          blocks,
                       W_          hp_alloc)
{
    EventsBuf *eb;

    eb = &capEventBuf[cap->no];

    if (!hasRoomForEvent(eb, EVENT_CAP_ALLOC)) {
        printAndClearEventBuf(eb);
    }

    postEventHeader(eb, EVENT_CAP_ALLOC);

    postWord64(eb, alloc);
    postWord64(eb, blocks);
    postWord64(eb, hp_alloc);
}

void postCapValueEvent(Capability *cap,
                       nat         tag,
                       W_          value)
{
    EventsBuf *eb;

    eb = &capEventBuf[cap->no];

    if (!hasRoomForEvent(eb, EVENT_CAP_VALUE)) {
        printAndClearEventBuf(eb);
    }

    postEventHeader(eb, EVENT_CAP_VALUE);

    postWord8(eb, tag);
    postWord64(eb, value);
}

void postTaskAcquireCapEvent(Capability *cap, EventTaskId taskId)
{
    EventsBuf *eb;

    eb = &capEventBuf[cap->no];

    if (!hasRoomForEvent(eb, EVENT_TASK_ACQUIRE_CAP)) {
        printAndClearEventBuf(eb);
    }

    postEventHeader(eb, EVENT_TASK_ACQUIRE_CAP);
    postTaskId(eb, taskId);
}

void postTaskReleaseCapEvent(Capability *cap, EventTaskId taskId)
{
    EventsBuf *eb;

    eb = &capEventBuf[cap->no];

    if (!hasRoomForEvent(eb, EVENT_TASK_RELEASE_CAP)) {
        printAndClearEventBuf(eb);
    }

    postEventHeader(eb, EVENT_TASK_RELEASE_CAP);
    postTaskId(eb, taskId);
}

void postTaskReturnCapEvent(EventTaskId taskId, EventCapNo capno)
{
    ACQUIRE_LOCK(&eventBufMutex);

    if (!hasRoomForEvent(&eventBuf, EVENT_TASK_RETURN_CAP)) {
        // Flush event buffer to make room for new event.
        printAndClearEventBuf(&eventBuf);
    }

    postEventHeader(&eventBuf, EVENT_TASK_RETURN_CAP);
    postTaskId(&eventBuf, taskId);
    postCapNo(&eventBuf, capno);

    RELEASE_LOCK(&eventBufMutex);
}

void closeBlockMarker (EventsBuf *ebuf)
{
    StgInt8* save_pos;

    if (ebuf->marker)
    {
        // (type:16, time:64, size:32, end_time:64)

        save_pos = ebuf->pos;
        ebuf->pos = ebuf->marker + sizeof(EventTypeNum) +
                    sizeof(EventTimestamp);
        postWord32(ebuf, save_pos - ebuf->marker);
        postTimestamp(ebuf);
        ebuf->pos = save_pos;
        ebuf->marker = NULL;
    }
}


void postBlockMarker (EventsBuf *eb)
{
    if (!hasRoomForEvent(eb, EVENT_BLOCK_MARKER)) {
        printAndClearEventBuf(eb);
    }

    closeBlockMarker(eb);

    eb->marker = eb->pos;
    postEventHeader(eb, EVENT_BLOCK_MARKER);
    postWord32(eb,0); // these get filled in later by closeBlockMarker();
    postWord64(eb,0);
    postCapNo(eb, eb->capno);
}

void printAndClearEventBuf (EventsBuf *ebuf)
{
    StgWord64 numBytes = 0, written = 0;

    closeBlockMarker(ebuf);

    if (ebuf->begin != NULL && ebuf->pos != ebuf->begin)
    {
        numBytes = ebuf->pos - ebuf->begin;
        
        written = fwrite(ebuf->begin, 1, numBytes, event_log_file);
        if (written != numBytes) {
            debugBelch(
                "printAndClearEventLog: fwrite() failed, written=%" FMT_Word64
                " doesn't match numBytes=%" FMT_Word64, written, numBytes);
            return;
        }
        
        resetEventsBuf(ebuf);
        flushCount++;

        postBlockMarker(ebuf);
    }
}

void initEventsBuf(EventsBuf* eb, StgWord64 size, EventCapNo capno)
{
    eb->begin = eb->pos = stgMallocBytes(size, "initEventsBuf");
    eb->size = size;
    eb->marker = NULL;
    eb->capno = capno;
}

void resetEventsBuf(EventsBuf* eb)
{
    eb->pos = eb->begin;
    eb->marker = NULL;
}

StgBool hasRoomForEvent(EventsBuf *eb, EventTypeNum eNum)
{
  nat size;

  size = sizeof(EventTypeNum) + sizeof(EventTimestamp) + eventTypes[eNum].size;

  if (eb->pos + size > eb->begin + eb->size) {
      return 0; // Not enough space.
  } else  {
      return 1; // Buf has enough space for the event.
  }
}

StgBool hasRoomForVariableEvent(EventsBuf *eb, nat payload_bytes)
{
  nat size;

  size = sizeof(EventTypeNum) + sizeof(EventTimestamp) +
      sizeof(EventPayloadSize) + payload_bytes;

  if (eb->pos + size > eb->begin + eb->size) {
      return 0; // Not enough space.
  } else  {
      return 1; // Buf has enough space for the event.
  }
}    

void postEventType(EventsBuf *eb, EventType *et)
{
    StgWord8 d;
    nat desclen;

    postInt32(eb, EVENT_ET_BEGIN);
    postEventTypeNum(eb, et->etNum);
    postWord16(eb, (StgWord16)et->size);
    desclen = strlen(et->desc);
    postWord32(eb, desclen);
    for (d = 0; d < desclen; ++d) {
        postInt8(eb, (StgInt8)et->desc[d]);
    }
    postWord32(eb, 0); // no extensions yet
    postInt32(eb, EVENT_ET_END);
}

#ifdef REPLAY
void
initEventLoggingReplay(void)
{
    StgInt32 i32;
    nat end;

    eventLogFilenameReplay = stgMallocBytes(strlen(prog_name)
                                            + 8  /* .replay */,
                                            "initEventLoggingReplay");

    // TODO: make defines to struct and use ELEMENTSOF() macro
    if (sizeof(EventDesc) / sizeof(char*) != NUM_GHC_EVENT_TAGS) {
        barf("EventDesc array has the wrong number of elements");
    }

    // TODO: what for forked processes
    sprintf(eventLogFilenameReplay, "%s.replay", prog_name);

    /* Open event log file for reading. */
    if ((fileReplay = fopen(eventLogFilenameReplay, "rb")) == NULL) {
        sysErrorBelch("initEventLoggingReplay: can't open %s", eventLogFilenameReplay);
        stg_exit(EXIT_FAILURE);
    }

    // getHeader

    readInt32(NULL, &i32);
    if (i32 != EVENT_HEADER_BEGIN)
        barf("Header begin marker not found");

    readInt32(NULL, &i32);
    if (i32 != EVENT_HET_BEGIN)
        barf("Header Event Type begin marker not found");;

    // read event types
    end = 1;
    while (end) {
        readInt32(NULL, &i32);
        switch (i32) {
        case EVENT_ET_BEGIN:
            readEventType();
            break;
        case EVENT_HET_END:
            end = 0;
            break;
        default:
            barf("Malformed list of Event Types in header");
        }
    }

    readInt32(NULL, &i32);
    if (i32 != EVENT_HEADER_END)
        barf("Header end marker not found");

    readInt32(NULL, &i32);
    if (i32 != EVENT_DATA_BEGIN)
        barf("Data begin marker not found");

    fileReplayStartSeek = ftell(fileReplay);
    // args have not been parsed yet
    moreCapEventBufsReplay(0, 1);

    initReplayBuf(&replayBuf, -1, fileReplay);
}

void
endEventLoggingReplay(void)
{
    nat c;

    if (fileReplay != NULL) {
        fclose(fileReplay);
    }

    for (c = 0; c < n_capabilities; c++) {
        if (capReplayBuf[c].f != NULL) {
            fclose(capReplayBuf[c].f);
        }
    }

    if (replayBuf.eb.begin != NULL) {
        stgFree(replayBuf.eb.begin);
    }

    for (c = 0; c < n_capabilities; ++c) {
        if (capReplayBuf[c].eb.begin != NULL)
            stgFree(capReplayBuf[c].eb.begin);
    }

    if (capReplayBuf != NULL)  {
        stgFree(capReplayBuf);
    }

    if (eventLogFilenameReplay != NULL) {
        stgFree(eventLogFilenameReplay);
    }

    ASSERT(eventList == NULL);
}

void
moreCapEventBufsReplay(nat from, nat to)
{
    nat c;
    FILE *f;

    if (from > 0) {
        capReplayBuf = stgReallocBytes(capReplayBuf,
                                       to * sizeof(ReplayBuf),
                                       "moreCapReplayBuf");
    } else {
        capReplayBuf = stgMallocBytes(to * sizeof(ReplayBuf),
                                      "moreCapReplayBuf");
    }

    for (c = from; c < to; ++c) {
        // Duplicate fd after reading event log header
        if ((f = fopen(eventLogFilenameReplay, "rb")) == NULL) {
            sysErrorBelch("moreCapReplayBuf: can't open %s", eventLogFilenameReplay);
            stg_exit(EXIT_FAILURE);
        }
        fseek(f, fileReplayStartSeek, SEEK_SET);
        initReplayBuf(&capReplayBuf[c], c, f);
    }
}

static void
initReplayBuf(ReplayBuf *rb, EventCapNo capno, FILE *f)
{
    initEventsBuf(&rb->eb, EVENT_LOG_SIZE, capno);
    rb->eb.pos = rb->eb.marker = rb->eb.begin;
    rb->f = f;
    rb->ce = NULL;
    rb->ceSize = 0;
}

static void
resetReplayBuf(ReplayBuf *rb)
{
    rb->eb.pos = rb->eb.marker = rb->eb.begin;
    rb->ce = NULL;
    rb->ceSize = 0;
}

static void
fillReplayBuf(ReplayBuf *rb)
{
    nat size, skip;
    StgWord32 block_size;
    EventsBuf *eb;
    EventTypeNum tag;
    EventCapNo capno;

    ASSERT(rb->f != NULL);

    resetReplayBuf(rb);

    eb = &rb->eb;
    while (1) {
        // Header
        readEventTypeNum(rb, &tag);
        if (tag == EVENT_DATA_END) {
            if (!readEof(rb)) {
                // TODO-H: debug only
                barf("Should have found EOF while reading '%s'", eventLogFilenameReplay);
            }
            fclose(rb->f);
            rb->f = NULL;
            if (eb->capno == (EventCapNo)-1) {
                fileReplay = NULL;
            }
            return;
        }

        // Event block
        ASSERTM(tag == EVENT_BLOCK_MARKER,
                "Block marker not found. Got tag %d (%s)", tag, EventDesc[tag]);
        skipSize(rb, sizeof(EventTimestamp));
        readWord32(rb, &block_size);
        skipSize(rb, sizeof(EventTimestamp));
        readCapNo(rb, &capno);
        skip = sizeof(EventTypeNum) + sizeof(EventTimestamp) +
               sizeof(StgWord32) + sizeof(EventTimestamp) +
               sizeof(EventCapNo);
        size = block_size - skip;
        if (capno == eb->capno) {
            ASSERT(size <= eb->size);
            readBuf(rb, (StgWord8 *)eb->begin, size);
            eb->marker = eb->begin + size;
            break;
        } else {
            // skip other capabilities blocks
            skipSize(rb, size);
        }
    }
}

static void
readEventType(void)
{
    nat d;
    EventTypeNum etNum;
    StgWord16 size;
    StgWord32 desclen, extralen;
    StgInt32 i32;

    readEventTypeNum(NULL, &etNum);
    readWord16(NULL, (StgWord16 *)&size);
    readWord32(NULL, &desclen);

    for (d = 0; d < desclen; ++d) {
        // TODO: skipSize(NULL, desclen);
        StgInt8 i;
        readInt8(NULL, &i);
    }

    readWord32(NULL, &extralen); // extension point (should be 0)
    ASSERT(extralen == 0);
    for (d = 0; d < extralen; d++) {
        // TODO: skipSize(NULL, extralen);
        StgWord8 w;
        readWord8(NULL, &w);
    }

    readInt32(NULL, &i32);
    if (i32 != EVENT_ET_END)
        barf("Event Type end marker not found");

    eventTypes[etNum].etNum = etNum;
    eventTypes[etNum].size = size;
    eventTypes[etNum].desc = EventDesc[etNum];
}

// Gets a new event from the buffer
static Event *
getEvent(ReplayBuf *rb)
{
    Event *ev;
    EventTypeNum tag;
    EventTimestamp ts;

    if (rb->f == NULL) {
        return NULL;
    }

    ASSERT(rb->eb.pos <= rb->eb.marker);
    if (rb->eb.pos >= rb->eb.marker) {
        fillReplayBuf(rb);
        return getEvent(rb);
    }

    getEventTypeNum(rb, &tag);
    getTimestamp(rb, &ts);

    switch (tag) {
    case EVENT_CREATE_THREAD:
    case EVENT_RUN_THREAD:
    case EVENT_THREAD_RUNNABLE:
    case EVENT_CREATE_SPARK_THREAD:
    {
        // FIXME-H: malloc
        EventThread *et = stgCallocBytes(1, sizeof(EventThread), "getEvent");
        getThreadID(rb, &et->thread);

        ev = (Event *)et;
        break;
    }
    case EVENT_STOP_THREAD:
    {
        EventStopThread *est = stgCallocBytes(1, sizeof(EventStopThread), "getEvent");
        getThreadID(rb, &est->thread);
        getThreadStatus(rb, &est->status);
        getThreadID(rb, &est->blocked_on);

        ev = (Event *)est;
        break;
    }
    case EVENT_MIGRATE_THREAD:
    case EVENT_THREAD_WAKEUP:
    {
        EventThreadCap *etc = stgCallocBytes(1, sizeof(EventThreadCap), "getEvent");
        getThreadID(rb, &etc->thread);
        getCapNo(rb, &etc->capno);

        ev = (Event *)etc;
        break;
    }
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
        ev = stgCallocBytes(1, sizeof(EventHeader), "getEvent");
        break;
    case EVENT_LOG_MSG:
    case EVENT_USER_MSG:
    case EVENT_USER_MARKER:
    {
        EventPayloadSize size;

        getPayloadSize(rb, &size);

        EventMsg *em = stgCallocBytes(1, sizeof(EventMsg) + size, "getEvent");
        em->size = size;
        getBuf(rb, (StgWord8 *)em->msg, size);

        ev = (Event *)em;
        break;
    }
    case EVENT_STARTUP:
    case EVENT_SPARK_STEAL:
    case EVENT_CAP_CREATE:
    case EVENT_CAP_DELETE:
    case EVENT_CAP_DISABLE:
    case EVENT_CAP_ENABLE:
    {
        struct _EventCapNo *ecn = stgCallocBytes(1, sizeof(struct _EventCapNo), "getEvent");
        getCapNo(rb, &ecn->capno);

        ev = (Event *)ecn;
        break;
    }
    case EVENT_BLOCK_MARKER:
    {
        barf("getEvent: EVENT_BLOCK_MARKER inside a block event");
        break;
    }
    case EVENT_CAPSET_CREATE:
    {
        EventCapsetCreate *ecc = stgCallocBytes(1, sizeof(EventCapsetCreate), "getEvent");
        getCapsetID(rb, &ecc->capset);
        getCapsetType(rb, &ecc->type);

        ev = (Event *)ecc;
        break;
    }
    case EVENT_CAPSET_DELETE:
    {
        EventCapsetDelete *ecd = stgCallocBytes(1, sizeof(EventCapsetDelete), "getEvent");
        getCapsetID(rb, &ecd->capset);

        ev = (Event *)ecd;
        break;
    }
    case EVENT_CAPSET_ASSIGN_CAP:
    case EVENT_CAPSET_REMOVE_CAP:
    {
        EventCapsetCapNo *eccn = stgCallocBytes(1, sizeof(EventCapsetCapNo), "getEvent");
        getCapsetID(rb, &eccn->capset);
        getCapNo(rb, &eccn->capno);

        ev = (Event *)eccn;
        break;
    }
    case EVENT_RTS_IDENTIFIER:
    case EVENT_PROGRAM_ARGS:
    case EVENT_PROGRAM_ENV:
    {
        EventPayloadSize size;

        getPayloadSize(rb, &size);

        EventCapsetMsg *ecm = stgCallocBytes(1, sizeof(EventMsg) + size, "getEvent");
        ecm->size = size;
        getCapsetID(rb, &ecm->capset);
        getBuf(rb, (StgWord8 *)ecm->msg, size - sizeof(EventCapsetID));

        ev = (Event *)ecm;
        break;
    }
    case EVENT_OSPROCESS_PID:
    case EVENT_OSPROCESS_PPID:
    {
        EventCapsetPid *ecp = stgCallocBytes(1, sizeof(EventCapsetPid), "getEvent");
        getCapsetID(rb, &ecp->capset);
        getWord32(rb, &ecp->pid);

        ev = (Event *)ecp;
        break;
    }
    case EVENT_SPARK_COUNTERS:
    {
        EventSparkCounters *esc = stgCallocBytes(1, sizeof(EventSparkCounters), "getEvent");
        getWord64(rb, &esc->created);
        getWord64(rb, &esc->dud);
        getWord64(rb, &esc->overflowed);
        getWord64(rb, &esc->converted);
        getWord64(rb, &esc->gcd);
        getWord64(rb, &esc->fizzled);
        getWord64(rb, &esc->remaining);

        ev = (Event *)esc;
        break;
    }
    case EVENT_WALL_CLOCK_TIME:
    {
        EventWallClockTime *ewct = stgCallocBytes(1, sizeof(EventWallClockTime), "getEvent");
        getCapsetID(rb, &ewct->capset);
        getWord64(rb, &ewct->sec);
        getWord32(rb, &ewct->nsec);

        ev = (Event *)ewct;
        break;
    }
    case EVENT_THREAD_LABEL:
    {
        EventPayloadSize size;

        getPayloadSize(rb, &size);

        EventThreadLabel *etl = stgCallocBytes(1, sizeof(EventMsg) + size, "getEvent");
        etl->size = size;
        getThreadID(rb, &etl->thread);
        getBuf(rb, (StgWord8 *)etl->label, size - sizeof(EventThreadID));

        ev = (Event *)etl;
        break;
    }
    case EVENT_HEAP_ALLOCATED:
    case EVENT_HEAP_SIZE:
    case EVENT_HEAP_LIVE:
    {
        EventCapsetBytes *ecb = stgCallocBytes(1, sizeof(EventCapsetBytes), "getEvent");
        getCapsetID(rb, &ecb->capset);
        getWord64(rb, &ecb->bytes);

        ev = (Event *)ecb;
        break;
    }
    case EVENT_HEAP_INFO_GHC:
    {
        EventHeapInfoGHC *ehig = stgCallocBytes(1, sizeof(EventHeapInfoGHC), "getEvent");
        getCapsetID(rb, &ehig->capset);
        getWord16(rb, &ehig->gens);
        getWord64(rb, &ehig->maxHeapSize);
        getWord64(rb, &ehig->allocAreaSize);
        getWord64(rb, &ehig->mblockSize);
        getWord64(rb, &ehig->blockSize);

        ev = (Event *)ehig;
        break;
    }
    case EVENT_GC_STATS_GHC:
    {
        EventGcStatsGHC *egsg = stgCallocBytes(1, sizeof(EventGcStatsGHC), "getEvent");
        getCapsetID(rb, &egsg->capset);
        getWord16(rb, &egsg->gen);
        getWord64(rb, &egsg->copied);
        getWord64(rb, &egsg->slop);
        getWord64(rb, &egsg->fragmentation);
        getWord32(rb, &egsg->par_n_threads);
        getWord64(rb, &egsg->par_max_copied);
        getWord64(rb, &egsg->par_tot_copied);

        ev = (Event *)egsg;
        break;
    }
    case EVENT_TASK_CREATE:
    {
        EventTaskCreate *etc = stgCallocBytes(1, sizeof(EventTaskCreate), "getEvent");
        getTaskId(rb, &etc->task);
        getCapNo(rb, &etc->capno);
        getKernelThreadId(rb, &etc->tid);

        ev = (Event *)etc;
        break;
    }
    case EVENT_TASK_MIGRATE:
    {
        EventTaskMigrate *etm = stgCallocBytes(1, sizeof(EventTaskMigrate), "getEvent");
        getTaskId(rb, &etm->task);
        getCapNo(rb, &etm->capno);
        getCapNo(rb, &etm->new_capno);

        ev = (Event *)etm;
        break;
    }
    case EVENT_TASK_DELETE:
    {
        EventTaskDelete *etd = stgCallocBytes(1, sizeof(EventTaskDelete), "getEvent");
        getTaskId(rb, &etd->task);

        ev = (Event *)etd;
        break;
    }
    case EVENT_CAP_ALLOC:
    {
        EventCapAlloc *eca = stgCallocBytes(1, sizeof(EventCapAlloc), "getEvent");
        getWord64(rb, &eca->alloc);
        getWord64(rb, &eca->blocks);
        getWord64(rb, &eca->hp_alloc);

        ev = (Event *)eca;
        break;
    }
    case EVENT_CAP_VALUE:
    {
        EventCapValue *ecv = stgCallocBytes(1, sizeof(EventCapValue), "getEvent");
        getWord8(rb, &ecv->tag);
        getWord64(rb, &ecv->value);

        ev = (Event *)ecv;
        break;
    }
    case EVENT_TASK_ACQUIRE_CAP:
    case EVENT_TASK_RELEASE_CAP:
    {
        EventTaskCap *etc = stgCallocBytes(1, sizeof(EventTaskCap), "getEvent");
        getTaskId(rb, &etc->task);

        ev = (Event *)etc;
        break;
    }
    case EVENT_TASK_RETURN_CAP:
    {
        EventTaskReturnCap *etrc = stgCallocBytes(1, sizeof(EventTaskReturnCap), "getEvent");
        getTaskId(rb, &etrc->task);
        getCapNo(rb, &etrc->capno);

        ev = (Event *)etrc;
        break;
    }
    default:
        barf ("getEvent: unknown event tag %d", tag);
    }

    ev->header.tag = tag;
    ev->header.time = ts;
    return ev;
}

// Returns the current event in the buffer,
// forces a read if not available.
static CapEvent *
replayBufCurrent(ReplayBuf *rb)
{
    CapEvent *ce;

    ce = NULL;
    if (rb->ce != NULL) {
        ce = rb->ce;
    } else {
        ASSERT(rb->ceSize == 0);
        ce = replayBufNext(rb);
    }

    return ce;
}

// Returns the nth event in a replay buffer
static CapEvent *
replayBufN(ReplayBuf *rb, nat n)
{
    ASSERT(rb != NULL);
    ASSERT(n < rb->ceSize);

    nat i;
    CapEvent *ce = rb->ce;
    for (i = 0; i < n; i++) {
        ASSERT(ce->next);
        ce = ce->next;
    }

    return ce;
}

// Reads a new event into the replay buffer
static CapEvent *
replayBufNext(ReplayBuf *rb)
{
    ASSERT(rb != NULL);

    Event *ev;
    CapEvent *ce;

    ce = rb->ce;
    if (ce != NULL) {
        while (ce->next != NULL) {
            ce = ce->next;
        }
    }

    ev = getEvent(rb);
    if (ev != NULL) {
        CapEvent *new;

        new = stgMallocBytes(sizeof(CapEvent), "replayBufNext");
        new->capno = rb->eb.capno;
        new->ev = ev;
        new->next = NULL;

        if (ce != NULL) {
            ce->next = new;
        } else {
            rb->ce = new;
        }
        rb->ceSize++;

        return new;
    }

    return NULL;
}

// Clears the currently cached event in its replay buffer
static CapEvent *
replayBufForward(ReplayBuf *rb)
{
    CapEvent *ce;

    ASSERT(rb->ce != NULL);
    ce = rb->ce;
    rb->ce = ce->next;
    rb->ceSize--;
    ce->next = NULL;
    return ce;
}

// Returns the nth event in eventList.
static CapEvent *
eventListN(nat n)
{
    ASSERT(eventList);
    ASSERT(n < eventListSize);

    CapEvent *ce = eventList;
    nat i = 0;
    for (i = 0; i < n; i++) {
        ASSERT(ce->next);
        ce = ce->next;
    }

    return ce;
}

// Caches a new event in the global eventList
static CapEvent *
eventListNew(CapEvent *new)
{
    CapEvent *ce;
#ifdef DEBUG
    nat n = 0;
#endif

    if (eventList == NULL) {
        ASSERT(eventListSize == 0);
        eventList = new;
    } else {
        ce = eventList;
        while (ce->next) {
            ce = ce->next;
#ifdef DEBUG
            n++;
#endif
        }
        ASSERT(n == eventListSize-1);
        ce->next = new;
    }
    eventListSize++;

    return new;
}

// Reads a new event into the global eventList
static CapEvent *
eventListNext(void)
{
    ReplayBuf *rb;
    CapEvent *ce, *other;
    nat c, n_caps;

#ifdef THREADED_RTS
    n_caps = RtsFlags.ParFlags.nNodes;
#else
    n_caps = 1;
#endif

    // check which event happened earlier
    rb = &replayBuf;
    ce = replayBufCurrent(rb);
    for (c = 0; c < n_caps; c++) {
        other = replayBufCurrent(&capReplayBuf[c]);
        if (ce == NULL ||
            (other != NULL && other->ev->header.time < ce->ev->header.time)) {
            rb = &capReplayBuf[c];
            ce = other;
        }
    }

    // we may have read all events
    if (ce == NULL) {
        ASSERT(replayBuf.f == NULL);
        for (c = 0; c < n_caps; c++) {
            ASSERT(capReplayBuf[c].f == NULL);
        }
        return NULL;
    }

    // advace the replay buffer and cache the current event in the global list
    replayBufForward(rb);
    return eventListNew(ce);
}

// Discards the first cached event in eventList.
static void
eventListForward(void)
{
    ASSERT(eventList != NULL);
    eventList = eventList->next;
    eventListSize--;
}

// Returns the global nth event
static CapEvent *
peekEvent(nat n)
{
    CapEvent *ce;

    if (eventListSize > n) {
        ce = eventListN(n);
    } else {
        while (eventListSize <= n) {
            ce = eventListNext();
        }
    }
    return ce;
}

// Returns the nth event from a specific capability
CapEvent *
peekEventCap(nat n, int capno)
{
    ReplayBuf *rb;
    CapEvent *ce;

    // search in eventList
    ce = eventList;
    while (ce != NULL) {
        if (ce->capno == capno) {
            if (n == 0) {
                return ce;
            }
            n--;
        }
        ce = ce->next;
    }

    // search in replayBuf[capno]
    if (capno == -1) {
        rb = &replayBuf;
    } else {
        rb = &capReplayBuf[capno];
    }

    if (rb->ceSize > n) {
        ce = replayBufN(rb, n);
    } else {
        while (rb->ceSize <= n) {
            ce = replayBufNext(rb);
            if (ce == NULL) {
                return NULL;
            }
        }
    }

    return ce;
}

CapEvent *
searchEventTagValueBefore(nat tag, W_ value, nat last)
{
    nat n = 0;
    CapEvent *ce;
    Event *ev;

    ASSERT(last < NUM_GHC_EVENT_TAGS);
    ev = createCapValueEvent(tag, value);
    do {
        ce = peekEvent(n++);
        if (ce == NULL || ce->ev->header.tag == last) {
            return NULL;
        }
    } while (!compareEvents(ce->ev, ev));
    return ce;
}

// Returns the current event.
CapEvent *
readEvent(void)
{
    if (eventListSize == 0) {
        return NULL;
    }
    ASSERT(eventList != NULL);
    return eventList;
}

// Read a new event from the event log (clears the last cached event and
// forces its buffer to read a new one before checking the new current one).
CapEvent *
nextEvent(void)
{
    CapEvent *ce;

    if (eventListSize > 0) {
        eventListForward();
    }

    if (eventListSize > 0) {
        ce = eventList;
    } else {
        ce = eventListNext();
    }

    return ce;
}

void
freeEvent(CapEvent *ce)
{
    stgFree(ce->ev);
    stgFree(ce);
}
#endif /* REPLAY */

#endif /* TRACING */
