/* -----------------------------------------------------------------------------
 *
 * (c) The GHC Team, 2014
 *
 * Events definition and utility functions
 *
 * -------------------------------------------------------------------------*/

#ifndef EVENT_H
#define EVENT_H

#include "BeginPrivate.h"

#include "rts/EventLogFormat.h"
#include "Sparks.h"

typedef struct _EventHeader {
    EventTypeNum   tag;
    EventTimestamp time;
} EventHeader;

typedef struct _Event {
    EventHeader header;
    void       *payload[FLEXIBLE_ARRAY];
} Event;

#ifdef REPLAY

typedef struct _CapEvent {
    EventCapNo  capno;
    Event      *ev;
    struct _CapEvent *next;
} CapEvent;

typedef struct _EventThread {
    EventHeader   header;
    EventThreadID thread;
}  EventThread;

typedef struct _EventThreadCap {
    EventHeader   header;
    EventThreadID thread;
    EventCapNo    capno;
} EventThreadCap;

typedef struct _EventMsg {
    EventHeader      header;
    EventPayloadSize size;
    StgWord8         msg[FLEXIBLE_ARRAY];
} EventMsg;

struct _EventCapNo {
    EventHeader header;
    EventCapNo  capno;
};

typedef struct _EventCapsetCapNo {
    EventHeader   header;
    EventCapsetID capset;
    EventCapNo    capno;
} EventCapsetCapNo;

typedef struct _EventCapsetMsg {
    EventHeader      header;
    EventPayloadSize size;
    EventCapsetID    capset;
    StgWord8         msg[FLEXIBLE_ARRAY];
} EventCapsetMsg;

typedef struct _EventCapsetPid {
    EventHeader   header;
    EventCapsetID capset;
    StgWord32     pid;
} EventCapsetPid;

typedef struct _EventCapsetBytes {
    EventHeader   header;
    EventCapsetID capset;
    StgWord64     bytes;
} EventCapsetBytes;

typedef struct _EventThread EventCreateThread;
typedef struct _EventThread EventRunThread;

typedef struct _EventStopThread {
    EventHeader       header;
    EventThreadID     thread;
    EventThreadStatus status;
    EventThreadID     blocked_on;
} EventStopThread;

typedef struct _EventThread EventThreadRunnable;
typedef struct _EventThreadCap EventMigrateThread;
typedef struct _EventThreadCap EventThreadWakeup;

typedef struct _Event EventGCStart;
typedef struct _Event EventGCEnd;
typedef struct _Event EventRequestSeqGC;
typedef struct _Event EventRequestParGC;

typedef struct _EventThread EventCreateSparkThread;

typedef struct _EventMsg EventLogMsg;

typedef struct _EventCapNo EventStartup;

typedef struct _EventMsg EventUserMsg;

typedef struct _Event EventGCIdle;
typedef struct _Event EventGCWork;
typedef struct _Event EventGCDone;

typedef struct _EventCapsetCreate {
    EventHeader     header;
    EventCapsetID   capset;
    EventCapsetType type;
} EventCapsetCreate;

typedef struct _EventCapsetDelete {
    EventHeader   header;
    EventCapsetID capset;
} EventCapsetDelete;

typedef struct _EventCapsetCapNo EventCapsetAssignCap;
typedef struct _EventCapsetCapNo EventCapsetRemoveCap;

typedef struct _EventCapsetMsg EventRtsIdentifier;
typedef struct _EventCapsetMsg EventProgramArgs;
typedef struct _EventCapsetMsg EventProgramEnv;

typedef struct _EventCapsetPid EventOSProcessPid;
typedef struct _EventCapsetPid EventOSProcessPPid;

typedef struct _EventSparkCounters {
    EventHeader header;
    StgWord64   created;
    StgWord64   dud;
    StgWord64   overflowed;
    StgWord64   converted;
    StgWord64   gcd;
    StgWord64   fizzled;
    StgWord64   remaining;
} EventSparkCounters;

typedef struct _Event EventSparkCreate;
typedef struct _Event EventSparkDud;
typedef struct _Event EventSparkOverflow;
typedef struct _Event EventSparkRun;

typedef struct _EventCapNo EventSparkSteal;

typedef struct _Event EventSparkFizzle;
typedef struct _Event EventSparkGc;

typedef struct _EventWallClockTime {
    EventHeader   header;
    EventCapsetID capset;
    StgWord64     sec;
    StgWord32     nsec;
} EventWallClockTime;

typedef struct _EventThreadLabel {
    EventHeader      header;
    EventPayloadSize size;
    EventThreadID    thread;
    StgWord8         label[FLEXIBLE_ARRAY];
} EventThreadLabel;

typedef struct _EventCapNo EventCapCreate;
typedef struct _EventCapNo EventCapDelete;
typedef struct _EventCapNo EventCapDisable;
typedef struct _EventCapNo EventCapEnable;

typedef struct _EventCapsetBytes EventHeapAllocated;
typedef struct _EventCapsetBytes EventHeapSize;
typedef struct _EventCapsetBytes EventHeapLive;

typedef struct _EventHeapInfoGHC {
    EventHeader   header;
    EventCapsetID capset;
    StgWord16     gens;
    StgWord64     maxHeapSize;
    StgWord64     allocAreaSize;
    StgWord64     mblockSize;
    StgWord64     blockSize;
} EventHeapInfoGHC;

typedef struct _EventGcStatsGHC {
    EventHeader   header;
    EventCapsetID capset;
    StgWord16     gen;
    StgWord64     copied;
    StgWord64     slop;
    StgWord64     fragmentation;
    StgWord32     par_n_threads;
    StgWord64     par_max_copied;
    StgWord64     par_tot_copied;
} EventGcStatsGHC;

typedef struct _Event EventGcSync;

typedef struct _EventTaskCreate {
    EventHeader         header;
    EventTaskId         task;
    EventCapNo          capno;
    EventKernelThreadId tid;
} EventTaskCreate;

typedef struct _EventTaskMigrate {
    EventHeader         header;
    EventTaskId         task;
    EventCapNo          capno;
    EventCapNo          new_capno;
} EventTaskMigrate;

typedef struct _EventTaskDelete {
    EventHeader         header;
    EventTaskId         task;
} EventTaskDelete;

typedef struct _EventMsg EventUserMarker;

typedef struct _EventCapAlloc {
    EventHeader header;
    StgWord64   alloc;
    StgWord64   blocks;
    StgWord64   hp_alloc;
} EventCapAlloc;

typedef struct _EventCapValue {
    EventHeader header;
    StgWord8    tag;
    StgWord64   value;
} EventCapValue;

typedef struct _EventTaskCap {
    EventHeader header;
    EventTaskId task;
} EventTaskCap;

typedef struct _EventTaskCap EventTaskAcquireCap;
typedef struct _EventTaskCap EventTaskReleaseCap;

typedef struct _EventTaskReturnCap {
    EventHeader header;
    EventTaskId task;
    EventCapNo  capno;
} EventTaskReturnCap;

rtsBool isVariableSizeEvent(EventTypeNum tag);
int eventSize(Event *ev);
void printEvent(Capability *cap, Event *ev);

Event *createEvent(EventTypeNum tag);
Event *createSchedEvent(EventTypeNum tag,
                        StgTSO      *tso,
                        StgWord      info1,
                        StgWord      info2);
Event *createStartupEvent(EventCapNo nocaps);
Event *createGcEvent(EventTypeNum tag);
Event *createHeapEvent(EventTypeNum  tag,
                       EventCapsetID heap_capset,
                       lnat          info1);
Event *createHeapInfoEvent(EventCapsetID heap_capset,
                           nat           gens,
                           W_            maxHeapSize,
                           W_            allocAreaSize,
                           W_            mblockSize,
                           W_            blockSize);
Event *createGcStatsEvent(EventCapsetID heap_capset,
                          nat           gen,
                          W_            copied,
                          W_            slop,
                          W_            fragmentation,
                          nat           par_n_threads,
                          W_            par_max_copied,
                          W_            par_tot_copied);
Event *createCapEvent(EventTypeNum tag, EventCapNo capno);
Event *createCapsetEvent(EventTypeNum tag,
                         EventCapsetID capset,
                         StgWord info);
Event *createWallClockTimeEvent(EventCapsetID capset);
Event *createCapsetStrEvent(EventTypeNum  tag,
                            EventCapsetID capset,
                            const char   *msg);
Event *createCapsetVecEvent(EventTypeNum  tag,
                            EventCapsetID capset,
                            int           argc,
                            const char   *argv[]);
Event *createSparkEvent(EventTypeNum tag, StgWord info1);
Event *createSparkCountersEvent(SparkCounters counters,
                                StgWord remaining);
Event *createMsgEvent(EventTypeNum type, const char *msg, va_list ap);
Event *createThreadLabelEvent(StgTSO *tso, const char *label);
Event *createTaskCreateEvent(EventTaskId taskId, EventCapNo capno);
Event *createTaskMigrateEvent(EventTaskId taskId, EventCapNo capno,
                              EventCapNo new_capno);
Event *createTaskDeleteEvent(EventTaskId taskId);
Event *createUserMarkerEvent(const char *markername);
Event *createCapAllocEvent(W_ alloc, W_ blocks, W_ hpAlloc);
Event *createCapValueEvent(nat tag, W_ value);
Event *createTaskAcquireCapEvent(EventTaskId taskId);
Event *createTaskReleaseCapEvent(EventTaskId taskId);
Event *createTaskReturnCapEvent(EventTaskId taskId, EventCapNo capno);

#endif

#include "EndPrivate.h"

#endif /* EVENT_H */
