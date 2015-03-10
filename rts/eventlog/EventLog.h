/* -----------------------------------------------------------------------------
 *
 * (c) The GHC Team, 2008-2009
 *
 * Support for fast binary event logging.
 *
 * ---------------------------------------------------------------------------*/

#ifndef EVENTLOG_H
#define EVENTLOG_H

#include "rts/EventLogFormat.h"
#include "Capability.h"
#include "Event.h"
#include "Stats.h"

#include "BeginPrivate.h"

#ifdef TRACING

/*
 * Descriptions of EventTags for events.
 */
extern const char *EventDesc[];

void initEventLogging(void);
void endEventLogging(void);
void freeEventLogging(void);
void abortEventLogging(void); // #4512 - after fork child needs to abort
void flushEventLog(void);     // event log inherited from parent
void moreCapEventBufs (nat from, nat to);

#ifdef REPLAY
void initEventLoggingReplay(void);
void endEventLoggingReplay(void);
void moreCapEventBufsReplay(nat from, nat to);

CapEvent *peekEventCap(nat n, int capno);
CapEvent *searchEventCap(int capno, nat tag);
CapEvent *searchEventTagValueBefore(nat tag, W_ value, nat last);
CapEvent *searchEventTagValueBeforeCap(nat tag, W_ value, nat last, int capno);
CapEvent *searchEventCapTagValueBefore(int capno, nat tag, W_ value, nat last);
rtsBool existsBlackHoleEventBeforeGC(W_ value);
CapEvent *readEvent(void);
CapEvent *nextEvent(void);
void freeEvent(CapEvent *ce);
#endif

INLINE_HEADER StgWord64 time_ns(void)
{ return TimeToNS(stat_getElapsedTime()); }


/*
 * Post a scheduler event to the capability's event buffer (an event
 * that has an associated thread).
 */
void postSchedEvent(Capability *cap, EventTypeNum tag,
                    StgThreadID id, StgWord info1, StgWord info2);

/*
 * Post a nullary event.
 */
void postEvent(Capability *cap, EventTypeNum tag);

void postEventAtTimestamp (Capability *cap, EventTimestamp ts,
                           EventTypeNum tag);

void postMsg(const char *msg, va_list ap);

void postUserMsg(Capability *cap, const char *msg, va_list ap);

void postCapMsg(Capability *cap, const char *msg, va_list ap);

void postUserMarker(Capability *cap, const char *markername);

void postEventStartup(EventCapNo n_caps);

/*
 * Post an event relating to a capability itself (create/delete/etc)
 */
void postCapEvent (EventTypeNum  tag,
                   EventCapNo    capno);

/*
 * Post an event that is associated with a capability set
 */
void postCapsetEvent (EventTypeNum tag,
                      EventCapsetID capset,
                      StgWord info);

/*
 * Post a capability set event with a string payload
 */
void postCapsetStrEvent (EventTypeNum tag,
                         EventCapsetID capset,
                         const char *msg);

/*
 * Post a capability set event with several strings payload
 */
void postCapsetVecEvent (EventTypeNum tag,
                         EventCapsetID capset,
                         int argc,
                         const char *msg[]);

void postWallClockTime (EventCapsetID capset);

/*
 * Post a `par` spark event
 */
void postSparkEvent(Capability *cap, EventTypeNum tag, StgWord info1);

/*
 * Post an event with several counters relating to `par` sparks.
 */
void postSparkCountersEvent (Capability *cap,
                             SparkCounters counters,
                             StgWord remaining);

/*
 * Post an event to annotate a thread with a label
 */
void postThreadLabel(Capability    *cap,
                     EventThreadID  id,
                     const char    *label);

/*
 * Various GC and heap events
 */
void postHeapEvent (Capability    *cap,
                    EventTypeNum   tag,
                    EventCapsetID  heap_capset,
                    W_           info1);

void postEventHeapInfo (EventCapsetID heap_capset,
                        nat           gens,
                        W_          maxHeapSize,
                        W_          allocAreaSize,
                        W_          mblockSize,
                        W_          blockSize);

void postEventGcStats  (Capability    *cap,
                        EventCapsetID  heap_capset,
                        nat            gen,
                        W_           copied,
                        W_           slop,
                        W_           fragmentation,
                        nat            par_n_threads,
                        W_           par_max_copied,
                        W_           par_tot_copied);

void postTaskCreateEvent (Capability *cap,
                          EventTaskId taskId,
                          EventCapNo capno,
                          EventKernelThreadId tid);

void postTaskMigrateEvent (EventTaskId taskId,
                           EventCapNo capno,
                           EventCapNo new_capno);

void postTaskDeleteEvent (Capability  *cap,
                          EventTaskId  taskId);

void postCapAllocEvent(Capability *cap,
                       W_          alloc,
                       W_          blocks,
                       W_          hp_alloc);

void postCapValueEvent(Capability *cap,
                       nat         tag,
                       W_          value);

void postTaskAcquireCapEvent(Capability *cap, EventTaskId taskId);
void postTaskReleaseCapEvent(Capability *cap, EventTaskId taskId);
void postTaskReturnCapEvent(EventTaskId taskId, EventCapNo capno);

#else /* !TRACING */

INLINE_HEADER void postSchedEvent (Capability *cap  STG_UNUSED,
                                   EventTypeNum tag STG_UNUSED,
                                   StgThreadID id   STG_UNUSED,
                                   StgWord info1    STG_UNUSED,
                                   StgWord info2    STG_UNUSED)
{ /* nothing */ }

INLINE_HEADER void postEvent (Capability *cap  STG_UNUSED,
                              EventTypeNum tag STG_UNUSED)
{ /* nothing */ }

INLINE_HEADER void postMsg (const char *msg STG_UNUSED,
                            va_list ap STG_UNUSED)
{ /* nothing */ }

INLINE_HEADER void postCapMsg (Capability *cap STG_UNUSED,
                               const char *msg STG_UNUSED,
                               va_list ap STG_UNUSED)
{ /* nothing */ }


INLINE_HEADER void postThreadLabel(Capability    *cap   STG_UNUSED,
                                   EventThreadID  id    STG_UNUSED,
                                   const char    *label STG_UNUSED)
{ /* nothing */ }

#endif

#include "EndPrivate.h"

#endif /* TRACING_H */
