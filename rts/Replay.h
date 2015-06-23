/* -----------------------------------------------------------------------------
 *
 * (c) The GHC Team, 2013
 *
 * Execution Replay implementation
 *
 * -------------------------------------------------------------------------*/

#ifndef REPLAY_H
#define REPLAY_H

#include "BeginPrivate.h"

#include "Event.h"
#include "Hash.h"
#include "Task.h"

#ifdef THREADED_RTS
#include "rts/OSThreads.h"
#endif

typedef struct _ReplayData {
    StgPtr  hp;         // Hp value when yielding
    W_      hp_adjust;  // case expressions allocate space for its worst-case
                        // branch and later deallocate it and allocate the
                        // real allocation of the branch taken. hp_adjust
                        // allows to increase HpLim temporarily and remember
    W_      hp_alloc;   // the adjustment and hp_alloc to check for cases
                        // where after the worst-case allocation was
                        // subtracted, the same amount is allocated in smaller
                        // steps

    bdescr *bd;         // To store the block where the thread is forced to stop

    StgPtr  last_hp;    // To store the block and heap pointer just before
    bdescr *last_bd;    // running a thread

    W_ alloc;      // total allocation in a capability
    W_ real_alloc; // DEBUG: to check with cap->total_allocated
    W_ blocks;     // blocks already allocated. The current allocation block is not counted here

    Task *sync_task;    // task to synchronise with after performing some
                        // computation (see replaySync_())
    StgClosure *sync_thunk;     // thunk to evaluate in some synchronisations
    StgClosure *restored_bh;    // thunk to blackhole as soon as possible after restoring

    W_ spark_id;
    StgClosure *restored_ind;
    StgInfoTable *restored_info;

    StgClosure **saved_sparks;
    int saved_sparks_idx;
    int saved_sparks_size;
} ReplayData;

extern rtsBool replay_enabled;

extern rtsBool eager_blackholing;

void initReplay(void);
void endReplay(void);

void debugReplay(char *s, ...);
void replayError(char *s, ...);
#if defined(DEBUG)
void replayCheckGCGeneric(StgPtr Hp, Capability *cap, StgPtr HpLim, bdescr *CurrentNursery, StgPtr Sp, StgPtr SpLim);
#endif

void replaySaveHp(Capability *cap);
void replaySaveAlloc(Capability *cap);

void replayEvent(Capability *cap, Event *ev);

#ifdef REPLAY
#define REPLAY_SHIFT        48
#define REPLAY_MASK         (((StgWord64)1 << REPLAY_SHIFT) - 1)

#define REPLAY_ID_ATOM      ((StgWord64)0x1111)
#define REPLAY_SPARK_ATOM   ((StgWord64)0x2222)
#define REPLAY_TSO_ATOM     ((StgWord64)0x3333)
#define REPLAY_SHARED_TSO   ((StgWord64)0x4444)
#define REPLAY_PTR_ATOM     ((StgWord64)0x5555)

#define REPLAY_ATOM(p)      ((StgWord64)(p) >> REPLAY_SHIFT)
#define REPLAY_IND_ATOM(p)  REPLAY_ATOM(((StgInd *)(p))->indirectee)

#define REPLAY_ID(p)            ((StgWord64)(StgWord32)(StgWord64)(p))
#define REPLAY_IND_ID(p)        REPLAY_ID(((StgInd *)(p))->indirectee)
#define REPLAY_SET_ID(p, tso)   ((StgClosure *)((StgWord64)(p) | (REPLAY_ID_ATOM << REPLAY_SHIFT) | ((W_)(tso)->id << 32)))
#define REPLAY_SET_SPARK(id)    ((StgClosure *)((StgWord64)(id) | (REPLAY_SPARK_ATOM << REPLAY_SHIFT)))

#define REPLAY_TSO(p)                   ((StgThreadID)(StgWord16)((StgWord64)(p) >> 32))
#define REPLAY_IND_TSO(p)               REPLAY_TSO(((StgInd *)(p))->indirectee)
#define REPLAY_SET_TSO(p, atom, tso)    ((StgClosure *)((StgWord64)(p) | ((atom) << REPLAY_SHIFT) | ((W_)(tso)->id << 32)))

#define REPLAY_PTR(p)       ((StgClosure *)((StgWord64)(p) & REPLAY_MASK))
#define REPLAY_IND_PTR(p)   REPLAY_PTR(((StgInd *)(p))->indirectee)
#define REPLAY_SET_PTR(p)   ((StgClosure *)((StgWord64)(p) | (REPLAY_PTR_ATOM << REPLAY_SHIFT)))

#define REPLAY_IS_THUNK(p)      (REPLAY_ATOM(p) == REPLAY_ID_ATOM || \
                                 REPLAY_ATOM(p) == REPLAY_SPARK_ATOM)
#define REPLAY_IND_IS_THUNK(p)  (REPLAY_IND_ATOM(p) == REPLAY_ID_ATOM || \
                                 REPLAY_IND_ATOM(p) == REPLAY_SPARK_ATOM)

#define REPLAY_IS_SHARED(p)         (REPLAY_ATOM(p) == REPLAY_SHARED_TSO ||     \
                                     REPLAY_ATOM(p) == REPLAY_PTR_ATOM)
#define REPLAY_IND_IS_SHARED(p)     (REPLAY_IND_ATOM(p) == REPLAY_SHARED_TSO ||     \
                                     REPLAY_IND_ATOM(p) == REPLAY_PTR_ATOM)

#define REPLAY_IS_TSO(p)            (REPLAY_ATOM(p) == REPLAY_TSO_ATOM ||       \
                                     REPLAY_ATOM(p) == REPLAY_SHARED_TSO)
#define REPLAY_IND_IS_TSO(p)        (REPLAY_IND_ATOM(p) == REPLAY_TSO_ATOM ||   \
                                     REPLAY_IND_ATOM(p) == REPLAY_SHARED_TSO)

#define REPLAY_IS_BH(p)             (REPLAY_ATOM(p) == REPLAY_TSO_ATOM   || \
                                     REPLAY_ATOM(p) == REPLAY_SHARED_TSO || \
                                     REPLAY_ATOM(p) == REPLAY_PTR_ATOM   || \
                                     (REPLAY_ATOM(p) == 0 &&                \
                                      GET_CLOSURE_TAG(p) != 0))
#define REPLAY_IND_IS_BH(p)         (REPLAY_IND_ATOM(p) == REPLAY_TSO_ATOM   || \
                                     REPLAY_IND_ATOM(p) == REPLAY_SHARED_TSO || \
                                     REPLAY_IND_ATOM(p) == REPLAY_PTR_ATOM   || \
                                     (REPLAY_IND_ATOM(p) == 0 &&                \
                                      GET_CLOSURE_TAG(((StgInd *)p)->indirectee) != 0))

#define REPLAY_RESET(p)     (((StgInd *)p)->indirectee = REPLAY_IND_PTR(p))
#define REPLAY_TSO_RESET(p) ASSERT(REPLAY_IND_IS_BH(p));                                                \
                            if (REPLAY_IND_ATOM(p) == REPLAY_TSO_ATOM ||                                \
                                REPLAY_IND_ATOM(p) == REPLAY_SHARED_TSO) {                              \
                                ((StgInd *)p)->indirectee = (StgClosure *)findThread(REPLAY_IND_TSO(p));\
                            } else if (REPLAY_IND_ATOM(p) == REPLAY_PTR_ATOM) {                         \
                                REPLAY_RESET(p);                                                        \
                            }

extern StgTSO **thread_from_id;

void replayTraceCapTag(Capability *cap, int tag);
void replayTraceCapValue(Capability *cap, int tag, W_ value);
W_ replayCapTag(Capability *cap, int tag);
void replayCapValue(Capability *cap, int tag, W_ value);

void replayNewThread(StgTSO *tso);
StgTSO *findThread(StgThreadID id);

#ifdef THREADED_RTS
extern rtsBool emit_dups;

extern OSThreadId  replay_init_thread;
extern int         replay_main_task;

extern HashTable *spark_ptrs;

void replayNewTask(Task *task);
void replayWorkerStart(Capability *cap, Task *task);
void replayStartWorkerTask(Capability *from, Task *task, Capability *cap);

void replayMVar(Capability *cap, StgClosure *p, const StgInfoTable *info, int tag, int value);

StgClosure *replayNewSpark(StgClosure *p);
void replayStealSpark(Capability *cap, StgClosure *spark);
void replayRestoreSpark(StgClosure *bh);
StgClosure *replayFindSpark(Capability *cap);
void replayReleaseCapability (Capability *from, Capability* cap);
void replayWaitForReturnCapability(Capability **pCap, Task *task);
void replayShutdownCapability(Capability *cap, Task *task);
void replayYieldCapability(Capability **pCap, Task *task);

void replayProcessInbox(Capability **pCap);
void replayActivateSpark(Capability *cap);
void replayPushWork(Capability *cap, Task *task);
void replayDetectDeadlock(Capability **pCap, Task *task);
void replayYield(Capability **pCap, Task *task);
rtsBool replayTryGrabCapability(Capability *cap, Task *task);
nat replayRequestSync(Capability **pCap, Task *task, nat sync_type);
void replayExitScheduler(Task *task);

void replayRtsUnlock(Capability *cap, Task *task);

rtsBool replayThreadPaused(Capability *cap, StgClosure *p);
StgClosure *replayBlackHole(Capability *cap, StgClosure *p, StgPtr Hp);
MessageBlackHole *replayMessageBlackHole(Capability *cap, StgClosure *bh);

StgClosure *replayMarkedUpdFrame(Capability *cap, StgClosure *updatee, StgClosure *ret, StgPtr Hp);
void replayUpdateWithIndirection(Capability *cap, StgClosure *p, StgClosure *ind);
StgClosure *replayUpdateThunk (Capability *cap, StgTSO *tso, StgClosure *thunk, StgClosure *val);
void replayLockCAF(Capability *cap, W_ id);

void replayStartGC(Capability *cap);
StgClosure *replayResetSpark(Capability *cap, StgClosure *spark);
void replayResetSparks(Capability *cap);
void replayEndGC(Capability *cap);
void replayWaitForGcThreads(Capability *cap);

void replayTraceExpression(Capability *cap, StgClosure *p, W_ id);
#endif
#endif

#include "EndPrivate.h"

#endif // REPLAY_H
