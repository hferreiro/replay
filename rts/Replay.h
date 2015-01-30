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
} ReplayData;

extern rtsBool replay_enabled;

void initReplay(void);
void endReplay(void);

void replayPrint(char *s, ...);
void replayError(char *s, ...);
#if defined(DEBUG)
void replayCheckGCGeneric(StgPtr Hp, Capability *cap, StgPtr HpLim, bdescr *CurrentNursery);
#endif

void replaySaveHp(Capability *cap);
void replaySaveAlloc(Capability *cap);

void replayEvent(Capability *cap, Event *ev);

#include "EndPrivate.h"

#endif // REPLAY_H
