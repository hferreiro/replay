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
void replaySaveHp(Capability *cap);
void replaySaveAlloc(Capability *cap, StgThreadReturnCode ret);

void replayEvent(Capability *cap, Event *ev);

#include "EndPrivate.h"

#endif // REPLAY_H
