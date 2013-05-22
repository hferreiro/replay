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

#ifdef REPLAY

typedef struct _ReplayData {
    StgPtr last_hp;
    bdescr *last_bd;
    W_ alloc;      // total allocation in a capability
    W_ real_alloc; // DEBUG: to check with cap->total_allocated
    W_ blocks;     // blocks already allocated. The current allocation block is not counted here
} ReplayData;

extern rtsBool replay_enabled;

#endif // REPLAY

void replayPrint(char *s, ...);
void replayError(char *s, ...);
#if defined(DEBUG)
void replayCheckGCGeneric(StgPtr Hp, Capability *cap, StgPtr HpLim, bdescr *CurrentNursery);
#endif

void replaySaveHp(Capability *cap);
void replaySaveAlloc(Capability *cap, StgThreadReturnCode ret);

#include "EndPrivate.h"

#endif // REPLAY_H
