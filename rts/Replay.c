/* ---------------------------------------------------------------------------
 *
 * (c) The GHC Team, 2013
 *
 * Execution Replay implementation
 *
 * --------------------------------------------------------------------------*/

#include "PosixSource.h"
#include "Rts.h"

#include "Capability.h"
#include "Trace.h"
#include "Replay.h"

#define _ptr(p) ((void *)((StgWord)(p) & 0x0fffff))

#if defined(REPLAY)
#define START(bd)       ((bd)->start - 1)
#define START_FREE(bd)  ((bd)->free - 1) // last written word
#define END(bd)         (START(bd) + (bd)->blocks * BLOCK_SIZE_W) // last writable word

#define SIZE(bd)    ((bd)->blocks * BLOCK_SIZE_W)
#define FILLED(bd)  (START_FREE(bd) - START(bd))
#define FREE(bd)    (END(bd) - START_FREE(bd))
#define SLOP(bd)    FREE(bd)

#define FULL(bd)    (FILLED(bd) == BLOCK_SIZE_W)
#define EMPTY(bd)   (FILLED(bd) == 0)

#define HP_IN_BD(bd,p) (((p) >= START(bd)) && ((p) <= END(bd)))

rtsBool replay_enabled = rtsTrue;

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
    replayPrint("  hp    = %p\n", _ptr(START_FREE(cap->r.rCurrentNursery)));
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
replaySaveAlloc(Capability *cap, StgThreadReturnCode ret)
{
    bdescr *oldBd = cap->replay.last_bd;
    StgPtr  oldHp = cap->replay.last_hp;

    bdescr *bd = cap->r.rCurrentNursery;
    StgPtr  hp = START_FREE(bd);

    ASSERT(hp == START_FREE(bd));
    ASSERT(HP_IN_BD(oldBd, oldHp));
    ASSERT(HP_IN_BD(bd, hp));

    W_ alloc = 0, n_blocks = 0;
    W_ slop = 0;
    replayPrint("Saving alloc...\n");
    replayPrint("  alloc before = %ld\n", cap->replay.alloc);
    // Still same block
    if (oldBd == bd) {
        ASSERT(hp >= oldHp);
        alloc += hp - oldHp;
        cap->replay.real_alloc += hp - oldHp;
    } else {
        alloc += END(oldBd) - oldHp;
        ASSERT(START_FREE(oldBd) >= oldHp);
        cap->replay.real_alloc += START_FREE(oldBd) - oldHp;

        bdescr *bd_ = oldBd->link;
        ASSERT(bd_ != NULL);
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

        // stopped just after securing a new block, we need to substract the
        // slop from the previous block to get the exact allocation at which
        // the thread stopped
        if (ret == ThreadYielding && cap->r.rHpAlloc == 0) {
            ASSERT(cap->interrupt || cap->context_switch);
            ASSERT(EMPTY(bd_));
            ASSERT(last_alloc == 0);
            slop = SLOP(bd->u.back);
        }
    }
    cap->replay.alloc += alloc;
    cap->replay.blocks += n_blocks;
    replayPrint("\n  Found %" FMT_Word " blocks (%" FMT_Word "), total %" FMT_Word "\n\n",
                n_blocks, alloc, cap->replay.alloc);

    traceCapAlloc(cap, cap->replay.alloc - slop, cap->replay.blocks, cap->r.rHpAlloc);

    cap->replay.last_bd = NULL;
    cap->replay.last_hp = NULL;
}
#else
void replayPrint(char *s STG_UNUSED, ...) {}
void replaySaveHp(Capability *cap STG_UNUSED) {}
void replaySaveAlloc(Capability *cap STG_UNUSED, StgThreadReturnCode ret STG_UNUSED) {}
#endif
