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
#include "RtsFlags.h"
#include "RtsUtils.h"
#include "Trace.h"
#include "Replay.h"

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


void
initReplay(void)
{
    replay_enabled = RtsFlags.ReplayFlags.replay == rtsTrue;

    if (replay_enabled) {
        initEventLoggingReplay();

        // flag setup
        initRtsFlagsDefaults();
        RtsFlags.MiscFlags.tickInterval = 0;

        // fake this events so that flags and env are copied
#ifdef THREADED_RTS
        replayEvent(NULL, createStartupEvent(RtsFlags.ParFlags.nNodes));
#else
        replayEvent(NULL, createStartupEvent(1));
#endif
        replayEvent(NULL, createEvent(EVENT_PROGRAM_ARGS));
        replayEvent(NULL, createEvent(EVENT_PROGRAM_ENV));
    }
}

void
endReplay(void)
{
    if (replay_enabled) {
        endEventLoggingReplay();
    }
}

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
replaySaveAlloc(Capability *cap)
{
    bdescr *oldBd, *bd, *bd_;
    StgPtr oldHp, hp;

    oldBd = cap->replay.last_bd;
    oldHp = cap->replay.last_hp;

    bd = cap->r.rCurrentNursery;
    hp = START_FREE(bd);

#ifdef DEBUG
    debugBelch("Yielded at %p\n", _ptr(hp));
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
    debugBelch("Got:\n  ");
    printEvent(cap, ev);
    debugBelch("Expected:\n  ");
    printEvent(cap, read);
    barf("replay: '%s' events did not match", EventDesc[ev->header.tag]);
}

static rtsBool
compareEvents(Event *ev, Event *read)
{
    ASSERT(ev);
    ASSERT(read);
    ASSERT(ev->header.tag == read->header.tag);

    rtsBool r = rtsTrue;

    if ((isVariableSizeEvent(ev->header.tag)
            && eventSize(ev) != eventSize(read)) ||
        (memcmp((StgWord8 *)ev + sizeof(EventHeader),
                (StgWord8 *)read + sizeof(EventHeader),
                eventSize(ev) - sizeof(EventHeader)) != 0)) {
        r = rtsFalse;
    }

    return r;
}

static void
setupNextEvent(void)
{
    CapEvent *ce;

    ce = peekEvent(1);
    if (ce == NULL) {
        return;
    }

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

            ce = peekEvent(2);
            ASSERT(ce);
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

void
replayEvent(Capability *cap, Event *ev)
{
    ASSERT(ev);

    CapEvent *ce;
    Event *read;
    nat tag;

    ce = nextEvent();
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
            freeRtsArgs();

            setFullProgArgv(argc, argv);
            setupRtsFlags(&argc, argv, RtsOptsAll, NULL, rtsTrue);
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
    case EVENT_THREAD_LABEL:
    {
        EventThreadLabel *etl, *etl_read;
        EventPayloadSize size;

        etl = (EventThreadLabel *)ev;
        etl_read = (EventThreadLabel *)read;
        // ignore tid
        size = etl->size - sizeof(EventThreadID);
        if (etl->size != etl_read->size ||
            strncmp((const char *)etl->label, (const char *)etl_read->label,
                    size) != 0) {
            failedMatch(cap, ev, read);
        }
        break;
    }
    case EVENT_TASK_CREATE:
    {
        EventTaskCreate *etc, *etc_read;

        etc = (EventTaskCreate *)ev;
        etc_read = (EventTaskCreate *)read;
        // ignore tid
        if (etc->task != etc_read->task || etc->capno != etc_read->capno) {
            failedMatch(cap, ev, read);
        }
        break;
    }
    default:
        if (!compareEvents(ev, read)) {
            failedMatch(cap, ev, read);
        }
        break;
    }

#ifdef DEBUG
    printEvent(cap, ev);
#endif

    freeEvent(ce);
    stgFree(ev);

    setupNextEvent();
}
#else
void initReplay(void) {}
void endReplay(void) {}
void replayPrint(char *s STG_UNUSED, ...) {}
void replayError(char *s STG_UNUSED, ...) {}
void replaySaveHp(Capability *cap STG_UNUSED) {}
void replaySaveAlloc(Capability *cap STG_UNUSED) {}
void replayEvent(Capability *cap STG_UNUSED, Event *ev STG_UNUSED) {}
#endif
