/* -----------------------------------------------------------------------------
 * $Id: Storage.c,v 1.34 2001/01/29 17:23:41 simonmar Exp $
 *
 * (c) The GHC Team, 1998-1999
 *
 * Storage manager front end
 *
 * ---------------------------------------------------------------------------*/

#include "Rts.h"
#include "RtsUtils.h"
#include "RtsFlags.h"
#include "Stats.h"
#include "Hooks.h"
#include "BlockAlloc.h"
#include "MBlock.h"
#include "Weak.h"
#include "Sanity.h"

#include "Storage.h"
#include "Schedule.h"
#include "StoragePriv.h"

#ifndef SMP
nat nursery_blocks;		/* number of blocks in the nursery */
#endif

StgClosure    *caf_list         = NULL;

bdescr *small_alloc_list;	/* allocate()d small objects */
bdescr *large_alloc_list;	/* allocate()d large objects */
nat alloc_blocks;		/* number of allocate()d blocks since GC */
nat alloc_blocks_lim;		/* approximate limit on alloc_blocks */

StgPtr alloc_Hp    = NULL;	/* next free byte in small_alloc_list */
StgPtr alloc_HpLim = NULL;	/* end of block at small_alloc_list   */

generation *generations;	/* all the generations */
generation *g0;			/* generation 0, for convenience */
generation *oldest_gen;		/* oldest generation, for convenience */
step *g0s0;			/* generation 0, step 0, for convenience */

lnat total_allocated = 0;	/* total memory allocated during run */

/*
 * Storage manager mutex:  protects all the above state from
 * simultaneous access by two STG threads.
 */
#ifdef SMP
pthread_mutex_t sm_mutex = PTHREAD_MUTEX_INITIALIZER;
#endif

/*
 * Forward references
 */
static void *stgAllocForGMP   (size_t size_in_bytes);
static void *stgReallocForGMP (void *ptr, size_t old_size, size_t new_size);
static void  stgDeallocForGMP (void *ptr, size_t size);

void
initStorage (void)
{
  nat g, s;
  step *stp;
  generation *gen;

  /* If we're doing heap profiling, we want a two-space heap with a
   * fixed-size allocation area so that we get roughly even-spaced
   * samples.
   */
#if defined(PROFILING) || defined(DEBUG)
  if (RtsFlags.ProfFlags.doHeapProfile) {
    RtsFlags.GcFlags.generations = 1;
    RtsFlags.GcFlags.steps = 1;
    RtsFlags.GcFlags.oldGenFactor = 0;
    RtsFlags.GcFlags.heapSizeSuggestion = 0;
  }
#endif

  if (RtsFlags.GcFlags.heapSizeSuggestion > 
      RtsFlags.GcFlags.maxHeapSize) {
    RtsFlags.GcFlags.maxHeapSize = RtsFlags.GcFlags.heapSizeSuggestion;
  }

  initBlockAllocator();
  
  /* allocate generation info array */
  generations = (generation *)stgMallocBytes(RtsFlags.GcFlags.generations 
					     * sizeof(struct _generation),
					     "initStorage: gens");

  /* Initialise all generations */
  for(g = 0; g < RtsFlags.GcFlags.generations; g++) {
    gen = &generations[g];
    gen->no = g;
    gen->mut_list = END_MUT_LIST;
    gen->mut_once_list = END_MUT_LIST;
    gen->collections = 0;
    gen->failed_promotions = 0;
    gen->max_blocks = 0;
  }

  /* A couple of convenience pointers */
  g0 = &generations[0];
  oldest_gen = &generations[RtsFlags.GcFlags.generations-1];

  /* Allocate step structures in each generation */
  if (RtsFlags.GcFlags.generations > 1) {
    /* Only for multiple-generations */

    /* Oldest generation: one step */
    oldest_gen->n_steps = 1;
    oldest_gen->steps = 
      stgMallocBytes(1 * sizeof(struct _step), "initStorage: last step");

    /* set up all except the oldest generation with 2 steps */
    for(g = 0; g < RtsFlags.GcFlags.generations-1; g++) {
      generations[g].n_steps = RtsFlags.GcFlags.steps;
      generations[g].steps  = 
	stgMallocBytes (RtsFlags.GcFlags.steps * sizeof(struct _step),
			"initStorage: steps");
    }
    
  } else {
    /* single generation, i.e. a two-space collector */
    g0->n_steps = 1;
    g0->steps = stgMallocBytes (sizeof(struct _step), "initStorage: steps");
  }

  /* Initialise all steps */
  for (g = 0; g < RtsFlags.GcFlags.generations; g++) {
    for (s = 0; s < generations[g].n_steps; s++) {
      stp = &generations[g].steps[s];
      stp->no = s;
      stp->blocks = NULL;
      stp->n_blocks = 0;
      stp->gen = &generations[g];
      stp->hp = NULL;
      stp->hpLim = NULL;
      stp->hp_bd = NULL;
      stp->scan = NULL;
      stp->scan_bd = NULL;
      stp->large_objects = NULL;
      stp->new_large_objects = NULL;
      stp->scavenged_large_objects = NULL;
    }
  }
  
  /* Set up the destination pointers in each younger gen. step */
  for (g = 0; g < RtsFlags.GcFlags.generations-1; g++) {
    for (s = 0; s < generations[g].n_steps-1; s++) {
      generations[g].steps[s].to = &generations[g].steps[s+1];
    }
    generations[g].steps[s].to = &generations[g+1].steps[0];
  }
  
  /* The oldest generation has one step and its destination is the
   * same step. */
  oldest_gen->steps[0].to = &oldest_gen->steps[0];

  /* generation 0 is special: that's the nursery */
  generations[0].max_blocks = 0;

  /* G0S0: the allocation area.  Policy: keep the allocation area
   * small to begin with, even if we have a large suggested heap
   * size.  Reason: we're going to do a major collection first, and we
   * don't want it to be a big one.  This vague idea is borne out by 
   * rigorous experimental evidence.
   */
  g0s0 = &generations[0].steps[0];

  allocNurseries();

  weak_ptr_list = NULL;
  caf_list = NULL;
   
  /* initialise the allocate() interface */
  small_alloc_list = NULL;
  large_alloc_list = NULL;
  alloc_blocks = 0;
  alloc_blocks_lim = RtsFlags.GcFlags.minAllocAreaSize;

  /* Tell GNU multi-precision pkg about our custom alloc functions */
  mp_set_memory_functions(stgAllocForGMP, stgReallocForGMP, stgDeallocForGMP);

#ifdef SMP
  pthread_mutex_init(&sm_mutex, NULL);
#endif

  IF_DEBUG(gc, stat_describe_gens());
}

void
exitStorage (void)
{
    stat_exit(calcAllocated());
}

/* -----------------------------------------------------------------------------
   CAF management.

   The entry code for every CAF does the following:
     
      - builds a CAF_BLACKHOLE in the heap
      - pushes an update frame pointing to the CAF_BLACKHOLE
      - invokes UPD_CAF(), which:
          - calls newCaf, below
	  - updates the CAF with a static indirection to the CAF_BLACKHOLE
      
   Why do we build a BLACKHOLE in the heap rather than just updating
   the thunk directly?  It's so that we only need one kind of update
   frame - otherwise we'd need a static version of the update frame too.

   newCaf() does the following:
       
      - it puts the CAF on the oldest generation's mut-once list.
        This is so that we can treat the CAF as a root when collecting
	younger generations.

   For GHCI, we have additional requirements when dealing with CAFs:

      - we must *retain* all dynamically-loaded CAFs ever entered,
        just in case we need them again.
      - we must be able to *revert* CAFs that have been evaluated, to
        their pre-evaluated form.

      To do this, we use an additional CAF list.  When newCaf() is
      called on a dynamically-loaded CAF, we add it to the CAF list
      instead of the old-generation mutable list, and save away its
      old info pointer (in caf->saved_info) for later reversion.

      To revert all the CAFs, we traverse the CAF list and reset the
      info pointer to caf->saved_info, then throw away the CAF list.
      (see GC.c:revertCAFs()).

      -- SDM 29/1/01

   -------------------------------------------------------------------------- */

void
newCAF(StgClosure* caf)
{
  /* Put this CAF on the mutable list for the old generation.
   * This is a HACK - the IND_STATIC closure doesn't really have
   * a mut_link field, but we pretend it has - in fact we re-use
   * the STATIC_LINK field for the time being, because when we
   * come to do a major GC we won't need the mut_link field
   * any more and can use it as a STATIC_LINK.
   */
  ACQUIRE_LOCK(&sm_mutex);

#ifdef GHCI
  if (is_dynamically_loaded_rwdata_ptr((StgPtr)caf)) {
      ((StgIndStatic *)caf)->saved_info  = (StgInfoTable *)caf->header.info;
      ((StgIndStatic *)caf)->static_link = caf_list;
      caf_list = caf;
  } else {
      ((StgIndStatic *)caf)->saved_info = NULL;
      ((StgMutClosure *)caf)->mut_link = oldest_gen->mut_once_list;
      oldest_gen->mut_once_list = (StgMutClosure *)caf;
  }
#else
  ASSERT( ((StgMutClosure*)caf)->mut_link == NULL );
  ((StgMutClosure *)caf)->mut_link = oldest_gen->mut_once_list;
  oldest_gen->mut_once_list = (StgMutClosure *)caf;
#endif

  RELEASE_LOCK(&sm_mutex);
}

#ifdef GHCI
void
markCafs( void )
{
    StgClosure *p;

    for (p = caf_list; p != NULL; p = STATIC_LINK2(get_itbl(p),p)) {
	MarkRoot(p);
    }
}
#endif /* GHCI */

/* -----------------------------------------------------------------------------
   Nursery management.
   -------------------------------------------------------------------------- */

void
allocNurseries( void )
{ 
#ifdef SMP
  {
    Capability *cap;
    bdescr *bd;

    g0s0->blocks = NULL;
    g0s0->n_blocks = 0;
    for (cap = free_capabilities; cap != NULL; cap = cap->link) {
      cap->rNursery = allocNursery(NULL, RtsFlags.GcFlags.minAllocAreaSize);
      cap->rCurrentNursery = cap->rNursery;
      for (bd = cap->rNursery; bd != NULL; bd = bd->link) {
	bd->back = (bdescr *)cap;
      }
    }
    /* Set the back links to be equal to the Capability,
     * so we can do slightly better informed locking.
     */
  }
#else /* SMP */
  nursery_blocks  = RtsFlags.GcFlags.minAllocAreaSize;
  g0s0->blocks    = allocNursery(NULL, nursery_blocks);
  g0s0->n_blocks  = nursery_blocks;
  g0s0->to_space  = NULL;
  MainRegTable.rNursery        = g0s0->blocks;
  MainRegTable.rCurrentNursery = g0s0->blocks;
  /* hp, hpLim, hp_bd, to_space etc. aren't used in G0S0 */
#endif
}
      
void
resetNurseries( void )
{
  bdescr *bd;
#ifdef SMP
  Capability *cap;
  
  /* All tasks must be stopped */
  ASSERT(n_free_capabilities == RtsFlags.ParFlags.nNodes);

  for (cap = free_capabilities; cap != NULL; cap = cap->link) {
    for (bd = cap->rNursery; bd; bd = bd->link) {
      bd->free = bd->start;
      ASSERT(bd->gen == g0);
      ASSERT(bd->step == g0s0);
      IF_DEBUG(sanity,memset(bd->start, 0xaa, BLOCK_SIZE));
    }
    cap->rCurrentNursery = cap->rNursery;
  }
#else
  for (bd = g0s0->blocks; bd; bd = bd->link) {
    bd->free = bd->start;
    ASSERT(bd->gen == g0);
    ASSERT(bd->step == g0s0);
    IF_DEBUG(sanity,memset(bd->start, 0xaa, BLOCK_SIZE));
  }
  MainRegTable.rNursery = g0s0->blocks;
  MainRegTable.rCurrentNursery = g0s0->blocks;
#endif
}

bdescr *
allocNursery (bdescr *last_bd, nat blocks)
{
  bdescr *bd;
  nat i;

  /* Allocate a nursery */
  for (i=0; i < blocks; i++) {
    bd = allocBlock();
    bd->link = last_bd;
    bd->step = g0s0;
    bd->gen = g0;
    bd->evacuated = 0;
    bd->free = bd->start;
    last_bd = bd;
  }
  return last_bd;
}

void
resizeNursery ( nat blocks )
{
  bdescr *bd;

#ifdef SMP
  barf("resizeNursery: can't resize in SMP mode");
#endif

  if (nursery_blocks == blocks) {
    ASSERT(g0s0->n_blocks == blocks);
    return;
  }

  else if (nursery_blocks < blocks) {
    IF_DEBUG(gc, fprintf(stderr, "Increasing size of nursery to %d blocks\n", 
			 blocks));
    g0s0->blocks = allocNursery(g0s0->blocks, blocks-nursery_blocks);
  } 

  else {
    bdescr *next_bd;
    
    IF_DEBUG(gc, fprintf(stderr, "Decreasing size of nursery to %d blocks\n", 
			 blocks));
    for (bd = g0s0->blocks; nursery_blocks > blocks; nursery_blocks--) {
      next_bd = bd->link;
      freeGroup(bd);
      bd = next_bd;
    }
    g0s0->blocks = bd;
  }
  
  g0s0->n_blocks = nursery_blocks = blocks;
}

/* -----------------------------------------------------------------------------
   The allocate() interface

   allocate(n) always succeeds, and returns a chunk of memory n words
   long.  n can be larger than the size of a block if necessary, in
   which case a contiguous block group will be allocated.
   -------------------------------------------------------------------------- */

StgPtr
allocate(nat n)
{
  bdescr *bd;
  StgPtr p;

  ACQUIRE_LOCK(&sm_mutex);

  TICK_ALLOC_HEAP_NOCTR(n);
  CCS_ALLOC(CCCS,n);

  /* big allocation (>LARGE_OBJECT_THRESHOLD) */
  /* ToDo: allocate directly into generation 1 */
  if (n >= LARGE_OBJECT_THRESHOLD/sizeof(W_)) {
    nat req_blocks =  (lnat)BLOCK_ROUND_UP(n*sizeof(W_)) / BLOCK_SIZE;
    bd = allocGroup(req_blocks);
    dbl_link_onto(bd, &g0s0->large_objects);
    bd->gen  = g0;
    bd->step = g0s0;
    bd->evacuated = 0;
    bd->free = bd->start;
    /* don't add these blocks to alloc_blocks, since we're assuming
     * that large objects are likely to remain live for quite a while
     * (eg. running threads), so garbage collecting early won't make
     * much difference.
     */
    alloc_blocks += req_blocks;
    RELEASE_LOCK(&sm_mutex);
    return bd->start;

  /* small allocation (<LARGE_OBJECT_THRESHOLD) */
  } else if (small_alloc_list == NULL || alloc_Hp + n > alloc_HpLim) {
    if (small_alloc_list) {
      small_alloc_list->free = alloc_Hp;
    }
    bd = allocBlock();
    bd->link = small_alloc_list;
    small_alloc_list = bd;
    bd->gen = g0;
    bd->step = g0s0;
    bd->evacuated = 0;
    alloc_Hp = bd->start;
    alloc_HpLim = bd->start + BLOCK_SIZE_W;
    alloc_blocks++;
  }
  
  p = alloc_Hp;
  alloc_Hp += n;
  RELEASE_LOCK(&sm_mutex);
  return p;
}

lnat allocated_bytes(void)
{
  return (alloc_blocks * BLOCK_SIZE_W - (alloc_HpLim - alloc_Hp));
}

/* -----------------------------------------------------------------------------
   Allocation functions for GMP.

   These all use the allocate() interface - we can't have any garbage
   collection going on during a gmp operation, so we use allocate()
   which always succeeds.  The gmp operations which might need to
   allocate will ask the storage manager (via doYouWantToGC()) whether
   a garbage collection is required, in case we get into a loop doing
   only allocate() style allocation.
   -------------------------------------------------------------------------- */

static void *
stgAllocForGMP (size_t size_in_bytes)
{
  StgArrWords* arr;
  nat data_size_in_words, total_size_in_words;
  
  /* should be a multiple of sizeof(StgWord) (whole no. of limbs) */
  ASSERT(size_in_bytes % sizeof(W_) == 0);
  
  data_size_in_words  = size_in_bytes / sizeof(W_);
  total_size_in_words = sizeofW(StgArrWords) + data_size_in_words;
  
  /* allocate and fill it in. */
  arr = (StgArrWords *)allocate(total_size_in_words);
  SET_ARR_HDR(arr, &stg_ARR_WORDS_info, CCCS, data_size_in_words);
  
  /* and return a ptr to the goods inside the array */
  return(BYTE_ARR_CTS(arr));
}

static void *
stgReallocForGMP (void *ptr, size_t old_size, size_t new_size)
{
    void *new_stuff_ptr = stgAllocForGMP(new_size);
    nat i = 0;
    char *p = (char *) ptr;
    char *q = (char *) new_stuff_ptr;

    for (; i < old_size; i++, p++, q++) {
	*q = *p;
    }

    return(new_stuff_ptr);
}

static void
stgDeallocForGMP (void *ptr STG_UNUSED, 
		  size_t size STG_UNUSED)
{
    /* easy for us: the garbage collector does the dealloc'n */
}

/* -----------------------------------------------------------------------------
 * Stats and stuff
 * -------------------------------------------------------------------------- */

/* -----------------------------------------------------------------------------
 * calcAllocated()
 *
 * Approximate how much we've allocated: number of blocks in the
 * nursery + blocks allocated via allocate() - unused nusery blocks.
 * This leaves a little slop at the end of each block, and doesn't
 * take into account large objects (ToDo).
 * -------------------------------------------------------------------------- */

lnat
calcAllocated( void )
{
  nat allocated;
  bdescr *bd;

#ifdef SMP
  Capability *cap;

  /* All tasks must be stopped.  Can't assert that all the
     capabilities are owned by the scheduler, though: one or more
     tasks might have been stopped while they were running (non-main)
     threads. */
  /*  ASSERT(n_free_capabilities == RtsFlags.ParFlags.nNodes); */

  allocated = 
    n_free_capabilities * RtsFlags.GcFlags.minAllocAreaSize * BLOCK_SIZE_W
    + allocated_bytes();

  for (cap = free_capabilities; cap != NULL; cap = cap->link) {
    for ( bd = cap->rCurrentNursery->link; bd != NULL; bd = bd->link ) {
      allocated -= BLOCK_SIZE_W;
    }
    if (cap->rCurrentNursery->free < cap->rCurrentNursery->start 
	+ BLOCK_SIZE_W) {
      allocated -= (cap->rCurrentNursery->start + BLOCK_SIZE_W)
	- cap->rCurrentNursery->free;
    }
  }

#else /* !SMP */
  bdescr *current_nursery = MainRegTable.rCurrentNursery;

  allocated = (nursery_blocks * BLOCK_SIZE_W) + allocated_bytes();
  for ( bd = current_nursery->link; bd != NULL; bd = bd->link ) {
    allocated -= BLOCK_SIZE_W;
  }
  if (current_nursery->free < current_nursery->start + BLOCK_SIZE_W) {
    allocated -= (current_nursery->start + BLOCK_SIZE_W)
      - current_nursery->free;
  }
#endif

  total_allocated += allocated;
  return allocated;
}  

/* Approximate the amount of live data in the heap.  To be called just
 * after garbage collection (see GarbageCollect()).
 */
extern lnat 
calcLive(void)
{
  nat g, s;
  lnat live = 0;
  step *stp;

  if (RtsFlags.GcFlags.generations == 1) {
    live = (g0s0->to_blocks - 1) * BLOCK_SIZE_W + 
      ((lnat)g0s0->hp_bd->free - (lnat)g0s0->hp_bd->start) / sizeof(W_);
    return live;
  }

  for (g = 0; g < RtsFlags.GcFlags.generations; g++) {
    for (s = 0; s < generations[g].n_steps; s++) {
      /* approximate amount of live data (doesn't take into account slop
       * at end of each block).
       */
      if (g == 0 && s == 0) { 
	  continue; 
      }
      stp = &generations[g].steps[s];
      live += (stp->n_blocks - 1) * BLOCK_SIZE_W +
	((lnat)stp->hp_bd->free - (lnat)stp->hp_bd->start) / sizeof(W_);
    }
  }
  return live;
}

/* Approximate the number of blocks that will be needed at the next
 * garbage collection.
 *
 * Assume: all data currently live will remain live.  Steps that will
 * be collected next time will therefore need twice as many blocks
 * since all the data will be copied.
 */
extern lnat 
calcNeeded(void)
{
  lnat needed = 0;
  nat g, s;
  step *stp;

  for (g = 0; g < RtsFlags.GcFlags.generations; g++) {
    for (s = 0; s < generations[g].n_steps; s++) {
      if (g == 0 && s == 0) { continue; }
      stp = &generations[g].steps[s];
      if (generations[g].steps[0].n_blocks > generations[g].max_blocks) {
	needed += 2 * stp->n_blocks;
      } else {
	needed += stp->n_blocks;
      }
    }
  }
  return needed;
}

/* -----------------------------------------------------------------------------
   Debugging

   memInventory() checks for memory leaks by counting up all the
   blocks we know about and comparing that to the number of blocks
   allegedly floating around in the system.
   -------------------------------------------------------------------------- */

#ifdef DEBUG

extern void
memInventory(void)
{
  nat g, s;
  step *stp;
  bdescr *bd;
  lnat total_blocks = 0, free_blocks = 0;

  /* count the blocks we current have */

  for (g = 0; g < RtsFlags.GcFlags.generations; g++) {
    for (s = 0; s < generations[g].n_steps; s++) {
      stp = &generations[g].steps[s];
      total_blocks += stp->n_blocks;
      if (RtsFlags.GcFlags.generations == 1) {
	/* two-space collector has a to-space too :-) */
	total_blocks += g0s0->to_blocks;
      }
      for (bd = stp->large_objects; bd; bd = bd->link) {
	total_blocks += bd->blocks;
	/* hack for megablock groups: they have an extra block or two in
	   the second and subsequent megablocks where the block
	   descriptors would normally go.
	*/
	if (bd->blocks > BLOCKS_PER_MBLOCK) {
	  total_blocks -= (MBLOCK_SIZE / BLOCK_SIZE - BLOCKS_PER_MBLOCK)
	                  * (bd->blocks/(MBLOCK_SIZE/BLOCK_SIZE));
	}
      }
    }
  }

  /* any blocks held by allocate() */
  for (bd = small_alloc_list; bd; bd = bd->link) {
    total_blocks += bd->blocks;
  }
  for (bd = large_alloc_list; bd; bd = bd->link) {
    total_blocks += bd->blocks;
  }
  
  /* count the blocks on the free list */
  free_blocks = countFreeList();

  ASSERT(total_blocks + free_blocks == mblocks_allocated * BLOCKS_PER_MBLOCK);

#if 0
  if (total_blocks + free_blocks != mblocks_allocated *
      BLOCKS_PER_MBLOCK) {
    fprintf(stderr, "Blocks: %ld live + %ld free  = %ld total (%ld around)\n",
	    total_blocks, free_blocks, total_blocks + free_blocks,
	    mblocks_allocated * BLOCKS_PER_MBLOCK);
  }
#endif
}

/* Full heap sanity check. */

extern void
checkSanity(nat N)
{
  nat g, s;

  if (RtsFlags.GcFlags.generations == 1) {
    checkHeap(g0s0->to_space, NULL);
    checkChain(g0s0->large_objects);
  } else {
    
    for (g = 0; g <= N; g++) {
      for (s = 0; s < generations[g].n_steps; s++) {
	if (g == 0 && s == 0) { continue; }
	checkHeap(generations[g].steps[s].blocks, NULL);
      }
    }
    for (g = N+1; g < RtsFlags.GcFlags.generations; g++) {
      for (s = 0; s < generations[g].n_steps; s++) {
	checkHeap(generations[g].steps[s].blocks,
		  generations[g].steps[s].blocks->start);
	checkChain(generations[g].steps[s].large_objects);
      }
    }
    checkFreeListSanity();
  }
}

#endif
