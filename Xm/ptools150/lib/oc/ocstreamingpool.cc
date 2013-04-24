
#if defined(OC_FACTOR_INTO_H_AND_CC)
# include "ocstreamingpool.h"
#endif

OC_BEGIN_NAMESPACE

OC_INLINE static bool isPowerOfTwo (int n)
{
  int bit_len = sizeof(n)*8;
  int single_bit = 1;
  for (int ii=0; ii<bit_len; ii++) {
    if (n==single_bit) return true;
    single_bit <<= 1;
  }
  return false;
}

OC_INLINE bool StreamingPool::isPristine () const
{
  SPInfo_* pool = pool_();
  return (freelist_.next_ == pool && freelist_.prev_ == pool &&
	  pool->isFree());
}

OC_INLINE size_t StreamingPool::maximumBlockSize_ ()
{
  // Figure out how much overhead the StreamingPool uses of total memory
  char* pool_start = (char*)pool_();
  char* memory = (char*) this;
  int streaming_pool_overhead = pool_start - memory; 

  // Max number of bytes a block COULD use, if we weren't concerned about
  // alignment restrictions (plus the SENTINEL at the end of memory)
  int raw_bytes = bytes_ - streaming_pool_overhead - sizeof(int_ptr); 

  if (raw_bytes>0) {
    int remainder = raw_bytes % alignment_;
    int usable_bytes = raw_bytes - remainder;
    if (usable_bytes>0) {
      return usable_bytes;
    }
  }
  
  throw logic_error("Too few bytes in the StreamingPool");
}


OC_INLINE StreamingPool::StreamingPool (char* /*memory*/, int bytes, 
				       int alignment_restrictions) :
  alignment_(alignment_restrictions),
  poolOffset_(0),
  scheduledForDeletion_(false),
  bytes_(bytes),
  lock_(true), // use across processes???????
  freelist_(0,0,0) // freelist node, not part of managed memory
{
  // Make sure the user memory will be aligned properly.
  // Only allow powers of 2
  // cerr << "alignment " << alignment_ << " " << isPowerOfTwo(alignment_) << endl;
  if (alignment_<4 || !isPowerOfTwo(alignment_) || alignment_>ALIGN_MAX) {
    throw logic_error("You can only create StreamingPools with alignment "
		      " restrictions of powers of 2 (or >=4 or <ALIGN_MAX)");
  }
 

  // The first "SPInfo_" has to be aligned properly to make sure all subsequent
  // allocations will cause the alignment restrictions to be met.
  SPInfo_* test = new (&poolData_[0]) SPInfo_(0,0,0);
  char* user_mem = test->userData();
  AVLP int_mem = (AVLP)user_mem;
  AVLP remainder = int_mem % alignment_;
  if (remainder==0) {
    poolOffset_ = 0;
  } else {
    poolOffset_ = alignment_-remainder;
  }

  // Memory starts out a single free node with the "rest of memory"
  // as its bytes.  It uses a negative size to indicate its free
  new (pool_()) SPInfo_(this, &freelist_, &freelist_);  

  // Reinit free list so that freelist points to floating first node
  new (&freelist_) SPInfo_(this, pool_(), pool_());

  // First and only block in memory when the pool is initialized
  int max_block_size = maximumBlockSize_();
  pool_()->setTotalBytes(max_block_size);
  pool_()->markAsFree();
		      
  // Set the sentinel boundaries
  
  // Lower addresses (may go into freelist_, but that's okay ...)
  char* lower = (char*)pool_(); 
  lower -= sizeof(int_ptr);
  int_ptr* lower_sentinel = (int_ptr*) lower;
  *lower_sentinel = MEMORY_BOUNDARY;

  // Upper addresses, just a lone int_ptr at the end the of memory
  char* upper = (char*)(&(pool_()->endTotalBytes()));
  upper += sizeof(int_ptr);
  int_ptr* upper_sentinel = (int_ptr*) upper;
  *upper_sentinel = MEMORY_BOUNDARY;
}


OC_INLINE char* StreamingPool::allocate (int bytes_requested)
{
  // An allocation request has to be at least as big as the
  // difference between an inuse node and a free node (there has
  // to be enough space to hold the next and prev pointer when
  // the deallocate happens)
  if (bytes_requested < int(2*sizeof(SPInfo_*))) {
    bytes_requested = 2*sizeof(SPInfo_*);
  }

  // Lock the transaction as we search and modify memory
  char* memory = 0;
  {
    ProtectScope ps(lock_);

    // Search the freelist for first fit
    SPInfo_* end = &freelist_;
    SPInfo_* sp = freelist_.nextFree();
    for (; sp != end; sp=sp->nextFree()) {
      int_4 blocksize = sp->canSatisfy(bytes_requested, alignment_);
      if (blocksize != 0) break;
    }
    // Whoops: made it to end
    if (sp==end) {
      throw runtime_error("Not enough memory in pool to statisfy request");
    }

    // Assertion: at a node with enough memory to satisfy
    SPInfo_ *allocated = sp->splitNode(bytes_requested, alignment_);
    memory = allocated->userData();
  }
  
  // make sure memory aligns correctly, or have to adjust
  AVLP int_mem = (AVLP)memory;
  int remainder = int_mem % alignment_;
  if (remainder) {
    throw runtime_error("Memory not aligned correctly");
  }
  
  return memory;
}


OC_INLINE void StreamingPool::deallocate (char* start_of_user_data)
{
  // Get the front of this struct that contains the user data
  SPInfo_* current = SPInfo_::GetSPInfo(start_of_user_data);

  // Is this already free?
  if (current->isFree()) {
    throw logic_error("Attempted to double delete a node already deallocated");
  }
  
  // Lock this transaction: starting to manipulate memory, so have to
  // make sure memory is in a consistent state across threads or
  // processes
  bool ready_to_detach = false;
  {
    ProtectScope ps(lock_);
    
    // Get ready for deallocation: now mark this node as free
    current->markAsFree();
    
    // Get the next node and see if it is free so we can coalesce with it
    SPInfo_* next = current->nextContiguous();
    if (next && next->isFree()) {
      current = current->coalesceWithNext(next, false);    
    }
  
    // Get the previous node and see if it is free so we can coalesce with it
    SPInfo_* prev = current->prevContiguous();
    if (prev && prev->isFree()) {
      current = prev->coalesceWithNext(current, true);
    }
    
    // Add this new node (potential coalesced) to freelist
    current->link(&freelist_);

    ready_to_detach = this->isPristine() && scheduledForDeletion_;
  }


  // Clean up ... let user handle deletion of memory itself?
  if (ready_to_detach) {
    this->~StreamingPool();
  }
 
}


OC_INLINE bool StreamingPool::isFull () const
{
  return (freelist_.next_ == &freelist_ && freelist_.prev_ == &freelist_);
}


OC_INLINE int StreamingPool::biggestFree () 
{
  int big_size = 0;
  
  // Lock the transaction as we search memory
  {
    ProtectScope ps(lock_);

    // Search the freelist for biggest
    SPInfo_* end = &freelist_;
    SPInfo_* sp = freelist_.nextFree();
    for (; sp != end; sp=sp->nextFree()) {
      if (sp->length() > big_size) {
	big_size = sp->length();
      }
    }
  }
  
  return big_size;
}


OC_INLINE void StreamingPool::scheduleForDeletion () 
{
  bool ready_to_detach = false; 
  {
    ProtectScope ps(lock_);
    scheduledForDeletion_ = true;
    ready_to_detach = this->isPristine() && scheduledForDeletion_;
  }
  // Clean up ... let user handle deletion of memory itself?
  if (ready_to_detach) {
    this->~StreamingPool();
  }
}


// Has to be in .cc file because of cyclical dependencies
OC_INLINE SPInfo_* GetFreeList (StreamingPool* sp) 
{
  return &sp->freelist_;
}



OC_INLINE StreamingPool& StreamingPool::GetStreamingPool (char* start_user_data)
{
  // Get the front of this struct and access to the StreamingPool
  SPInfo_* current = SPInfo_::GetSPInfo(start_user_data);
  StreamingPool& pool = current->streamingPool();
  return pool;
}


#define MAGIC_COOKIE_MASK 0xFADE

OC_INLINE void StreamingPool::setMagicCookie (char* memory) 
{
  // This marks memory with a distinguishing mark so you can detect
  // it came from this allocator.  This assumes that a memory
  // request has to have enough space for at least 8 bytes
  // (currently enforced by the allocator because it needs next and
  // prev fields)
  int_4* ip = (int_4*)memory;
  ip[0] = MAGIC_COOKIE_MASK;
  AVLP this_allocator = (AVLP) this;
  ip[0] ^= this_allocator;
  
  ip[1] = MAGIC_COOKIE_MASK;
  AVLP this_address = (AVLP)memory;
  ip[1] ^= this_address;
}


OC_INLINE 
StreamingPool* StreamingPool::CheckMagicCookie (char* start_of_user_data)
{
  int_4* ip = (int_4*)start_of_user_data;
  AVLP address = ip[1] ^ MAGIC_COOKIE_MASK;
  char* check_address = (char*) address;
  if (start_of_user_data != check_address) {
    return 0; // Nope, memory wasn't marked with cookie
  }
  
  // Assertion, at this point, we know there seems to
  // be a magic cookie, which will tell us if there is an
  // allocator.  We double check the allocator!!
  AVLP allocator = ip[0] ^ MAGIC_COOKIE_MASK;
  StreamingPool* check_allocator = (StreamingPool*) allocator;
  
  // The allocator is actually in each node, so we can double check to
  // make sure they are the same.
  SPInfo_* current = SPInfo_::GetSPInfo(start_of_user_data);
  if (current->pool_ != check_allocator) {
    return 0;
  }
  return check_allocator;
}

OC_INLINE void cleanMagicCookie (char* memory) 
{
  int_4* ip = (int_4*)memory;
  ip[0] = 0;
  ip[1] = 0;
}


OC_INLINE SPInfo_* StreamingPool::firstAllocated ()
{
  SPInfo_* sp = pool_();
  while (sp->isFree()) {
    sp = sp->nextContiguous();
    if (!sp) break;
  }
  return sp;
}

OC_END_NAMESPACE
