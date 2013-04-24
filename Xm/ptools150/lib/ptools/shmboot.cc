
#include "shmboot.h"  // "set-up" and start the memory section

PTOOLS_BEGIN_NAMESPACE

// Dump memory: utility function for debug
#define MEMDUMPMAXLINE 20
void memdump(void* memo, int bytes)
{
  //uintptr_t mmm = (uintptr_t) memo;

  fprintf(stderr, "****Memory %p\n", memo);
  unsigned char* mem = (unsigned char*)memo;
  for (int ii=0; ii<bytes; ii+=MEMDUMPMAXLINE) {
    unsigned char* off_mem = &mem[ii];
    
    // Last line
    int line_len = MEMDUMPMAXLINE;
    int ending = 0;
    if (ii+MEMDUMPMAXLINE > bytes) {
      line_len = bytes % MEMDUMPMAXLINE;
      ending = 16 - line_len;
    }

    for (int jj=0; jj<MEMDUMPMAXLINE; jj++) {
      if (jj<line_len) {
	fprintf(stderr, "%2x ", int(off_mem[jj]));
      } else {
	fprintf(stderr, "   ");
      }
    }
    for (int jj=0; jj<MEMDUMPMAXLINE; jj++) {
      if (jj<line_len) {
	if (isprint(off_mem[jj])) {
	  fprintf(stderr, "%c", off_mem[jj]);
	} else {
	  fprintf(stderr, "-");
	}
      } else {
	fprintf(stderr, " ");
      }
    }
    fprintf(stderr, "\n");
  }
}


// //////////////////////////////////////////////////// SHMStub_

// Heuristic check to see if Address Randomization is on
bool SHMStub_::addressRandomization () const
{
  bool stack_ok = true;   // See if stack looks in right place
  bool bigmem_ok = true;  // See if "big mem" allocations in right place
  bool heap_ok   = true;  // See if smalller heap allocations in right place

  if (sizeof(void*)==8) { // 64-bit address space (prolly 48-bit addresses)

    // Check stack: hopefully in 0x7fff ffff XXXX
    int x;
    AVLP xadr = (AVLP)&x;
    stack_ok = (xadr >> 16) == 0x7fffffff;  
    
    // Check normal heap: hopefully in  0x 00xx xxxx
    char* smallmem = new char;
    AVLP small_addr = (AVLP)smallmem;
    heap_ok = (small_addr >> 24) == 0x00;
    delete smallmem;
    
    // Check big-mem: hopefully in 0x 2aXX XXXX XXXX
    char* bigmem = new char[100000000];
    AVLP bmadr = (AVLP)bigmem;
    //bigmem_ok = (bmadr >> 40) == 0x2a; // so no warning on 32-bit ...
    bigmem_ok = (bmadr >> sizeof(void*)*5) == 0x2a;
    delete [] bigmem;

  } else if (sizeof(void*)==4) { // 32-bit address space
    
    // Check stack: hopefully in 0xbfffXXXX
    int x;
    AVLP xadr = (AVLP)&x;
    stack_ok = (xadr >> 16) == 0xbfff;

    // Check normal heap: hopefully in  0x80xxxxx
    char* smallmem = new char;
    AVLP small_addr = (AVLP)smallmem;
    heap_ok = (small_addr >> 20) == 0x80;
    delete smallmem;

    // Check big-mem: hopefully in 0x40XXXXXX
    char* bigmem = new char[1000000];
    AVLP bmadr = (AVLP)bigmem;
    bigmem_ok = (bmadr >> 24) == 0x40;
    //cerr << hex << (bmadr>>24) << " " << dec << bmadr << endl;
    delete [] bigmem;

  } else {
    throw logic_error("Can only handle 32-bit and 64-bit addresses");
  }

  // Again, this is a heuristic: we are guessing whether or not we
  // think its on.
  return (!stack_ok && !bigmem_ok && !heap_ok);
}

// This is a heuristic check ... it will work (at the time of this writing)
// on 32-bit and 64-bit Intel Linux machines, but hey, may not work
// for you, so this check can (1) be turned off or (2) overridden.
void SHMStub_::checkAddressRandomization_ ()
{
  if (addressRandomization()) {
    if (warning_) {
      cerr << "\n"

"  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! WARNING !!!!!!!!!!!!!!!!!!!!!!!!!\n"
"  ! It appears that the address randomization feature is still on.\n"
"  ! Your SHMMain/ServerSide/ClientSide is unlikely to work correctly.\n"
"  ! Program will continue running ... but may not run correctly ...\n"
"  !\n"
"  ! Make sure the process that's gets started up has this feature\n"
"  ! turned off using setarch.  For example:\n"
"  !  % setarch i386 -L -R startup_program     # on 32-bit machines\n"
"  !        or  \n"
"  !  % setarch x64_64 -L -R startup_program   # on 64-bit machines\n"
"  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n"
	 << endl;
    }

  }
}

// //////////////////////////////////////////////////// SHMMain

State_e SHMMain::start ()
{
  checkAddressRandomization_();

  // If already started or trying to start, just return
  if (state_ != STOPPED) return state_;
  
  // ... now trying to start
  state_ = STARTING;
  
  // Start 'em up
  StreamingPool* shm = 0;
  
  // create a small bootstrap memory pool:  In this pool, we will
  // have the starting conditions
  Bootstrap_t* boot = 
    (Bootstrap_t*)SHMCreate(bootname_.c_str(), sizeof(Bootstrap_t), debug_);
  if (boot==0) {
    throw runtime_error("Couldn't create boot pool:"+bootname_);
  } else {
    if (debug_) {
      cerr << "...boot pool " << bootname_ << " created" << endl;
      cout << "  [MainSHM, boot pool at memory location:" << (void*)boot << "]" << endl;
    }
  }
  
  // Create the main pool
  char* mem = (char*)SHMCreate(name_.c_str(), bytesInMainPool_, debug_, 
			       forcedAddr_);
  if (mem==0) {
    throw runtime_error("Couldn't create main pool:"+name_);
  } else {
    if (debug_) {
      cerr << "...main pool " << name_ << " created" << endl;
      cerr << "  [MainSHM, main pool at memory location:" << (void*)mem << "]" << endl;
    }
  }
  
  // Both pools are created in shared memory, 
  // they still need to be initialized
  shm=StreamingPool::CreateStreamingPool(mem, bytesInMainPool_, 8);
  
  // Initialize so other pool can see
  boot->main_pool = shm;
  boot->bytes_in_main_pool = bytesInMainPool_;
  
  // create the mapping names to pipe in shared memory
  Proxy p = Shared(shm, Tab());
  void* proxy_space = shm->allocate(sizeof(Proxy));
  boot->data = new (proxy_space) Proxy(p);
  
  // Tell other pools ready to go
  SHMInitialize(boot, sizeof(Bootstrap_t));
  SHMInitialize(mem, bytesInMainPool_);
  
  // And all done
  state_ = STARTED;
  bootSection_ = boot;
  mainPool_    = shm;
  return state_;
}



// //////////////////////////////////////////////////// SHMConnector_

State_e SHMConnector_::start ()
{
  checkAddressRandomization_();

  if (state_ != STOPPED) return state_;
  state_ = STARTING;

  // Try to map in the bootstrap
  size_t bbytes;
  Bootstrap_t* boot =(Bootstrap_t*)SHMAttach(bootname_.c_str(), 0, 
					     bbytes, debug_);
  if (boot==0) {
    throw runtime_error("Couldn't attach to the bootstrap "
			"session"+bootname_);
  }
  if (sizeof(Bootstrap_t)!=bbytes) {
    if (debug_) {
      cerr << "WARNING: Expected the number of bytes for " 
	   << "bootstrap pool to be the same" << endl;
    }
  }
  
  // Client waits for boot to be ready
  while (!SHMInitialized(boot, sizeof(Bootstrap_t))) {
    if (debug_) {
      cerr << "...trying to attach to boot session " << bootname_ << endl;
    }
    // make sure memory maps before we start looking at it
    usleep(10000);  // wait 1/10th of second 
  }
  if (debug_) {
    cerr << "...client connected to bootstrap mem " << bootname_ 
	 << " at location " << (void*)boot << endl;
  }
  
  // Assertion: boot area mapped in and initialized: get relevant data
  bootSection_          = 0;  // Immediately detached, so not used
  bytesInMainPool_      = boot->bytes_in_main_pool;
  mainPool_             = 0; // Gets filled in below AFTER successful attach
  nameToPipeProxyPtr_   = (Proxy*)boot->data;
  void* start = (void*)boot->main_pool;
  
  // Check and make sure the addresses match
  if (forcedAddr_!=0 && forcedAddr_ != start) {
    string message = "forced address of " + Stringize(uintptr_t(forcedAddr_)) +
      " and found addr of " + Stringize(uintptr_t(start)) + " don't match";
    throw runtime_error(message.c_str());
  }
  
  // Strictly speaking, don't need boot anymore .. throw away!
  // Why? Because it "may" interfere with where main pool ends up
  SHMDetach(boot, sizeof(*boot));
  
  // Client needs to attach to main pool
  size_t bytes;
  void* mem = SHMAttach(name_.c_str(), start, 
			bytes, debug_);
  if (mem==0) {
    throw runtime_error("Couldn't attach to the main session"+name_);
  }
  if (bytes!=bytesInMainPool_) {
    if (debug_) {
      cerr << "WARNING: Expected the number of bytes for bootstrap pool to be the same" << endl;
    }
  }
  StreamingPool* client_shm = (StreamingPool*) mem;
  
  // Client waits for boot to be ready
  while (!SHMInitialized(mem, bytesInMainPool_)) {
    if (debug_) {
      cerr << "...trying to attach to main session" << name_ << endl;
      // make sure memory maps before we start looking at it
      usleep(10000);  // wait 1/10th of second 
    }
  }
  if (debug_) {
    cerr << "...connector connected to main mem " << name_ << " at " 
	 << (void*) mem << endl;
  }
  mainPool_ = client_shm;
  
  // All done
  state_ = STARTED;
  return state_;
}


// //////////////////////////////////////////////////// ServerSide

State_e ServerSide::start () 
{ 
  if (state() != STOPPED) return SHMConnector_::state();
  
  // get the server up and it's memory initialized
  State_e state = SHMConnector_::start();
  
  // Once memory is up, create a CQ pipe
  StreamingPool* pool = this->pool();
  
  char* mem = pool->allocate(sizeof(CQ));
  CQ* cqp = new (mem) CQ(pipeCapacity_, pool, true);
  queue_ = cqp;
  
  // Put name of pipe into table
  { 
    TransactionLock tl(*nameToPipeProxyPtr_);
    Tab& t = *nameToPipeProxyPtr_;
    t[pipename_] = Val(AVLP(cqp));
    if (debug_) {
      cerr << "INSERTED pipename:" << pipename_ << " with " 
	   << t[pipename_] << endl;
    }
  }
  
  return state;
}

// //////////////////////////////////////////////////// ClientSide

State_e ClientSide::start ()
{
  if (state() != STOPPED) return SHMConnector_::state();
  
  // get the server up and it's memory initialized
  State_e state = SHMConnector_::start();
  
  // Once memory is up, get the appropriate pipe
  while (1) {
    {
      TransactionLock tl(*nameToPipeProxyPtr_);
      Tab& t = *nameToPipeProxyPtr_;
      if (debug_) {
	cerr << "... clientside attempting to connect to pipe:" << pipename_
	     << endl;
      }
      if (t.contains(pipename_)) {
	Val& look = t[pipename_];
	AVLP t = look;
	queue_ = (CQ*) t;
	break;
      }
    }
    usleep(300000); // Wait .3 seconds and look again
  }
  
  return state;
}

PTOOLS_END_NAMESPACE
