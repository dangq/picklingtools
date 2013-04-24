
#if defined(OC_FACTOR_INTO_H_AND_CC)
# include "ocserialize.h"

OC_BEGIN_NAMESPACE
  // Explicit instantiations
#define OC_INST_BTS(T) template size_t BytesToSerialize<T>(const Array<T>& a);

OC_INST_BTS(int_1)
OC_INST_BTS(int_u1)
OC_INST_BTS(int_2)
OC_INST_BTS(int_u2)
OC_INST_BTS(int_4)
OC_INST_BTS(int_u4)
OC_INST_BTS(int_8)
OC_INST_BTS(int_u8)
OC_INST_BTS(bool)
OC_INST_BTS(real_4)
OC_INST_BTS(real_8)
OC_INST_BTS(complex_8)
OC_INST_BTS(complex_16)

#define OC_INST_SER(T) template char* Serialize<T>(const Array<T>&, char*);

OC_INST_SER(int_1)
OC_INST_SER(int_u1)
OC_INST_SER(int_2)
OC_INST_SER(int_u2)
OC_INST_SER(int_4)
OC_INST_SER(int_u4)
OC_INST_SER(int_8)
OC_INST_SER(int_u8)
OC_INST_SER(bool)
OC_INST_SER(real_4)
OC_INST_SER(real_8)
OC_INST_SER(complex_8)
OC_INST_SER(complex_16)

OC_END_NAMESPACE

#endif

#include "ochashtable.h"  // Gives me the right HashFunction for OCDumpContext

OC_BEGIN_NAMESPACE

// This is an implementation class:  It allows us to track proxies so
// we don't serialize them twice.
struct OCDumpContext_ {
  OCDumpContext_ (char* start_mem, bool compat) : 
    mem(start_mem), compat_(compat) { }

  char* mem;  // Where we currently are in the buffer we are dumping into

  // Lookup table for looking up the markers:  When a proxy comes
  // in, we need to know if we have seen it before, and if so,
  // what proxy it refers to.  Note that we hold each proxy by
  // pointer because we are ASSUMING that nothing is changing the
  // table in question (and avoids extra locking)
  AVLHashT<void*, int_4, 8> lookup_;

  // Compatibility mode: Starting with PicklingTools 1.2.0, we support
  // OTab and Tup: if previous sytems don't support these, we need to
  // be able to turn OTabs->Tab and Tup->Arr.
  bool compat_;  // true means convert OTab->Tab, Tup->Arr

}; // OCDumpContext_


// Forwards
OC_INLINE size_t BytesToSerialize (const Tab& t, OCDumpContext_& dc);
OC_INLINE size_t BytesToSerialize (const OTab& t, OCDumpContext_& dc);
OC_INLINE size_t BytesToSerialize (const Tup& t, OCDumpContext_& dc);
OC_INLINE size_t BytesToSerialize (const int_n& t, OCDumpContext_& dc);
OC_INLINE size_t BytesToSerialize (const int_un& t, OCDumpContext_& dc);
OC_INLINE size_t BytesToSerialize (const Arr& t, OCDumpContext_& dc);
OC_INLINE void Serialize (const Tab& t, OCDumpContext_& dc);
OC_INLINE void Serialize (const OTab& t, OCDumpContext_& dc);
OC_INLINE void Serialize (const Tup& t, OCDumpContext_& dc);
OC_INLINE void Serialize (const int_n& t, OCDumpContext_& dc);
OC_INLINE void Serialize (const int_un& t, OCDumpContext_& dc);
OC_INLINE void Serialize (const Arr& t, OCDumpContext_& dc);
OC_INLINE void SerializeProxy (const Proxy& p, OCDumpContext_& dc);

#define OCBYTESPROXY(T) { Array<T>*t=(Array<T>*)p.data_();bytes+=BytesToSerialize(*t);}
OC_INLINE size_t BytesToSerializeProxy (const Proxy& p, OCDumpContext_& dc)
{
  size_t bytes = 1+4; // P + marker .. ALWAYS there!

  // Check to see if already been serialized and get the marker,
  // otherwise we'll just be appending new marker
  RefCount_<void*>* handle = (RefCount_<void*>*)p.handle_;
  bool already_serialized = dc.lookup_.contains(handle);
  int_4 marker = (already_serialized)?dc.lookup_(handle):dc.lookup_.entries();

  // Just need marker and handle if serialized, otherwise, a lot more!
  if (!already_serialized) {
    dc.lookup_[handle] = marker;     // Put marker in table
    bytes += 3;  // locked+adopted+allocator  (plus P + marker already there)
    switch (p.tag) {
    case 't': { Tab*t=(Tab*)p.data_(); bytes+=BytesToSerialize(*t,dc); } break;
    case 'o': { OTab*t=(OTab*)p.data_();bytes+=BytesToSerialize(*t,dc);} break;
    case 'u': { Tup*t=(Tup*)p.data_(); bytes+=BytesToSerialize(*t,dc); } break;
    case 'n': {
      switch (p.subtype) {
      case 's': OCBYTESPROXY(int_1);  break;
      case 'S': OCBYTESPROXY(int_u1); break;
      case 'i': OCBYTESPROXY(int_2);  break;
      case 'I': OCBYTESPROXY(int_u2); break;
      case 'l': OCBYTESPROXY(int_4);  break;
      case 'L': OCBYTESPROXY(int_u4); break;
      case 'x': OCBYTESPROXY(int_8);  break;
      case 'X': OCBYTESPROXY(int_u8); break;
      case 'b': OCBYTESPROXY(bool);   break;
      case 'f': OCBYTESPROXY(real_4); break;
      case 'd': OCBYTESPROXY(real_8); break;
      case 'F': OCBYTESPROXY(complex_8); break;
      case 'D': OCBYTESPROXY(complex_16); break;
      case 'Z': { Arr*t=(Arr*)p.data_(); bytes+=BytesToSerialize(*t); } break;
      case 'a': { Array<OCString>*t=(Array<OCString>*)p.data_(); bytes+=BytesToSerialize(*t); } break;
      case 't': { Array<Tab>*t=(Array<Tab>*)p.data_(); bytes+=BytesToSerialize(*t); } break;
      case 'n': 
      default: unknownType_("BytesToSerializeProxyPreamble", p.subtype);
      }
      break;
    }
    default: unknownType_("BytesToSerializeProxyPreamble", p.tag);
    } 
  }
  return bytes;
}

#define VALCOMPUTE(T,N) bytes+=sizeof(T)
#define VALARRCOMPUTE(T) {Array<T>*ap=(Array<T>*)&v.u.n;bytes=BytesToSerialize(*ap);}
OC_INLINE size_t BytesToSerialize (const Val& v, OCDumpContext_& dc) 
{
  size_t bytes = 1; // At least one for tag

  if (IsProxy(v)) {
    // Preamble
    bytes = BytesToSerializeProxy(v, dc);
    return bytes;
  }

  // Otherwise, not a proxy and we can get the impl straight out
  switch(v.tag) {
  case 's': VALCOMPUTE(int_1,  v.u.s); break;
  case 'S': VALCOMPUTE(int_u1, v.u.S); break;
  case 'i': VALCOMPUTE(int_2,  v.u.i); break;
  case 'I': VALCOMPUTE(int_u2, v.u.I); break;
  case 'l': VALCOMPUTE(int_4,  v.u.l); break;
  case 'L': VALCOMPUTE(int_u4, v.u.L); break;
  case 'x': VALCOMPUTE(int_8,  v.u.x); break;
  case 'X': VALCOMPUTE(int_u8, v.u.X); break;
  case 'b': VALCOMPUTE(bool,   v.u.b); break;
  case 'f': VALCOMPUTE(real_4, v.u.f); break;
  case 'd': VALCOMPUTE(real_8, v.u.d); break;
  case 'F': VALCOMPUTE(complex_8, v.u.F); break;
  case 'D': VALCOMPUTE(complex_16, v.u.D); break;
  case 'a': { OCString*sp=(OCString*)&v.u.a;bytes=BytesToSerialize(*sp);break;}
  case 't': { Tab*tp=(Tab*)&v.u.t; bytes=BytesToSerialize(*tp, dc); break;}
  case 'o': { OTab*tp=(OTab*)&v.u.o; bytes=BytesToSerialize(*tp, dc); break;}
  case 'u': { Tup*tp=(Tup*)&v.u.u; bytes=BytesToSerialize(*tp, dc); break;}
  case 'q': { int_n*tp=(int_n*)&v.u.q; bytes=BytesToSerialize(*tp, dc); break;}
  case 'Q': { int_un*tp=(int_un*)&v.u.Q;bytes=BytesToSerialize(*tp, dc);break;}
  case 'n': { 
    switch (v.subtype) {
    case 's': VALARRCOMPUTE(int_1);  break;
    case 'S': VALARRCOMPUTE(int_u1); break;
    case 'i': VALARRCOMPUTE(int_2);  break;
    case 'I': VALARRCOMPUTE(int_u2); break;
    case 'l': VALARRCOMPUTE(int_4);  break;
    case 'L': VALARRCOMPUTE(int_u4); break;
    case 'x': VALARRCOMPUTE(int_8);  break;
    case 'X': VALARRCOMPUTE(int_u8); break;
    case 'b': VALARRCOMPUTE(bool);   break;
    case 'f': VALARRCOMPUTE(real_4); break;
    case 'd': VALARRCOMPUTE(real_8); break;
    case 'F': VALARRCOMPUTE(complex_8); break;
    case 'D': VALARRCOMPUTE(complex_16); break;
    case 'a': 
    case 't':
    case 'o': 
    case 'u': throw logic_error("Can't have arrays of non-POD data");
    case 'n': throw logic_error("Can't have arrays of arrays");
    case 'Z': {Arr*ap=(Arr*)&v.u.n;bytes=BytesToSerialize(*ap, dc); break;}
    }
  }
  case 'Z': break; // Just a tag
  default: unknownType_("BytesToSerialize", v.tag);
  }
  return bytes;
}

#if !defined(OC_USE_OC_STRING)
OC_INLINE size_t BytesToSerialize (const Str& s)
{ return 1 + sizeof(int_u4) + s.length(); } // 'a' tag, then length, then data
#endif

//#if !defined(OC_USE_OC_STRING)
OC_INLINE size_t BytesToSerialize (const OCString& s)
{ return 1 + sizeof(int_u4) + s.length(); } // 'a' tag, then length, then data
//#endif 

OC_INLINE size_t BytesToSerialize (const Tab& t, OCDumpContext_& dc)
{
  // A 't' marker (actually single byte, not the full Val) starts, plus len
  size_t bytes = 1 + sizeof(int_u4);
  // ... then key/value pairs.  When we look for a key and see a None
  // marker, then we know we are at the end of the table.
  for (It ii(t); ii(); ) {
    bytes += BytesToSerialize(ii.key(),dc) + BytesToSerialize(ii.value(),dc);
  }
  return bytes;
}

OC_INLINE size_t BytesToSerialize (const OTab& o, OCDumpContext_& dc)
{
  // As Tab if in compat mode
  if (dc.compat_) { 
    // No difference in size .. this if just here to remind us to
    // think about compatibility
  }

  // A 'o' marker (actually single byte, not the full Val) starts, plus len
  size_t bytes = 1 + sizeof(int_u4);
  // ... then key/value pairs.  When we look for a key and see a None
  // marker, then we know we are at the end of the table.
  for (It ii(o); ii(); ) {
    bytes += BytesToSerialize(ii.key(),dc) + BytesToSerialize(ii.value(),dc);
  }
  return bytes;
}


OC_INLINE size_t BytesToSerialize (const Tup& t, OCDumpContext_& dc)
{
  // As Arr if in compat mode.
  if (dc.compat_) { 
    return BytesToSerialize((Arr&)t.impl(), dc); 
  }

  // An 'u' marker, a 'Z' marker then length
  size_t bytes = 1+1 + sizeof(int_u4);

  // Then n vals
  // ... then n vals. 
  const int len = t.length();
  for (int ii=0; ii<len; ii++) { 
    bytes += BytesToSerialize(t[ii],dc);
  }
  return bytes;
}

OC_INLINE size_t BytesToSerialize (const int_n& t, OCDumpContext_& dc)
{
  // As Str if in compat mode.
  if (dc.compat_) { 
    string s = t.stringize();
    return BytesToSerialize(s); 
  }

  // An 'q' marker, int_u4 len, then len bytes
  size_t bytes = 1+sizeof(int_u4);
  string s = MakeBinaryFromBigInt(t); // is there a better way? maybe
  bytes+= s.length();
  return bytes;
}

OC_INLINE size_t BytesToSerialize (const int_un& t, OCDumpContext_& dc)
{
  // As Str if in compat mode.
  if (dc.compat_) { 
    string s = t.stringize();
    return BytesToSerialize(s); 
  }

  // An 'q' marker, int_u4 len, then len bytes
  size_t bytes = 1+sizeof(int_u4);
  string s = MakeBinaryFromBigUInt(t); // is there a better way? maybe
  bytes+= s.length();
  return bytes;
}


OC_INLINE size_t BytesToSerialize (const Arr& a, OCDumpContext_& dc)
{
  // An 'n' marker (actually single byte, not the full Val) starts,
  // the the subtype (a Z for Vals) 
  // then the length ....
  size_t bytes = 1 + 1 + sizeof(int_u4);
 
  // ... then n vals. 
  const int len = a.length();
  for (int ii=0; ii<len; ii++) { 
    bytes += BytesToSerialize(a[ii],dc);
  }
  return bytes;
}


template <class T>
OC_INLINE size_t BytesToSerialize (const Array<T>& a)
{
  // An 'n' marker (actually single byte, not the full Val) starts,
  // then the subtype of the array, then the length
  size_t bytes = 1 + 1 + sizeof(int_u4);
 
  // ... then n vals. 
  const int len = a.length() * sizeof(T);
  return bytes + len;
}



// Macro for copying into buffer with right types/fields
#define VALCOPY(T,N) { memcpy(mem,&N,sizeof(T));mem+=sizeof(T); }
#define VVVCOPY(T,N) { *mem++=v.tag; memcpy(mem,&N,sizeof(T));mem+=sizeof(T); }
#define VALARRCOPY(T) { Array<T>*ap=(Array<T>*)&v.u.n;mem=Serialize(*ap, mem);}
OC_INLINE void Serialize (const Val& v, OCDumpContext_& dc)
{
  char*& mem = dc.mem; // Important that this is a RERERENCE!

  if (IsProxy(v)) { return SerializeProxy(v, dc); }

  switch(v.tag) {
  case 's': VVVCOPY(int_1,  v.u.s); break;
  case 'S': VVVCOPY(int_u1, v.u.S); break;
  case 'i': VVVCOPY(int_2,  v.u.i); break;
  case 'I': VVVCOPY(int_u2, v.u.I); break;
  case 'l': VVVCOPY(int_4,  v.u.l); break;
  case 'L': VVVCOPY(int_u4, v.u.L); break;
  case 'x': VVVCOPY(int_8,  v.u.x); break;
  case 'X': VVVCOPY(int_u8, v.u.X); break;
  case 'b': VVVCOPY(bool, v.u.b); break;
  case 'f': VVVCOPY(real_4, v.u.f); break;
  case 'd': VVVCOPY(real_8, v.u.d); break;
  case 'F': VVVCOPY(complex_8, v.u.F); break;
  case 'D': VVVCOPY(complex_16, v.u.D); break;
  case 'a': { OCString*sp=(OCString*)&v.u.a; mem=Serialize(*sp,mem);break;}
  case 't': { Tab*tp=(Tab*)&v.u.t; Serialize(*tp,dc);break; }
  case 'o': { OTab*tp=(OTab*)&v.u.o; Serialize(*tp,dc);break; }
  case 'u': { Tup*tp=(Tup*)&v.u.u; Serialize(*tp,dc);break; }
  case 'q': { int_n*ip=(int_n*)&v.u.q; Serialize(*ip,dc);break; }
  case 'Q': { int_un*ip=(int_un*)&v.u.Q; Serialize(*ip,dc);break; }
  case 'n': { 
    switch (v.subtype) {
    case 's': VALARRCOPY(int_1);  break;
    case 'S': VALARRCOPY(int_u1); break;
    case 'i': VALARRCOPY(int_2);  break;
    case 'I': VALARRCOPY(int_u2); break;
    case 'l': VALARRCOPY(int_4);  break;
    case 'L': VALARRCOPY(int_u4); break;
    case 'x': VALARRCOPY(int_8);  break;
    case 'X': VALARRCOPY(int_u8); break;
    case 'b': VALARRCOPY(bool); break;
    case 'f': VALARRCOPY(real_4); break;
    case 'd': VALARRCOPY(real_8); break;
    case 'F': VALARRCOPY(complex_8); break;
    case 'D': VALARRCOPY(complex_16); break;
    case 'a': 
    case 't': 
    case 'o': 
    case 'u': throw logic_error("Can't have arrays of non POD data");
    case 'n': throw logic_error("Can't have arrays of arrays");
    case 'Z': { Arr*ap=(Arr*)&v.u.n;Serialize(*ap,dc); break;}
    }
    break;
  }
  case 'Z': *mem++ = v.tag; break;
  default: unknownType_("Serialize into circular buffer", v.tag);
  }
}


#define OCSERPROXY(T) { Array<T>*t=(Array<T>*)p.data_(); dc.mem=Serialize(*t, dc.mem);}
OC_INLINE void SerializeProxy (const Proxy& p, OCDumpContext_& dc)
{
  char*& mem = dc.mem;

  // Check to see if already been serialized and get the marker,
  // otherwise we'll just be appending new marker
  RefCount_<void*>* handle = (RefCount_<void*>*)p.handle_;
  bool already_serialized = dc.lookup_.contains(handle);
  int_4 marker = (already_serialized)?dc.lookup_(handle):dc.lookup_.entries();

  // Always do at least this
  *mem++ = 'P';
  VALCOPY(int_u4, marker);
  if (already_serialized) return;  // All done is proxy already in table

  // Preamble for proxy
  // copy in the Proxy marker, and proxy flags:adopt flag, lock flag, alloc ind
  *mem++ = p.adopt;
  *mem++ = p.lock;
  *mem++ = ' ';  // TODO: Allocator indicator

  // Now plop in main proxy
  dc.lookup_[handle] = marker;     // Put marker in table
  switch (p.tag) {
  case 't': { Tab*t=(Tab*)p.data_(); Serialize(*t, dc); } break;
  case 'o': { OTab*t=(OTab*)p.data_(); Serialize(*t, dc); } break;
  case 'u': { Tup*t=(Tup*)p.data_(); Serialize(*t, dc); } break;
  case 'n': {
    switch (p.subtype) {
    case 's': OCSERPROXY(int_1);  break;
    case 'S': OCSERPROXY(int_u1); break;
    case 'i': OCSERPROXY(int_2);  break;
    case 'I': OCSERPROXY(int_u2); break;
    case 'l': OCSERPROXY(int_4);  break;
    case 'L': OCSERPROXY(int_u4); break;
    case 'x': OCSERPROXY(int_8);  break;
    case 'X': OCSERPROXY(int_u8); break;
    case 'b': OCSERPROXY(bool);   break;
    case 'f': OCSERPROXY(real_4); break;
    case 'd': OCSERPROXY(real_8); break;
    case 'F': OCSERPROXY(complex_8); break;
    case 'D': OCSERPROXY(complex_16); break;
    case 'a': 
    case 't': throw logic_error("Can't have arrays of strings or tables");
    case 'Z': { Arr*t=(Arr*)p.data_(); Serialize(*t, dc); } break;
    case 'n': throw logic_error("Can't have arrays of arrays");
    default: unknownType_("SerializeProxy: Array of ", p.subtype);
    }
    break;
  }
  default: unknownType_("SerializeProxy:", p.tag);
  } 
}


#if !defined(OC_USE_OC_STRING)
OC_INLINE char* Serialize (const Str& s, char* mem, bool)
{
  *mem++ = 'a'; // Always need tag
  // Strings: 'a', int_u4 length, (length) bytes of data
  const int_u4 len=s.length(); 
  VALCOPY(int_u4, len);
  memcpy(mem, s.c_str(), len);
  mem += len;
  return mem;
}
#endif

//#if !defined OC_USE_OC_STRING
OC_INLINE char* Serialize (const OCString& s, char* mem)
{
  *mem++ = 'a'; // Always need tag
  // Strings: 'a', int_u4 length, (length) bytes of data
  const int_u4 len=s.length(); 
  VALCOPY(int_u4, len);
  memcpy(mem, s.c_str(), len);
  mem += len;
  return mem;
}
//#endif


OC_INLINE void Serialize (const Tab& t, OCDumpContext_& dc)
{
  char*& mem = dc.mem;

  *mem++ = 't'; // Always need tag
  // Tables: 't', int_u4 length, (length) Key/Value pairs
  const int_u4 len = t.entries();
  VALCOPY(int_u4, len);
  for (It ii(t); ii(); ) {
    const Val& key = ii.key();
    Serialize(key, dc);

    const Val& value = ii.value();
    Serialize(value, dc);
  }
}

OC_INLINE void Serialize (const OTab& t, OCDumpContext_& dc)
{
  char*& mem = dc.mem;

  // Always need tag .. serializes same way except for tag
  *mem++ = dc.compat_ ? 't' : 'o'; 
  //*mem++ = 'o'; // Always need tag
  // Tables: 't' or 'o', int_u4 length, (length) Key/Value pairs
  const int_u4 len = t.entries();
  VALCOPY(int_u4, len);
  for (It ii(t); ii(); ) {
    const Val& key = ii.key();
    Serialize(key, dc);

    const Val& value = ii.value();
    Serialize(value, dc);
  }
}

OC_INLINE void Serialize (const Tup& t, OCDumpContext_& dc)
{
  if (dc.compat_) { 
    Serialize((Arr&)t.impl(), dc);
    return;
  }

  char*& mem = dc.mem;

  // Tup: 'u', 'Z' subtype, int_u4 length, (length) vals
  *mem++ = 'u'; // Always need tag
  *mem++ = 'Z'; // Strictly no necessary, but makes deserialize code a little easier
  const int_u4 len = t.length();
  VALCOPY(int_u4, len);

  int ilen = len;
  for (int ii=0; ii<ilen; ii++) {
    Serialize(t[ii], dc);
  }
}

OC_INLINE void Serialize (const int_n& t, OCDumpContext_& dc)
{
  if (dc.compat_) { 
    const Str s = t.stringize();
    dc.mem = Serialize(s, dc.mem);
    return;
  }

  char*& mem = dc.mem;

  // int_n: 'q', int_u4 length, (length) chars
  *mem++ = 'q'; // Always need tag
  string repr = MakeBinaryFromBigInt(t);
  const int_u4 len = repr.length();
  VALCOPY(int_u4, len);

  memcpy(mem, repr.data(), len);
  mem += len;
}

OC_INLINE void Serialize (const int_un& t, OCDumpContext_& dc)
{
  if (dc.compat_) { 
    const Str s = t.stringize();
    dc.mem = Serialize(s, dc.mem);
    return;
  }

  char*& mem = dc.mem;

  // int_n: 'q', int_u4 length, (length) chars
  *mem++ = 'q'; // Always need tag
  string repr = MakeBinaryFromBigUInt(t);
  const int_u4 len = repr.length();
  VALCOPY(int_u4, len);

  memcpy(mem, repr.data(), len);
  mem += len;
}


// Specialization because Arrays of Vals serialize differently
OC_INLINE void Serialize (const Arr& a, OCDumpContext_& dc)
{
  char*& mem = dc.mem;

  // Arrays: 'n', subtype, int_u4 length, (length) vals
  *mem++ = 'n'; // Always need tag
  *mem++ = 'Z'; // subtype
  const int_u4 len = a.length();
  VALCOPY(int_u4, len);

  int ilen = len;
  for (int ii=0; ii<ilen; ii++) {
    Serialize(a[ii], dc);
  }
}


template <class T>
OC_INLINE char* Serialize (const Array<T>& a, char* mem)
{
  // Arrays: 'n', subtype, int_u4 length, (length) vals
  *mem++ = 'n'; // Always need tag

  Val sub_type = T();
  *mem++ = sub_type.tag; // subtype

  const int_u4 len = a.length(); // int_u4 len 
  VALCOPY(int_u4, len);

  Array<T>& a_non = const_cast<Array<T>&>(a);
  const int_u4 byte_len = sizeof(T)*len;
  const T* a_data = a_non.data();

  memcpy(mem, a_data, byte_len);
  mem += byte_len;  

  return mem;
}


// These are really just for backwards compatibility: We really should
// be supporting the "Dump(..OCDumpContext)" iterface, but backwards
// compatibility is king.
OC_INLINE size_t BytesToSerialize (const Val& v, bool compat) 
{ OCDumpContext_ dc(0,compat); return BytesToSerialize(v, dc); }
OC_INLINE size_t BytesToSerialize (const Tab& t, bool compat) 
{ OCDumpContext_ dc(0,compat); return BytesToSerialize(t, dc); }
OC_INLINE size_t BytesToSerialize (const OTab& t, bool compat) 
{ OCDumpContext_ dc(0,compat); return BytesToSerialize(t, dc); }
OC_INLINE size_t BytesToSerialize (const Tup& t, bool compat) 
{ OCDumpContext_ dc(0,compat); return BytesToSerialize(t, dc); }
OC_INLINE size_t BytesToSerialize (const int_n& t, bool compat) 
{ OCDumpContext_ dc(0,compat); return BytesToSerialize(t, dc); }
OC_INLINE size_t BytesToSerialize (const int_un& t, bool compat) 
{ OCDumpContext_ dc(0,compat); return BytesToSerialize(t, dc); }
OC_INLINE size_t BytesToSerialize (const Arr& a, bool compat) 
{ OCDumpContext_ dc(0,compat); return BytesToSerialize(a, dc); }

OC_INLINE char* Serialize (const Val& v, char* mem, bool compat)
{ OCDumpContext_ dc(mem,compat); Serialize(v, dc); return dc.mem; }
OC_INLINE char* Serialize (const Tab& t, char* mem, bool compat)
{ OCDumpContext_ dc(mem,compat); Serialize(t, dc); return dc.mem; }
OC_INLINE char* Serialize (const OTab& t, char* mem, bool compat)
{ OCDumpContext_ dc(mem,compat); Serialize(t, dc); return dc.mem; }
OC_INLINE char* Serialize (const Tup& t, char* mem, bool compat)
{ OCDumpContext_ dc(mem,compat); Serialize(t, dc); return dc.mem; }
OC_INLINE char* Serialize (const int_n& t, char* mem, bool compat)
{ OCDumpContext_ dc(mem,compat); Serialize(t, dc); return dc.mem; }
OC_INLINE char* Serialize (const int_un& t, char* mem, bool compat)
{ OCDumpContext_ dc(mem,compat); Serialize(t, dc); return dc.mem; }
OC_INLINE char* Serialize (const Arr& a, char* mem, bool compat)
{ OCDumpContext_ dc(mem,compat); Serialize(a, dc); return dc.mem; }

/////////////////////////// Deserialize

// Helper class to keep track of all the Proxy's we've seen so we don't have
// to unserialize again
struct OCLoadContext_ {
  OCLoadContext_ (char* start_mem, bool compat) : 
    mem(start_mem), compat_(compat) { }

  char* mem; // Where we are in the buffer
  // When we see a marker, see if we have already deserialized it.  If
  // we see it, there is some Proxys already out there
  AVLHashT<int_4, Proxy, 8> lookup_;
  
  // See OCDumpContext for discussion of compat_
  bool compat_;

}; // OCLoadContext_

// Forward
OC_INLINE void DeserializeProxy (Val& v, OCLoadContext_& lc);



#define VALDECOPY(T,N) { memcpy(&N,mem,sizeof(T)); mem+=sizeof(T); }
#define VALDECOPY2(T) { Array<T>*ap = (Array<T>*)&v.u.n; new (ap) Array<T>(len); ap->expandTo(len); memcpy(ap->data(),mem,sizeof(T)*len); mem+=sizeof(T)*len; }
#define VALDECOPY3(T) {Array<T>*ap=(Array<T>*)&v.u.n;new(ap)Array<T>(len); for(int ii=0;ii<len;ii++){ap->append(T());Deserialize((*ap)[ii], lc);}}
#define VALDECOPY4(T) {Array<T>*ap=(Array<T>*)&v.u.n;new(ap)Array<T>(len); for(int ii=0;ii<len;ii++){ap->append(T());Val temp;Deserialize(temp, lc); (*ap)[ii]=temp;}}
OC_INLINE void Deserialize (Val& v, OCLoadContext_& lc)
{
  char*& mem = lc.mem;

  if (v.tag!='Z') { // Don't let anything be serialized EXCEPT empty Val
    throw logic_error("You can only deserialize into an empty Val.");
  }
  
  if (*mem=='P') { 
    DeserializeProxy(v, lc); 
    return;
  }

  v.tag = *mem++; // Grab tag 
  switch(v.tag) {
  case 's': VALDECOPY(int_1,  v.u.s); break;
  case 'S': VALDECOPY(int_u1, v.u.S); break;
  case 'i': VALDECOPY(int_2,  v.u.i); break;
  case 'I': VALDECOPY(int_u2, v.u.I); break;
  case 'l': VALDECOPY(int_4,  v.u.l); break;
  case 'L': VALDECOPY(int_u4, v.u.L); break;
  case 'x': VALDECOPY(int_8,  v.u.x); break;
  case 'X': VALDECOPY(int_u8, v.u.X); break;
  case 'b': VALDECOPY(bool,   v.u.b); break;
  case 'f': VALDECOPY(real_4, v.u.f); break;
  case 'd': VALDECOPY(real_8, v.u.d); break;
  case 'F': VALDECOPY(complex_8, v.u.F); break;
  case 'D': VALDECOPY(complex_16, v.u.D); break;

  case 'q': { 
    int_u4 len; VALDECOPY(int_u4, len);
    int_n* ip=(int_n*)&v.u.q; 
    new (ip) int_n;
    MakeBigIntFromBinary(mem, len, *ip);
    mem += len;
    if (lc.compat_) {
      string s = ip->stringize();
      v = s;
    }
    break;
  }
  case 'Q': { 
    int_u4 len; VALDECOPY(int_u4, len);
    int_un* ip=(int_un*)&v.u.Q; 
    new (ip) int_n;
    MakeBigUIntFromBinary(mem, len, *ip);
    mem += len;
    if (lc.compat_) {
      string s = ip->stringize();
      v = s;
    }
    break;
  }

  case 'P': // already handled above 

  case 'a':
    { // Strs: 'a' then int_u4 len, then len bytes
      int_u4 len; VALDECOPY(int_u4, len);
      OCString* sp = (OCString*)&v.u.a; new (sp) OCString(mem, len);
      mem += len;
      break;
    } 
  case 't':  // Both deserialize very similarly
  case 'o':
    if (lc.compat_ || v.tag=='t') {
      { // Tables: 't', then int_u4 length, then length ley/value pairs
	v.tag = 't';
	int_u4 len; VALDECOPY(int_u4, len);
	Tab* tp = (Tab*)&v.u.t; new (tp) Tab(); 
	int ilen = len;
	for (int ii=0; ii<ilen; ii++) {
	  Val key;
	  Deserialize(key, lc);    // Get the key
	  Val& value = (*tp)[key];        // Insert it with a default Val
	  Deserialize(value, lc);  // ... copy in the REAL value
	}
	break;
      }
    } else { 
      { // Tables: 'o', then int_u4 length, then length ley/value pairs
	int_u4 len; VALDECOPY(int_u4, len);
	OTab* tp = (OTab*)&v.u.o; new (tp) OTab(); 
	int ilen = len;
	for (int ii=0; ii<ilen; ii++) {
	  Val key;
	  Deserialize(key, lc);    // Get the key
	  Val& value = (*tp)[key];        // Insert it with a default Val
	  Deserialize(value, lc);  // ... copy in the REAL value
	}
      }
      break;
    }
  case 'u':
  case 'n': 
    { // Arrays: 'n', then subtype, then int_u4 len, then number of
      // values following)
      v.subtype = *mem++;
      int_u4 len; VALDECOPY(int_u4, len);
      int ilen = len;
      switch(v.subtype) {
      case 's': VALDECOPY2(int_1);  break;
      case 'S': VALDECOPY2(int_u1); break;
      case 'i': VALDECOPY2(int_2);  break;
      case 'I': VALDECOPY2(int_u2); break;
      case 'l': VALDECOPY2(int_4);  break;
      case 'L': VALDECOPY2(int_u4); break;
      case 'x': VALDECOPY2(int_8);  break;
      case 'X': VALDECOPY2(int_u8); break;
      case 'b': VALDECOPY2(bool); break;
      case 'f': VALDECOPY2(real_4); break;
      case 'd': VALDECOPY2(real_8); break;
      case 'F': VALDECOPY2(complex_8); break;
      case 'D': VALDECOPY2(complex_16); break;
      case 'a': {
	Array<Str>*ap=(Array<Str>*)&v.u.n;
	new (ap) Array<Str>(len); 
	for(int ii=0;ii<ilen;ii++) {
	  ap->append("");
	  // Strs: 'a' then int_u4 len, then len bytes
	  int_u4 len; VALDECOPY(int_u4, len);
	  (*ap)[ii] = Str(mem, len);
	  mem += sizeof(Str);
	}
	break;
      }
      case 't': 
      case 'o': if (lc.compat_ || v.subtype=='t') 
	{
	  //v.subtype = 't';
	  Array<Tab>*ap=(Array<Tab>*)&v.u.n;
	  new (ap) Array<Tab>(len); 
	  for(int ii=0;ii<ilen;ii++) {
	    ap->append(Tab());
	    // Tables: 't', then int_u4 length, then length ley/value pairs
	    int_u4 len; VALDECOPY(int_u4, len);
	    Tab& tp = (*ap)[ii];
	    int ilen = len;
	    for (int ii=0; ii<ilen; ii++) {
	      Val key;
	      Deserialize(key, lc);    // Get the key
	      Val& value = tp[key];        // Insert it with a default Val
	      Deserialize(value, lc);  // ... copy in the REAL value
	    }
	  }
	} else {
	  Array<OTab>*ap=(Array<OTab>*)&v.u.n;
	  new (ap) Array<OTab>(len); 
	  for(int ii=0;ii<ilen;ii++) {
	    ap->append(OTab());
	    // Tables: 'o', then int_u4 length, then length ley/value pairs
	    int_u4 len; VALDECOPY(int_u4, len);
	    OTab& tp = (*ap)[ii];
	    int ilen = len;
	    for (int ii=0; ii<ilen; ii++) {
	      Val key;
	      Deserialize(key, lc);    // Get the key
	      Val& value = tp[key];        // Insert it with a default Val
	      Deserialize(value, lc);  // ... copy in the REAL value
	    }
	  }
	}
	break;
      case 'u': throw logic_error("Can't have POD arrays of tuples");
      case 'n': throw logic_error("Can't have POD arrays of POD arrays");
      case 'Z': 
	if (lc.compat_ || v.tag=='n') {
	  v.tag = 'n'; v.subtype = 'Z';
	  Arr*ap=(Arr*)&v.u.n;
	  new (ap) Arr(len); 
	  for(int ii=0; ii<ilen; ii++) {
	    ap->append(Val());
	    Deserialize((*ap)[ii], lc);
	  }
	} else {
	  Tup*tp=(Tup*)&v.u.u;
	  new (tp) Tup();
	  Arr& impl = (Arr&)tp->impl();
	  for(int ii=0; ii<ilen; ii++) {
	    impl.append(Val());
	    Deserialize(impl[ii], lc);
	  }
	}
	break;
      
      default: unknownType_("Deserialize Array", v.subtype);
      }
      break;
    }
  case 'Z': break; // Already copied the tag out, nothing else
  default: unknownType_("Deserialize", v.tag);
  }

}


OC_INLINE void DeserializeProxy (Val& v, OCLoadContext_& lc)
{
  char*& mem = lc.mem;  // HAVE NOT SEEN THE 'P'!!
  if (*mem++!='P') throw runtime_error("load:not starting Proxy");
  int_4 marker;
  VALDECOPY(int_4, marker);

  bool already_seen = lc.lookup_.contains(marker);
  if (already_seen) {
    v = lc.lookup_(marker);     // There, just attach to new Proxy
    return;
  }

  // Assertion: NOT AVAILABLE! The first time we have deserialized
  // this proxy, so we have to completely pull it out.  First,
  // Deserialize the rest of the preamble
  bool adopt, lock, shm;
  VALDECOPY(bool, adopt);
  VALDECOPY(bool, lock);
  VALDECOPY(bool, shm);  // TODO; do something with this?

  // Now, Deserialize the full body as normal, then turn into proxy in
  // O(1) time
  Deserialize(v, lc);   
  v.Proxyize(adopt, lock);

  // Add marker back in
  lc.lookup_[marker] = v;
}



// Backwards compatibility
char* Deserialize (Val& v, char* mem, bool compatibility)
{
  OCLoadContext_ lc(mem, compatibility); 
  Deserialize(v, lc);
  return lc.mem;
}


OC_END_NAMESPACE
