#ifndef OCNUMPYTOOLS_H_

// A few helper functions/defines for help for dealing with Numeric
// (Python Numeric)
#include "ocport.h"


OC_BEGIN_NAMESPACE

// Convert from a Val tag to a Python Numeric Tab
inline const char* OCTagToNumPy (char tag)
{
  switch (tag) {
  case 's': return "int8"; break;
  case 'S': return "uint8"; break; 
  case 'i': return "int16"; break; 
  case 'I': return "uint16"; break; 
  case 'l': return "int32"; break; 
  case 'L': return "uint32"; break; 
  case 'x': return "int64"; break; 
  case 'X': return "uint64"; break; 
  case 'b': return "bool"; break; 
  case 'f': return "float32"; break; 
  case 'd': return "float64"; break; 
  case 'F': return "complex64"; break; 
  case 'D': return "complex128"; break; 
  default: throw runtime_error("No corresponding NumPy type for Val type");
  }
  return 0;
}


// Convert from Numeric tag to OC Tag
inline char NumPyStringToOC (const char* tag)
{

  char ret = '*';
  if (tag==NULL || tag[0]=='\0') {
    throw runtime_error("No corresponding OC tag for NumPy tag");
  }
  typedef AVLHashT<string, char, 16> TABLE;
  static TABLE* lookup = 0;
  if (lookup == 0) { // Eh, any thread that reaches here will do a commit
    TABLE& temp = *new TABLE();
    temp["bool"]  = 'b';
    temp["int8"]  = 's';      
    temp["uint8"] = 'S';
    temp["int16"] = 'i';      
    temp["uint16"] ='I';
    temp["int32"] = 'l';     
    temp["uint32"] ='L';
    temp["int64"]  ='x';
    temp["uint64"] ='X';
    temp["float32"]='f';
    temp["float64"]='d';
    temp["complex64"]='F';
    temp["complex128"]='D';
    lookup = &temp;
  }

  //AVLHashTIterator<string, char, 16> it(*lookup);
  //while (it()) {
  //  cout << it.key() << ":" << it.value() << endl;
  //}
  // If found, return full char, otherwise -1 to inidcate it failed
  if (lookup->findValue(tag, ret)) {
    return int_1(ret);
  } else {
    return -1;
  }
}

OC_END_NAMESPACE


#define OCNUMPYTOOLS_H_
#endif // OCNUMPYTOOLS_H_
