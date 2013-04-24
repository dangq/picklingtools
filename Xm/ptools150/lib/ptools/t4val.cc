#include <t4val.h>
#include <ocserialize.h>
#include <vector>

PTOOLS_BEGIN_NAMESPACE

const int_4 MAX_BUF_SIZE = 327868/8;

///////////////////////////////////////////////////////////////////////////////
// FUNCTION: initT4Val
//
// DECRIPTION:
//   This function initializes and opens a T4000 file/pipe.
//
///////////////////////////////////////////////////////////////////////////////
void initT4Val(CPHEADER& hcb, string name, int_4 hcbfmode)
{
	m_init(hcb, name, "4000", "", 0);
	if ((hcbfmode & HCBF_INPUT) == HCBF_INPUT) {
		m_open(hcb, hcbfmode);

		// Setup to grab 1 vrb at a time and non-blocking.
		hcb.xfer_len = 1;
		hcb.cons_len = -2;
	}
	else if ((hcbfmode & HCBF_OUTPUT) == HCBF_OUTPUT) {
		hcbfmode |= HCBF_NOABORT;
		m_open(hcb, hcbfmode);
		hcb.vrecord_length = 0;
	}
	else {
		m_error("[initT4Val]: HCBF mode not specified.");
	}
}

///////////////////////////////////////////////////////////////////////////////
// FUNCTION: sendT4Val
//
// DECRIPTION:
//   This function sends a Val through an already open T4000 file/pipe.
//   This is done by using a VRBSTRUCT simply as a pointer to an external
//   buffer which is then sent down the pipe.  The receiving end must know
//   that what it receives is not a normal VRBSTRUCT and handle the buffer
//   appropriately.  In this case the buffer contains a serialized Val
//   object, so the receiver must deserialize it to gain access to the Val.
//   recvT4Val was written for this purpose.
//
// NOTES:
//   In order to use these functions properly, initT4Val should be used
//   initialize the file/pipe and sendT4Val and recvT4Val should be used to
//   add and grab data to or from the file/pipe.
//
///////////////////////////////////////////////////////////////////////////////
void sendT4Val(CPHEADER& hcb, const Val& v, string vrb_key)
{
	// Up front error checking.
  if (hcb.type != 4000) throw logic_error(hcb.file_name + " is not type 4000");
  if (!hcb.open) return;
  
  VRB vrb;
  (void)m_vrbinit(vrb, 0, hcb.vrecord_length);

  int sz = BytesToSerialize(v);
  char* buf = new char[sz];
  Serialize(v, buf);
	
  //int_4 pval =
  (void)m_vrbput(vrb, vrb_key, buf, sz, 0, 'B');
  m_filad(hcb, &vrb, 1);

	m_vrbfree(vrb, 0);

  delete[] buf;
}

///////////////////////////////////////////////////////////////////////////////
// FUNCTION: recvT4Val
//
// DECRIPTION:
//   This function receives a Val through an already open T4000 file/pipe.
//   This is done by using the T4000 VRBSTRUCT.
//
// NOTES:
//   In order to use these functions properly, initT4Val should be used
//   initialize the file/pipe and sendT4Val and recvT4Val should be used to
//   add and grab data to or from the file/pipe.
//   The recvT4Val function is non-blocking, so the returned Val must be
//   emptiness-tested before use.
//
///////////////////////////////////////////////////////////////////////////////
bool recvT4Val(CPHEADER& hcb, Val& v, string vrb_key)
{
  // Up front error checking.
  if (hcb.type != 4000) throw logic_error(hcb.file_name + " is not type 4000");
  if (hcb.cons_len > -2) throw logic_error(hcb.file_name + " uses blocking grabs");
  if (!hcb.open) return false;

  static int ngot;
  VRB vrb;
  vector<char> cut_buf;
  cut_buf.reserve(MAX_BUF_SIZE);

  (void)m_vrbinit(vrb, 0, -1);
  m_vrbclr(vrb, 0);
  m_grabx(hcb, &vrb, ngot);
	
	// A successful ngot is of hcb.xfer_len, which is 1.
  if (ngot != 1) {
		m_vrbfree(vrb, 0);
		return false;
	}

  void* in_buf;
  int_4 mbs = MAX_BUF_SIZE;
  char fmt = 'B';
  int_4 buf_size = m_vrbfind(vrb, vrb_key, in_buf, mbs, 0, fmt);
  if (buf_size != -1) {
    cut_buf.resize(buf_size);
    int num_bytes = m_vrbget(vrb, vrb_key, &cut_buf[0], 
			     buf_size, 0, fmt);
		try {
			v = None;
    	Deserialize(v, &cut_buf[0]);
			m_vrbfree(vrb, 0);
			return (v.tag != 'Z');
		} catch (exception& e) {
			cerr << "[recvT4Val] " << e.what() << endl;
			m_vrbfree(vrb, 0);
			return false;
		}
  }

	m_vrbfree(vrb, 0);
	return false;
}


PTOOLS_END_NAMESPACE
