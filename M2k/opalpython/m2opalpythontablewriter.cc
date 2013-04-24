//


// ///////////////////////////////////////////// Include Files

#include "m2opalpythontablewriter.h"
#include "m2filenames.h"
#include "m2opalpython.h"


// ///////////////////////////////////////////// OpalPythonTableWriter methods

OpalPythonTableWriter::OpalPythonTableWriter () :
  M2ATTR_VAL(FileName, ""),
  M2ATTR(StreamInput),
  M2ATTR_VAL(QueueSize, 2000),
  M2ATTR_VAL(CloseStream, OpalValue()),
  M2ATTR_VAL(WithNumeric, false),
  streamOpen_(false)
{
//  StreamInput.queueSize(2000);
  StreamInput.queueSize(QueueSize.value());
  addWakeUp_(StreamInput, "Stream Input");
  addWakeUp_(QueueSize, "Queue Size");
  addWakeUp_(FileName, "File Name");
}   // OpalPythonTableWriter::OpalPythonTableWriter


bool OpalPythonTableWriter::actOnWakeUp_ (const string& waker_id)
{
  if (waker_id == "Queue Size") {
    //LJB_MSG("Changing queue size to " + Stringize(QueueSize.value()));
//    StreamInput.queueSize(QueueSize.value());
//    StreamInput.broadcast();
  }
  else if (waker_id == "File Name") {
    if (streamOpen_)
      stream_.close();
    string fn = FileName.value();
    try {
      if (Names->extensionOf(fn).length() > 0) {
	stream_.open(getDataPathFor_(fn).c_str(), std::ios::out);
      }
      else {
	stream_.open(getDataPathFor_(fn+".tbl").c_str(), std::ios::out);
      }
      streamOpen_ = true;
    }
    catch (const MidasException& e) {
      log_.error("Problem opening file for writing:\n" + e.problem());
    }
  }
  else if (waker_id == "Close Stream") {
    stream_.close();
    streamOpen_ = false;
  }
  else if (waker_id == "Stream Input") {
    Size entries = StreamInput.queueEntries();
    for (Index i = 0; i < entries; i++) {
      if (DequeueOpalValue(*this, StreamInput, false)) {
	//LJB_MSG("Got OpalValue")
	try {
	  //LJB_POST(streamOpen_)
	  if (streamOpen_) {
	    processStreamInput_(StreamInput.value());
	    //LJB_MSG("streamed")
	  }
        }
	catch (MidasException& e) {
	  log_.warning("Error processing StreamInput");
	}
      }
    }
  }
  else {
    log_.warning("Invalid waking condition");
  }
  return true;
}   // OpalPythonTableWriter::actOnWakeUp_


void OpalPythonTableWriter::processStreamInput_ (const OpalTable& ot)
{
  bool with_numeric = WithNumeric.value();
  OpalValue ov = ot;
  Array<char> a(1024);
  {
    PythonBufferPickler<OpalValue> pbp(a, with_numeric);
    pbp.dump(ov);
  }
  string stringized(a.data(), a.length());
  stream_ << stringized;
}   // OpalPythonTableWriter::processStreamInput_
