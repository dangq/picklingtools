///////////////////////////////////////////////////////////////////////////////
//
//  CLASSIFICATION: UNCLASSIFIED
//
//  FILENAME: tab2opal.cc
//
//  DESCRIPTION:
//    Takes in a Tab file and converts it to an Opal table.
//    Will also take in an Opal table and output a Tab file.
//
//  SYNTAX:
//    TAB2OPAL <input>
//
//  REVISION HISTORY:
//    NAME    DATE        DESCRIPTION OF CHANGE
//    ------------------------------------------------------------------------
//    jdg     25Jan08     Initial
//
///////////////////////////////////////////////////////////////////////////////
#include <primitive.h>
#include <iostream>
#include <ocval.h>
#include <ocvalreader.h>
#include <opalutils.h>

#if defined(OC_FORCE_NAMESPACE)
using namespace OC;
#endif


void mainroutine()
{
  string file_in = m_apick(1);

  m_lowercase(file_in);
  string filehead;
  m_head(file_in, filehead);
  string fileroot;
  m_root(file_in, fileroot);
  string fileext;
  m_ext(file_in, fileext);

  string file_out = m_apick(2);
  m_lowercase(file_out);
  if (file_out == " ") {
    file_out = "";
  }

  Tab t;

  bool print_only = (m_get_switch("PRINT") > 0);
  if (Mu->verbose && print_only) {
    m_info("/PRINT switch detected, output to terminal only");
  }

  bool tab_in   = (m_get_switch("TABIN") > 0);
  bool tab_out  = (m_get_switch("TABOUT") > 0);
  bool opal_in  = (m_get_switch("OPALIN") > 0);
  bool opal_out = (m_get_switch("OPALOUT") > 0);

  // Defaults
  if (!tab_in && !opal_in) {
    tab_in = true;
  }
  if (!tab_out && !opal_out) {
    opal_out = true;
  }

  // Error checking
  if (tab_in && opal_in) {
    m_error("/TABIN and /OPALIN switches detected, only one is allowed");
  }
  if (tab_out && opal_out) {
    m_error("/TABOUT and /OPALOUT switches detected, only one is allowed");
  }

  // Info stuff
  if (Mu->verbose && tab_in && opal_out) {
    m_info("TAB->OPAL");
  }
  if (Mu->verbose && tab_in && tab_out) {
    m_info("TAB->TAB");
  }
  if (Mu->verbose && opal_in && opal_out) {
    m_info("OPAL->OPAL");
  }
  if (Mu->verbose && opal_in && tab_out) {
    m_info("OPAL->TAB");
  }

  // Read in the Tab or Opal Table
  if (tab_in) {
    try {
      ReadTabFromFile(file_in, t);
    } catch (const exception& e) {
      if (Mu->verbose) cerr << e.what() << endl;
      try {
        ReadTabFromFile(m_apick(1), t);
      } catch (const exception& e) {
	if (Mu->verbose) cerr << e.what() << endl;
        m_error("Could not read file as tab: " + file_in);
      }
    }
  }
  else if (opal_in) {
    try {
      ReadTabFromOpalFile(file_in, t);
    } catch (const exception& e) {
      if (Mu->verbose) cerr << e.what() << endl;
      try {
        ReadTabFromOpalFile(m_apick(1), t);
      } catch (const exception& e) {
	if (Mu->verbose) cerr << e.what() << endl;
        m_error("Could not read file as opal: " + file_in);
      }
    }
  }
  
  if (Mu->verbose) {
    m_info("Input: " + file_in);
  }

  // Write out the Tab or Opal Table
  if (tab_out) {
    if (print_only) {
      t.prettyPrint( cerr);
    }
    else {
      if (file_out == "") {
        file_out = filehead + fileroot + "_tab.tab";
      }
      try {
        WriteTabToFile(t, file_out);
      } catch (const exception& e) {
	if (Mu->verbose) cerr << e.what() << endl;
        m_error("Could not write file as tab: " + file_out);
      }
    }
  }
  else if (opal_out) {
    if (print_only) {
      prettyPrintOpal(t, cerr);
    }
    else {
      if (file_out == "") {
        file_out = filehead + fileroot + "_opal.tbl";
      }
      try {
        WriteTabToOpalFile(t, file_out);
      } catch (const exception& e) {
	if (Mu->verbose) cerr << e.what() << endl;
        m_error("Could not write file as tbl: " + file_out);
      }
    }
  }

  if (Mu->verbose && !print_only) {
    m_info("Output: " + file_out);
  }

  m_sync();
}
