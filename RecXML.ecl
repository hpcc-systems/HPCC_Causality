/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems.  All rights reserved.
############################################################################## */
/**
  * RecXML extracts the metadata from a record format as XML.
  * It sets an attribute containing the xml string based on sOut parameter.

  * Examples:
  * <pre>
  *   IMPORT HPCC_causality as HC;
  *   HC.RecXML(myFormat, xmlFmt); // xmlFmt will contain the XML string.
  * </pre>
  *
  * @param rec The name of the record format to interpret.
  * @param sOut The name of the resulting XML string.
  * @return Nothing. The MACRO creates new attributes in-line as described above.
  */
EXPORT RecXML(rec, sOut):=MACRO
  #DECLARE(xstr);
  #EXPORT(xstr,rec);
  #EXPAND(#TEXT(sOut)+':=\'\'\''+%'xstr'%+'\'\'\';');
ENDMACRO;