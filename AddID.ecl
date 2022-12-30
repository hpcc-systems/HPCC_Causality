/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems.  All rights reserved.
############################################################################## */
/**
  * Prepend a sequential ID field to the input dataset.
  *
  * Examples:
  * <pre>
  *   HC.AddID(dsOrig, dsNew);
  * </pre>
  *
  * @param dIn The name of the input dataset.  Note: This is an attribute name, so
  *               no quotes are used.
  * @param dOut The name of the resulting dataset.  Note: This is attribute name, so
  *               no quotes are used.
  * 
  * @return Nothing. The MACRO creates new attribute in-line as described above.
  */
EXPORT AddId(dsIn, dsOut):=MACRO
  // Import Python for use in the macro
  IMPORT Python3 AS Python;

  // The following two lines produce an 
  // xml string of the input dataset's format.
  #UNIQUENAME(format); %format% := RECORDOF(dsIn);
  #DECLARE(xstr); #EXPORT(xstr,%format%);


  STRING pyFunc(STRING dsname, STRING outname, STRING recxml) := EMBED(Python: fold)
    # First we'll output the format, then the dataset.
    formatStr = """{out}_format := RECORD
      UNSIGNED id;
      {{RECORDOF({dsname})}};
    END;
    """.format(out=outname, dsname=dsname)
    # outname := PROJECT(inDS, TRANSFORM(outname_format, SELF.id := COUNTER, SELF:=LEFT));
    dsStr = """{out}:= PROJECT({dsname}, TRANSFORM({out}_format,
                                SELF.id := COUNTER,
                                SELF := LEFT));
                                """.format(out=outname, dsname=dsname)
    outStr = formatStr + dsStr
    return outStr
  ENDEMBED;
  
  // We call the python function and expand the result inline.  Change pyFunc to match
  // the name of your python function, and add any additional arguments as needed.
  // Retain the signature of the first three arguments.
 
  //dsOut := pyFunc(#TEXT(dsIn), #TEXT(dsOut), %'xstr'%);
  #EXPAND(pyFunc(#TEXT(dsIn), #TEXT(dsOut), %'xstr'%));
ENDMACRO;