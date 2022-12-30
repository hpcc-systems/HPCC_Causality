/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems.  All rights reserved.
############################################################################## */
/**
  * Convert a record-oriented dataset to a cell-oriented "AnyField" dataset,
  * where each field may contain textual or numeric data.
  * <p>ToField Macro takes a record-oriented dataset, with each row containing
  * an ID and one or more additional fields, and expands it into the
  * AnyField format used by Causality.
  *
  * <p>Note that as a Macro, nothing is returned, but new attributes are created
  * in-line for use in subsequent definitions.
  *
  * <p>Along with creating the NumericField table, this macro produces two
  * simple functions to assist the user in mapping the field names to their
  * corresponding numbers.  These are "STRING dOut_ToName(UNSIGNED)" and
  * "UNSIGNED dOut_ToNumber(STRING)", where the "dOut" portion of the function
  * name is the name passed into that parameter of the macro.
  *
  * Examples:
  * <pre>
  *   HC.ToAnyField(dOrig,dAF);
  *   ML.ToField(dOrig, dAF,,'age,weight,height,gender'); // Only return the four named fields.
  *   ML.ToField(dOrig, dAF, 'personId'); // Use personId as the unique record id.
  *   dAF_fields;    // returns a set of all fields in the correct order.
  * </pre>
  *
  * @param dIn The name of the input dataset.  Note: This is an attribute name, so
  *               no quotes are used.
  * @param dOut The name of the resulting dataset.  Note: This is attribute name, so
  *               no quotes are used.
  * @param idfield [OPTIONAL] The name of the field that contains the Record ID for
  *                each row.  If omitted, it is assumed to be the first field.
  *                Note: This is a field name and should be in quotes.
  * @param datafields [OPTIONAL] A STRING containing a comma-delimited list of the
  *                   fields to include.  If omitted, all numeric
  *                   fields are included.
  * 
  * @return Nothing. The MACRO creates new attributes in-line as described above.
  */
EXPORT ToAnyField(dIn, dOut, idfield='', datafields=''):=MACRO
  IMPORT Python3 AS Python;
  IMPORT HPCC_causality AS _HC_;
  IMPORT HC.Types AS _Types_;

  HC.RecXML(RECORDOF(dIn), xmlstr);

  STRING pyToAF(STRING dsname, STRING outname, STRING recxml, STRING idfld='', STRING dataflds='') := EMBED(Python: fold)
      import xml.etree.ElementTree as ET
      values = []
      textVals = []
      # Parse the XML description of the data
      root = ET.fromstring(recxml)
      fnum = 0
      # Id field to use
      foundIdField = ''
      # Clean up the designated id field if present.
      idfld = idfld.strip().lower()
      validFields = []
      # Turn the data fields input into a clean list
      if dataflds:
        validFields = dataflds.split(',')
        validFields = [field.strip().lower() for field in validFields]
      fieldOrder = []
      # Iterate over the fields in the xml.
      for x in root:
          attribs = x.attrib
          # Use this field if no datafields specified, or this field in list.
          if not validFields or attribs['label'].lower() in validFields:
            ftype = attribs['type']
            # Handle numeric and textual fields
            if ftype in ['unsigned','integer','real','decimal','udecimal']:
                values.append('(REAL8)LEFT.' + attribs['label'])
                textVals.append("""''""")
            else:
                values.append('0')
                textVals.append('(STRING)LEFT.' + attribs['label'])
            fieldOrder.append(attribs['label'])
            fnum += 1
          # Use the first field as the id field, unless another field was specified.
          if (idfld == '' and not foundIdField) or attribs['label'].lower() == idfld:
            foundIdField = attribs['label']
      # Format the ECL.
      valStr = "CHOOSE(COUNTER, {values})".format(values=','.join(values))
      textValStr = "CHOOSE(COUNTER, {values})".format(values=','.join(textVals))
      outStr = """{out} := NORMALIZE({dsname}, {numValues}, TRANSFORM(_Types_.AnyField,
                                SELF.wi := 1,
                                SELF.id := LEFT.{foundIdField},
                                SELF.number := COUNTER,
                                SELF.value := {valStr},
                                SELF.textVal := {textValStr}));
          {out}_fields := {fields};
          """.format(out=outname, dsname=dsname,numValues=str(len(values)),
                  foundIdField=foundIdField, valStr=valStr, textValStr=textValStr,
                  fields=str(fieldOrder))
      return outStr

  ENDEMBED;
  //OUTPUT(pyToAF(#TEXT(dIn), xmlstr, 'aa', idfield, datafields)); // Uncomment to debug expansion
  #EXPAND(pyToAF(#TEXT(dIn), #TEXT(dOut), xmlstr, idfield, datafields));
ENDMACRO;