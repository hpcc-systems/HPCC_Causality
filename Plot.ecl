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
EXPORT Plot(query, plotname, PS) := MACRO
    IMPORT Python3 AS Python;
    IMPORT HPCC_causality AS _HC_;
    IMPORT _HC_.Types AS _Types_;

    _viz_ := _HC_.viz;

    _v_ := _viz_(PS);

    _dg_ := _v_.GetDataGrid(#TEXT(query),  PS);

    OUTPUT(_dg_, ALL, NAMED(plotname + '_data'));

    _chartinf_ := _v_.GetChartInfo(#TEXT(query), PS);

    OUTPUT(_chartinf_, ALL, NAMED(plotname + '_info'));
ENDMACRO;  
