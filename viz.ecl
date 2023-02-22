IMPORT Python3 AS Python;
IMPORT Std.System.Thorlib;
IMPORT $ AS HC;
IMPORT HC.Types;

AnyField := Types.AnyField;
ChartGrid := Types.ChartGrid;
ChartData := Types.ChartData;
ChartInfo := Types.ChartInfo;

node := Thorlib.node();
globalScope := 'probspace' + node + '.ecl';

EXPORT viz := MODULE

    SHARED ParseVar := RECORD
        STRING varName;
        SET OF REAL numVals;
        SET OF STRING strVals;
        BOOLEAN isList := False;
    END;

    SHARED ParseResults := RECORD
        STRING qtype;
        DATASET(ParseVar) targets;
        DATASET(ParseVar) conds;
        DATASET(ParseVar) interventions;
    END;

    EXPORT DATASET(ChartGrid) getGrid(DATASET(ParseResults) presults, UNSIGNED ps) := EMBED(Python: globalscope(globalScope), persist('query'))
        from because.visualization import grid2 as grid
        negInf = -9999999
        inf = 9999999
        try:
            assert 'PS' in globals(), 'PS is not initialized.'
            for result in presults:
                qtype, targs, conds, intervs = result
                if qtype in ['prob', 'cprob']:
                    # For prob and cprob, we use all variables in the query
                    # and all variables in the grid.
                    vars = conds + targs
                    gvars = conds + targs
                elif qtype == 'bprob':
                    # For bound prob, we use all vars in the query,
                    # but only conditionals in the grid.
                    vars = conds + targs
                    gvars = conds
                elif qtype == 'expct':
                    # For expectation, we use only conds for both the query
                    # and the grid.
                    vars = conds
                    gvars = conds
                # For the grid, we only need the variable names.
                gvars = [v[0] for v in gvars]
                G = grid.Grid(PS, gvars)
                gDat = G.makeGrid()
                # We will emit the grid, but in the case of bprob (bound probability),
                # we must also emit a constant extra term containing the target's
                # query specification
                bProbSpecs = []
                if qtype == 'bprob':
                    bProbSpecs = []
                    bpVars = vars[len(gvars):]
                    for varSpec in bpVars:
                        var, numVals, strVals, isList = varSpec                        
                        if strVals:
                            args = tuple(strVals)
                        else:
                            args = tuple(numVals)
                        if isList:
                            # prepend a designator so that we know it's a list.
                            args = ('__list__',) + args
                        gitem = ('n/a',) + args
                        bProbSpecs.append(gitem)
                    # Extend each row of the grid with these fixed terms.
                    gDatNew = []
                    for item in gDat:
                        gDatNew.append(item + tuple(bProbSpecs))
                    gDat = gDatNew
                i = 0
                #assert False, 'gDat = ' + str(gDat[:3])
                for item in gDat:
                    outItems = []
                    for j in range(len(item)):
                        var = vars[j][0]
                        varVal = item[j]
                        for k in range(len(varVal)):
                            val = varVal[k]
                            if val is None:
                                if k == 2:
                                    val = inf
                                else:
                                    val = negInf
                            strVal = ''
                            if PS.isStringVal(var):
                                if type(val) != type(''):
                                    strVal = PS.numToStr(var, val)
                                else:
                                    strVal = val
                                val = 0.0
                            if val == 'n/a':
                                strVal = val
                                val = 0.0
                            val = float(val)
                            outItem = (1,j+1,k+1,val, strVal)
                            outItems.append(outItem)
                    yield((i+1, outItems))
                    i += 1
        except:
            from because.hpcc_utils import format_exc
            raise RuntimeError(format_exc.format('viz.getGrid'))
    ENDEMBED; // GetGrid

    EXPORT DATASET(ChartGrid) getHeatmapGrid(DATASET(ParseResults) presults, UNSIGNED ps) := EMBED(Python: globalscope(globalScope), persist('query'))
        try:
            # Return grid of [(var1, var2)], undirected.
            assert 'PS' in globals(), 'PS is not initialized.'
            for result in presults:
                power = 0
                sensitivity = 5
                varGrid = []
                qtype, targs, conds, intervs = result
                if qtype == 'dep':
                    power = 0
                else:
                    power = 5
                for i in range(len(targs)):
                    targ1 = targs[i]
                    var1 = targ1[0]
                    for j in range(i+1, len(targs)):
                        targ2 = targs[j]
                        var2 = targ2[0]
                        varGrid.append((var1, var2))
                for i in range(len(conds)):
                    # Conditions may include only power and or sensitivity specifications.
                    spec = conds[i]
                    var = spec[0]
                    assert var in ['power', 'sensitivity'] and len(spec == 2) and type(spec[1]) not in [type((0,)), type([])], \
                        'Correlation and dependence conditional clause may only contain exact matches for power or sensitivity'
                    value = spec[1]
                    if var == 'power' and qtype == 'dep':
                        # Power is always zero for correlation
                        power = value
                    elif var == 'sensitivity':
                        sensitivity = value
                for i in range(len(varGrid)):
                    outItems = []
                    item = varGrid[i]
                    var1 = item[0]
                    var2 = item[1]
                    for j in range(4):
                        # (var1, var2, power, sensitivity)
                        val = 0.0
                        if j == 0:
                            strVal = var1
                        elif j == 1:
                            strVal = var2
                        elif j == 2:
                            val = float(power)
                            strVal = ''
                        elif j == 3:
                            val = float(sensitivity)
                            strVal = ''
                        outItem = (1,j+1, 1,val, strVal)
                        outItems.append(outItem)
                    yield((i+1, outItems))
        except:
            from because.hpcc_utils import format_exc
            raise RuntimeError(format_exc.format('viz.getHeatmapGrid'))
    ENDEMBED; // getHeatmapGrid

    EXPORT STREAMED DATASET(ChartData) fillDataGrid(STREAMED DATASET(ChartGrid) grid, SET OF STRING vars, STRING queryType, UNSIGNED ps) := 
            EMBED(Python: globalscope(globalScope), persist('query'), activity)
        ranges = [5, 16, 84, 95]
        try:
            assert 'PS' in globals(), 'PS is not initialized.'
            for rec in grid:
                varSpecs = []
                terms = []
                currVar = 0
                currTerm = 0
                id, items = rec
                items.append((0, 0, 0, 0, ''))
                for item in items:
                    wi, varid, term, value, textVal = item
                    if varid != currVar:
                        if currVar:
                            varSpecs.append(tuple(terms))
                        currVar = varid
                        terms = []
                    if textVal:
                        terms.append(textVal)
                    else:
                        terms.append(value)
                allSpecs = []
                nominals = []
                for i in range(len(vars)):
                    var = vars[i]
                    if i < len(varSpecs):
                        varSpec = varSpecs[i]
                        args = varSpec[1:]
                        if args[0] == '__list__': # Special case to detect list items.
                            listItems = tuple(args[1:])
                            args = (listItems,)  # Put the tuple in the first arg.
                        spec = (var, ) + args
                        nom = str(varSpec[0])
                        if nom != 'n/a':
                            # Filter out n/a items from bprob
                            nominals.append(nom)
                    else:
                        spec = (var,)
                    allSpecs.append(spec)
                rangesTup = (0.0, 0.0, 0.0, 0.0)
                if queryType == 'prob':
                    targets = allSpecs
                    conds = []
                    result = PS.P(targets, conds)
                elif queryType == 'cprob':
                    targets = allSpecs[-1:]
                    conds = allSpecs[:-1]
                    result = PS.P(targets, conds)
                elif queryType == 'bprob':
                    targets = allSpecs[-1:]
                    conds = allSpecs[:-1]
                    #assert False, 'bprob targs = ' + str(targets) + ', conds = ' + str(conds)
                    result = PS.P(targets, conds)
                elif queryType == 'expct':
                    targets = allSpecs[-1:]
                    conds = allSpecs[:-1]
                    result = PS.E(targets, conds)
                    if len(conds) == 1:
                        d = PS.distr(targets, conds)
                        r2low = d.percentile(ranges[0])
                        r1low = d.percentile(ranges[1])
                        r1high = d.percentile(ranges[2])
                        r2high = d.percentile(ranges[3])
                        rangesTup = (r1low, r1high, r2low, r2high)
                gridVals = nominals
                gridVals.append(str(result))
                if len(gridVals) < 3:
                    gridVals.append('')
                yield (id,) + tuple(gridVals) + rangesTup
        except:
            from because.hpcc_utils import format_exc
            raise RuntimeError(format_exc.format('viz.fillDataGrid'))
    ENDEMBED; // fillDataGrid

    EXPORT STREAMED DATASET(ChartData) fillHeatmapGrid(STREAMED DATASET(ChartGrid) grid, SET OF STRING vars, STRING queryType, UNSIGNED ps) := 
            EMBED(Python: globalscope(globalScope), persist('query'), activity)
        ranges = [5, 16, 84, 95]
        try:
            assert 'PS' in globals(), 'PS is not initialized.'
            for rec in grid:
                varSpecs = []
                terms = []
                currVar = 0
                currTerm = 0
                id, items = rec
                items.append((0, 0, 0, 0, ''))
                for item in items:
                    wi, varid, term, value, textVal = item
                    if varid != currVar:
                        if currVar:
                            varSpecs.append(tuple(terms))
                        currVar = varid
                        terms = []
                    if textVal:
                        terms.append(textVal)
                    else:
                        terms.append(value)
                rangesTup = (0.0, 0.0, 0.0, 0.0)
                var1 = varSpecs[0][0]
                var2 = varSpecs[1][0]
                power = varSpecs[2][0]
                sensitivity = varSpecs[3][0]
                if queryType == 'cor':
                    result = PS.corrCoef(var1, var2)
                else:
                    # Dep(endence)
                    result = PS.dependence(var1, var2, power=power, sensitivity=sensitivity)
                gridVals = (var1, var2, str(result))
                yield (id,) + tuple(gridVals) + rangesTup
                # Emit records for both directions
                gridValsR = (var2, var1, str(result))
                yield (id,) + tuple(gridValsR) + rangesTup
        except:
            from because.hpcc_utils import format_exc
            raise RuntimeError(format_exc.format('viz.fillHeatmipGrid'))
    ENDEMBED; // fillHeatmapGrid

    EXPORT DATASET(ParseResults) parseQuery(STRING query, UNSIGNED ps) := EMBED(Python: globalscope(globalScope), persist('query'))
        from because.hpcc_utils.parseQuery import Parser
        try:
            assert 'PS' in globals(), 'PS is not initialized.'
            queries = [query]
            PARSER = Parser()
            specList = PARSER.parse(queries)
            spec = specList[0]
            cmd, targs, conds, intervs = spec
            qtype = 'unknown'
            if cmd == 'D': # Unbound distribution
                assert len(conds) <= 1, 'Probability Charts only support zero or one conditional.  Got: ' + query
                if len(conds) == 0:
                    assert len(targs) <= 2, 'Probability charts only support one or two target variables.'
                    qtype = 'prob'
                else:
                    assert len(targs) == 1, 'Probability charts only support a single target and up to one conditional. Got: ' + query
                    qtype = 'cprob'
            elif cmd == 'P': # Probability. Treat as Bound Probability.
                qtype = 'bprob'
                assert len(conds) > 0 and len(conds) <= 2 , 'Bound Probability Charts require one or two conditionals. Got: ' + query
            elif cmd == 'E': # Expectation
                qtype = 'expct'
                assert len(conds) > 0 and len(conds) <= 2 and len(targs) == 1, 'Expectation Charts only support a single target and one or two conditionals. Got: ' + query
            elif cmd in ['DEPENDENCE', 'CORRELATION']:
                # Dependence or correlation heatmap. qtype is 'dep' or 'cor'
                qtype = cmd.lower()[:3]
                assert len(targs) >= 2, 'Dependence or correlation heatmaps require at least two target variables'
            allVars = PS.getVarNames()
            def formatSpecs(specs):
                outSpecs = []
                for spec in specs:
                    var = spec[0]
                    assert var in allVars, 'Variable name ' + var + ' is not valid. Valid variable names are: ' + str(allVars)
                    args = list(spec[1:])
                    isList = False
                    if args and type(args[0]) == type((0,)):
                        # It's a list.  Pull out the individual elements.
                        args = list(args[0])
                        isList = True
                    if args and type(args[0]) == type(''):
                        strArgs = args
                        numArgs = []
                    else:
                        numArgs = [float(a) for a in args]
                        strArgs = []
                    outSpec = (var, numArgs, strArgs, isList)
                    outSpecs.append(outSpec)
                return outSpecs
            targSpecs = formatSpecs(targs)
            condSpecs = formatSpecs(conds)
            intervSpecs = formatSpecs(intervs)
            yield (qtype, targSpecs, condSpecs, intervSpecs)
        except:
            from because.hpcc_utils import format_exc
            raise RuntimeError(format_exc.format('viz.parseQuery'))
    ENDEMBED; // parseQuery

    EXPORT SET OF STRING getVarNames(DATASET(ParseResults) presults) := EMBED(Python)
        # Extract variable names from the parse results
        # Should only be one record.
        for result in presults:
            qtype, targs, conds, intervs = result
            tvars = [s[0] for s in targs]
            cvars = [s[0] for s in conds]
            # Put conditionals first.
            varNames = cvars + tvars
            return varNames
    ENDEMBED;

    EXPORT DATASET({BOOLEAN val}) needSorting(STRING qtype, SET OF STRING pyvars, UNSIGNED ps) := EMBED(Python: globalscope(globalScope), persist('query'))
        if qtype == 'prob':
            return [(False,)]
        else:
            # For types other than prob, if the first cond var is categorical,
            # Then we need to sort.
            condVar = pyvars[0]
            assert 'PS' in globals(), 'PS is not initialized.'
            if PS.isCategorical(condVar):
                return [(True,)]
        return [(False,)]
    ENDEMBED;

    EXPORT DATASET(ChartData) getDataGrid(STRING query, UNSIGNED PS) := FUNCTION
        qresults := parseQuery(query, PS);
        qresult := qresults[1];
        queryType := qresult.qtype;
        testGrid := IF(queryType = 'dep' OR queryType = 'cor',
            getHeatmapGrid(qresults, PS), 
            getGrid(qresults, PS));
        //testGrid := getGrid(qresults, PS);
        testGrid_D := DISTRIBUTE(testGrid, id);
        vars := getVarNames(qresults);
        dataGrid := IF(queryType = 'dep' OR queryType = 'cor',
            fillHeatmapGrid(testGrid_D, vars, queryType, PS), 
            fillDataGrid(testGrid_D, vars, queryType, PS));
        //dataGrid := fillDataGrid(testGrid_D, vars, queryType, PS);
        dataGrid_S := IF(needSorting(queryType, vars, PS)[1].val, SORT(dataGrid, y_), SORT(dataGrid, id));
        RETURN dataGrid_S;
    END;
    
    EXPORT DATASET(ChartInfo) fillChartInfo(SET OF STRING vars, STRING query, STRING querytype, STRING dataname, UNSIGNED ps) := 
            EMBED(Python: globalscope(globalScope), persist('query'))
        try:
            assert 'PS' in globals(), 'PS is not initialized.'
            target = vars[-1]
            conds = vars[:-1]
            dims = 2
            if querytype == 'prob':
                dims = 2
                xlabel = 'x'
                ylabel = 'P(' + target + ' = x)'
                mean = PS.E(target)
                d = PS.distr(target)
                ranges = [5, 16, 84, 95]
                r2low = d.percentile(ranges[0])
                r1low = d.percentile(ranges[1])
                r1high = d.percentile(ranges[2])
                r2high = d.percentile(ranges[3])
                info = (dataname, querytype, dims, xlabel, ylabel, '', mean, r1low, r1high, r2low, r2high)
            elif querytype == 'cprob':
                dims = 3
                xlabel = conds[0]
                ylabel = 'x'
                zlabel = 'P(' + target + ' = x | ' + conds[0] + ')'
                info = (dataname, querytype, dims, xlabel, ylabel, '', 0.0, 0.0, 0.0, 0.0, 0.0)
            elif querytype == 'bprob':
                zlabel = ''
                if len(conds) == 1:
                    dims = 2
                    xlabel = conds[0]
                    ylabel = query
                elif len(conds) == 2:
                    dims = 3
                    xlabel = conds[0]
                    ylabel = conds[1]
                    zlabel = query
                info = (dataname, querytype, dims, xlabel, ylabel, zlabel, 0.0, 0.0, 0.0, 0.0, 0.0)
            elif querytype == 'expct':
                zlabel = ''
                if len(conds) == 1:
                    dims = 2
                    xlabel = conds[0]
                    ylabel = 'E(' + target + ' | ' + conds[0] + ')'
                elif len(conds) == 2:
                    dims = 3
                    xlabel = conds[0]
                    ylabel = conds[1]
                    zlabel = 'E(' + target + ' | ' + conds[0] + ', ' + conds[1]+ ')'
                info = (dataname, querytype, dims, xlabel, ylabel, zlabel, 0.0, 0.0, 0.0, 0.0, 0.0)
            elif querytype in ['dep', 'cor']:
                # Dependence or correlation heatmap
                dims = 3
                xlabel = ''
                ylabel = ''
                zlabel = query
                info = (dataname, querytype, dims, xlabel, ylabel, zlabel, 0.0, 0.0, 0.0, 0.0, 0.0)
            yield info
        except:
            from because.hpcc_utils import format_exc
            raise RuntimeError(format_exc.format('viz.fillChartInfo'))
    ENDEMBED; // fillChartInfo

    EXPORT DATASET(ChartInfo) getChartInfo(STRING query, STRING dataname, UNSIGNED PS) := FUNCTION
        qresults := parseQuery(query, PS);
        qresult := qresults[1];
        queryType := qresult.qtype;
        vars := getVarNames(qresults);
        results := fillChartInfo(vars, query, queryType, dataname, PS);
        RETURN results;
    END;

    EXPORT Plot(queries, PS)  := MACRO
        IMPORT Python3 AS Python;
        STRING _plotPyFunc(SET OF STRING pyqueries, STRING pyps) := EMBED(Python: fold)
            outStr = """
                    IMPORT HPCC_causality AS _HC;
                    _v := _HC.viz;
                    _PS := {ps};
                    """.format(ps = pyps)
            plotNames = []
            plotprefix = '_plot'
            for i in range(len(pyqueries)):
                tempStr = """_dg{num} := _v.getDataGrid('{query}', _PS);
                            OUTPUT(_dg{num}, ALL, NAMED('{prefix}{num}_data'));
                            _ci{num} := _v.getChartInfo('{query}', '{prefix}{num}_data', _PS);
                            OUTPUT(_ci{num}, ALL, NAMED('{prefix}{num}_meta'));
                            """.format(num = i, prefix = plotprefix, query = pyqueries[i])
                plotNames.append(plotprefix + str(i) + '_meta')
                outStr += tempStr
            plotNameStrs = []
            for plotName in plotNames:
                plotNameStrs.append('{' + '\'' + plotName + '\'' + '}')
            plotNameStr = '[' + ','.join(plotNameStrs) + ']'
            finalStr = """OUTPUT(DATASET({plots}, {{STRING name}}), NAMED('_plots'));
                        """.format(plots = plotNameStr)
            outStr += finalStr
            return outStr
        ENDEMBED;
        cmd := _plotPyFunc(queries, #TEXT(PS));
        //OUTPUT(cmd, NAMED('cmd'));
        #EXPAND(cmd);
    ENDMACRO;
END;
