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
    END;

    SHARED ParseResults := RECORD
        STRING qtype;
        DATASET(ParseVar) targets;
        DATASET(ParseVar) conds;
        DATASET(ParseVar) interventions;
    END;

    EXPORT DATASET(ChartGrid) GetGrid(DATASET(ParseResults) presults, UNSIGNED ps) := EMBED(Python: globalscope(globalScope), persist('query'))
        from because.visualization import grid2 as grid
        assert 'PS' in globals(), 'Viz.GetGrid: PS is not initialized.'
        negInf = -9999999
        inf = 9999999
        try:
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
                        var, numVals, strVals = varSpec
                        if strVals:
                            args = tuple(strVals)
                        else:
                            args = tuple(numVals)
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
                                strVal = PS.numToStr(var, val)
                                val = 0.0
                            elif val == 'n/a':
                                strVal = val
                                val = 0.0
                            val = float(val)
                            outItem = (1,j+1,k+1,val, strVal)
                            outItems.append(outItem)
                    yield((i+1, outItems))
                    i += 1
        except:
            from because.hpcc_utils import format_exc
            assert False, format_exc.format('viz.GetGrid')
    ENDEMBED;

    EXPORT STREAMED DATASET(ChartData) fillDataGrid(STREAMED DATASET(ChartGrid) grid, SET OF STRING vars, STRING queryType, UNSIGNED ps) := 
            EMBED(Python: globalscope(globalScope), persist('query'), activity)
        assert 'PS' in globals(), 'Viz.fillDataGrid: PS is not initialized.'
        ranges = [5, 16, 84, 95]
        try:
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
            assert False, format_exc.format('viz.fillDataGrid')
    ENDEMBED;

    EXPORT DATASET(ParseResults) parseQuery(STRING query, UNSIGNED ps) := EMBED(Python: globalscope(globalScope), persist('query'))
        from because.hpcc_utils.parseQuery import Parser
        #try:
        if True:
            queries = [query]
            PARSER = Parser()
            specList = PARSER.parse(queries)
            spec = specList[0]
            cmd, targs, conds, intervs = spec
            qtype = 'unknown'
            if cmd == 'D': # Unbound distribution
                assert len(conds) <= 1, 'viz.parseQuery: Probability Charts only support zero or one conditional.  Got: ' + query
                if len(conds) == 0:
                    assert len(targs) <= 2, 'viz.parseQuery: Probability charts only support one or two target variables.'
                    qtype = 'prob'
                else:
                    assert len(targs) == 1, 'viz.parseQuery: Probability charts only support a single target and up to one conditional. Got: ' + query
                    qtype = 'cprob'
            elif cmd == 'P': # Probability. Treat as Bound Probability.
                qtype = 'bprob'
                assert len(conds) > 0 and len(conds) <= 2 , 'viz.parseQuery: Bound Probability Charts require one or two conditionals. Got: ' + query
            elif cmd == 'E': # Expectation
                qtype = 'expct'
                assert len(conds) > 0 and len(conds) <= 2 and len(targs) == 1, 'viz.parseQuery: Expectation Charts only support a single target and one or two conditionals. Got: ' + query
            def formatSpecs(specs):
                outSpecs = []
                for spec in specs:
                    var = spec[0]
                    args = list(spec[1:])
                    if args and type(args[0]) == type(''):
                        strArgs = args
                        numArgs = []
                    else:
                        numArgs = [float(a) for a in args]
                        strArgs = []
                    outSpec = (var, numArgs, strArgs)
                    outSpecs.append(outSpec)
                return outSpecs
            targSpecs = formatSpecs(targs)
            condSpecs = formatSpecs(conds)
            intervSpecs = formatSpecs(intervs)
            #assert False, str((qtype, targSpecs, condSpecs, intervSpecs))
            yield (qtype, targSpecs, condSpecs, intervSpecs)
        #except:
        #    from because.hpcc_utils import format_exc
        #    assert False, format_exc.format('viz.parseQuery')
    ENDEMBED;

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

    EXPORT DATASET(ChartData) GetDataGrid(STRING query, UNSIGNED PS) := FUNCTION
        qresults := parseQuery(query, PS);
        qresult := qresults[1];
        queryType := qresult.qtype;
        testGrid := GetGrid(qresults, PS);
        testGrid_D := DISTRIBUTE(testGrid, id);
        vars := getVarNames(qresults);
        dataGrid := fillDataGrid(testGrid_D, vars, queryType, PS);
        dataGrid_S := SORT(dataGrid, id);
        RETURN dataGrid_S;
    END;
    
    EXPORT DATASET(ChartInfo) fillChartInfo(SET OF STRING vars, STRING query, STRING querytype, STRING dataname, UNSIGNED ps) := 
            EMBED(Python: globalscope(globalScope), persist('query'))
        assert 'PS' in globals(), 'Viz.fillDataGrid: PS is not initialized.'
        try:
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
            yield info
        except:
            from because.hpcc_utils import format_exc
            assert False, format_exc.format('viz.fillChartInfo')
    ENDEMBED;

    EXPORT DATASET(ChartInfo) GetChartInfo(STRING query, STRING dataname, UNSIGNED PS) := FUNCTION
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
                tempStr = """_dg{num} := _v.GetDataGrid('{query}', _PS);
                            OUTPUT(_dg{num}, ALL, NAMED('{prefix}{num}_data'));
                            _ci{num} := _v.GetChartInfo('{query}', '{prefix}{num}_data', _PS);
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
