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
    SHARED ParseResults := RECORD
        STRING qtype;
        SET OF STRING vars;
    END;

    EXPORT DATASET(ChartGrid) GetGrid(SET OF STRING vars, UNSIGNED ps) := EMBED(Python: globalscope(globalScope), persist('query'))
        from because.visualization import grid2 as grid
        assert 'PS' in globals(), 'Viz.GetGrid: PS is not initialized.'
        negInf = -9999999
        inf = 9999999
        try:
            G = grid.Grid(PS, vars)
            gDat = G.makeGrid()
            #outDat = [list(tup) for tup in gDat]
            #assert False, 'outDat = ' + repr(outDat[:10])
            i = 0
            for item in gDat:
                outItems = []
                for j in range(len(item)):
                    var = vars[j]
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
                        if len(varSpec) > 2:
                            spec = (var, varSpec[1], varSpec[2])
                        else:
                            spec = (var, varSpec[1])
                        nominals.append(str(varSpec[0]))
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
        try:
            queries = [query]
            PARSER = Parser()
            specList = PARSER.parse(queries)
            spec = specList[0]
            cmd, targ, cond, interv = spec
            if cmd == 'D': # Unbound distribution
                assert len(cond) <= 1, 'viz.parseQuery: Probability Charts only support zero or one conditional.  Got: ' + query
                if len(cond) == 0:
                    assert len(targ) <= 2, 'viz.parseQuery: Probability charts only support one or two target variables.'
                    qtype = 'prob'
                else:
                    assert len(targ) == 1, 'viz.parseQuery: Probability charts only support a single target and up to one conditional. Got: ' + query
                    qtype = 'cprob'
                vars = [s[0] for s in cond + targ]
            elif cmd == 'E': # Expectation
                qtype = 'expct'
                assert len(cond) <= 2 and len(targ) == 1, 'viz.parseQuery: Expectation Charts only support a single target and one or two conditionals. Got: ' + query
                vars = [s[0] for s in cond + targ]
            yield (qtype, vars)
        except:
            from because.hpcc_utils import format_exc
            assert False, format_exc.format('viz.parseQuery')
    ENDEMBED;

    EXPORT DATASET(ChartData) GetDataGrid(STRING query, UNSIGNED PS) := FUNCTION
        qresults := parseQuery(query, PS);
        qresult := qresults[1];
        queryType := qresult.qtype;
        vars := qresult.vars;
        gridVars := IF(queryType != 'expct', vars, vars[..COUNT(vars)-1]);
        testGrid := GetGrid(gridVars, PS);
        testGrid_D := DISTRIBUTE(testGrid, id);
        dataGrid := fillDataGrid(testGrid_D, vars, queryType, PS);
        dataGrid_S := SORT(dataGrid, id);
        RETURN dataGrid_S;
    END;
    
    EXPORT DATASET(ChartInfo) fillChartInfo(SET OF STRING vars, STRING querytype, STRING dataname, UNSIGNED ps) := 
            EMBED(Python: globalscope(globalScope), persist('query'))
        assert 'PS' in globals(), 'Viz.fillDataGrid: PS is not initialized.'
        try:
            target = vars[-1]
            conds = vars[:-1]
            if querytype == 'prob':
                xlabel = 'x'
                ylabel = 'P(' + target + ' = x)'
                mean = PS.E(target)
                d = PS.distr(target)
                ranges = [5, 16, 84, 95]
                r2low = d.percentile(ranges[0])
                r1low = d.percentile(ranges[1])
                r1high = d.percentile(ranges[2])
                r2high = d.percentile(ranges[3])
                info = (dataname, querytype, xlabel, ylabel, '', mean, r1low, r1high, r2low, r2high)
            elif querytype == 'cprob':
                xlabel = conds[0]
                ylabel = 'P(' + target + ' | ' + conds[0] + ')'
                info = (dataname, querytype, xlabel, ylabel, '', 0.0, 0.0, 0.0, 0.0, 0.0)
            elif querytype == 'expct':
                zlabel = ''
                if len(conds) == 1:
                    xlabel = conds[0]
                    ylabel = 'E(' + target + ' | ' + conds[0] + ')'
                elif len(conds) == 2:
                    xlabel = conds[0]
                    ylabel = conds[1]
                    zlabel = 'E(' + target + ' | ' + conds[0] + ', ' + conds[1]+ ')'
                info = (dataname, querytype, xlabel, ylabel, zlabel, 0.0, 0.0, 0.0, 0.0, 0.0)
            yield info
        except:
            from because.hpcc_utils import format_exc
            assert False, format_exc.format('viz.fillChartInfo')
    ENDEMBED;

    EXPORT DATASET(ChartInfo) GetChartInfo(STRING query, STRING dataname, UNSIGNED PS) := FUNCTION
        qresults := parseQuery(query, PS);
        qresult := qresults[1];
        queryType := qresult.qtype;
        vars := qresult.vars;
        results := fillChartInfo(vars, queryType, dataname, PS);
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
