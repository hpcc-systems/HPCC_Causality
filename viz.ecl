IMPORT Python3 AS Python;
IMPORT Std.System.Thorlib;
IMPORT $ AS HC;
IMPORT HC.Types;
//IMPORT HC.Causality;

AnyField := Types.AnyField;
ChartGrid := Types.ChartGrid;
ChartData := Types.ChartData;
ChartInfo := Types.ChartInfo;
cModelTyp := Types.cModel;

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
        DATASET(ParseVar) controls;
        DATASET(ParseVar) filters;
        DATASET(ParseVar) interventions;
        DATASET(ParseVar) counterfactuals;
    END;

    EXPORT DATASET(ChartGrid) getGrid(DATASET(ParseResults) presults, UNSIGNED ps) := EMBED(Python: globalscope(globalScope), persist('query'))
        from because.visualization import grid2 as grid
        negInf = -9999999
        inf = 9999999
        assert 'PSDict' in globals(), 'viz.getGrid: PSDict is not initialized.'
        assert ps in PSDict, 'viz.getGrid: invalid probspace id = ' + str(ps)
        PS = PSDict[ps]
        try:
            for result in presults:
                qtype, targs, conds, controls, filters, intervs, cfac = result
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
        assert 'PSDict' in globals(), 'viz.getHeatmapGrid: PSDict is not initialized.'
        assert ps in PSDict, 'viz.getHeatmapGrid: invalid probspace id = ' + str(ps)
        PS = PSDict[ps]
        try:
            # Return grid of [(var1, var2, power, sensitivity)], undirected.
            outRecs = []
            for result in presults:
                power = 5
                sensitivity = 5
                varGrid = []
                qtype, targs, conds, controls, filters, intervs, cfacs = result
                #assert False, 'targs, conds, filters = ' + str((targs, conds, filters))
                for i in range(len(targs)):
                    targ1 = targs[i]
                    var1 = targ1[0]
                    for j in range(i+1, len(targs)):
                        targ2 = targs[j]
                        var2 = targ2[0]
                        varGrid.append((var1, var2))
                for i in range(len(filters)):
                    # Conditions may include only power and or sensitivity specifications.
                    spec = filters[i]
                    var = spec[0]
                    assert var in ['$power', '$sensitivity'] and len(spec[1]) == 1, \
                        'Correlation and dependence conditional clause may only contain exact matches for $power or $sensitivity'
                    value = spec[1][0]
                    if var == '$power':
                        power = value
                    elif var == '$sensitivity':
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
                        outItem = (1, j+1, 1, val, strVal)
                        outItems.append(outItem)
                    outRecs.append((i+1, outItems))
                    #yield((i+1, outItems))
            #assert False, 'outRecs = ' + str(outRecs[-5:])
            return outRecs
        except:
            from because.hpcc_utils import format_exc
            raise RuntimeError(format_exc.format('viz.getHeatmapGrid'))
    ENDEMBED; // getHeatmapGrid

    EXPORT STREAMED DATASET(ChartData) fillDataGrid(STREAMED DATASET(ChartGrid) grid, DATASET(ParseResults) presults, UNSIGNED ps) := 
            EMBED(Python: globalscope(globalScope), persist('query'), activity)
        ranges = [5, 16, 84, 95]
        assert 'PSDict' in globals(), 'viz.fillDataGrid: PSDict is not initialized.'
        assert ps in PSDict, 'viz.fillDataGrid: invalid probspace id = ' + str(ps)
        PS = PSDict[ps]
        def isnum(instr):
            # Function to test for a numeric string.
            try:
                float(instr)
                return True
            except:
                return False
        
        try:
            for result in presults:
                # Should only be one record on each node.
                queryType, targs, conds, controls, filters, intervs, cfac = result
            controlSpecs = [(control[0],) for control in controls]
            targVars = [targ[0] for targ in targs]
            condVars = [cond[0] for cond in conds]
            # FilterSpecs are a little more involved
            filtSpecs = []
            for filt in filters:
                var, numVals, strVals, isList = filt
                if strVals:
                    vals = strVals
                else:
                    vals = numVals
                if isList:
                    vals = [tuple(vals)]
                filtSpec = (var, ) + tuple(vals)
                filtSpecs.append(filtSpec)
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
                vars = condVars + targVars
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
                            if PS.isCategorical(var):
                                if isnum(nom):
                                    nom = '_' + str(int(varSpec[0])) 
                            nominals.append(nom)
                    else:
                        spec = (var,)
                    allSpecs.append(spec)
                rangesTup = (0.0, 0.0, 0.0, 0.0)
                if queryType == 'prob':
                    # One or two targets, no conditionals
                    targets = allSpecs
                    conds = filtSpecs # Don't need controls when no conditionals
                    #assert False, 'targets = ' + str(targets) + ', conds = ' + str(conds) + ', vars = ' + str(vars)
                    result = PS.P(targets, conds)
                elif queryType == 'cprob':
                    # 1 target and one conditional
                    targets = allSpecs[-1:]
                    conds = allSpecs[:-1] + filtSpecs + controlSpecs
                    result = PS.P(targets, conds)
                elif queryType == 'bprob':
                    # Any number of targets (joint probability), and 1 or two conditionals
                    nConds = len(condVars)
                    targets = allSpecs[nConds:]
                    conds = allSpecs[:nConds] + filtSpecs + controlSpecs
                    #assert False, 'bprob targs = ' + str(targets) + ', conds = ' + str(conds)
                    result = PS.P(targets, conds)
                    #assert False, 'targets, conds = ' + str(targets) + ', ' + str(conds) + ',' + str(result)
                elif queryType == 'expct':
                    targets = allSpecs[-1:]
                    conds = allSpecs[:-1] + filtSpecs + controlSpecs
                    #assert False, 'E: targets, conds = ' + str(targets) + ', ' + str(conds)
                    result = PS.E(targets, conds)
                    if len(condVars) == 1:
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

    EXPORT STREAMED DATASET(ChartData) fillHeatmapGrid(STREAMED DATASET(ChartGrid) grid,
            SET OF STRING vars, STRING queryType, DATASET(parseVar) conds, UNSIGNED ps) := 
            EMBED(Python: globalscope(globalScope), persist('query'), activity)
        ranges = [5, 16, 84, 95]
        assert 'PSDict' in globals(), 'viz.fillHeatmapGrid: PSDict is not initialized.'
        assert ps in PSDict, 'viz.fillHeatmapGrid: invalid probspace id = ' + str(ps)
        PS = PSDict[ps]
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
                #assert False, 'varspecs = ' + str(varSpecs)
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
            raise RuntimeError(format_exc.format('viz.fillHeatmapGrid'))
    ENDEMBED; // fillHeatmapGrid

    EXPORT STREAMED DATASET(ChartData) fillDiscGrid(SET OF STRING vars, DATASET(parseVar) conds, UNSIGNED ps) := FUNCTION
        cm := HC.Causality(DATASET([], cModelTyp), ps);
        // Extract $power, $sensitivity, and $depth from conds
        pwrDat := conds(varName='$power');
        sensDat := conds(varName='$sensitivity');
        depthDat := conds(varName='$depth');
        pwr := IF(COUNT(pwrDat) > 0, pwrDat[1].numVals[1], 5);
        sens := IF(COUNT(sensDat) > 0, sensDat[1].numVals[1], 10);
        depth := IF(COUNT(depthDat) > 0, depthDat[1].numVals[1], 2);
        disc := cm.DiscoverModel(vars, pwr:=pwr, sensitivity:=sens, depth:=depth);
        // x_ is the cause variable; y_ is the effect, and z_ is the directional strength.
        // Use range1low and range1high for correlation and MDE respectively.
        grid := PROJECT(disc, TRANSFORM(ChartData,
                SELF.id := COUNTER,
                SELF.x_ := LEFT.causeVar,
                SELF.y_ := LEFT.effectVar,
                SELF.z_ := (STRING)LEFT.strength,
                SELF.range1low := LEFT.correlation,
                SELF.range1high := LEFT.MDE));
        RETURN grid;
    END; // fillDiscGrid

    EXPORT DATASET(ParseResults) parseQuery(STRING query, UNSIGNED ps) := EMBED(Python: globalscope(globalScope), persist('query'))
        from because.hpcc_utils.parseQuery import Parser
        assert 'PSDict' in globals(), 'viz.parseQuery(ECL): PSDict is not initialized.'
        assert ps in PSDict, 'viz.parseQuery(ECL): invalid probspace id = ' + str(ps)
        PS = PSDict[ps]
        try:
            allVars = PS.getVarNames()[:]
            if 'id' in allVars:
                allVars.remove('id') # Don't consider the id field.
            queries = [query]
            PARSER = Parser()
            specList = PARSER.parse(queries, isGraph=True)
            spec = specList[0]
            cmd, targs, conds, ctrlfor, intervs, cfac = spec
            qtype = 'unknown'
            # Split conditionals into bound and unbound subsets.  Bound conditionals are
            # considered filters, while unbound are used to form the grid.
            uconds = []
            filters = []
            for cond in conds:
                if len(cond) > 1:
                    filters.append(cond)
                else:
                    uconds.append(cond)
            conds = uconds
            if cmd == 'D': # Unbound distribution
                assert len(conds) <= 1, 'Probability Charts only support zero or one unbound conditional.  Got: ' + query
                if len(conds) == 0:
                    assert len(targs) <= 2, 'Probability charts only support one or two target variables.'
                    qtype = 'prob'
                else:
                    assert len(targs) == 1, 'Probability charts only support a single target and up to one unbound conditional. Got: ' + query
                    qtype = 'cprob'
            elif cmd == 'P': # Probability. Treat as Bound Probability.
                qtype = 'bprob'
                assert len(conds) > 0 and len(conds) <= 2 , 'Bound Probability Charts require one or two unbound conditionals. Got: ' + query
            elif cmd == 'E': # Expectation
                qtype = 'expct'
                assert len(conds) > 0 and len(conds) <= 2 and len(targs) == 1, 'Expectation Charts only support a single target and one or two unbound conditionals. Got: ' + query
            elif cmd in ['DEPENDENCE', 'CORRELATION']:
                # Dependence or correlation heatmap. qtype is 'dep' or 'cor'
                maxVars = 25
                qtype = cmd.lower()[:3]
                if len(targs) == 0:
                    targs = [(var,) for var in allVars][:maxVars]
                assert len(targs) >= 2 and len(targs) <= maxVars, 'Dependence or correlation heatmaps require between 2 and ' + str(maxVars) + ' target variables.' + \
                    '  An empty set indicates all variables in dataset.'
            elif cmd == 'CMODEL':
                qtype = 'cmodel'
                if len(targs) == 0:
                    targs = [(var,) for var in allVars]
                assert len(targs) >= 2, 'Causal model requires at least two target variables.'

            def formatSpecs(specs):
                outSpecs = []
                for spec in specs:
                    var = spec[0]
                    assert var[0] == '$' or var in allVars, 'Variable name ' + var + ' is not valid. Valid variable names are: ' + str(allVars)
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
            controlSpecs = formatSpecs(ctrlfor)
            filtSpecs = formatSpecs(filters)
            intervSpecs = formatSpecs(intervs)
            cfacSpecs = formatSpecs(cfac)
            yield (qtype, targSpecs, condSpecs, controlSpecs, filtSpecs, intervSpecs, cfacSpecs)
        except:
            from because.hpcc_utils import format_exc
            raise RuntimeError(format_exc.format('viz.parseQuery'))
    ENDEMBED; // parseQuery

    EXPORT SET OF STRING getVarNames(DATASET(ParseResults) presults) := EMBED(Python)
        # Extract variable names from the parse results
        # Should only be one record.
        for result in presults:
            qtype, targs, conds, controls, filters, intervs, cfacs = result
            tvars = [s[0] for s in targs]
            cvars = [s[0] for s in conds]
            if qtype in ['cmodel', 'correlation', 'dependence']:
                # For these types, don't include the conditionals
                varNames = tvars
            else:
                # Put conditionals first.
                varNames = cvars + tvars
            return varNames
    ENDEMBED;

    EXPORT DATASET({BOOLEAN val}) needSorting(STRING qtype, SET OF STRING pyvars, UNSIGNED ps) := EMBED(Python: globalscope(globalScope), persist('query'))
        assert 'PSDict' in globals(), 'viz.needSorting: PSDict is not initialized.'
        assert ps in PSDict, 'viz.needSorting: invalid probspace id = ' + str(ps)
        PS = PSDict[ps]
        if qtype == 'prob' or qtype == 'cdisc' or qtype == 'dep' or qtype == 'cor':
            return [(False,)]
        else:
            # For types other than prob or cdisc or dep or cor, if the first cond var is categorical,
            # Then we need to sort.
            condVar = pyvars[0]
            if PS.isCategorical(condVar):
                return [(True,)]
        return [(False,)]
    ENDEMBED;

    EXPORT DATASET(ChartData) getDataGrid(STRING query, UNSIGNED PS) := FUNCTION
        qresults := parseQuery(query, PS);
        qresult := qresults[1];
        queryType := qresult.qtype;
        testGrid := IF(queryType = 'dep' OR queryType = 'cor',
            getHeatmapGrid(qresults, PS), IF(queryType = 'cmodel',
            DATASET([], ChartGrid), // Don't need grid for cmodel
            getGrid(qresults, PS)));
        //testGrid := getGrid(qresults, PS);
        testGrid_D := DISTRIBUTE(testGrid, id);
        vars := getVarNames(qresults);
        filters := qresult.filters; // fillDiscGrid needs the filters to get the special params.
        controls := qresult.controls; // fillDatagrid needs the controFor variables
        dataGrid := IF(queryType = 'dep' OR queryType = 'cor',
            fillHeatmapGrid(testGrid_D, vars, queryType, filters, PS), IF(queryType = 'cmodel',
            fillDiscGrid(vars, filters, PS),
            fillDataGrid(testGrid_D, qresults, PS)));
        //dataGrid := fillDataGrid(testGrid_D, vars, queryType, PS);
        dataGrid_S := IF(needSorting(queryType, vars, PS)[1].val, SORT(dataGrid, y_), SORT(dataGrid, id));
        RETURN dataGrid_S;
    END;
    
    EXPORT DATASET(ChartInfo) fillChartInfo(SET OF STRING vars, STRING query, DATASET(ParseResults) qresults, STRING dataname, UNSIGNED ps) := 
            EMBED(Python: globalscope(globalScope), persist('query'))
        assert 'PSDict' in globals(), 'viz.fillChartInfo: PSDict is not initialized.'
        assert ps in PSDict, 'viz.fillChartInfo: invalid probspace id = ' + str(ps)
        PS = PSDict[ps]

        try:
            for result in qresults:
                # Should be only one RECORD
                querytype, targs, conds, controls, filters, intervs, cfac = result
            targetvars = vars[len(conds):]
            condvars = vars[:len(conds)]
            dims = 2
            if querytype == 'prob':
                if len(vars) == 1:
                    dims = 2
                    title = 'Probabiity Distribution -- ' + query
                    xlabel = 'x'
                    ylabel = 'P(' + vars[0] + ' = x)'
                    zlabel = ''
                    # To get the mean and ranges, we need to interpret the filter portion
                    # of the parsed query
                    filtSpecs = []
                    for filter in filters:
                        var, numVals, strVals, isList = filter
                        if strVals:
                            vals = strVals
                        else:
                            vals = numVals
                        if isList:
                            vals = [tuple(vals)]
                        filtSpec = (var,) + tuple(vals)
                        filtSpecs.append(filtSpec)
                    mean = PS.E(targetvars[0], filtSpecs)
                    d = PS.distr(targetvars[0], filtSpecs)
                    ranges = [5, 16, 84, 95]
                    r2low = d.percentile(ranges[0])
                    r1low = d.percentile(ranges[1])
                    r1high = d.percentile(ranges[2])
                    r2high = d.percentile(ranges[3])
                else:
                    dims = 3
                    title = 'Joint Probabiity Distribution -- ' + query
                    xlabel = 'x'
                    ylabel = 'y'
                    zlabel = 'P(' + vars[0] + ' = x, ' + vars[1]  + ' = y)'
                    # 3d Graph doesn't show range bands
                    mean = 0.0
                    r2low = 0.0
                    r1low = 0.0
                    r1high = 0.0
                    r2high = 0.0

                info = (dataname, querytype, dims, title, xlabel, ylabel, zlabel, mean, r1low, r1high, r2low, r2high)
            elif querytype == 'cprob':
                dims = 3
                title = 'Probability Plot -- ' + query
                xlabel = condvars[0]
                ylabel = 'x'
                zlabel = 'P(' + targetvars[0] + ' = x | ' + condvars[0] + ')'
                info = (dataname, querytype, dims, title, xlabel, ylabel, zlabel, 0.0, 0.0, 0.0, 0.0, 0.0)
            elif querytype == 'bprob':
                dims = 2
                title = 'Probability Plot -- ' + query
                zlabel = ''
                if len(condvars) == 1:
                    dims = 2
                    xlabel = condvars[0]
                    ylabel = query
                elif len(condvars) == 2:
                    dims = 3
                    xlabel = condvars[0]
                    ylabel = condvars[1]
                    zlabel = query
                info = (dataname, querytype, dims, title, xlabel, ylabel, zlabel, 0.0, 0.0, 0.0, 0.0, 0.0)
            elif querytype == 'expct':
                title = 'Expectation Plot -- ' + query
                zlabel = ''
                if len(condvars) == 1:
                    dims = 2
                    xlabel = condvars[0]
                    ylabel = 'E(' + targetvars[0] + ' | ' + condvars[0] + ')'
                elif len(condvars) == 2:
                    dims = 3
                    xlabel = condvars[0]
                    ylabel = condvars[1]
                    zlabel = 'E(' + targetvars[0] + ' | ' + condvars[0] + ', ' + condvars[1]+ ')'
                info = (dataname, querytype, dims, title, xlabel, ylabel, zlabel, 0.0, 0.0, 0.0, 0.0, 0.0)
            elif querytype in ['dep', 'cor']:
                # Dependence or correlation heatmap
                if querytype == 'dep':
                    title = 'Dependency Heatmap'
                else:
                    title = 'Correlation Heatmap'
                dims = 3
                xlabel = ''
                ylabel = ''
                zlabel = query
                info = (dataname, querytype, dims, title, xlabel, ylabel, zlabel, 0.0, 0.0, 0.0, 0.0, 0.0)
            elif querytype == 'cmodel':
                dims = 3
                title = 'Causal Model'
                xlabel = ''
                ylabel = ''
                zlabel = query
                info = (dataname, querytype, dims, title, xlabel, ylabel, zlabel, 0.0, 0.0, 0.0, 0.0, 0.0)
            yield info
        except:
            from because.hpcc_utils import format_exc
            raise RuntimeError(format_exc.format('viz.fillChartInfo'))
    ENDEMBED; // fillChartInfo

    EXPORT DATASET(ChartInfo) getChartInfo(STRING query, STRING dataname, UNSIGNED PS) := FUNCTION
        qresults := parseQuery(query, PS);
        vars := getVarNames(qresults);
        results := fillChartInfo(vars, query, qresults, dataname, PS);
        RETURN results;
    END;

    EXPORT Plot(queries, PS)  := MACRO
        IMPORT Python3 AS Python;
        STRING _plotPyFunc(SET OF STRING pyqueries, STRING pyps) := EMBED(Python: fold)
            outStr = """
                    IMPORT HPCC_causality AS _HC;
                    // IMPORT $.^.^ AS _HC;    // GJS Testing
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
            finalStr = """OUTPUT(DATASET({plots}, {{STRING name}}), NAMED('__hpcc_index_html'));
                        """.format(plots = plotNameStr)
            outStr += finalStr
            return outStr
        ENDEMBED;
        cmd := _plotPyFunc(queries, #TEXT(PS));
        //OUTPUT(cmd, NAMED('cmd'));
        #EXPAND(cmd);
    ENDMACRO;
END;
