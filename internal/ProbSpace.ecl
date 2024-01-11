IMPORT Python3 AS Python;
IMPORT ML_CORE.Types AS cTypes;
IMPORT $.^ AS HPCC_Causality;
IMPORT HPCC_Causality.Types;
IMPORT Std.System.Thorlib;

nNodes := Thorlib.nodes();
node := Thorlib.node();

NumericField := cTypes.NumericField;
ProbQuery := Types.ProbQuery;
PDist := Types.Distribution;
AnyField := Types.AnyField;
nlQuery := Types.nlQuery;
DatasetSummary := Types.DatasetSummary;

globalScope := 'probspace' + node + '.ecl';

dummyRec := RECORD
    UNSIGNED id;
END;

/**
  * Internal module providing interface into the "Because.probability" python module.
  * This is used by the top-level Probability.ecl module.
  */
EXPORT ProbSpace := MODULE
    /**
      * Init takes a list of variables in as well as a dataset in NumericField format.
      * The variable name list must be in the same order as the field numbers in the 
      * NumericField dataset.
      * It returns an UNSIGNED, which is passed on to all other functions to make sure
      * that Init is run before them.
      */
    EXPORT UNSIGNED Init(DATASET(AnyField) ds, SET OF STRING varNames, SET OF STRING categoricals=[]) := FUNCTION

        STREAMED DATASET(dummyRec) pyInit(STREAMED DATASET(AnyField) ds, SET OF STRING vars,
                            SET OF STRING pycategoricals,
                            UNSIGNED pynode, UNSIGNED pynnodes) :=
                            EMBED(Python: globalscope(globalScope), persist('query'), activity)
            from because.probability import ProbSpace
            from because.hpcc_utils import globlock # Global lock
            global extractSpec
            def _extractSpec(inSpecs):
                """
                """
                outSpecs = []
                for inSpec in inSpecs:
                    var, args, strArgs, isEnum = inSpec
                    #assert False, 'inSpec = ' + var + ',' + str(args) + ',' + str(strArgs) + ',' + str(isEnum) + ',' + str(len(args))
                    if len(args) > 2 or isEnum or (strArgs and len(strArgs) > 1):
                        # Is an enumeration of valid values
                        if strArgs:
                            # String arguments
                            outSpec = (var, strArgs)
                        else:
                            # Numeric arguments
                            outSpec = (var, args) 
                    else:                
                        # Is bare variable, exact value, or range.
                        if strArgs:
                            outSpec = (var, strArgs[0])
                        else:
                            if len(args) == 0:
                                outSpec = (var,)
                            elif len(args) == 1:
                                outSpec = (var, args[0])
                            else:
                                outSpec = (var,) + tuple(args)
                    outSpecs.append(outSpec)
                return outSpecs
            extractSpec = _extractSpec
            globlock.allocate()
            globlock.acquire()
            global PSDict
            try:
                if 'PSDict' not in globals():
                    PSDict = {}
                DS = {}
                varMap = {}
                for i in range(len(vars)):
                    var = vars[i]
                    DS[var] = []
                    varMap[i+1] = var
                ids = []
                lastId = None
                for rec in ds:
                    wi, id, num, val, strVal = rec
                    if strVal:
                        DS[varMap[num]].append(strVal)
                    else:
                        DS[varMap[num]].append(val)
                    if id != lastId:
                        ids.append(id)
                        lastId = id
                PS = ProbSpace(DS, categorical=pycategoricals)
                psID = len(PSDict) + 1
                PSDict[psID] = PS
                # Release the global lock
                globlock.release()
                return [(psID,)]
            except:
                from because.hpcc_utils import format_exc
                # Release the global lock
                globlock.release()
                assert False, format_exc.format('ProbSpace,Init')
        ENDEMBED;
        ds_distr := DISTRIBUTE(ds, ALL);
        ds_S := SORT(NOCOMBINE(ds_distr), id, number, LOCAL);
        psds := pyInit(NOCOMBINE(ds_S), varNames, categoricals, node, nNodes);
        psid := MAX(psds, id);
        RETURN psid;
    END;

    /** 
      * Dataset Summary
      */
    EXPORT DatasetSummary getSummary(UNSIGNED ps) := 
        EMBED(Python: globalscope(globalScope), persist('query'))
        assert 'PSDict' in globals(), 'ProbSpace.DatasetSummary: PSDict is not initialized.'
        assert ps in PSDict, 'ProbSpace.DatasetSummary: invalid probspace id = ' + str(ps)
        PS = PSDict[ps]
        try:
            nRecs = PS.N
            vars = PS.getVarNames()
            varDetails = []
            for var in vars:
                vals = []
                strVals = []
                isDisc = PS.isDiscrete(var)
                isCat = PS.isCategorical(var)
                isStr = PS.isStringVal(var)
                if isDisc:
                    card = PS.cardinality(var)
                    rawvals = PS.getValues(var)
                    if isStr:
                        strVals = rawvals
                        for val in rawvals:
                            nval = PS.strToNum(var, val)
                            vals.append(float(nval))
                    else:
                        vals = [float(rawval) for rawval in rawvals]
                else:
                    card = nRecs
                varDetails.append((var, isDisc, isCat, isStr, card, vals, strVals))
            return ((nRecs, vars, varDetails))
        except:
            from because.hpcc_utils import format_exc
            assert False, format_exc.format('ProbSpace.getSummary')
        
    ENDEMBED;

    EXPORT STREAMED DATASET(dummyRec) SubSpace(STREAMED DATASET(nlQuery) filters, UNSIGNED ps) := 
        EMBED(Python: globalscope(globalScope), persist('query'), activity)
        from because.hpcc_utils.parseQuery import Parser
        assert 'PSDict' in globals(), 'ProbSpace.SubSpace: PSDict is not initialized.'
        assert ps in PSDict, 'ProbSpace.SubSpace: invalid probspace id = ' + str(ps)
        PS = PSDict[ps]
        # Should only be one query in the dataset
        try:
            for filter in filters:
                id, filt = filter
                # We modify the filter to make it look like a probability query so that
                # we can re-use the parser
                query = 'P(' + filt + ')'
                qs = [query]
                PARSER = Parser()
                specList = PARSER.parse(qs)
                spec = specList[0]
                cmd, targs, conds, ctrlfor, intervs, cfac = spec
                # We only care about the target clause, which is now a structured filter.
                sfilt = targs
                ss = PS.SubSpace(sfilt)
                psid = len(PSDict) + 1
                PSDict[psid] = ss
                return ([(psid,)])
        except:
            from because.hpcc_utils import format_exc
            assert False, format_exc.format('ProbSpace.SubSpace')
    ENDEMBED;



    /**
      * Call the ProbSpace.P() function with a set of queries.
      *
      * Queries are distributed among nodes so that the run in parallel.
      *
      */
    EXPORT STREAMED DATASET(NumericField) P(STREAMED DATASET(ProbQuery) queries, UNSIGNED ps) := 
        EMBED(Python: globalscope(globalScope), persist('query'), activity)
        assert 'PSDict' in globals(), 'ProbSpace.P: PSDict is not initialized.'
        assert ps in PSDict, 'ProbSpace.P: invalid probspace id = ' + str(ps)
        PS = PSDict[ps]
        try:
            results = []
            for query in queries:
                targets = []
                id, targs, conds = query[:3]
                targets = extractSpec(targs)
                for target in targets:
                    assert len(target) > 1, 'ProbSpace.P: Targets must be bound (i.e. have at least 1 argument supplied).'
                conditions = extractSpec(conds)
                result = PS.P(targets, conditions)
                #assert False, 'targets, conditions, result = ' + str(targets) + ',' + str(conditions) + ',' + str(result) + ',' + str(PS.getVarNames())
                results.append((1, id, 1, result))
            return results
        except:
            from because.hpcc_utils import format_exc
            assert False, format_exc.format('ProbSpace.P')
    ENDEMBED;

    /**
      * Call the ProbSpace.E() function with a set of queries.
      *
      * Queries are distributed among nodes so that the run in parallel.
      *
      */
    EXPORT STREAMED DATASET(AnyField) E(STREAMED DATASET(ProbQuery) queries, UNSIGNED ps) := 
        EMBED(Python: globalscope(globalScope), persist('query'), activity)
        assert 'PSDict' in globals(), 'ProbSpace.E: PSDict is not initialized.'
        assert ps in PSDict, 'ProbSpace.E: invalid probspace id = ' + str(ps)
        PS = PSDict[ps]
        try:
            results = []
            for query in queries:
                targets = []
                id, targs, conds = query[:3]
                targets = extractSpec(targs)
                assert len(targets) == 1 and len(targets[0]) == 1, 'ProbSpace.E: Target must be single and unbound (i.e. No arguments provided).'
                targets = targets[0]
                conditions = extractSpec(conds)
                result = PS.E(targets, conditions)
                if result is None:
                    # No data fits the condition.  Numeric answer cannot be returned.
                    results.append((1, id, 1, 0.0, 'Expectation Error -- No data fits condition'))
                    result
                elif type(result) == type(''):
                    results.append((1, id, 1, 0.0, result))
                else:
                    results.append((1, id, 1, float(result), ''))
            return results
        except:
            from because.hpcc_utils import format_exc
            assert False, format_exc.format('ProbSpace.E')
    ENDEMBED;

    EXPORT STREAMED DATASET(AnyField) Query(STREAMED DATASET(nlQuery) queries, UNSIGNED ps) := 
        EMBED(Python: globalscope(globalScope), persist('query'), activity)
        from because.probability import probquery
        assert 'PSDict' in globals(), 'ProbSpace.Query: PSDict is not initialized.'
        assert ps in PSDict, 'ProbSpace.Query: invalid probspace id = ' + str(ps)
        PS = PSDict[ps]
        try:
            inQueries = []
            inIds = []
            for item in queries:
                id, query = item
                inQueries.append(query)
                inIds.append(id)
            results = probquery.queryList(PS, inQueries, allowedResults=['P','E'])
            for i in range(len(results)):
                result = results[i]
                id = inIds[i]
                if type(result) == type(''):
                    yield (1, id, 1, 0.0, result)
                else:
                    yield (1, id, 1, float(result), '')
        except:
            from because.hpcc_utils import format_exc
            assert False, format_exc.format('ProbSpace.Query')
    ENDEMBED;

    /**
      * Call the ProbSpace.distr() function with a set of natural language
      * queries.
      *
      * Queries are distributed among nodes so that the run in parallel.
      * Returns distributions as Types.Distribution dataset
      *
      */
    EXPORT STREAMED DATASET(PDist) QueryDistr(STREAMED DATASET(nlQuery) queries, UNSIGNED ps) := 
        EMBED(Python: globalscope(globalScope), persist('query'), activity)
        from because.probability import probquery
        import numpy as np
        assert 'PSDict' in globals(), 'ProbSpace.QueryDistr: PSDict is not initialized.'
        assert ps in PSDict, 'ProbSpace.QueryDistr: invalid probspace id = ' + str(ps)
        PS = PSDict[ps]
        try:
            inQueries = []
            inIds = []
            for item in queries:
                id, query = item
                inQueries.append(query)
                inIds.append(id)
            results = probquery.queryList(PS, inQueries, allowedResults=['D'])
            for i in range(len(results)):
                id = inIds[i]
                dist = results[i]
                if dist is None:
                    # No distribution can be created.
                    yield ( id,
                            inQueries[i],
                            0,
                            False,
                            False,
                            0.0,
                            0.0,
                            0.0,
                            0.0,
                            0.0,
                            0.0,
                            0.0,
                            0.0,
                            [False, False],
                            [0.0, 0.0],
                            0,
                            [],
                            [],
                            [(0, 'Distribution Error -- Not enough data points to assess distribution.')]
                            )
                else:
                    hist = []
                    for entry in dist.ToHistTuple():
                        minv, maxv, p = entry
                        hist.append((float(minv), float(maxv), float(p)))
                    isDiscrete = dist.isDiscrete
                    isCategorical = PS.isCategorical(dist.rvName)
                    deciles = []
                    if not isDiscrete:
                        # Only do deciles for continuous data.
                        for p in range(10, 100, 10):
                            decile = float(dist.percentile(p))
                            deciles.append((float(p), float(p), decile))
                    stringVals = []
                    if PS.isStringVal(dist.rvName):
                        strVals = PS.getValues(dist.rvName)
                        for j in range(len(strVals)):
                            strVal = strVals[j]
                            numVal = int(PS.getNumValue(dist.rvName, strVal))
                            stringVals.append((numVal, strVal))
                        stringVals.sort()
                    bounds = dist.truncation()
                    isBoundedL = not isDiscrete and bounds[0] is not None
                    isBoundedU = not isDiscrete and bounds[1] is not None
                    boundL = boundU = 0.0
                    if isBoundedL:
                        boundL = bounds[0]
                    if isBoundedU:
                        boundU = bounds[1]
                    modality = dist.modality()

                    yield ( id,
                            inQueries[i],
                            dist.N,
                            isDiscrete,
                            isCategorical,
                            float(dist.minVal()),
                            float(dist.maxVal()),
                            dist.E(),
                            dist.stDev(),
                            dist.skew(),
                            dist.kurtosis(),
                            float(dist.median()),
                            float(dist.mode()),
                            [isBoundedL, isBoundedU],
                            [boundL, boundU],
                            modality,
                            hist,
                            deciles,
                            stringVals
                            )
        except:
            from because.hpcc_utils import format_exc
            assert False, format_exc.format('QueryDistr')
    ENDEMBED;

    /**
      * Call the ProbSpace.distr() function with a set of queries.
      *
      * Queries are distributed among nodes so that the run in parallel.
      * Returns distributions as Types.Distribution dataset
      *
      */
    EXPORT STREAMED DATASET(PDist) Distr(STREAMED DATASET(ProbQuery) queries, UNSIGNED ps) := 
        EMBED(Python: globalscope(globalScope), persist('query'), activity)
        from because.hpcc_utils import formatQuery
        import numpy as np
        assert 'PSDict' in globals(), 'ProbSpace.Distr: PSDict is not initialized.'
        assert ps in PSDict, 'ProbSpace.Distr: invalid probspace id = ' + str(ps)
        PS = PSDict[ps]
        try:
            results = []
            for query in queries:
                targets = []
                id, targs, conds = query[:3]
                targets = extractSpec(targs)
                assert len(targets) == 1 and len(targets[0]) == 1, 'ProbSpace.Distr: Target must be single and unbound (i.e. No arguments provided).'
                targets = targets[0]
                conditions = extractSpec(conds)
                dist = PS.distr(targets, conditions)
                if dist is None:
                    # No distribution can be created.
                    yield ( id,
                            formatQuery.format('distr', [targets], conditions),
                            0,
                            False,
                            False,
                            0.0,
                            0.0,
                            0.0,
                            0.0,
                            0.0,
                            0.0,
                            0.0,
                            0.0,
                            [False, False],
                            [0.0, 0.0],
                            0,
                            [],
                            [],
                            [(0, 'Distribution Error -- Not enough data points to assess distribution.')]
                            )
                    continue                 
                hist = []
                for entry in dist.ToHistTuple():
                    minv, maxv, p = entry
                    hist.append((float(minv), float(maxv), float(p)))
                isDiscrete = dist.isDiscrete
                isCategorical = PS.isCategorical(dist.rvName)
                deciles = []
                if not isDiscrete:
                    # Only do deciles for continuous data.
                    for p in range(10, 100, 10):
                        decile = float(dist.percentile(p))
                        deciles.append((float(p), float(p), decile))
                stringVals = []
                if PS.isStringVal(dist.rvName):
                    strVals = PS.getValues(dist.rvName)
                    for j in range(len(strVals)):
                        strVal = strVals[j]
                        numVal = int(PS.getNumValue(dist.rvName, strVal))
                        stringVals.append((numVal, strVal))
                    stringVals.sort()
                bounds = dist.truncation()
                isBoundedL = not isDiscrete and bounds[0] is not None
                isBoundedU = not isDiscrete and bounds[1] is not None
                boundL = boundU = 0.0
                if isBoundedL:
                  boundL = bounds[0]
                if isBoundedU:
                  boundU = bounds[1]
                modality = dist.modality()

                yield ( id,
                        formatQuery.format('distr', [targets], conditions),
                        dist.N,
                        isDiscrete,
                        isCategorical,
                        float(dist.minVal()),
                        float(dist.maxVal()),
                        dist.E(),
                        dist.stDev(),
                        dist.skew(),
                        dist.kurtosis(),
                        float(dist.median()),
                        float(dist.mode()),
                        [isBoundedL, isBoundedU],
                        [boundL, boundU],
                        modality,
                        hist,
                        deciles,
                        stringVals
                        )

            return results
        except:
            from because.hpcc_utils import format_exc
            assert False, format_exc.format('Distr')
    ENDEMBED;

    /**
      * Call the ProbSpace.Dependence() function with a set of queries.
      *
      * Queries are distributed among nodes so that the run in parallel.
      *
      */

    EXPORT STREAMED DATASET(NumericField) Dependence(STREAMED DATASET(ProbQuery) queries, UNSIGNED ps) := 
        EMBED(Python: globalscope(globalScope), persist('query'), activity)
        assert 'PSDict' in globals(), 'ProbSpace.Dependence: PSDict is not initialized.'
        assert ps in PSDict, 'ProbSpace.Dependnce: invalid probspace id = ' + str(ps)
        PS = PSDict[ps]
        try:
            results = []
            for query in queries:
                targets = []
                id, targs, conds = query[:3]
                for targ in targs:
                    var, args, strArgs = targ[:3]
                    assert len(args) == 0 and len(strArgs) == 0, 'ProbSpace.Dependence: Target must be unbound (i.e. No arguments provided).'
                    targSpec = var
                    targets.append(targSpec)
                assert len(targets) == 2, 'ProbSpace.Dependence:  Dependence requires two targets. ' + str(len(targets)) + ' were given.'
                v1 = targets[0]
                v2 = targets[1]
                conditions = []
                for cond in conds:
                    cVar, cNumArgs, cStrArgs, cIsList = cond
                    if len(cStrArgs) > 0:
                        cArgs = cStrArgs
                    else:
                        cArgs = cNumArgs

                    if cIsList:
                        condition = (cVar,) + (tuple(cArgs),)
                    else:
                        condition = (cVar,) + tuple(cArgs)
                    conditions.append(condition)
                result = PS.dependence(v1, v2, conditions)
                results.append((1, id, 1, result))
            return results
        except:
            from because.hpcc_utils import format_exc
            assert False, format_exc.format('ProbSpace.Dependence')
    ENDEMBED;

    /**
      * Call the ProbSpace.Predict() function with a set of queries.
      *
      * Queries are distributed among nodes so that the run in parallel.
      *
      */
    EXPORT STREAMED DATASET(NumericField) Predict(STREAMED DATASET(NumericField) ds, 
            SET OF STRING varNames, STRING target, UNSIGNED ps) := 
        EMBED(Python: globalscope(globalScope), persist('query'), activity)
        assert 'PSDict' in globals(), 'ProbSpace.Predict: PSDict is not initialized.'
        assert ps in PSDict, 'ProbSpace.Predict: invalid probspace id = ' + str(ps)
        PS = PSDict[ps]
        try:
            ids = []
            prevId = None
            pyds = {}
            for varName in varNames:
                pyds[varName] = []
            for cell in ds:
                wi, id, num, val = cell
                if prevId != id:
                    ids.append(id)
                    prevId = id
                varName = varNames[num-1]
                pyds[varName].append(val)
            lastLen = None
            for varName in varNames:
                thisLen = len(pyds[varName])
                if lastLen is not None:
                    assert thisLen == lastLen, 'ProbSpace.Predict: Length of variable lists differ.' + \
                        ' Each variable must have the same number of values.'
                lastLen = thisLen
            preds = PS.Predict(target, pyds, varNames)
            for i in range(len(preds)):
                pred = preds[i]
                id = ids[i]
                yield (1, id, 1, pred)
        except:
            from because.hpcc_utils import format_exc
            assert False, format_exc.format('ProbSpace.Predict')
    ENDEMBED;

    /**
      * Call the ProbSpace.Classify() function with a set of queries.
      *
      * Queries are distributed among nodes so that the run in parallel.
      *
      */
    EXPORT STREAMED DATASET(NumericField) Classify(STREAMED DATASET(NumericField) ds, SET OF STRING varNames, STRING target, UNSIGNED ps) := 
        EMBED(Python: globalscope(globalScope), persist('query'), activity)
        assert 'PSDict' in globals(), 'ProbSpace.Classify: PSDict is not initialized.'
        assert ps in PSDict, 'ProbSpace.Classify: invalid probspace id = ' + str(ps)
        PS = PSDict[ps]
        try:
            ids = []
            prevId = None
            pyds = {}
            for varName in varNames:
                pyds[varName] = []
            for cell in ds:
                wi, id, num, val = cell
                if prevId != id:
                    ids.append(id)
                    prevId = id
                varName = varNames[num-1]
                pyds[varName].append(val)
            lastLen = None
            for varName in varNames:
                thisLen = len(pyds[varName])
                if lastLen is not None:
                    assert thisLen == lastLen, 'ProbSpace.Classify: Length of variable lists differ.' + \
                        ' Each variable must have the same number of values.'
                lastLen = thisLen
            preds = PS.Classify(target, pyds, varNames)
            for i in range(len(preds)):
                pred = preds[i]
                id = ids[i]
                yield (1, id, 1, float(pred))
        except:
            from because.hpcc_utils import format_exc
            assert False, format_exc.format('ProbSpace.Classify')
    ENDEMBED;

END;