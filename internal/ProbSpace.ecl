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
    EXPORT UNSIGNED Init(DATASET(NumericField) ds, SET OF STRING varNames) := FUNCTION

        STREAMED DATASET(dummyRec) pyInit(STREAMED DATASET(NumericField) ds, SET OF STRING vars,
                            UNSIGNED pynode, UNSIGNED pynnodes) :=
                            EMBED(Python: globalscope(globalScope), persist('query'), activity)
            from because.probability import ProbSpace
            from because.hpcc_utils import globlock # Global lock
            globlock.allocate()
            globlock.acquire()
            global PS
            if 'PS' in globals():
                # Probspace already allocated on this node (by another thread).  We're done.
                # Release the global lock
                globlock.release()
                return [(1,)] 
            try:
                DS = {}
                varMap = {}
                for i in range(len(vars)):
                    var = vars[i]
                    DS[var] = []
                    varMap[i+1] = var
                ids = []
                lastId = None
                for rec in ds:
                    wi, id, num, val = rec
                    DS[varMap[num]].append(val)
                    if id != lastId:
                        ids.append(id)
                        lastId = id
                PS = ProbSpace(DS)
                # Release the global lock
                globlock.release()
                return [(1,)]
            except:
                from because.hpcc_utils import format_exc
                # Release the global lock
                globlock.release()
                assert False, format_exc.format('ProbSpace,Init')
        ENDEMBED;
        ds_distr := DISTRIBUTE(ds, ALL);
        ds_S := SORT(NOCOMBINE(ds_distr), id, number, LOCAL);
        psds := pyInit(NOCOMBINE(ds_S), varNames, node, nNodes);
        ps := SUM(psds, id);
        RETURN ps;
    END;

    /**
      * Call the ProbSpace.P() function with a set of queries.
      *
      * Queries are distributed among nodes so that the run in parallel.
      *
      */
    EXPORT STREAMED DATASET(NumericField) P(STREAMED DATASET(ProbQuery) queries, UNSIGNED ps) := 
        EMBED(Python: globalscope(globalScope), persist('query'), activity)
        assert 'PS' in globals(), 'ProbSpace.P: PS is not initialized.'
        try:
            results = []
            for query in queries:
                targets = []
                id, targs, conds = query[:3]
                for targ in targs:
                    var, args = targ
                    assert len(args) > 0, 'ProbSpace.P: Target must be bound (i.e. have at least 1 argument supplied).'
                    targSpec = (var,) + tuple(args)
                    targets.append(targSpec)
                if len(targets) == 1:
                    targets = targets[0]

                conditions = []
                for cond in conds:
                    cVar, cArgs = cond
                    if len(cArgs) >= 1:
                        condition = (cVar,) + tuple(cArgs)
                    else:
                        condition = cVar
                    conditions.append(condition)
                result = PS.P(targets, conditions)
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
    EXPORT STREAMED DATASET(NumericField) E(STREAMED DATASET(ProbQuery) queries, UNSIGNED ps) := 
        EMBED(Python: globalscope(globalScope), persist('query'), activity)
        assert 'PS' in globals(), 'ProbSpace.E: PS is not initialized.'
        try:
            results = []
            for query in queries:
                targets = []
                id, targs, conds = query[:3]
                for targ in targs:
                    var, args = targ
                    assert len(args) == 0, 'ProbSpace.E: Target must be unbound (i.e. No arguments provided).'
                    targSpec = var
                    targets.append(targSpec)
                assert len(targets) == 1, 'ProbSpace.E:  Expectation can only be given for a single target. ' + str(len(targets)) + ' were given.'
                targets = targets[0]
                conditions = []
                for cond in conds:
                    cVar, cArgs = cond
                    if len(cArgs) >= 1:
                        condition = (cVar,) + tuple(cArgs)
                    else:
                        condition = cVar
                    conditions.append(condition)
                result = PS.E(targets, conditions)
                results.append((1, id, 1, result))
            return results
        except:
            from because.hpcc_utils import format_exc
            assert False, format_exc.format('ProbSpace.E')
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
        assert 'PS' in globals(), 'ProbSpace.Distr: PS is not initialized.'
        try:
            results = []
            for query in queries:
                targets = []
                id, targs, conds = query[:3]
                for targ in targs:
                    var, args = targ
                    assert len(args) == 0, 'ProbSpace.Distr: Target must be unbound (i.e. No arguments provided).'
                    targSpec = var
                    targets.append(targSpec)
                assert len(targets) == 1, 'ProbSpace.Distr:  Distribution can only be given for a single target. ' + str(len(targets)) + ' were given.'
                targets = targets[0]
                conditions = []
                for cond in conds:
                    cVar, cArgs = cond
                    if len(cArgs) >= 1:
                        condition = (cVar,) + tuple(cArgs)
                    else:
                        condition = cVar
                    conditions.append(condition)
                dist = PS.distr(targets, conditions)
                hist = []
                for entry in dist.ToHistTuple():
                    minv, maxv, p = entry
                    hist.append((float(minv), float(maxv), float(p)))
                isDiscrete = dist.isDiscrete
                deciles = []
                if not isDiscrete:
                    # Only do deciles for continuous data.
                    for p in range(10, 100, 10):
                        decile = float(dist.percentile(p))
                        deciles.append((float(p), float(p), decile))
                results.append((id,
                                formatQuery.format('distr', [(targets,)], conditions),
                                dist.N,
                                isDiscrete,
                                float(dist.minVal()),
                                float(dist.maxVal()),
                                dist.E(),
                                dist.stDev(),
                                dist.skew(),
                                dist.kurtosis(),
                                float(dist.median()),
                                float(dist.mode()),
                                hist,
                                deciles
                                ))
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
        assert 'PS' in globals(), 'ProbSpace.Dependence: PS is not initialized.'
        try:
            results = []
            for query in queries:
                targets = []
                id, targs, conds = query[:3]
                for targ in targs:
                    var, args = targ
                    assert len(args) == 0, 'ProbSpace.Dependence: Target must be unbound (i.e. No arguments provided).'
                    targSpec = var
                    targets.append(targSpec)
                assert len(targets) == 2, 'ProbSpace.Dependence:  Dependence requires two targets. ' + str(len(targets)) + ' were given.'
                v1 = targets[0]
                v2 = targets[1]
                conditions = []
                for cond in conds:
                    cVar, cArgs = cond
                    if len(cArgs) >= 1:
                        condition = (cVar,) + tuple(cArgs)
                    else:
                        condition = cVar
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
    EXPORT STREAMED DATASET(NumericField) Predict(STREAMED DATASET(NumericField) ds, SET OF STRING varNames, STRING target, UNSIGNED ps) := 
        EMBED(Python: globalscope(globalScope), persist('query'), activity)
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