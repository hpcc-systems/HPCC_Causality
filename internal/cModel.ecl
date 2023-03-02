
IMPORT Python3 AS Python;
IMPORT ML_CORE.Types AS cTypes;
IMPORT $.^ AS HPCC_Causality;
IMPORT HPCC_Causality.Types;
IMPORT Std.System.Thorlib;

nNodes := Thorlib.nodes();
node := Thorlib.node();

NumericField := cTypes.NumericField;
cModelTyp := Types.cModel;
PDist := Types.Distribution;
ValidationReport := Types.ValidationReport;
ProbQuery := Types.ProbQuery;
cMetrics := Types.cMetrics;
ScanReport := Types.ScanReport;
DiscResult := Types.DiscoveryResult;

globalScope := 'probspace' + node + '.ecl';

dummyRec := RECORD
    UNSIGNED id;
END;

varMetricsItem := RECORD
    SET OF STRING key;
    REAL value;
END;

varMetrics := RECORD
    DATASET(varMetricsItem) indeps;
    DATASET(varMetricsItem) directions;
END;

varsRec := RECORD
    SET OF STRING vars;
END;

/**
  * Internal Module to provide access to the Causality methods of the python "Because.causality" package.
  *
  * This is used by the Causality.ecl module to communicate to the python methods.
  * It allocates a cModel python object in global memory during the Init function, 
  * and all other functions refer to that global object "CM". 
  */
EXPORT cModel := MODULE
    /**
      * Init takes a Types.cModel model in as well as a dataset in NumericField format.
      * It returns an UNSIGNED, which is passed on to all other functions to make sure
      * that Init is run before them.
      */
    EXPORT UNSIGNED Init(DATASET(cModelTyp) mod, UNSIGNED PS) := FUNCTION

        STREAMED DATASET(dummyRec) pyInit(STREAMED DATASET(cModelTyp) mods, UNSIGNED pyps,
                            UNSIGNED pynode, UNSIGNED pynnodes) :=
                            EMBED(Python: globalscope(globalScope), persist('query'), activity)
            from because.causality import cgraph
            from because.causality import rv
            from because.hpcc_utils import globlock
            global CM, NODE, NNODES
            globlock.allocate()
            globlock.acquire()
            if 'CM' in globals():
                # Already Initialized.  We are done.
                # Release the global lock
                globlock.release()
                return [(1,)]
            try:
                assert 'PS' in globals(), 'Causality.Init: PS is not initialized.'
                # Save the node number and the number of nodes
                NODE = pynode
                NNODES = pynnodes
                RVs = []
                DS = {}
                varMap = {}
                for mod in mods:
                    # Should only be one model
                    modName = mod[0]
                    pyrvs = mod[1]
                    for i in range(len(pyrvs)):
                        pyrv = pyrvs[i]
                        rvName = pyrv[0]
                        rvParents = pyrv[1]
                        rvIsDiscrete = pyrv[2]
                        rvType = pyrv[3]
                        DS[rvName] = []
                        varMap[i+1] = rvName
                        newrv = rv.RV(rvName, rvParents, rvIsDiscrete, rvType)
                        RVs.append(newrv)
                ids = []
                lastId = None
                CM = cgraph.cGraph(RVs, ps=PS)
                # Release the global lock
                globlock.release()
                return [(1,)]
            except:
                from because.hpcc_utils import format_exc
                # Release the global lock
                globlock.release()
                assert False, format_exc.format('cModel.Init')
        ENDEMBED;
        mod_distr := DISTRIBUTE(mod, ALL);
        cmds := pyInit(mod_distr, PS, node, nNodes);
        cm := MAX(cmds, id);
        RETURN cm;
    END;
    /**
      * Call the cModel TestModel function and return a Validation Report.
      *
      * This function is executed on all cluster nodes.  The set of tests and their order
      * is deterministic, so each node runs a subset of the tests based on its node number.
      * Since each node only runs a subset of the tests, the final score produced by each node
      * is not meaningful.  The set of results from all nodes should be passed to the "ScoreModel"
      * function below to produce a final score.  All of the heavy lifting (i.e. running the tests)
      * is fully paralellized.  Calculating the final score is light weight.  
      *
      */
    EXPORT STREAMED DATASET(ValidationReport) TestModel(UNSIGNED order, UNSIGNED power, UNSIGNED cm) := 
        EMBED(Python: globalscope(globalScope), persist('query'), activity)
        try:
            rep = None
            deps = CM.computeDependencies(order)
            myDeps = []
            for i in range(len(deps)):
                # Each node will only test 1/NNODES of the dependencies.  They will each get tested
                # by one nodes.
                if NODE == i % NNODES:
                    myDeps.append(deps[i])
            edges = CM.getEdges()
            myEdges = []
            for i in range(len(edges)):
                # Each node will only test 1/NNODES of the edges.  They will each get tested
                # by one node.
                if NODE == i % NNODES:
                    myEdges.append(edges[i])
            rep = CM.TestModel(order = order, power=power, deps = myDeps, edges = myEdges)
            #   - confidence is an estimate of the likelihood that the data generating process defined
            #       by the model produced the data being tested.  Ranges from 0.0 to 1.0.
            #   - numTotalTests is the number of independencies and dependencies implied by the model.
            #   - numTestsPerType is a list, for each error type, 0 - nTypes, of the number of tests that
            #       test for the given error type.
            #   - numErrsPerType is a list, for each error type, of the number of failed tests.
            #   - numWarnsPerType is a list, for each error type, of the number of tests with warnings.
            #   - errorDetails is a list of failed tests, each with the following format:
            #       [(errType, x, y, z, isDep, errStr)]
            #       Where:
            #           errType = 0 (Exogenous variables not independent) or;
            #                    1 (Expected independence not observed) or; 
            #                   2 (Expected dependence not observed)
            #           x, y, z are each a list of variable names that
            #               comprise the statement x _||_ y | z.
            #               That is x is independent of y given z.
            #           isDep True if a dependence is expected.  False for 
            #               independence
            #           pval -- The p-val returned from the independence test
            #           errStr A human readable error string describing the error
            #   - warningDetails is a list of tests with warnings.  Format is the same as
            #       for errorDetails above.
            conf, numTotal, numPerType, numErrsPerType, numWarnsPerType, errorDetails, warnDetails = rep
            errStrs = [err[6] for err in errorDetails]
            warnStrs = [warn[6] for warn in warnDetails]
            return [(conf, numTotal, numPerType, numErrsPerType, numWarnsPerType, errStrs, warnStrs)]
        except:
            from because.hpcc_utils import format_exc
            assert False, format_exc.format('TestModel')
    ENDEMBED;

    /**
      * Given the rolled up results of a set of tests conducted on each node, a final score
      * is produced on a single node.
      *
      * This function executes on a single node.
      */
    EXPORT REAL ScoreModel(SET OF UNSIGNED numtestspertype, SET OF UNSIGNED numerrspertype,
                            SET OF UNSIGNED numwarnspertype, UNSIGNED cm) := 
        EMBED(Python: globalscope(globalScope), persist('query'))
        try:
            confidence = CM.scoreModel(numtestspertype, numerrspertype, numwarnspertype)
            return confidence
        except:
            from because.hpcc_utils import format_exc
            assert False, format_exc.format('ScoreModel')
    ENDEMBED;

    EXPORT STREAMED DATASET(PDist) Intervene(STREAMED DATASET(ProbQuery) queries, UNSIGNED pwr, UNSIGNED cm) := 
        EMBED(Python: globalscope(globalScope), persist('query'), activity)
        import numpy as np
        from because.hpcc_utils import formatQuery
        assert 'CM' in globals(), 'cModel.Intervene: CM is not initialized.'
        try:
            results = []
            for query in queries:
                targets = []
                id, targs, conds, dolist = query[:4]
                assert len(targs) == 1, 'cModel.Intervene: Intervention target must be singular.'
                for targ in targs:
                    var, args = targ
                    assert len(args) == 0, 'cModel.Intervene: Target must be unbound (i.e. No arguments provided).'
                    targSpec = var
                    targets.append(targSpec)
                assert len(targets) == 1, 'cModel.Intervene:  Distribution can only be given for a single target. ' + str(len(targets)) + ' were given.'
                targets = targets[0]
                interventions = []
                for do in dolist:
                    cVar, cArgs = do
                    assert len(cArgs) == 1, 'cModel.Intervene: Do specifications must be an exact value (i.e. Args must have one and only one entry).'
                    intervention = (cVar, cArgs[0])
                    interventions.append(intervention)
                conditions = []
                for cond in conds:
                    cVar, cArgs = cond
                    if len(cArgs) >= 1:
                        condition = (cVar,) + tuple(cArgs)
                    else:
                        condition = cVar
                    conditions.append(condition)
                dist = CM.intervene(targets, interventions, conditions, power=pwr)
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
                                formatQuery.format('distr', targets, conditions, interventions),
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
            assert False, format_exc.format('cModel.Intervene')
    ENDEMBED;

    /**
      * Calculate causal metrics: Average Causal Effect, Controlled Direct Affect, and Indirect Effect
      * for a list of variable pairs.
      *
      * The work is divvied up among the nodes, so is fully parallelized.
      *
      */
    EXPORT STREAMED DATASET(cMetrics) Metrics(STREAMED DATASET(ProbQuery) queries, UNSIGNED pwr, UNSIGNED cm) := 
        EMBED(Python: globalscope(globalScope), persist('query'), activity)
        import numpy as np
        assert 'CM' in globals(), 'cModel.Metrics: CM is not initialized.'
        try:
            results = []
            for query in queries:
                targets = []
                id, targs, conds, dolist = query[:4]
                for targ in targs:
                    var, args = targ
                    assert len(args) == 0, 'cModel.Metrics: Target must be unbound (i.e. No arguments provided).'
                    targSpec = var
                    targets.append(targSpec)
                assert len(targets) == 2, 'cModel.Metrics:  Metrics require two unbound targets ' + str(len(targets)) + ' were given.'
                interventions = []
                assert len(conds) == 0, 'cModel.Metrics: Conditions are not allowed in metrics query.'
                assert len(dolist) == 0, 'cModel.Metrics: Interventions are not allowed in metrics query.'
                cause = targets[0]
                effect = targets[1]
                ace = CM.ACE(cause, effect, power=pwr)
                cde = CM.CDE(cause, effect, power=pwr)
                cie = ace - cde
                results.append((id, cause + ' -> ' + effect, ace, cde, cie))
            return results
        except:
            from because.hpcc_utils import format_exc
            assert False, format_exc.format('cModel.Metrics')
    ENDEMBED;

    EXPORT dummyRec := RECORD
        UNSIGNED dummy;
    END;
    SHARED CacheEntry := RECORD
        SET OF STRING cacheKey;
        REAL cacheVal;
    END;
    /**
      * Results of pyGetCache method.
      *
      * Currently only indepCache is produced. 
      */
    EXPORT Cache := RECORD
        DATASET(cacheEntry) indepCache;
        DATASET(cacheEntry) dirCache;
        UNSIGNED node;
        UNSIGNED nnodes;
    END;

    /**
      * Embed method to estimate the causal direction for all variable pairs, and return as a cache.
      *
      * The workload is automatically allocated among the nodes, so that each variable pairing
      * is executed on only one node.
      */
    EXPORT DATASET(Cache) pyGetCache(STREAMED DATASET(varsRec) vars, UNSIGNED cm, UNSIGNED pynode, UNSIGNED pynnodes, UNSIGNED order=3, UNSIGNED pwr=1) := 
                EMBED(Python: globalscope(globalScope), persist('query'), activity)
        assert 'CM' in globals(), 'cModel.getCache: CM is not initialized.'
        try:
            indCache = {}
            dirCache = {}
            vars = []
            for rec in vars:
                # Should be exactly one RECORD
                vars = rec.vars
            if not vars:
                # If not supplied, use all vars.
                vars = CM.prob.getVarNames()
            item = 0
            for i in range(len(vars)):
                v1 = vars[i]
                for j in range(i+1, len(vars)):
                    v2 = vars[j]
                    dirCacheKey = (v1, v2)
                    if item % pynnodes != pynode:
                        item += 1
                        continue
                    item += 1
                    rho = CM.testDirection(v1, v2)
                    dirCache[dirCacheKey] = rho
                    revKey = (v2, v1)
                    dirCache[revKey] = -rho
            outDirCache = []
            outIndCache = []
            for key in dirCache:
                rho = dirCache[key]
                outDirCache.append(([key[0], key[1]], rho))
            outRec = (outIndCache, outDirCache, pynode, pynnodes)
            return [outRec]
        except:
            from because.hpcc_utils import format_exc
            assert False, format_exc.format('cModel.getCache')
    ENDEMBED;

    /**
      * Estimate the causal direction for each pair of variables.
      *
      * The work is automatically distributed among nodes using pyGetCache (above).
      *
      */
    EXPORT getCache(SET OF STRING vars, UNSIGNED cm, UNSIGNED order=3, UNSIGNED pwr=1) := FUNCTION
        // Create a vars record on each node
        varsDat := DATASET([{vars}], varsRec, LOCAL);
        cache := pyGetCache(varsDat, cm, node, nnodes, order, pwr);
        return cache;
    END;

    /**
      * Embedded function to execute a causal scan  on a single node.
      *
      * The cache parameter allows much of the heavy lifting to be done in parallel
      * using getCache (above).  There is still quite a bit of work that is done on a single
      * node and cannot be distributed.
      */
    SHARED ScanReport pyScanModel(UNSIGNED pwr, UNSIGNED cm, DATASET(cache) pycache=DATASET([], cache)) := 
            EMBED(Python: globalscope(globalScope), persist('query'))
        from because.causality import cscan
        assert 'CM' in globals(), 'cModel.pyDiscoverModel: CM is not initialized.'
        try:
            dirCache = {}
            for rec in pycache:
                dirC = rec[1]
                for item in dirC:
                    key, val = item
                    cacheKey = tuple(key)
                    dirCache[cacheKey] = val
            if dirCache:
                CM.setDirCache(dirCache)
            scanner = cscan.Scanner(cg=CM, power=pwr)
            results = scanner.scan()
            clustNameMap = {}
            clustNames = []
            for clustTup in results['clusters']: 
                clustName = '~'.join(clustTup)
                clustNames.append(clustName)
                clustNameMap[clustTup] = clustName
            clustMembersOut = []
            clustMembers = results['clustMembers']
            for clustTup in clustMembers:
                clustName = clustNameMap[clustTup]
                vars = clustMembers[clustTup]
                clustMembersOut.append((clustName, vars))
            clustGraphOut = []
            clustGraph = results['clustGraph']
            for clustTup in clustGraph:
                parentNames = []
                clustName = clustNameMap[clustTup]
                parentTups = clustGraph[clustTup]
                for parentTup in parentTups:
                    parentName = clustNameMap[parentTup]
                    parentNames.append(parentName)
                clustGraphOut.append((clustName, parentNames))
            varGraphOut = []
            varGraph = results['varGraph']
            exos = results['exoVars']
            for varName in varGraph:
                varParents = varGraph[varName]
                varGraphOut.append((varName, varParents))
            return (exos, clustNames, clustMembersOut, clustGraphOut, varGraphOut)
        except:
            from because.hpcc_utils import format_exc
            assert False, format_exc.format('cModel.pyDiscoverModel')

    ENDEMBED; // PyScanModel

        /**
      * Embedded function to execute a causal discovery  on a single node.
      *
      * The cache parameter allows much of the heavy lifting to be done in parallel
      * using getCache (above).  There is still quite a bit of work that is done on a single
      * node and cannot be distributed.
      */
    SHARED DATASET(DiscResult) pyDiscModel(SET OF STRING vars, REAL pwr, REAL sensitivity, UNSIGNED depth, UNSIGNED cm, DATASET(cache) pycache=DATASET([], cache)) := 
            EMBED(Python: globalscope(globalScope), persist('query'))
        from because.causality import cdisc
        assert 'CM' in globals(), 'cModel.pyDiscModel: CM is not initialized.'
        #assert False, 'pwr, sensitivity, depth = ' + repr(pwr) + ', ' + repr(sensitivity) + ', ' + repr(depth)
        try:
            dirCache = {}
            for rec in pycache:
                dirC = rec[1]
                for item in dirC:
                    key, val = item
                    cacheKey = tuple(key)
                    dirCache[cacheKey] = val
            if dirCache:
                CM.setDirCache(dirCache)
            ps = CM.prob  # Get the probspace instance from CM
            #assert False, 'ps.N = ' + str(ps.N)
            if not vars:
                vars = ps.getVarNames()
            newCM = cdisc.discover(ps, vars, maxLevel=depth, power=pwr, sensitivity=sensitivity)
            edges = newCM.getEdges()
            edgeNodes = {}
            for edge in edges:
                cause, effect = edge
                edgeNodes[cause] = True
                edgeNodes[effect] = True
                strength = newCM.getEdgeProp(edge, 'dir_rho')
                corr = ps.corrCoef(cause, effect)
                MDE = 0.0;
                yield (cause, effect, float(strength), float(corr), float(MDE))
            # If any variable was not in an edge, add a pseudo-edge, so
            # that it shows in the graph.
            for var in vars:
                edgeVar = edgeNodes.get(var, None)
                if edgeVar is None:
                    # Not in any edge
                    yield(var, '', 0.0, 0.0, 0.0)
        except:
            from because.hpcc_utils import format_exc
            assert False, format_exc.format('cModel.pyDiscModel')

    ENDEMBED; // pyDiscModel

    /**
      * Function to execute a causal scan  on a single node.
      *
      * The cache parameter allows much of the heavy lifting to be done in parallel
      * using getCache (above).  There is still quite a bit of work that is done on a single
      * node and cannot be distributed.
      * Uses pyDiscoverModel above to communicate with the python package.
      */
    EXPORT ScanReport ScanModel(UNSIGNED pwr, UNSIGNED cm) := FUNCTION
        vars := [];
        cache := getCache(vars, CM, pwr:=pwr);
        rpt := pyScanModel(pwr, cm, cache);
        RETURN rpt;
    END;

    /**
      * Function to execute a discovery on a single node.
      *
      * The cache parameter allows much of the heavy lifting to be done in parallel
      * using getCache (above).  There is still quite a bit of work that is done on a single
      * node and cannot be distributed.
      * Uses pyDiscoverModel above to communicate with the python package.
      */
    EXPORT DATASET(DiscResult) DiscoverModel(SET OF STRING vars, REAL pwr, REAL sensitivity, UNSIGNED depth, UNSIGNED cm) := FUNCTION
        //cache := getCache(vars, cm, pwr:=pwr);
        rslt := pyDiscModel(vars, pwr, sensitivity, depth, cm);
        RETURN rslt;
    END;
END;