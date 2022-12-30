IMPORT Std.System.Thorlib;
IMPORT ML_CORE.Types AS cTypes;
IMPORT $ AS HPCC_Causality;
IMPORT HPCC_Causality.Types;
IMPORT Python3 AS Python;
SEM := Types.SEM;
nNodes := Thorlib.nodes();
node := Thorlib.node();
NumericField := cTypes.NumericField;
AnyField := Types.AnyField;

/**
  * Module to produce a synthetic, multivariate dataset from a Structural Equation
  * Model (SEM).
  *
  * This allows creation of datasets with known distributional and or causal characteristics.
  *
  * @param semDef  A Structural Equation Model in Types.SEM format.
  * @see Types.SEM
  * @see Test/Synth/synthTest.ecl for an example
  *
  */
EXPORT Synth(DATASET(SEM) semDef) := MODULE
    globalScope := 'causality' + node + '.ecl';
    /**
      * Generate the data.
      *
      * Data generation is fully parallelized, each node generates numRecs / nNodes samples
      * from the same multivariate distribution.     *
      * @param numRecs The number of samples (multivariate observations) to generate.
      * @return The generated samples in NumericField format.  The field numbers correspond
      *    to the order of variables specified in the SEM.
      * @see ML_Core.Types.NumericField
      */
    EXPORT DATASET(AnyField) Generate(UNSIGNED numRecs) := FUNCTION
        /**
          * Embed function to do the gereration using the "Because.synth" python module.
          * @private
          */
        STREAMED DATASET(AnyField) pySynth(STREAMED DATASET(SEM) pysem,
                    UNSIGNED nrecs, UNSIGNED pynnodes, UNSIGNED pynode)
                        := EMBED(Python: globalscope(globalScope), persist('query'), activity)
            from math import ceil
            import because
            import importlib
            importlib.reload(because)
            from because.synth import gen_data
            from because.hpcc_utils import format_exc
            fullmods = [x for x in pysem]
            init, vars, sem = fullmods[0]
            recsPerNode = ceil(nrecs / pynnodes)
            firstId = pynode * recsPerNode + 1
            if pynode == pynnodes - 1:
                numRecs = nrecs - firstId + 1
            else:
                numRecs = recsPerNode
            gen = gen_data.Gen(mod=vars, sem=sem, init=init)
            try:
                recs = gen.samples(numRecs)
            except:
                exc = format_exc.format('generate')
                assert False, exc
            outrecs = []
            i = 0
            try:
                for rec in recs:
                    for j in range(len(rec)):
                        val = rec[j]
                        if type(val) == type(''):
                            outrec = (1, firstId + i, j+1, 0.0, val)
                        else:
                            outrec = (1, firstId + i, j+1, float(val), '')
                        outrecs.append(outrec)
                    i += 1
            except:
                exc = format_exc.format('retrieve')
                assert False, exc
            return outrecs
        ENDEMBED;

        // Distribute SEM to All Nodes
        semDist := DISTRIBUTE(semDef, ALL);
        outData0 := pySynth(semDist, numRecs, nNodes, node);
        outData := SORT(outData0, id, number, LOCAL);
        RETURN outData;    
    END;
END;
