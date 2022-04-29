/**
  * Test and example for Model Validation.
  *
  * Uses the Synth module to generate M8 model.
  * Tests against the M8 model definition:
  * B is Exogenous
  * F is Exogenous
  * G is Exogenous
  * A <- B,F
  * D <- A,G
  * C <- B,A,D
  * E <- C
  *
  */
IMPORT $.^.^ AS Cause;
IMPORT Cause.Types;

IMPORT ML_CORE.Types AS cTypes;


NumericField := cTypes.NumericField;
SEM := Types.SEM;

// Number of test records.
nTestRecs := 100000;


// SEM should be a dataset containing a single row.
// SEM is Model M8.
semRow := ROW({
    [],
    ['A', 'B', 'C', 'D', 'E', 'F', 'G'], // Variable names
    // Equations
    ['B = logistic(0,1)',  // Can use any distribution defined in numpy.random
    'F = logistic(0,1)',
    'G = logistic(0,1)',
    'A = (B + F) / 2.0 + logistic(0,.4)',
    'D = (A + G) / 2.0 + logistic(0,.4)',
    'C = (B + A + D) / 3.0 + logistic(0,.4)',
    'E = C + logistic(0,.4)'
    ]}, SEM);

mySEM := DATASET([semRow], SEM);

testDat := Cause.Synth(mySEM).Generate(nTestRecs);

// Note: The order of variables in the model much match the order of varNames in the SEM.
RVs := DATASET([
                {'A', ['B','F']},
                {'B', []},
                {'C', ['B', 'A', 'D']},
                {'D', ['A','G']},
                {'E', ['C']},
                {'F', []},
                {'G', []}
                ], Types.RV);
mod := DATASET([{'M8', RVs}], Types.cModel);

OUTPUT(mySEM, NAMED('SEM'));
OUTPUT(mod, NAMED('Model'));

cm := Cause.Causality(mod, testDat);

rept := cm.DiscoverModel();
OUTPUT(rept, NAMED('DiscoveryReport'));

//cache := cm.getCache();
//OUTPUT(cache, NAMED('Cache'));