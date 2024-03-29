/**
  * Test and example for Model Validation.
  *
  * Uses the Synth module to generate M8 model:
  * B is Exogenous
  * F is Exogenous
  * G is Exogenous
  * A <- B,F
  * D <- A,G
  * C <- B,A,D
  * E <- C
  *
  */
IMPORT $.^.^ AS HPCC_Causality;
IMPORT HPCC_Causality.Types;


IMPORT ML_CORE.Types AS cTypes;

NumericField := cTypes.NumericField;
SEM := Types.SEM;

// Number of test records.
nTestRecs := 10000;


// SEM should be a dataset containing a single row.
// SEM is Model M2.
semRow := ROW({
    [],
    ['A', 'B', 'C', 'D', 'E', 'F', 'G'], // Variable names 
    // Equations
    ['B = logistic(0,1)',  // Can use any distribution defined in numpy.random
    'F = logistic(-1,1)',
    'G = logistic(1,1)',
    'A = (B + F) / 2.0 + logistic(0,.5)',
    'D = (A + G) / 2.0 + logistic(0,.5)',
    'C = (B + A + D) / 3.0 + logistic(0,.5)',
    'E = C + logistic(0,.5)'
    ]}, SEM);

mySEM := DATASET([semRow], SEM);

testDat := HPCC_Causality.Synth(mySEM).Generate(nTestRecs);

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
OUTPUT(testDat, NAMED('DATA'));

// Create a Probability Space (ProbSpace) given the test data.
prob := HPCC_Causality.Probability(testDat, semRow.varNames);
// Create a causal graph given the ProbSpace and the Causal Module
cg := HPCC_Causality.Causality(mod, prob.PS);

rept := cg.ValidateModel(order:=2, pwr:=1);
OUTPUT(rept, NAMED('ValidationReport'));
