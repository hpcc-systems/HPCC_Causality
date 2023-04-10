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
IMPORT $.^.^ AS HC;
IMPORT HC.Types;

IMPORT ML_CORE.Types AS cTypes;

Probability := HC.Probability;
Causality := HC.Causality;
viz := HC.viz;

SEM := Types.SEM;

// Number of test records.
nTestRecs := 100000;


// SEM should be a dataset containing a single row.
// SEM is Model M8.
semRow := ROW({
    [],
    ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'], // Variable names
    // Equations
    ['B = logistic(0,1)',  // Can use any distribution defined in numpy.random
    'F = logistic(0,1)',
    'G = logistic(0,1)',
    'A = sin(B + F) + logistic(0,.4)',
    'D = tanh(A + G * 2.0) + logistic(0,.4)',
    'C = tanh(B + A + D) + logistic(0,.4)',
    'E = C + logistic(0,.4)',
    'H = "small" if E < 0 else "med" if E < 1 else "large"'
    ]}, SEM);

mySEM := DATASET([semRow], SEM);

testDat := HC.Synth(mySEM).Generate(nTestRecs);

OUTPUT(testDat, NAMED('Dat'));

// Note: The order of variables in the model much match the order of varNames in the SEM.
// RVs := DATASET([
//                 {'A', ['B','F']},
//                 {'B', []},
//                 {'C', ['B', 'A', 'D']},
//                 {'D', ['A','G']},
//                 {'E', ['C']},
//                 {'F', []},
//                 {'G', []}
//                 'H',
//                 ], Types.RV);
// mod := DATASET([{'M8', RVs}], Types.cModel);

OUTPUT(mySEM, NAMED('SEM'));

prob := Probability(testDat, semRow.VarNames, categoricals:=['H']);

queries := [
    'P(A)',
    'P(A|B)',
    'E(D | A)',
    'E(D | A, G)',
    'P(H = med | E)',
    'Correlation(A,B,C,D,E,F,G,H)',
    'CModel()'
    ];

viz.Plot(queries, prob.PS);
 