/**
  * Test Intervention Layer features:
  * - Intervene
  * - Average Causal Effect
  * - Controlled Direct Effect
  */

IMPORT $.^.^ AS Cause;
IMPORT Cause.Types;


IMPORT ML_CORE.Types AS cTypes;

ProbSpec := Types.ProbSpec;
ProbQuery := Types.ProbQuery;
NumericField := cTypes.NumericField;
SEM := Types.SEM;

// Number of test records.
nTestRecs := 50000;


// SEM should be a dataset containing a single row.
// SEM is Model M8.
semRow := ROW({
    [],
    ['A', 'B', 'C', 'D', 'E', 'F', 'G'], // Variable names 
    // Equations
    ['B = logistic(0,1)',  // Can use any distribution defined in numpy.random
    'F = logistic(0,1)',
    'G = logistic(0,1)',
    'A = (B + F) / 2.0 + logistic(0,.1)',
    'D = (A + G) / 2.0 + logistic(0,.1)',
    'C = (B + A + D) / 3.0 + logistic(0,.1)',
    'E = C + logistic(0,.1)'
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

testsD := DATASET([{1, DATASET([{'C'}], ProbSpec), DATASET([], ProbSpec), DATASET([{'A',[1]}], ProbSpec)},
                  {2, DATASET([{'C'}], ProbSpec), DATASET([], ProbSpec), DATASET([{'A',[1]},{'B',[1]}], ProbSpec)},
                  {3, DATASET([{'E'}], ProbSpec), DATASET([], ProbSpec), DATASET([{'C',[.5]}], ProbSpec)}
        ], ProbQuery);



distrs := cm.Intervene(testsD);
OUTPUT(distrs, NAMED('Interventions'));

// Test all combinations of variables for causal effects
numVars := COUNT(semRow.varNames);
ProbQuery makeSpec(UNSIGNED ctr) := TRANSFORM
  SELF.id := ctr;
  var1 := semRow.varNames[(ctr - 1) DIV numVars + 1];
  var2 := semRow.varNames[(ctr - 1) % numVars + 1];
  targDat :=  DATASET([{var1}, {var2}], ProbSpec);
  SELF.target := targDat;
END;
// Note: we filter out any with the same variable for source and dest.
testsM := DATASET(numVars * numVars, makeSpec(COUNTER))(target[1].varName != target[2].varName);
OUTPUT(testsM, NAMED('testsM'));

metrics := cm.Metrics(testsM);

OUTPUT(metrics, NAMED('Metrics'));
