/**
  * Test continuous probabilities using a samples from various distributions with
  * non-linear interdependence.
  *
  * Uses the Synth module to generate the test data.
  */
IMPORT $.^.^ AS HPCC_Causality;
IMPORT HPCC_Causality.Types;
IMPORT ML_CORE.Types AS cTypes;
IMPORT ML_CORE;

numRecs := 10000;
numTestRecs := TRUNCATE(numRecs * .1);

NumericField := cTypes.NumericField;
SEM := Types.SEM;
Probability := HPCC_Causality.Probability;
ProbSpec := Types.ProbSpec;
ProbQuery := Types.ProbQuery;

// SEM for a roll of 2 dice (ala Craps).
semRow := ROW({
    [], // Init
    ['Y1', 'Y2', 'Y3', 'Y4', 'Y5', 'X1', 'X2', 'X3', 'X4', 'X5', 'IV1', 'IV2'], // Variable names 
    // Equations
    [
        // Y1 is dependent on X1, Y2 is dependent on X1 and X2, ...
        // Y5 is dependent on X1, ... ,X5
        'X1 = normal(2.5, 1)',
        'X2 = logistic(0, 2)',
        'X3 = beta(2,5)',
        'X4 = abs(normal(0, 2))',
        'X5 = exponential() * .2',
        'Y1 = 2 * X1 + normal(0, .1)',
        'Y2 = -Y1 + tanh(X2*3) + normal(0,.1)',
        'Y3 = -2 * X1 + tanh(X2*3) + sin(X3) + normal(0,.1)',
        'Y4 = Y3 + log(X4, 2) + normal(0, .1)',
        'Y5 = Y4 + 1.1**X5 + normal(0,.1)',
        // IV1 and IV2 create an inverted-v formation
        //  IV1 <- X1 -> IV2 so that we can test conditionalization
        'IV1 = .5 * X1 + normal(0,.1)',
        'IV2 = tanh(-.75 * X1) + normal(0,.1)'
    ]}, SEM);

mySEM := DATASET([semRow], SEM);

// Generate the records to test with
dat := HPCC_Causality.Synth(mySEM).Generate(numRecs);

OUTPUT(dat[..10000], ALL, NAMED('Samples'));

prob := Probability(dat, semRow.VarNames);

tests := DATASET([{1, DATASET([{'X1', [-100, 2.5]}], ProbSpec), DATASET([], ProbSpec)}, // exp:.5
                  {2, DATASET([{'X1', [1.5, 3.5]}], ProbSpec), DATASET([], ProbSpec)}, // exp=.68
                  {3, DATASET([{'X5', [-100,0]}], ProbSpec), DATASET([], ProbSpec)}, // exp=0
                  {4, DATASET([{'X5', [0, 100]}], ProbSpec), DATASET([], ProbSpec)},  // exp=1
                  {5, DATASET([{'Y1', [5.9, 6.1]}], ProbSpec), DATASET([{'X1', [3]}], ProbSpec)},  
                  {6, DATASET([{'Y1', [5.9, 6.1]}], ProbSpec), DATASET([{'X1', [2.95, 3.05]}], ProbSpec)}, 
                  {7, DATASET([{'Y2', [0,1]}], ProbSpec), DATASET([{'X1', [0]}], ProbSpec)},
                  {8, DATASET([{'X5', [0,1]}], ProbSpec), DATASET([], ProbSpec)}
        ], ProbQuery);

results := prob.P(tests);

OUTPUT(results, ALL, NAMED('Probabilities'));

// Now test some Expected Values

testsE := DATASET([{1, DATASET([{'X1'}], ProbSpec), DATASET([], ProbSpec)}, // exp=2.5
                  {2, DATASET([{'Y1'}], ProbSpec), DATASET([], ProbSpec)}, // exp=5
                  {3, DATASET([{'Y1'}], ProbSpec), DATASET([{'X1', [2.5,100]}], ProbSpec)},
                  {4, DATASET([{'Y1'}], ProbSpec), DATASET([{'X1', [1]}], ProbSpec)} // exp=2
                  //{5, DATASET([{'ROLL'}], ProbSpec), DATASET([{'D1', [1,4]}, {'D2', [6]}], ProbSpec)} // exp=8
        ], ProbQuery);

resultsE := prob.E(testsE);

OUTPUT(resultsE, ALL, NAMED('Expectations'));

// Test Full Distributions

testsD := DATASET([{1, DATASET([{'X1'}], ProbSpec), DATASET([], ProbSpec)}, // Should be a normal distribution
                  {2, DATASET([{'X1'}], ProbSpec), DATASET([{'X2',[-100,0]}], ProbSpec)}, // X2 is independent. Should not change the distr.
                  {3, DATASET([{'Y1'}], ProbSpec), DATASET([], ProbSpec)},
                  {4, DATASET([{'Y1'}], ProbSpec), DATASET([{'X1', [2]}], ProbSpec)},
                  {5, DATASET([{'Y2'}], ProbSpec), DATASET([], ProbSpec)},
                  {6, DATASET([{'Y5'}], ProbSpec), DATASET([{'X2',[-100,0]},{'X3',[1]}], ProbSpec)}
        ], ProbQuery);

resultsD := prob.Distr(testsD);

OUTPUT(resultsD, ALL, NAMED('Distributions'));

// Now, test Dependence

testsDep := DATASET([{1, DATASET([{'X1'}, {'X2'}], ProbSpec), DATASET([], ProbSpec)}, // exp: <.5
                  {2, DATASET([{'Y1'}, {'X1'}], ProbSpec), DATASET([], ProbSpec)}, // exp: >.5
                  {3, DATASET([{'Y2'}, {'X2'}], ProbSpec), DATASET([], ProbSpec)}, // exp: >.5
                  {4, DATASET([{'Y2'}, {'X1'}], ProbSpec), DATASET([], ProbSpec)}, // exp: >.5
                  {5, DATASET([{'X1'}, {'X2'}], ProbSpec), DATASET([{'Y2'}], ProbSpec)}, // exp: >.5
                  {6, DATASET([{'IV1'}, {'IV2'}], ProbSpec), DATASET([], ProbSpec)}, // exp: >.5
                  {7, DATASET([{'IV1'}, {'IV2'}], ProbSpec), DATASET([{'X1'}], ProbSpec)} // exp: <.5
                  
        ], ProbQuery);

resultsDep := prob.Dependence(testsDep);

OUTPUT(resultsDep, ALL, NAMED('Dependence'));

// Test isIndependent()

resultsDep2 := prob.isIndependent(testsDep); // exp: 1,0,0,0,0,0,1
OUTPUT(resultsDep2, ALL, NAMED('isIndependent'));

// Test Predict

// Predictor Variables (independents)

// First, generate some test records from the same distribution as the original data

dat2 := HPCC_Causality.Synth(mySEM).Generate(numTestRecs);
indVarsNums := [6,7,8];
depVarNum := 3;
indDat0 := dat2(number IN indVarsNums);
indDat := PROJECT(indDat0, TRANSFORM(RECORDOF(LEFT),
              SELF.number := LEFT.number - 5,
              SELF := LEFT), LOCAL);
depDat := PROJECT(dat2(number = depVarNum), TRANSFORM(RECORDOF(LEFT),
              SELF.number := 1,
              SELF := LEFT), LOCAL);
predVars := ['X1', 'X2', 'X3'];
targetVar := 'Y3';

//OUTPUT(indDat, NAMED('indDat'));
preds := prob.Predict(targetVar, predVars, indDat);

OUTPUT(preds, ALL, NAMED('Predictions'));

accuracy := ML_Core.Analysis.Regression.Accuracy(preds, depDat);

OUTPUT(accuracy, NAMED('PredAccuracy'));