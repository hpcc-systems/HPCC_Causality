/**
  * Test a combination of discrete and continuous variables, both as
  * target and conditional.
  *
  * Uses the Synth module to generate the test data.
  */
IMPORT $.^.^ AS Causality;
IMPORT Causality.Types;
IMPORT ML_CORE.Types AS cTypes;

numRecs := 10000;
numTestRecs := TRUNCATE(numRecs * .1);

NumericField := cTypes.NumericField;
SEM := Types.SEM;
Probability := Causality.Probability;
ProbSpec := Types.ProbSpec;
ProbQuery := Types.ProbQuery;

// SEM for a roll of 2 dice (ala Craps).
semRow := ROW({
    [], // Init
    ['Coin', 'Dist', 'Score'], // Variable names 
    // Equations
    [
        'Coin = choice([0,1])', // 0 is tails, 1 is heads
        'DistH = normal(2.5, .5)',
        'DistT = logistic(0, .6)',
        'Dist = DistT if Coin == 0 else DistH',
        'Score = 1 if Dist < 0 else 2 if Dist < 2 else 3' // 1 = Low, 2 = Medium, 3 = High
    ]}, SEM);

mySEM := DATASET([semRow], SEM);

// Generate the records to test with
dat := Causality.Synth(mySEM).Generate(numRecs);

OUTPUT(dat[..10000], ALL, NAMED('Samples'));

prob := Probability(dat, semRow.VarNames);

tests := DATASET([{1, DATASET([{'Coin', [0]}], ProbSpec), DATASET([], ProbSpec)}, // exp:.5
                  {2, DATASET([{'Score', [1,3]}], ProbSpec), DATASET([], ProbSpec)}, // Med or Low
                  {3, DATASET([{'Dist', [-100,0]}], ProbSpec), DATASET([], ProbSpec)}, // exp: .25
                  {4, DATASET([{'Dist', [-100,0]}], ProbSpec), DATASET([{'Coin', [0]}], ProbSpec)}, // exp: .5
                  {5, DATASET([{'Dist', [2.5,100]}], ProbSpec), DATASET([{'Coin', [1]}], ProbSpec)} // exp: .5
        ], ProbQuery);

results := prob.P(tests);

OUTPUT(results, ALL, NAMED('Probabilities'));

// Now test some Expected Values

testsE := DATASET([{1, DATASET([{'Coin'}], ProbSpec), DATASET([], ProbSpec)}, // exp: .5
                  {2, DATASET([{'Score'}], ProbSpec), DATASET([], ProbSpec)}, // ?
                  {3, DATASET([{'Dist'}], ProbSpec), DATASET([{'Coin', [0]}], ProbSpec)}, // exp: 0
                  {4, DATASET([{'Dist'}], ProbSpec), DATASET([{'Coin', [1]}], ProbSpec)}, // exp: 2.5
                  {5, DATASET([{'Coin'}], ProbSpec), DATASET([{'Dist', [3,100]}], ProbSpec)} // exp: close to 1

                  //{5, DATASET([{'ROLL'}], ProbSpec), DATASET([{'D1', [1,4]}, {'D2', [6]}], ProbSpec)} // exp=8
        ], ProbQuery);

resultsE := prob.E(testsE);

OUTPUT(resultsE, ALL, NAMED('Expectations'));

// Test Full Distributions

testsD := DATASET([{1, DATASET([{'Coin'}], ProbSpec), DATASET([], ProbSpec)}, // Should be 50/50
                  {2, DATASET([{'Dist'}], ProbSpec), DATASET([], ProbSpec)}, // Should be bimodal mixture
                  {3, DATASET([{'Score'}], ProbSpec), DATASET([], ProbSpec)}, // ???
                  {4, DATASET([{'Score'}], ProbSpec), DATASET([{'Coin', [0]}], ProbSpec)}, // Should tend to low side
                  {5, DATASET([{'Score'}], ProbSpec), DATASET([{'Coin', [1]}], ProbSpec)}, // Should tend to high side
                  {6, DATASET([{'Dist'}], ProbSpec), DATASET([{'Coin', [0]}], ProbSpec)}, // Should be logistic u=0
                  {6, DATASET([{'Dist'}], ProbSpec), DATASET([{'Coin', [1]}], ProbSpec)}, // Should be normal u=2.5
                  {7, DATASET([{'Score'}], ProbSpec), DATASET([{'Dist', [-100,2.5]}], ProbSpec)}, // ??
                  {8, DATASET([{'Coin'}], ProbSpec), DATASET([{'Score', [3]}], ProbSpec)} // Should be close to 1
        ], ProbQuery);

resultsD := prob.Distr(testsD);

OUTPUT(resultsD, ALL, NAMED('Distributions'));

// Now, test Dependence

testsDep := DATASET([{1, DATASET([{'Coin'}, {'Score'}], ProbSpec), DATASET([], ProbSpec)}, // exp: >.5
                  {2, DATASET([{'Dist'}, {'Coin'}], ProbSpec), DATASET([], ProbSpec)}, // exp: >.5
                  {3, DATASET([{'Coin'}, {'Score'}], ProbSpec), DATASET([{'Dist'}], ProbSpec)} // exp: <.5
        ], ProbQuery);

resultsDep := prob.Dependence(testsDep);

OUTPUT(resultsDep, ALL, NAMED('Dependence'));

// Test isIndependent()

resultsDep2 := prob.isIndependent(testsDep); // exp: 0,0,1
OUTPUT(resultsDep2, ALL, NAMED('isIndependent'));

