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

numRecs := 100000;
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

tests := DATASET([{1, DATASET([{'X2', [-100, 0]}], ProbSpec), DATASET([], ProbSpec)}, 
                  {2, DATASET([{'X3', [.6, 6.5]}], ProbSpec), DATASET([], ProbSpec)}, 
                  {3, DATASET([{'X3', [.65, 7.0]}], ProbSpec), DATASET([], ProbSpec)}, 
                  {4, DATASET([{'X2', [-100, 0]},{'X3', [3, 3.5]}], ProbSpec), DATASET([], ProbSpec)} 
         ], ProbQuery);

results := prob.P(tests);

OUTPUT(results, ALL, NAMED('Probabilities'));

// Now test some Expected Values

testsE := DATASET([
                  //{4, DATASET([{'Y1'}], ProbSpec), DATASET([{'X1', [1]}], ProbSpec)} // exp=2
                  //{5, DATASET([{'ROLL'}], ProbSpec), DATASET([{'D1', [1,4]}, {'D2', [6]}], ProbSpec)} // exp=8
                  {1, DATASET([{'Y5'}], ProbSpec), DATASET([{'X2',[-100,0]},{'X3',[3,3.5]}], ProbSpec)}
        ], ProbQuery);

resultsE := prob.E(testsE);

OUTPUT(resultsE, ALL, NAMED('Expectations'));

resultsEQ := prob.Query(['E(Y5 | X2 between [-100, 0], X3 between [3, 3.5])']);
OUTPUT(resultsEQ, NAMED('ExpectationQueries'));

// Test Full Distributions

testsD := DATASET([
                  {1, DATASET([{'X3'}], ProbSpec), DATASET([], ProbSpec)},
                  {2, DATASET([{'Y5'}], ProbSpec), DATASET([{'X2',[-100,0]},{'X3',[3,3.5]}], ProbSpec)}
        ], ProbQuery);

resultsD := prob.Distr(testsD);
OUTPUT(resultsD, ALL, NAMED('Distributions'));

resultsDQ := prob.QueryDistr(['P(Y5 | X2 between [-100, 0], X3 between [3, 3.5])']);
OUTPUT(resultsDQ, NAMED('DistributionQueries'));



