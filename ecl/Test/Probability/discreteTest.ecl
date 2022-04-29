/**
  * Test discrete probabilities using a simulated roll of a pair of dice.
  *
  * Uses the Synth module to generate the test data.
  */
IMPORT $.^.^ AS Causality;
IMPORT Causality.Types;
IMPORT ML_Core.Types AS cTypes;
IMPORT ML_Core;

#OPTION('hdCompressorType', 'LZW');
numRecs := 10000;
numTestRecs := TRUNCATE(numRecs * .1);

NumericField := cTypes.NumericField;
DiscreteField := cTypes.DiscreteField;
SEM := Types.SEM;
Probability := Causality.Probability;
ProbSpec := Types.ProbSpec;
ProbQuery := Types.ProbQuery;

// SEM for a roll of 2 dice (ala Craps).
semRow := ROW({
    [], // Init
    ['D1', 'D2', 'ROLL'], // Variable names 
    // Equations
    [
        'D1 = choice(range(1, 7))',
        'D2 = choice(range(1, 7))',
        'ROLL = D1 + D2'
    ]}, SEM);

mySEM := DATASET([semRow], SEM);

// Generate the records to test with
diceDat := Causality.Synth(mySEM).Generate(numRecs);

OUTPUT(diceDat, ALL, NAMED('DiceRolls'));

prob := Probability(diceDat, semRow.VarNames);

tests := DATASET([{1, DATASET([{'D1', [1]}], ProbSpec), DATASET([], ProbSpec)}, // exp=.166...
                  {2, DATASET([{'D1', [1,4]}], ProbSpec), DATASET([], ProbSpec)}, // exp=.5
                  {3, DATASET([{'D1', [1,7]}], ProbSpec), DATASET([], ProbSpec)}, // exp=1
                  {4, DATASET([{'D1', [7]}], ProbSpec), DATASET([], ProbSpec)},  // exp=0
                  {5, DATASET([{'ROLL', [7]}], ProbSpec), DATASET([], ProbSpec)},  // exp=.166...
                  {6, DATASET([{'ROLL', [7]}], ProbSpec), DATASET([{'D1', [2,7]}, {'D2', [0,5]}], ProbSpec)}, // exp=.2
                  {7, DATASET([{'ROLL', [2,4]}], ProbSpec), DATASET([{'D2', [1]}], ProbSpec)},  // exp=.333..
                  {8, DATASET([{'ROLL', [7]}, {'D1', [5,7]}], ProbSpec), DATASET([], ProbSpec)}  // exp=.0555..
        ], ProbQuery);

results := prob.P(tests);

OUTPUT(results, ALL, NAMED('Probabilities'));

// Now test some Expected Values

testsE := DATASET([{1, DATASET([{'D1'}], ProbSpec), DATASET([], ProbSpec)}, // exp=3.5
                  {2, DATASET([{'D2'}], ProbSpec), DATASET([{'D1', [6]}], ProbSpec)}, // exp=3.5
                  {3, DATASET([{'ROLL'}], ProbSpec), DATASET([], ProbSpec)}, // exp=7
                  {4, DATASET([{'ROLL'}], ProbSpec), DATASET([{'D1', [1,4]}], ProbSpec)}, // exp=5.5
                  {5, DATASET([{'ROLL'}], ProbSpec), DATASET([{'D1', [1,4]}, {'D2', [6]}], ProbSpec)} // exp=8
        ], ProbQuery);

resultsE := prob.E(testsE);

OUTPUT(resultsE, ALL, NAMED('Expectations'));

// Test full distributions

testsD := DATASET([{1, DATASET([{'D1'}], ProbSpec), DATASET([], ProbSpec)},
                  {2, DATASET([{'D2'}], ProbSpec), DATASET([{'D1', [6]}], ProbSpec)}, 
                  {3, DATASET([{'ROLL'}], ProbSpec), DATASET([], ProbSpec)},
                  {4, DATASET([{'ROLL'}], ProbSpec), DATASET([{'D1', [1,4]}], ProbSpec)},
                  {5, DATASET([{'ROLL'}], ProbSpec), DATASET([{'D1', [1,4]}, {'D2', [6]}], ProbSpec)}
        ], ProbQuery);


resultsD := prob.Distr(testsD);

OUTPUT(resultsD, ALL, NAMED('Distributions'));

// Now, test Dependence

testsDep := DATASET([{1, DATASET([{'D1'}, {'D2'}], ProbSpec), DATASET([], ProbSpec)}, // exp: <.5
                  {2, DATASET([{'D1'}, {'ROLL'}], ProbSpec), DATASET([], ProbSpec)}, // exp: >.5
                  {3, DATASET([{'D1'}, {'D2'}], ProbSpec), DATASET([{'ROLL', [11,13]}], ProbSpec)}, // exp: >.5
                  {4, DATASET([{'D1'}, {'D2'}], ProbSpec), DATASET([{'ROLL', [7]}], ProbSpec)} // exp: >.5
        ], ProbQuery);

resultsDep := prob.Dependence(testsDep);

OUTPUT(resultsDep, ALL, NAMED('Dependence'));

// Test isIndependent()

resultsDep2 := prob.isIndependent(testsDep); // exp: 1,0,0,0
OUTPUT(resultsDep2, ALL, NAMED('isIndependent'));

// Test Classification.  This is a silly test because it is easy and
// deterministic, but does exercise the function.

dat2 := Causality.Synth(mySEM).Generate(numTestRecs);
indeps := ['D1', 'D2'];
indVarNums := [1,2];
dep := 'ROLL';
depVarNum := 3;
indDat := dat2(number in indVarNums);
OUTPUT(dat2, ALL, NAMED('TestDat'));
depDat := PROJECT(dat2(number = depVarNum), TRANSFORM(DiscreteField,
                                                SELF.number := 1,
                                                SELF := LEFT));
preds := prob.Classify(dep, indeps, indDat);
predsD := PROJECT(preds, TRANSFORM(DiscreteField,
                              SELF := LEFT));

OUTPUT(preds, NAMED('Preds'));

ver := JOIN(preds, depDat, LEFT.id = RIGHT.id, TRANSFORM({NumericField, REAL expected, BOOLEAN err},
                                                      SELF.expected := RIGHT.value,
                                                      SELF.err := LEFT.value != RIGHT.value,
                                                      SELF := LEFT), HASH);

OUTPUT(ver(err=TRUE), NAMED('Errors'));

accuracy := ML_Core.analysis.Classification.Accuracy(predsD, depDat);

OUTPUT(accuracy, NAMED('ClassificationAccuracy'));