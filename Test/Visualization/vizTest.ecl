/**
  * Test the natural language query mechanism for Probabilities,
  * Expectations, and Distributions.  Include textual variables.
  *
  * Uses the Synth module to generate the test data.
  */
IMPORT HPCC_Causality AS HC;
IMPORT HC.Types;

numRecs := 10000;

SEM := Types.SEM;
Probability := HC.Probability;
viz := HC.viz;
nlQuery := Types.nlQuery;

semRow := ROW({
    [], // Init
    ['Y1', 'Y2', 'Y3', 'Y4', 'Y5', 'X1', 'X2', 'X3', 'X4', 'X5', 'IV1', 'IV2', 'TV'], // Variable names 
    // Equations
    [
        // Y1 is dependent on X1, Y2 is dependent on X1 and X2, ...
        // Y5 is dependent on X1, ... ,X5
        'X1 = normal(2.5, 1)',
        'X2 = choice([logistic(-2, .5), logistic(2,.5)])',
        'X3 = beta(2,5)',
        'X4 = truncated("normal(0, 2)", .5, None)',
        'X5 = exponential() * .2',
        'Y1 = 2 * X1 + normal(0, .1)',
        'Y2 = -Y1 + tanh(X2*3) + normal(0,.1)',
        'Y3 = -2 * X1 + tanh(X2*3) + sin(X3) + normal(0,.1)',
        'Y4 = Y3 + log(X4, 2) + normal(0, .1)',
        'Y5 = Y4 + 1.1**X5 + normal(0,.1)',
        // IV1 and IV2 create an inverted-v formation
        //  IV1 <- X1 -> IV2 so that we can test conditionalization
        'IV1 = .5 * X1 + normal(0,.1)',
        'IV2 = tanh(-.75 * X1) + normal(0,.1)',
        // TV is a textual variable.
        'TV = "small" if X2 < -1 else "large" if X2 > 1 else "medium"'
    ]}, SEM);

mySEM := DATASET([semRow], SEM);


// First test Probabilities and Expectations.

// Generate the records to test with
dat := HC.Synth(mySEM).Generate(numRecs);

//OUTPUT(dat[..10000], ALL, NAMED('Samples'));

// Treat TV as a categorical variable so that expectations come out as strings.
prob := Probability(dat, semRow.VarNames, categoricals:=['TV']);

// Include various use of spacing around terms to check the parser's
// resilience to spacing variations.
tests := DATASET([{1, 'P(X1 >= 2)'},
                {2, 'P(TV = medium | X5 between [.4,.8])'},
                {3, 'E(Y4)'},
                {4, 'E(TV| X2 < -.75)'},
                {5, 'P(X2 between[-.5, .5] | TV in [medium, large])'},
                {6, 'P(TV=medium)'},
                {7, 'E(TV |X2 between[-1.1, 1.1])'}
                //{8, 'P(TV)'}  // Should fail
                ], nlQuery);

results := prob.Query(tests);

//OUTPUT(results, ALL, NAMED('Probabilities'));

// Now do Distribution queries.
dtests := DATASET([
                  {1, 'P(X1)'},
                  {2, 'P(TV)'},
                  {3, 'P(TV|X2 > -.5)'},
                  {4, 'P(Y1 | X1=1)'},
                  {5, 'P(X2)'},
                  {6, 'P(X4)'}
                ], nlQuery);

dresults := prob.QueryDistr(dtests);

//OUTPUT(dresults, ALL, NAMED('Distributions'));


viz.Plot(['P(Y2)', 'E( Y2 | X1)'], prob.PS);
