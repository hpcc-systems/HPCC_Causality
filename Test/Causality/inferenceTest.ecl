/**
  * Using a simulated "IceCream <- Temperature -> Crime model,
  * test causal interventions and metrics.
  * - Assess the apparent effect of ice cream on crime
  * - Assess the causal effect of ice cream on crime using intervention
  * - Use metrics to measure the causal effects
  */

IMPORT $.^.^ AS HPCC_Causality;
IMPORT HPCC_Causality.Types;


IMPORT ML_CORE.Types AS cTypes;

ProbSpec := Types.ProbSpec;
ProbQuery := Types.ProbQuery;
MetricQuery := Types.MetricQuery;
//umericField := cTypes.NumericField;
SEM := Types.SEM;

// Number of test records.
nTestRecs := 100000;


// SEM should be a dataset containing a single row.
// SEM is Model M8.
semRow := ROW({
    [],
    ['Temperature', 'IceCream', 'Crime'], // Variable names 
    // Equations
    [ 'ce = 0.0', // The causal effect we expect to observe
      'Day = uniform(0, 364)',
      'Temperature = sin(Day / (2*pi)) * 30 + 40 + normal(0,5)',
      'IceCream = 1000 + 10 * Temperature + normal(0,300)',
      'Crime = ce * IceCream + 50 + 3 * Temperature + normal(0, 2)' 
    ]}, SEM);

mySEM := DATASET([semRow], SEM);

// Use Synth to generate a dataset from the SEM.
testDat := HPCC_Causality.Synth(mySEM).Generate(nTestRecs);

// Note: The order of variables in the model much match the order of varNames in the SEM.
// Create a causal model that assumes there is an effect between IceCream and Crime.
RVs := DATASET([
                {'Temperature', []},
                {'IceCream', ['Temperature']},
                {'Crime', ['Temperature', 'IceCream']}
                ], Types.RV);
mod := DATASET([{'Ice Cream', RVs}], Types.cModel);

OUTPUT(mySEM, NAMED('SEM'));
OUTPUT(mod, NAMED('Model'));
OUTPUT(testDat[..1000], ALL, NAMED('Data'));

// Create a probability model using the data
prob := HPCC_Causality.Probability(testDat, semRow.varNames);
// Create a causal graph with the causal model and the probabililty model
cg := HPCC_Causality.Causality(mod, prob.PS);

// Distribution tests.  Assess the distribution of each variable
testsD := ['P(Temperature)', 'P(IceCream)', 'P(Crime)'];
distrs := cg.QueryDistr(testsD);
OUTPUT(distrs, Named('QDistResults'));

// Look at the exppectations of:
// - Each Variable
// - The apparent effect of ice cream on crime (Using a high and low level for ice cream)
// - The causal effect of ice cream on crime using interventions.
iHigh := 1700;
iLow := 1300;
testsP := ['E(Temperature)', 'E(IceCream)', 'E(Crime)', 'E(Crime | IceCream = ' + (STRING)iHigh + ')',
          'E(Crime | IceCream = ' + (STRING)iLow + ')', 'E(Crime | do(IceCream=' + (STRING)iHigh + '))',
          'E(Crime | do(IceCream='+ (STRING)iLow+ '))'];

// Calculate
rslts := cg.Query(testsP);
OUTPUT(rslts, NAMED('Qresults'));

// Calcumate some metrics
h1 := rslts[4].value;
l1 := rslts[5].value;
h2 := rslts[6].value;
l2 := rslts[7].value;
// Note: apparent effect (appEff) should be non-zero, due to correlation with Temp.
// Causal Effect (causEff) should be appoximately the value of "ce" in the SEM (above),
// which we usually set to zero.
appEff := (h1 - l1) / (iHigh - iLow);
causEff := (h2 - l2) / (iHigh - iLow);
OUTPUT(appEff, NAMED('ApparentEffect'));
OUTPUT(causEff, NAMED('CausalEffect'));

metricQueries := DATASET([
  {1, 'Temperature', 'IceCream'},
  {2, 'Temperature', 'Crime'},
  {3, 'IceCream', 'Temperature'},
  {4, 'Crime', 'Temperature'},
  {5, 'IceCream', 'Crime'},
  {6, 'Crime', 'IceCream'}
], MetricQuery);

metrics := cg.Metrics(metricQueries);
OUTPUT(metrics, NAMED('Metrics'));
