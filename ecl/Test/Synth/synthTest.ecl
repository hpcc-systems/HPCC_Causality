/**
  * Test and example for generating synthetic multivariate datasets.
  *
  * Uses the Synth module.
  *
  */
IMPORT $.^.^ AS Causality;
IMPORT Causality.Types;
IMPORT ML_CORE.Types AS cTypes;

NumericField := cTypes.NumericField;
SEM := Types.SEM;

/**
  * SEM record contains three parts:
  * - Init -- Seldom used, but allows one-time initialization of 
  *         variables.  For example, for a time series generation
  *         could contain 't = 0'.
  * - VarNames -- is a list of the variables to return for each sample.
  * - EQ -- A list of equations to run in the order in which they
  *     are to be executed.  These equations must include the calculation
  *     of each variable's value for each sample, and may also adjust
  *     other variables that were set by Init (above), such as
  *     't = t+1'.
  */

// SEM should be a dataset containing a single row.
semRow := ROW({
    ['t = 0'],   // Init is typically empty, but we are using it
                // to initialize a variable 't', so that we can create a
                // time-dependent series for illustrative purposes.
    ['A', 'B', 'C', 'D'], // Variable names 
    // Equations
    ['A = normal(0,1)',  // Can use any distribution defined in numpy.random
    'B = normal(0,1)',
    'C = A + B',
    'D = tanh(C) + sin(t/(2*pi)) + geometric(.1)', // Can use nearly any math function
                                                    // from python math library.
    't = t + 1' // t is not a returned variable, but is used (in this case)
            // to produce a time-dependent series (note sin in D calculation above).
    ]}, SEM);

mySEM := DATASET([semRow], SEM);

//mySynth := Synth(mySEM);

outDat := Causality.Synth(mySEM).Generate(11);

OUTPUT(outDat, ALL, NAMED('GeneratedData'));



