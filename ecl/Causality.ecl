IMPORT $ AS Cause;
IMPORT Cause.Types;
IMPORT ML_Core.Types AS cTypes;
IMPORT Cause.internal.cModel;

cModelTyp := Types.cModel;
validationReport := Types.validationReport;
cMetrics := Types.cMetrics;
ProbQuery := Types.ProbQuery;
Distr := Types.Distribution;
DiscoveryReport := Types.DiscoveryReport;

NumericField := cTypes.NumericField;

/**
  * Causal Model Module.
  *
  * Causal level methods require a combination of a causal model, and a dataset.
  *
  * Methods include:
  * - ValidateModel -- Analyze the data against the provided causal model and
  *         evaluate the degree of correspondence between the two.
  * - Intervene -- Simulate the effect on a target variable of a causal 
  *     intervention on one or more variable
  * - Metrics -- Evaluate various causal metrics on desgnated pairs 
  *      [source, destination] of variables.
  *
  * @param mod A causal model in DATASET(cModel) format.  The dataset should
  *    contain only a single record, defining the model.
  * @param dat The data in NumericField format.  The field number should
  *    correspond to the order of variables specified in the model.
  *
  * @see Types.cModel
  * @see ML_Core.Types.NumericField
  *
  */
EXPORT Causality(DATASET(cModelTyp) mod, DATASET(NumericField) dat)  := MODULE
    SHARED CM := cModel.Init(mod, dat);
    /**
      * Validate the causal model relative to the data.
      *
      * @param order The largest number of variables to consider at a time (default 3).
      *     Higher values lead to exponentially increasing run times, and diminishing
      *     evaluation accuracy.  Very large datasets are required in order to evaluate
      *     higher order evaluations (default=3, recommended).
      * @param strength The thoroughness to be used in conditionalizing on variables.
      *     Allows a tradeoff between run-time and certainty of discrimination.  Strength
      *     = 1 is sufficient to distinguish linear relationships, where higher numbers
      *     are needed to distinguish subtle non-linear relationships.  Range [1,100].
      *     For practical purposes, strength > 5 should not be needed. Default = 1.
      * @return A detailed validation report in Types.ValidationReport format
      * @see Types.ValidationReport
      */
    EXPORT ValidationReport ValidateModel(UNSIGNED order=3, UNSIGNED strength=1) := FUNCTION
        ValidationReport rollupReport(ValidationReport l, ValidationReport r) := TRANSFORM
            SELF.confidence := 0.0;
            SELF.NumTotalTests := l.NumTotalTests + r.NumTotalTests;
            tpt := IF(COUNT(r.NumTestsPerType) > 0, r.NumTestsPerType, [0,0,0,0]);
            ept := IF(COUNT(r.NumErrsPerType) > 0, r.NumErrsPerType, [0,0,0,0]);
            wpt := IF(COUNT(r.NumWarnsPerType) > 0, r.NumWarnsPerType, [0,0,0,0]);
            SELF.NumTestsPerType := [l.NumTestsPerType[1] + tpt[1],
                                    l.NumTestsPerType[2] + tpt[2],
                                    l.NumTestsPerType[3] + tpt[3],
                                    l.NumTestsPerType[4] + tpt[4]];
            SELF.NumErrsPerType := [l.NumErrsPerType[1] + ept[1],
                                    l.NumErrsPerType[2] + ept[2],
                                    l.NumErrsPerType[3] + ept[3],
                                    l.NumErrsPerType[4] + ept[4]];
            SELF.NumWarnsPerType := [l.NumWarnsPerType[1] + wpt[1],
                                    l.NumWarnsPerType[2] + wpt[2],
                                    l.NumWarnsPerType[3] + wpt[3],
                                    l.NumWarnsPerType[4] + wpt[4]];
            SELF.Errors := l.Errors + r.Errors;
            SELF.Warnings := l.Warnings + r.Warnings;
        END;
        results0 := cModel.TestModel(order, strength, CM);
        results1 := ROLLUP(results0, TRUE, rollupReport(LEFT, RIGHT));
        resultsRec := results1[1];
        score := cModel.ScoreModel(resultsRec.NumTestsPerType,
                                    resultsRec.NumErrsPerType,
                                    resultsRec.NumWarnsPerType, CM);
        final := PROJECT(results1, TRANSFORM(RECORDOF(LEFT),
                                        SELF.confidence := score,
                                        SELF := LEFT));
        finalRec := final[1];
        RETURN finalRec;
    END;

    /**
      * Calculate the results of a Causal Intervention.
      *
      * Interventions simulate the effect of setting a variable or variables
      * to fixed values, while breaking the links from those variables' parents.
      * The distribution of a target variable given the interventions is returned
      * for each query.  This is roughly equivalent to performing a randomized
      * study.
      *
      * Interventions are of the form:
      * - Distribution = (Var | List of interventions)
      *
      * @param queries A list of queries.  Exactly 1 target per query must be specified,
      *        and the target must be unbound (i.e. with zero arguments).  One or more
      *        interventions can be provided for each variable.  Interventions must be
      *        of an exact value (e.g. do(var = value)).  This is indicated by a single
      *        arg in the intervention ProbSpec.
      *
      * @return A set of Types.Distr records, describing each of the queried distributions.
      *
      */
    EXPORT DATASET(Distr) Intervene(DATASET(ProbQuery) queries, UNSIGNED pwr=1) := FUNCTION
        queries_D := DISTRIBUTE(queries, id);
        distrs := cModel.Intervene(queries_D, pwr, CM);
        distrs_S := SORT(distrs, id);
        RETURN distrs_S;
    END;

    /**
      * Calculate a set of causal metrics from a designated source variable to a designated
      * destination variable.
      *
      * The following metrics are produce for each source / destination pair:
      * - Average Causal Effect (ACE) -- The average effect on the destination variable of
      *        a unit intervention on the source variable.
      * - Controlled Direct Effect (CDE) -- The direct effect on the destination variable of
      *        a unit intervention on the source variable.
      * - Indirect Effect (IE) -- The indirect effect (i.e. via intermediate variables) on
      *        the destination variable of a unit intervention on the source variable.
      *
      * @param queries A list of queries, each with two targets [source, destination], and
      *    no conditions or interventions.  Targets should be unbound (i.e. no args).
      *
      * @return Dataset of cMetrics records, one per query, with id corresponding to the
      *     id of the original query.
      *
      */
    EXPORT DATASET(cMetrics) Metrics(DATASET(ProbQuery) queries, UNSIGNED pwr=1) := FUNCTION
        queries_D :=  DISTRIBUTE(queries, id);
        metrics := cModel.Metrics(queries_D, pwr, CM);
        metrics_S := SORT(metrics, id);
        RETURN metrics_S;
    END;

    /**
      * Analyze the data to estimate the causal relationships between variables.
      *
      * Produces information that is useful for understanding the variables' relationships,
      * and attempts to build a full causal model.
      * Discovery is done hierarchically, first determining "clusters" that share a common
      * set of exogenous variables.  Then each cluster is analyzed for topology, and finally,
      * the inter-cluster relationships are estimated.
      *
      * Note that this function does not use the model information supplied to the module
      * except for a list of variable names.  It, rather, produces an estimated of the model
      * that generated the data.
      *
      * @param pwr The power to use for statisitical queries.  Range [1, 100].  The higher power,
      *           the more accuracy, but longer runtime.  Power=1 suffices for liner relationships.
      *           Power > 10 is not recommended due to very long runtimes.
      *
      * @return A DATASET(DiscoveryReport) with a single record representing the results
      *           of the discovery.
      * @see Types.DiscoveryReport
      *
      * 
      */
    EXPORT DATASET(DiscoveryReport) DiscoverModel( UNSIGNED pwr=1) := FUNCTION
        discRpt := cModel.DiscoverModel(pwr, CM);
        RETURN discRpt;
    END;
END;