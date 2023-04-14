IMPORT $ AS HPCC_Causality;
IMPORT HPCC_Causality.Types;
IMPORT ML_Core.Types AS cTypes;
IMPORT HPCC_Causality.internal.cModel;

powerDefault := 1;

cModelTyp := Types.cModel;
validationReport := Types.validationReport;
MetricQuery := Types.MetricQuery;
cMetrics := Types.cMetrics;
ProbQuery := Types.ProbQuery;
Distr := Types.Distribution;
ScanReport := Types.ScanReport;
DiscResult := Types.DiscoveryResult;
nlQuery := Types.nlQuery;
nlQueryRslt := Types.nlQueryRslt;
AnyField := Types.AnyField;
NumericField := cTypes.NumericField;

/**
  * Causal Model Module.
  *
  * Causal level methods require a combination of a causal model, and a dataset.
  *
  * Methods include:
  * - ValidateModel -- Analyze the data against the provided causal model and
  *         evaluate the degree of correspondence between the two.
  * - Causal (or Probabilistic) Query.  This is a superset of probability queries,
  *     adding  support for causal interventions  that simulate the effect on a target variable of a causal 
  *     intervention on one or more variable.  See Query for details.
  * - Metrics -- Evaluate various causal metrics on desgnated pairs 
  *      [source, destination] of variables.
  *
  * @param mod A causal model in DATASET(cModel) format.  The dataset should
  *    contain only a single record, defining the model.
  * @param PS The id of a Probability Space or Subspace containing the
  *    dataset. This is obtained by <probabilityInstance>.PS, or as returned from
  *    a probability.SubSpace() call.
  *
  * @see Types.cModel
  * @see ML_Core.Types.NumericField
  *
  */
EXPORT Causality(DATASET(cModelTyp) mod, UNSIGNED PS)  := MODULE
    SHARED CM := cModel.Init(mod, PS);
    /**
      * Validate the causal model relative to the data.
      *
      * @param order The largest number of variables to consider at a time (default 3).
      *     Higher values lead to exponentially increasing run times, and diminishing
      *     evaluation accuracy.  Very large datasets are required in order to evaluate
      *     higher order evaluations (default=3, recommended).
      * @param pwr Power. The thoroughness to be used in conditionalizing on variables.
      *     Allows a tradeoff between run-time and certainty of discrimination.  Power
      *     = 1 is sufficient to distinguish linear relationships, where higher numbers
      *     are needed to distinguish subtle non-linear relationships.  Range [1,100].
      *     For practical purposes, power > 5 should not be needed. Default = 1.
      * @param sensitivity
      * @return A detailed validation report in Types.ValidationReport format
      * @see Types.ValidationReport
      */
    EXPORT ValidationReport ValidateModel(UNSIGNED order=3, REAL pwr=powerDefault, REAL sensitivity=10) := FUNCTION
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
        results0 := cModel.TestModel(order, pwr, sensitivity, CM);
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
      * Calculate the distributions resulting from  a set of Causal Probability Queries.
      *
      * Causal Proabability queries are a superset of probability queries that may
      * contain an intervention (i.e. do()) clause.
      * 
      * If no do() clause is present, then the results will be the same as a normal 
      * probability query.
      *
      * The target portion must specify a bare (unbound) variable, as we are looking
      * for a distribution as a result.
      * 
      * Do() clauses are specified within the "given" portion of the query, and
      * can only use equality designation.  For example:
      *   'P(A | do(B=1, C=2), D between [-1,3])'
      * This is the probability distribution of A given that D is between -1 and 3,
      * and that we intervened to force the value of B to 1 and the value of C to 2.
      *
      * Interventions simulate the effect of setting a variable or variables
      * to fixed values, while breaking the links from those variables' parents.
      * The distribution of a target variable given the interventions is returned
      * for each query.  This is roughly equivalent to performing a randomized
      * study.
      *
      *
      * @param queries A list of queries.  Exactly 1 target per query must be specified,
      *        and the target must be unbound (i.e. with no comparitor).  One or more
      *        interventions can be provided for each variable.  Interventions must be
      *        of an exact value (e.g. do(var = value)).
      *
      * @return A set of Types.Distr records, describing each of the queried distributions.
      *
      */
    EXPORT DATASET(Distr) QueryDistr(SET OF STRING queries, REAL pwr=powerDefault) := FUNCTION
      dummy := DATASET([{1}], {UNSIGNED d});
      queryRecs := NORMALIZE(dummy, COUNT(queries), TRANSFORM(nlQuery, SELF.id:=COUNTER, 
              SELF.query:=queries[COUNTER]));
      queries_D := DISTRIBUTE(queryRecs, id);
      distrs := cModel.QueryDistr(queries_D, CM);
      distrs_S := SORT(distrs, id);
      RETURN distrs_S;
    END;

    /**
      * Calculate the probabilities or expectations resulting from  a set of 
      * Causal Probability Queries.
      *
      * Causal Proabability queries are a superset of probability queries that may
      * contain an intervention (i.e. do()) specification.
      * 
      * If no do() clause is present, then the results will be the same as a normal 
      * probability query.
      *
      * Probabilities or expectations may be requested by the query. For example:
      *   'P(A between [1,3] | B < 0)' # The probability that A is between 1 and 3 given
      *                                # that B is less than zero.
      *   'E(A | B < 0)'               # The expected value of A given that B is less than
      *                                # zero.
      *
      * Note that for probability queries that the target must be "bound" (i.e. includes a
      * comparison), while for expectation queries, the target must be "unbound" (i.e. a bare
      * variable name).
      *
      * For details of the query syntax, see the README file.
      *
      * Do() clauses are specified within the "given" portion of the query, and
      * can only use equality designation.  For example:
      *   'E(A | do(B=1, C=2), D between [-1,3])'
      * This is the expected value of A given that D is between -1 and 3,
      * and that we intervened to force the value of B to 1 and the value of C to 2.
      *
      * Interventions simulate the effect of setting a variable or variables
      * to fixed values, while breaking the links from those variables' parents.
      * The distribution of a target variable given the interventions is returned
      * for each query.  This is roughly equivalent to performing a randomized
      * study.
      *
      * @param queries A list of queries.  One or more
      *        interventions can be provided for each variable.  Interventions must be
      *        of an exact value (e.g. do(var = value)).
      *
      * @return A set of Types.AnyField records, containing the numeric (or textual).
      *        result of each query.
      *
      */
    EXPORT DATASET(nlQueryRslt) Query(SET OF STRING queries, REAL pwr=powerDefault) := FUNCTION
      dummy := DATASET([{1}], {UNSIGNED d});
      queryRecs := NORMALIZE(dummy, COUNT(queries), TRANSFORM(nlQuery, SELF.id:=COUNTER, 
              SELF.query:=queries[COUNTER]));
      queries_D := DISTRIBUTE(queryRecs, id);
      results := cModel.Query(queries_D, CM);
      results_S := SORT(results, id);
      RETURN results_S;
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
    EXPORT DATASET(cMetrics) Metrics(DATASET(MetricQuery) queries, REAL pwr=powerDefault) := FUNCTION
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
    EXPORT DATASET(ScanReport) ScanModel( REAL pwr=powerDefault) := FUNCTION
        rpt := cModel.ScanModel(pwr, CM);
        RETURN rpt;
    END;

    EXPORT DATASET(DiscResult) DiscoverModel(SET OF STRING vars,  REAL pwr=powerDefault, REAL sensitivity=10, UNSIGNED depth=2) := FUNCTION
      result := cModel.DiscoverModel(vars, pwr, sensitivity, depth, CM);
      RETURN result;
    END;
    
END;