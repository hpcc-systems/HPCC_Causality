IMPORT $ AS HPCC_Causality;
IMPORT HPCC_Causality.Types;
IMPORT ML_Core.Types AS cTypes;
IMPORT Python3 AS Python;
IMPORT HPCC_Causality.internal.ProbSpace;

ProbQuery := Types.ProbQuery;
ProbSpec := Types.ProbSpec;
Distr := Types.Distribution;
DatasetSummary := Types.DatasetSummary;
NumericField := cTypes.NumericField;
AnyField := Types.AnyField;
nlQuery := Types.nlQuery;

/**
  * Probability Module
  *
  * Contains a set of probability functions to execute against a multivariate dataset.
  * The dataset consists of a set of variable names, and a set of observations for
  * each variable.
  * The observations are in NumericField format, with the field number corresponding to
  * the order of the variable names.
  *
  * Probability functions include:
  * - "Natural Language" probability queries.  These queries are much easier to specify
  *     than the "structured" queries below.  See README.md for query syntax.
  *   - Query(...) -- Queries for probabilities or expectations, returning scalar values.
  *   - QueryDistr(...) -- Queries returning a univariate distribution.
  * - "Structured probability queries.  These use nested structures to represent the
  *     query.  They are more difficult to use, but may lend themselves to programmatically
  *     built queries.
  *   - P(...) -- Unconditional, Conditional and Joint Numerical Probabilities.
  *   - E(...) -- Unconditional and Conditional Expectations
  *   - Distr(...) -- Unconditional and Conditional Distributions
  * - Dependence(...) -- Test of Dependence and Conditional Dependence Between Variables
  * - isIndependent(...) -- Boolean Independence and Conditional Independence Test.
  * - Predict(...) -- Machine Learning style regression without training required.
  * - Classify(...) -- Machine Learning style classification without training required.
  *
  * @param ds -- Set of multivariate observations in AnyField format.  Each observation
  *               shares an id (1 - numObservations), and field numbers correspond to the
  *               order of variable names in the varNames parameter.  This dataset can
  *               be produced from a record-oriented dataset using the ToAnyField() macro.
  *               See README.md for details.
  * @param varNames -- An ordered list of variable name strings.  This set may be extracted
  *               from the AnyField() macro.  See README.md.
  */
EXPORT Probability(DATASET(AnyField) ds, SET OF STRING varNames, SET OF STRING categoricals=[]) := MODULE
    // This is a module-level initialized ProbSpace.  Initialization happens when
    // The first probability function is called.  At that point, the dataset
    // is sent to each node.
    EXPORT PS := ProbSpace.Init(ds, varNames, categoricals);
    /**
      * Summary
      */
    EXPORT DatasetSummary Summary(UNSIGNED psId=PS) := FUNCTION
      RETURN ProbSpace.getSummary(psId);
    END;
    
    /**
      * Produce a Subspace from the indicated probability space by applying
      * a filter.  The filter is specified the same as the conditional portion
      * of a Natural probability query.
      * The returned ProbabilitySpace Id can be used in subsequent calls to
      * any of the probability functions.
      * SubSpaces essentially create a multivariate conditional distribution
      * of the original Probability Space, but filtering the set of records.
      * This can be useful for cleaning a probability space (i.e. by eliminating
      * records where certain attributes have certain values), or for generating
      * a conditional space to issue multiple queries against.
      *
      * Example:
      *   // Generate a subspace of married men in very good or excellent health.
      *   new_psid := prob.SubSpace('gender=male, married=yes, genhealth in [4-verygood, 5-excellent]', prob.PS);
      *
      * @param filter A bound query condition.
      * @param psid The probability space id of the space to be subspaced.
      * @return A new probability space id for use in future queries.
      * 
      */
    EXPORT UNSIGNED SubSpace(STRING filter, UNSIGNED psId=PS) :=  FUNCTION
      filtDS := DATASET([{1, filter}], nlQuery);
      filtDS_D := DISTRIBUTE(filtDS, ALL);
      psidDS := ProbSpace.SubSpace(filtDS_D, psId);
      newPsid := MAX(psidDS, id);
      RETURN newPsid;
    END;

    // Natural Language Queries
    /**
      * Natural Language Probability or Expectation query.
      *
      * @param gueries A set of "natural" mathematical query strings.  See README.md
      *         for details on query syntax.
      * @param psid An UNSIGNED identifier from the main probability space (probspace.PS) 
      *         or a SubSpace.
      * @return A list of numeric or string results in Types.AnyField format.
      *        
      */
    EXPORT DATASET(AnyField) Query(SET OF STRING queries, UNSIGNED psid=PS) := FUNCTION
      dummy := DATASET([{1}], {UNSIGNED d});
      queryRecs := NORMALIZE(dummy, COUNT(queries), TRANSFORM(nlQuery, SELF.id:=COUNTER, 
              SELF.query:=queries[COUNTER]));
      queries_D := DISTRIBUTE(queryRecs, id);
      results := ProbSpace.Query(queries_D, psid);
      return SORT(results, id);
    END;

    /**
      * Natural Language Probability Distribution query
      *
      * @param gueries A set of "natural" mathematical query strings.  See README.md
      *         for details on query syntax.
      * @param psid An UNSIGNED identifier from the main probability space (probspace.PS) 
      *         or a SubSpace.
      * @return A dataset of Types.Distribution records, summarizing each requested
      *         distribution.
      *
      */
    EXPORT DATASET(Distr) QueryDistr(SET OF STRING queries, UNSIGNED psid=PS) := FUNCTION
      dummy := DATASET([{1}], {UNSIGNED d});
      queryRecs := NORMALIZE(dummy, COUNT(queries), TRANSFORM(nlQuery, SELF.id:=COUNTER, 
              SELF.query:=queries[COUNTER]));
      queries_D := DISTRIBUTE(queryRecs, id);
      results := ProbSpace.QueryDistr(queries_D, psid);
      return SORT(results, id);
    END;

    // End Natural Language Queries

    // Structured Queries
    
    /**
      * Calculate a series of numerical probabilities.
      *
      * Queries are of the form:
      * - Exact Query -- P(Var = Val | List of Conditions)
      * - Range Query -- P(Val1 <= Var <= Val2 | List of Conditions)
      * - Joint Probability -- P([Exact or Range Query 1, ...] | List of Conditions)
      *
      * @param queries A list of queries.  One or more target may be specified for each
      *        query, and the targets must be bound (i.e. with 1 or 2 arguments).
      *
      * @return A set of NumericField records, with value being the probability
      *         of the query as field-number 1.
      */
    EXPORT DATASET(NumericField) P(DATASET(ProbQuery) queries, UNSIGNED psid=PS) := FUNCTION
        queries_D := DISTRIBUTE(queries, id);
        probs := ProbSpace.P(queries_D, psid);
        probs_S := SORT(probs, id);
        RETURN probs_S;
    END;

    /**
      * Calculate a series of numerical expected values.
      *
      * Expectations are of the form:
      * - E(Var | List of Conditions)
      *
      * @param queries A list of queries.  Exactly 1 target per query must be specified,
      *        and the target must be unbound (i.e. with zero arguments).
      *
      * @return A set of NumericField records, with value being the Expected Value of each
      *         query.
      */
    EXPORT DATASET(NumericField) E(DATASET(ProbQuery) queries, UNSIGNED psid=PS) := FUNCTION
        queries_D := DISTRIBUTE(queries, id);
        exps := ProbSpace.E(queries_D, psid);
        exps_S := SORT(exps, id);
        RETURN exps_S;
    END;

    /**
      * Calculate a series of Distributions.
      *
      * Distributions are of the form:
      * - Distr(Var | List of Conditions)
      *
      * @param queries A list of queries.  Exactly 1 target per query must be specified,
      *        and the target must be unbound (i.e. with zero arguments).
      *
      * @return A set of Types.Distr records, describing each of the queried distributions.
      */
    EXPORT DATASET(Distr) Distr(DATASET(ProbQuery) queries, UNSIGNED psid=PS) := FUNCTION
        queries_D := DISTRIBUTE(queries, id);
        distrs := ProbSpace.Distr(queries_D, psid);
        distrs_S := SORT(distrs, id);
        RETURN distrs_S;
    END;

    // End Structured Queries

    /**
      * Perform a series of dependency tests.
      *
      * Form:
      *    - Dependency(target1, target2 | List of conditions)
      *
      * @param queries A list of queries.  Exactly 2 targets per query must be specified.
      *
      * @return  a list of p-values with .5 confidence, in NumericField
      *     format.
      *     Values less than .5 indicate probable independence.
      *     Values greater than .5 indicate probable dependence
      */
    EXPORT DATASET(NumericField) Dependence(DATASET(ProbQuery) queries, UNSIGNED psid=PS) := FUNCTION
        queries_D := DISTRIBUTE(queries, id);
        deps := ProbSpace.Dependence(queries_D, psid);
        deps_S := SORT(deps, id);
        RETURN deps_S;
    END;
    /**
      * Perform a series of dependency tests and evaluate the results
      * as a Boolean.
      *
      * Form:
      *    - isIndependent(target1, target2 | List of conditions)
      *
      * @param queries A list of queries.  Exactly 2 targets per query must be specified.
      *
      * @return A list of results as NumericField.  Result of 1 indicates that the two
      *     targets are most likely independent.  0 indicates probable dependence.
      *
      */
    EXPORT DATASET(NumericField) isIndependent(DATASET(ProbQuery) queries, UNSIGNED psid=PS) := FUNCTION
        queries_D := DISTRIBUTE(queries, id);
        deps := ProbSpace.Dependence(queries_D, psid);
        deps_B := PROJECT(deps, TRANSFORM(RECORDOF(LEFT),
                                    SELF.value := IF(LEFT.value > .5, 0, 1),
                                    SELF := LEFT), LOCAL);
        deps_S := SORT(deps_B, id);
        RETURN deps_S;
    END;

    /**
      * Perform a set of regression style predictions on a continuous variable.
      * 
      * Form:
      *   - E(target | conditions)
      *
      * @param target The dependent variable (i.e. prediction target).  The target should be a continuous
      *                 variable.
      * @param varNames The names of the independent variables to be used for prediction
      * @param varDat The values of the conditional variables in NumericField format.  The field numbers
      *               correspond to the order of the varNames list.
      * @return A DATASET(NumericField) with the prediction values in field number 1.
      * 
      */
    EXPORT DATASET(NumericField) Predict(STRING target, SET OF STRING varNames, DATASET(NumericField) varDat, UNSIGNED psid=PS) := FUNCTION
      dat_D := DISTRIBUTE(varDat, id);
      dat_S := SORT(dat_D, id, number, LOCAL);
      preds := ProbSpace.Predict(dat_S, varNames, target, psid);
      preds_S := SORT(preds, id);
      RETURN preds_S;
    END;
    /**
      * Perform a set of classification predictions on a discrete target variable.
      * 
      * Form:
      *   - E(target | conditions)
      *
      * @param target The dependent variable (i.e. prediction target).  The target should be a discrete
      *                 variable.
      * @param varNames The names of the independent variables to be used for prediction
      * @param varDat The values of the conditional variables in NumericField format.  The field numbers
      *               correspond to the order of the varNames list.
      * @return A DATASET(NumericField) with the prediction values in field number 1.
      * 
      */
    EXPORT DATASET(NumericField) Classify(STRING target, SET OF STRING varNames, DATASET(NumericField) varDat, UNSIGNED psid=PS) := FUNCTION
      dat_D := DISTRIBUTE(varDat, id);
      dat_S := SORT(dat_D, id, number, LOCAL);
      preds := ProbSpace.Classify(dat_S, varNames, target, psid);
      preds_S := SORT(preds, id);
      RETURN preds_S;
    END;
END; // Probability Module
