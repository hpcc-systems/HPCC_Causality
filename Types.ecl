IMPORT ML_Core.Types AS mlcTypes;

NumericField := mlcTypes.NumericField;

/**
  * Module provides all common record types for the Causality Bundle
  */
EXPORT Types := MODULE
    /**
      * AnyField record layout
      *
      * AnyField extends NumericField to handle textual as well as numeric data.
      * The textVal field is added, which overrides the val field if the value is
      * textual.
      * @see ML_Core.Types.NumericField
      * @field textVal -- The string value of the field.
      *
      */
    EXPORT AnyField := RECORD(NumericField)
        STRING textVal := '';
    END;

    /**
      * Natural Language Query.
      *
      * Supports probability queries in a simple format.
      */
    EXPORT nlQuery := RECORD
      UNSIGNED id;
      STRING query;
    END;
    /**
      * Record layout for Probability Query parameters
      *
      * Various forms are supported:
      * 1) Variable Name alone (i.e. Unbound variable)
      * 2) Variable Name and One Argument (i.e. Val = arg1)
      * 3) Variable Name and Two Arguments (i.e. arg1 <= Val <= arg2)
      * 4) Variable Name and an Enumerated Set of Values (i.e. Val in Set(args))
      *     For discrete variables only.
      * 5) Variable Name and one or more string arguments (i.e. Val in Set(strArgs)).
      *     For text-based variables.
      *
      * @field VarName -- The variable name
      * @field Args -- The arguments for the query spec.  Any number arguments
      *                    may be provided depending on the context (for Form 1-4 above)
      * @field strArgs -- A set of string values (for Form 5 above only)
      * @field isEnum -- Boolean field. If true, then Args will be treated as Form
      *                   4 above.  To avoid confusion between Form 3 and Form 4 with
      *                   two values enumerated.
      */
    EXPORT ProbSpec := RECORD
        STRING VarName;
        SET OF REAL Args := [];
        SET OF STRING strArgs := [];
        BOOLEAN isEnum := False;
    END;

    /**
      * Record Layout for Probability Queries.
      *
      * Also used for Causality Queries (i.e. Interventional, Counterfactual)
      * General form for Probability Queries:
      * - P(target | conditions) -- e.g., P(Y=1 | X1=1.5, X2=-.3, .5 <= X3 <= 1.0)
      * - E(target | conditions)
      * - distr(target | conditions)
      *
      * @field id Unique id for each query, used to correlate results.
      * @field target The target of the query (e.g. 'Y', 'Y'=1, .5 <= 'Y' <= 1.0)
      * @field conditions The set of conditions to apply to the target query
      *         (e.g. ['X1', 'X2'=1, .5 <= 'X3' <= 1.0]).  Defaults to empty set
      *         meaning no conditions.
      * @field interventions The set of interventions for causal (interventional)
      *         queries.  These represent "do()" operations, and must be set
      *         to exact values (e.g. 'X1'=1.0).
      */
    EXPORT ProbQuery := RECORD
        UNSIGNED id;
        DATASET(ProbSpec) target;
        DATASET(ProbSpec) conditions := DATASET([], ProbSpec);
        DATASET(ProbSpec) interventions := DATASET([], ProbSpec);
        DATASET(ProbSpec) counterfacs := DATASET([], ProbSpec);
    END;

    /**
      * Histogram Entry
      *
      * Represents one bin of a discretized probability histogram
      *
      * @field Min The minimum value for this bin
      * @field Max The maximum value for this bin.  Values
      *            within this bin fall into the interval [Min, Max).
      *            For discrete variables, Min and Max will both equal the
      *            discrete value.
      * @field P The probability that the random variable will take on a value
      *            within this bin.
      */
    EXPORT HistEntry := RECORD
        REAL Min;
        REAL Max;
        REAL P;
    END;

    /**
      * Record to represent the Distribution of a single random variable
      *
      * Values are discretized.  For discrete variables, there will be as
      * many bins as the cardinality of the variable.  For continuous
      * variables, the number of bins is determined automatically based
      * on the number of observations.  Datasets with more observations
      * are discretized more finely than smaller datasets.
      *
      * @field id Identifier for the given requested distribution.
      *              Matches the id of the corresponding request.
      * @field query A representation of the query in (near) standard
      *         Pearl notation.  Format: 
      *          Distr<counterfactual>(target | conditions, do(interventions))
      *         The fields: counterfactual, conditions and
      *         do(interventions) may or may not appear in any given query.
      *         Angle brackets <> are used in place of subscripting as in
      *         Pearl notation. 
      * @field nSamples The number of samples upon which the distribution
      *             is based.
      * @field isDiscrete Boolean is TRUE if this is a discrete variable,
      *             otherwise FALSE.
      * @field minVal The minimum observed value of the variable.
      * @field maxVal The maximum observed value of the variable.
      * @field Mean The sample mean of the variable.
      * @field StDev The sample standard deviation of the variable.
      * @field Skew The sample skew of the variable.
      * @field Kurtosis The sample excess kurtosis of the variable.
      * @field Median The median sample value of the variable.
      * @field Mode The most common value of the variable.  For
      *             continuous variables, this is the midpoint of
      *             the bin containing the most samples.
      * @field Histogram The set of discretized bins representing
      *                  the distribution's PDF.
      * @field Deciles The Deciles of the variable's distribution
      *                 From 10 to 90.
      */
    EXPORT Distribution := RECORD
        UNSIGNED id;
        STRING query;
        UNSIGNED nSamples;
        Boolean isDiscrete;
        REAL minVal;
        REAL maxVal;
        REAL Mean;
        REAL StDev;
        REAL Skew;
        REAL Kurtosis;
        REAL Median;
        REAL Mode;
        DATASET(HistEntry) Histogram;
        DATASET(HistEntry) Deciles;
    END;

    /**
      * Enumeration for Random Variable Data Type (see RV below).
      */
    EXPORT DatTypeEnum := ENUM(None=0, Numeric=1, Categorical=2);

    /**
      * Random Variable Record type for causal model representation
      *
      * @field Name The name of the Random Variable.
      * @field Parents A set of RV Names representing the causal parents
      *                of this variable.
      * @field isObserved Boolean is TRUE if this variable has measurable
      *                 data associated with it. Otherwise FALSE.
      * @field DataType Enumeration of the data type associated with this
      *             variable.  Currently only Numeric and Categorical are
      *             supported.
      */
    EXPORT RV := RECORD
        STRING Name;
        SET OF STRING Parents;
        BOOLEAN isObserved := TRUE;
        DatTypeEnum DataType := DatTypeEnum.None;
    END;

    /**
      * Causal Model Definition Record
      *
      * @field Name The name of the model.
      * @field Nodes The list of Random Variables that comprise the model.
      *   This must be in the order of variable in the dataset.
      */
    EXPORT cModel := RECORD
        STRING Name;
        DATASET(RV) Nodes;
    END;

    /**
      * Record to represent a Structural Equation Model (SEM)
      *
      * See Synth/synthTest.ecl for details on use of fields.
      * 
      * @field Init An ordered list of statements to be executed once
      *         to do any required variable initialization.
      * @field VarNames An ordered set of variables representing the
      *         output of the SEM. The produced data will follow the
      *         order of variable in this set.
      * @field EQ An ordered set of equations that will be executed to
      *         generate each observation of the generated dataset.
      *         Equations may refer to variables initialized during Init
      *         processing, or variables set by previous equations.
      */
    EXPORT SEM := RECORD
        SET OF STRING Init;
        SET OF STRING VarNames;
        SET OF STRING EQ;
    END;

    /**
      * Model Validation Report
      *
      * Shows result of a model validation test.
      * Four types of tests are conducted:
      * - Type 0: Verify all exogenous variables are independent
      *           of one another.
      * - Type 1: Verify expected independencies.
      * - Type 2: Verified expected dependencies.
      * - Type 3: Verify causal direction.
      *
      * @field Confidence The confidence in the model between 0 and 1.
      *   0 implies no confidence.  1 implies perfect confidence.
      * @field NumTotalTests The total number of tests conducted.
      * @field NumTestsByType An array of four values indicating the
      *   number of tests of each type 0-3 conducted.
      * @field NumErrsPerType An array of four values indicating the
      *   number of errors detected for each test type 0-3.
      * @field NumWarnsPerType An array of four values indicating the
      *   number of warnings detected for each test type 0-3.
      * @field Errors Array of strings describing each error that
      *   occurred.
      * @field Warnings Array of strings describing each warning that
      *   occurred.
      */
    EXPORT ValidationReport := RECORD
        REAL Confidence;
        UNSIGNED NumTotalTests;
        SET OF UNSIGNED NumTestsPerType;
        SET OF UNSIGNED NumErrsPerType;
        SET OF UNSIGNED NumWarnsPerType;
        SET OF STRING Errors;
        SET OF STRING Warnings;
    END;

    /**
      * Record type for the results of a metrics query.
      *
      * @field id The id of the result corresponding to the original id
      *       in the query.
      * @field query A representation of the original query e.g., 
      *       Source -> Destination.
      * @field AveCausalEffect The average causal effect (ACE) of the
      *       source variable on the destination variable.
      * @field ContDirEffect The controlled direct effect (CDE) of the
      *       source variable on the destination variable.
      * @field IndirEffect The indirect effect (via other variables)
      *       of the source variable on the destination variable.
      */ 
    EXPORT cMetrics := RECORD
      UNSIGNED id;
      STRING query;
      REAL AveCausalEffect;
      REAL ContrDirEffect;
      REAL IndirEffect;
    END;

    /**
      * Represents a named set along with its members
      *
      * @field Name The identifier of the set
      * @field Members A list of unique set member identifiers
      */
    EXPORT SetMembers := RECORD
      STRING Name;
      SET OF STRING Members;
    END;

    /**
      * Results of the DiscoverModel function.
      *
      * Provides the information about what was discovered
      * from analyzing the dataset.
      *
      * @field Exos A list of exogenous variables.
      * @field Clusters A list of all of the discovered data cluster names.
      * @field ClustMembers A list of each cluster and its members.
      * @field ClustGraph A list of clusters and the set of parent clusters for
      *   each, representing a Directed Acyclic Graph (DAG) of cluster-to-cluster
      *   relationships.
      * @field VarGraph A list of variables and the set of parents for
      *   each, representing a Directed Acyclic Graph (DAG) of variable relationships.
      */
    EXPORT DiscoveryReport := RECORD
      SET OF STRING Exos;
      SET OF STRING Clusters;
      DATASET(SetMembers) ClustMembers;
      DATASET(SetMembers) ClustGraph;
      DATASET(SetMembers) VarGraph;
    END;
END;
