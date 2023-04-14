# HPCC_Causality
Causality Bundle for HPCC Systems Platform

This bundle supports research into Causal Analysis of data.

It provides four main modules:
- Synth -- Allows the generation of complex synthetic datasets with known causal relationships, using structural equation models.  These can be used with the other layers of this toolkit or with other HPCC Machine Learning bundles.
- Probability -- Provides a rich probability analysis system, supporting conditional probabilities, conditionalizing, independence testing, and predictive capabilities.
- Causality -- Provides a range of Causal Analysis algorithms.  This layer requires a Causal Model, which combined with a dataset, allows questions to be asked that are beyond the realm of Statistics.  Specifically, this module allows: Model Hypothesis Testing, Causal Analysis, Causal Metrics, Counterfactual Analysis (future), and limited Causal Discovery.
- Visualization -- Visualize probabilistic and Causal relationships.  Provides a range of Plots showing probabilities, joint probabilities, conditional probabilities, and causal relationships.

The above methods are computationally intense, and are fully parallelized when running on an HPCC Systems Cluster.

It is built on the underlying capabilities of the Python-based "Because" Causal Analytic Library.

## Installation

This bundle requires python3, PIP,  and "Because" on each HPCC Node

### Installing Because:

Clone the repository https://github.com/RogerDev/Because.git

Run: sudo -H pip3 install <path to Because>

Example: sudo -H pip3 install ~/source/Because

This must be done on each HPCC Cluster node.  It is important to use sudo so that the bundle is installed for all users.  Since HPCC nodes run as special user hpcc, installing as the current user would not allow the module to be found by hpcc.

### Installing HPCC_Causality

On your client system, where HPCC Clienttools was installed, run
ecl bundle install https://github.com/RogerDev/HPCC_Causality.git

## Using HPCC_Causality

Each of the main modules provides documentation and examples of use.
Documentation is provided within the module and examples are in the test folder.
  
### Probability Queries
The probability, causality, and Visualization modules share a common query format.  Natural textual queries are used as they allow both simple and sophisticated queries to be composed without complex nested data structures.
  
These queries are provided as close as possible to standard statistical notation, with causal extensions as used by Pearl[1].

For example:
- 'P(height > 60 | gender = female)' -- The probability that height is greater than 60 (inches) given that gender is female.
- 'P(height > 60, weight between [100,150] | gender = female, age >= 20)' -- The joint probability that height is > 60 and weight is between
      100 and 150 (pounds) given that gender is female and age is greater than or equal 20.
- 'E(income | genhealth in [good, verygood, excellent])' -- The expectation of income given that general health is good, verygood, or excellent.

