# HPCC-Causality
Causality Bundle for HPCC Systems Platform

This bundle supports research into Causal Analysis of data.

It provides three main modules:
- Synth -- Allows the generation of complex synthetic datasets with known causal relationships, using structural equation models.  These can be used with the other layers of this toolkit or with other HPCC Machine Learning bundles.
- Probability -- Provides a rich probability analysis system, supporting conditional probabilities, conditioinalizing, independence testing, and predictive capabilities.
- Causality -- Provides a range of Causal Analysis algorithms.  This layer requires a Causal Model, which combined with a dataset, allows questions to be asked that are beyond the realm of Statistics.  Specifically, this module allows: Model Hypothesis Testing, Causal Analysis, Causal Metrics, Counterfactual Analysis (future), and limited Causal Discovery.

The above methods are computationally intense, and are fully parallelized when running on an HPCC Systems Cluster.

It is built on the underlying capabilities of the Python-based "Because" Causal Analytic Library.

## Installation

This bundle requires python3, PIP,  and "Because" on each HPCC Node

### Installing Because:

Clone the repository https://github.com/RogerDev/Because.git

Run: sudo pip3 install <path to Because>

Example: sudo pip3 install ~/source/Because

This must be done on each HPCC Cluster node.  It is important to use sudo so that the bundle is installed for all users.  Since HPCC nodes run as special user hpcc, installing as the current user would not allow the module to be found by hpcc.

### Installing HPCC-Causality

On your client system, where HPCC Clienttools was installed, run
ecl bundle install https://github.com/RogerDev/HPCC-Causality.git

## Using HPCC-Causality

Each of the main modules provides documentation and examples of use.
Documentation is provided within the module and examples are in the test folder.
