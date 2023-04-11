# Motion

[![motion](https://github.com/dm4ml/motion/workflows/motion/badge.svg)](https://github.com/dm4ml/motion/actions?query=workflow:"motion")
[![docs](https://github.com/dm4ml/motion/workflows/docs/badge.svg)](https://github.com/dm4ml/motion/actions?query=workflow:"docs")
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![GitHub tag](https://img.shields.io/github/tag/dm4ml/motion?include_prereleases=&sort=semver&color=blue)](https://github.com/dm4ml/motion/releases/)
[![PyPI version](https://badge.fury.io/py/motion-python.svg)](https://badge.fury.io/py/motion-python)

Motion is a framework for building machine learning (ML) applications in Python, designed to give developers **fine-grained control** over **continually-updating** state (e.g., models, indexes, or other data structures).


## Why Motion?

Building production ML applications with reasonable degrees of customization (e.g., fine-tuning on user feedback) can be tedious. First, a developer must set up a database to store their data, data collection pipelines, data preprocessing pipelines, model training pipelines, model serving pipelines, and an interface to interact with the application. To run all pipelines regularly, a developer must also configure and maintain some workflow orchestrator, painstakingly encoding interactions between different pipelines. 

Motion makes this easier by:

1. Abstracting away the details of coordinating different components and pipelines in an ML application, 
2. Automating recurring operations (e.g., data ingestion, model retraining, updating an index), and
3. (In progress) Automatically monitoring data for drift and anomalies, recomputing state (e.g., models) as needed.


## Should I use Motion?

Motion is especially useful for applications that:

* Have dynamic data ingestion (e.g., regularly scrape a website)
* Run models with stateful information (e.g. recent user queries), or
* Regularly update (e.g., fine-tune) state such as models, indexes, or other data structures
* Have different pipelines that share the same state

Motion is built for developers who know how to code in Python and want to be able to control operations in their ML applications. For low-code and domain-specific development patterns (e.g., enhancing videos), you may want to check out other tools.

Finally, Motion is a framework, not necessarily just a library, and thus supports the usage of _any_ Python library or ML model for individual operations in an ML pipeline. 

## Where did Motion come from?

Motion is developed and maintained by researchers at the [UC Berkeley EPIC Lab](https://epic.berkeley.edu) who specialize in data management for ML pipelines.

## Getting Started

Check out the [docs](https://dm4ml.github.io/motion/) for more information.

Motion is currently in alpha. We are actively working on improving the documentation and adding more features. If you are interested in using Motion and would like dedicated support from one of our team members, please reach out to us at [shreyashankar@berkeley.edu](mailto:shreyashankar@berkeley.edu).
