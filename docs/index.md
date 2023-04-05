# Welcome to Motion

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

Finally, Motion is a framework, not necessarily just a library, and thus supports the usage of any Python libraries or ML models for individual operations in an ML pipeline. 

## Where did Motion come from?

Motion is developed and maintained by researchers at the UC Berkeley EPIC Lab who specialize in data management for ML pipelines.


<!-- ## Commands

* `mkdocs new [dir-name]` - Create a new project.
* `mkdocs serve` - Start the live-reloading docs server.
* `mkdocs build` - Build the documentation site.
* `mkdocs -h` - Print help message and exit.

## Project layout

mkdocs.yml    # The configuration file.
docs/
    index.md  # The documentation homepage.
    ...       # Other markdown pages, images and other files. -->
