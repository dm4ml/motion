# Welcome to Motion

Motion is a framework to build end-to-end machine learning (ML) applications in Python, with minimal distinction between development and deployment.

## Why Motion?

Building production ML applications with reasonable degrees of customization, such as fine-tuning on user feedback, can be tedious. First, a developer must first set up and coordinate a database, a web server, a model training pipeline, and a model serving pipeline. To run all pipelines regularly, including data collection, a developer must also set up and maintain some workflow orchestrator. Finally, a developer must also set up a monitoring system to ensure that the application is running smoothly.

Motion aims to make this process easier by providing a framework that (1) abstracts away the details of coordinating different components of an ML application, and (2) automates recurring operations like data ingestion and model retraining. 


## Should I use Motion?

Motion is a framework, not a library, and thus supports the usage of any Python libraries or ML models for individual operations in an ML pipeline. Motion is especially useful for applications that:

* Have different pipelines that share the same model(s)
* Have dynamic data ingestion (e.g., regularly scrape a website)
* Run models with stateful information (e.g. recent user queries), or
* Leverage fine-tuned models

Motion is built for developers who know how to code in Python and want to be able to control operations in their ML applications.

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
