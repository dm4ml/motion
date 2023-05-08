# Welcome to Motion

Motion is a lightweight **framework** for building machine learning (ML) applications, designed to **reduce the MLOps burdens** of making sure your models, prompts, and other stateful objects are **up-to-date with your data.**

!!! tip "Alpha Release"

    Motion is currently in alpha. We are actively working on improving the documentation and adding more features. If you are interested in using Motion and would like dedicated support from one of our team members, please reach out to us at [shreyashankar@berkeley.edu](mailto:shreyashankar@berkeley.edu).

## Why Motion?

While building an ML application demo is easier than ever thanks to state-of-the-art models and open-source libraries, making sure ML applications _update with new data over time_ is still a challenge. As a result, developers painstakingly stitch together and maintain pipelines that run on a schedule (e.g., fine-tuning, updating indexes).

With Motion, a dataflow-based framework, your state is automatically updated in the background when you run your pipelines on new data. Moreover, multiple pipelines can easily share the same state.

## Should I use Motion?

Motion is especially useful for applications that:

- Need to continually fine-tune models or do any online learning
- Need to update prompts based on new data (e.g., maintain a dynamic list of examples in the prompt)

Motion is built for developers who know how to code in Python and want to be able to control operations in their ML applications. For low-code and domain-specific development patterns (e.g., enhancing videos), you may want to check out other tools.

Finally, Motion is a framework, not necessarily just a library, and thus supports the usage of _any_ Python library or ML model for individual operations in an ML pipeline.

## Where did Motion come from?

Motion is developed and maintained by researchers at the [UC Berkeley EPIC Lab](https://epic.berkeley.edu) who specialize in data management for ML pipelines.

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
