# Welcome to Motion

Motion is a **Python framework** for incrementally maintaining prompts and other LLM pipeline state as new data arrives. Motion components are **stateful** and **incremental**, and can be run anywhere (e.g., in a notebook, in a serverless function, in a web app) because they are backed by a key-value store.

!!! tip "Alpha Release"

    Motion is currently in alpha. We are actively working on improving the documentation and adding more features. If you are interested in using Motion and would like dedicated support from one of our team members, please reach out to us at [shreyashankar@berkeley.edu](mailto:shreyashankar@berkeley.edu).

## Should I use Motion?

Motion is especially useful for applications that:

- Need to update prompts based on new data (e.g., maintain a dynamic list of examples in the prompt)
- Need to continually fine-tune models or do any online learning
- Want to maintain state across multiple requests
- Want a Pythonic interface to build a distributed system of LLM application components

Motion is built for developers who know how to code in Python and want to be able to control operations in their ML applications. For low-code and domain-specific development patterns (e.g., enhancing videos), you may want to check out other tools.

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
