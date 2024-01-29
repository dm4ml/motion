# Welcome to Motion

Motion is a system for defining and incrementally maintaining **reactive prompts** in Python.

!!! tip "Alpha Release"

    Motion is currently in alpha. We are actively working on improving the documentation and adding more features. If you are interested in using Motion and would like dedicated support from one of our team members, please reach out to us at [shreyashankar@berkeley.edu](mailto:shreyashankar@berkeley.edu).

## Why Reactive Prompts?

LLM accuracy often significantly improves with more context. Consider an e-commerce focused LLM pipeline that recommends products to users. The recommendations might improve if the prompt considers the user's past purchases and browsing history. Ideally, any new information about the user (e.g., a new purchase or browsing event) should be incorporated into the LLM pipeline's prompts as soon as possible; thus, we call them **reactive prompts**.

### Why is it Hard to Use Reactive Prompts?

Consider the e-commerce example above. The prompt might grow to be very long---so long that there's a bunch of redundant or event useless information in the prompt. So, we might want to summarize the user's past purchases and browsing history into a single prompt. However, summarizing the user's past purchases and browsing history every time we log a new purchase or browsing event, or whenever the user requests a new recommendation, **can take too long** and thus prohibitively increase end-to-end latency for getting a recommendation.

In general, we may want to use LLMs or run some other expensive operation when incrementally processing new information, e.g., through summarization, extracting structured information, or generating new data. When there is a lot of information to process, the best LLMs can take upwards of 30 seconds. This can be unacceptable for production latency.

## What is Motion?

As LLM pipeline developers, we want a few things when building and using reactive prompts:

- **Flexibility**: We want to be able to define our sub-parts of prompts (e.g., summaries). We also want to be able to define our own logic for how to turn sub-parts into string prompts and reactively update sub-parts.
- **Availability**: We want there to always be some version of prompt sub-parts available, even if they are a little stale. This way we can minimize end-to-end latency.
- **Freshness**: Prompts should incorporate as much of the latest information as possible. In the case where information arrives faster than we can process it, it may be desirable to ignore older information.

Motion allows LLM pipeline developers to define and incrementally maintain reactive prompts in Python. With Motion, we define **components** that represent prompt sub-parts, and **flows** that represent how to assemble sub-parts into a prompt for an LLM in real-time and how to reactively update sub-parts in the background based on new information.

Motion's execution engine serves cached prompt sub-parts for minimal real-time latency and handles concurrency and sub-part consistency when running flows that update sub-parts. All prompt sub-parts are backed by a key-value store. You can run Motion components anywhere and in any number of Python processes (e.g., in a notebook, in a serverless function, in a web server) at the same time for maximal availability.

## Should I use Motion?

Motion is especially useful for LLM pipelines

- Need to update prompts based on new data (e.g., maintain a dynamic summary in the prompt)
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
