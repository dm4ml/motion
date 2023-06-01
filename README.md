# Motion

[![motion](https://github.com/dm4ml/motion/workflows/motion/badge.svg)](https://github.com/dm4ml/motion/actions?query=workflow:"motion")
[![lint (via ruff)](https://github.com/dm4ml/motion/workflows/lint/badge.svg)](https://github.com/dm4ml/motion/actions?query=workflow:"lint")
[![docs](https://github.com/dm4ml/motion/workflows/docs/badge.svg)](https://github.com/dm4ml/motion/actions?query=workflow:"docs")
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![GitHub tag](https://img.shields.io/github/tag/dm4ml/motion?include_prereleases=&sort=semver&color=blue)](https://github.com/dm4ml/motion/releases/)
[![PyPI version](https://badge.fury.io/py/motion-python.svg?branch=main&kill_cache=1)](https://badge.fury.io/py/motion-python)

Motion is a lightweight **framework** for building machine learning (ML) applications, designed to **reduce the MLOps burdens** of making sure your models, prompts, and other stateful objects are **up-to-date with your data.**

## Why Motion?

While building an ML application demo is easier than ever thanks to state-of-the-art models and open-source libraries, making sure ML applications _update with new data over time_ is still a challenge. As a result, developers painstakingly stitch together and maintain pipelines that run on a schedule (e.g., fine-tuning, updating indexes).

With Motion, a dataflow-based framework, your state is automatically updated in the background when you run your pipelines on new data. Moreover, multiple pipelines can easily share the same state.

## Should I use Motion?

Motion is especially useful for applications that:

- Need to continually fine-tune models or do any online learning
- Need to update prompts based on new data (e.g., maintain a dynamic list of examples in the prompt)

Motion is built for developers who know how to code in Python and want to be able to control operations in their ML applications. For low-code and domain-specific development patterns (e.g., enhancing videos), you may want to check out other tools.

## Where did Motion come from?

Motion is developed and maintained by researchers at the [UC Berkeley EPIC Lab](https://epic.berkeley.edu) who specialize in data management for ML pipelines.

## Getting Started

Check out the [docs](https://dm4ml.github.io/motion/) for more information.

Motion is currently in alpha. We are actively working on improving the documentation and adding more features. If you are interested in using Motion and would like dedicated support from one of our team members, please reach out to us at [shreyashankar@berkeley.edu](mailto:shreyashankar@berkeley.edu).

## Testing and Development

You can run `make install` to install an editable source of Motion. We use `poetry` to manage dependencies.

To run tests, we use `pytest` and a local Redis cache. You should run Redis on port 6381 before you run `make tests`. To run Redis with Docker, either run the `docker-compose.yml` file in this repo (i.e., `docker-compose up`) or run the following command in your terminal:

```bash
docker run -p 6381:6379 --name motion-backend-testing redis/redis-stack-server:latest
```

Then when you run `make tests`, your tests should pass.
