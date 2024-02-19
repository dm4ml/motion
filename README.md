# Motion

[![motion](https://github.com/dm4ml/motion/workflows/motion/badge.svg)](https://github.com/dm4ml/motion/actions?query=workflow:"motion")
[![lint (via ruff)](https://github.com/dm4ml/motion/workflows/lint/badge.svg)](https://github.com/dm4ml/motion/actions?query=workflow:"lint")
[![docs](https://github.com/dm4ml/motion/workflows/docs/badge.svg)](https://github.com/dm4ml/motion/actions?query=workflow:"docs")
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![GitHub tag](https://img.shields.io/github/tag/dm4ml/motion?include_prereleases=&sort=semver&color=blue)](https://github.com/dm4ml/motion/releases/)
[![PyPI version](https://badge.fury.io/py/motion-python.svg?branch=main&kill_cache=1)](https://badge.fury.io/py/motion-python)

Motion is a system for defining and incrementally maintaining **self-updating prompts** in Python.

## Why Self-Updating Prompts?

LLM accuracy often significantly improves with more context. Consider an e-commerce focused LLM pipeline that recommends products to users. The recommendations might improve if the prompt considers the user's past purchases and browsing history. A **self-updating prompt** could change over time by including LLM-generated insights from user interactions. For a concrete example of this, consider the following sequence of user interactions, where 1, 3, and 4 show prompts for event styling queries and 2 corresponds to user feedback:

1. "What apparel items should I buy for `SIGMOD in Chile`?"
2. _User `disliked "purple blazer"` and `liked "wide-leg jeans."`_
3. "`I work in tech. I dress casually.` What apparel items should I buy for `hiking in the Bay Area`?"
4. "`I work in tech and have an active lifestyle. I dress casually.` What apparel items should I buy for `coffee with a friend`?"

In the above sequence, `phrases in backticks` are dynamically generated based on previous user-generated activity. The new context can improve the quality of responses.

### Why is it Hard to Use Self-Updating Prompts?

Consider the e-commerce example above. The prompt might grow to be very long---so long that there's a bunch of redundant or event useless information in the prompt. So, we might want to summarize the user's past purchases and browsing history into a single prompt. However, summarizing the user's past purchases and browsing history every time we log a new purchase or browsing event, or whenever the user requests a new recommendation, **can take too long** (e.g., 30+ seconds) and thus prohibitively increase end-to-end latency for getting a recommendation.

## What is Motion?

Motion allows LLM pipeline developers to define and incrementally maintain self-updating prompts in Python. With Motion, developers define **components** that represent prompt sub-parts, and **flows** that represent how to assemble sub-parts into a prompt for an LLM in real-time and how to self-updatingly update sub-parts in the background based on new information.

Motion's execution engine serves cached prompt sub-parts for minimal real-time latency and handles concurrency and sub-part consistency when running flows that update sub-parts. All prompt sub-parts are backed by a key-value store. You can run Motion components anywhere and in any number of Python processes (e.g., in a notebook, in a serverless function, in a web server) at the same time for maximal availability.

As LLM pipeline developers, we want a few things when building and using self-updating prompts:

- **Flexibility**: We want to be able to define our sub-parts of prompts (e.g., summaries). We also want to be able to define our own logic for how to turn sub-parts into string prompts and self-updatingly update sub-parts.
- **Availability**: We want there to always be some version of prompt sub-parts available, even if they are a little stale. This way we can minimize end-to-end latency.
- **Freshness**: Prompts should incorporate as much of the latest information as possible. In the case where information arrives faster than we can process it, it may be desirable to ignore older information.

## An Example Motion Component

It's hard to understand Motion without an example. In Motion, you define components, which are stateful objects that can be updated incrementally with new data. A component has an `init_state` method that initializes the state of the component, and any number of **flows**, where each flow consists of a `serve` operation (state read-only) and an `update` operation (can read and write state). These operations are arbitrary user-defined Python functions.

Here's an example of a component that recommends apparel to buy for an event, personalized to each user:

```python
from motion import Component

ECommercePrompt = Component("E-Commerce")

@ECommercePrompt.init_state
def setup():
  return {"query_summary": "No queries yet.", "preference_summary": "No preference information yet."}

@ECommercePrompt.serve("styling_query")
def generate_recs(state, props):
    # Props = properties to this specific flow's execution
    # First retrieve products from the catalog
    catalog_products = retrieve(props['event'])
    prompt = f"Consider the following lifestyle and preference information about me: {state['query_summary']}, {state['preference_summary']}. Suggest 3-5 apparel items for me to buy for {props['event']}, using the catalog: {catalog_products}."
    return llm(prompt)

@ECommercePrompt.update("styling_query")
def query_summary(state, props):
    # props.serve_result contains the result from the serve op
    prompt = f"You recommended a user buy {props.serve_result} for {props['event']}. The information we currently have about them is: {state['query_summary']}. Based on their query history, give a new 3-sentence summary about their lifestyle."
    query_summary = llm(prompt)
    # Update state
    return {"query_summary": query_summary}
```

In the above example, the `serve` operation recommends items to buy based on an event styling query, and the `update` operation updates the context to be used in future recommendations (i.e., "styling_query" serve operations). `serve` operations execute first and cannot modify state, while `update` operations can modify state and execute after `serve` operations in the background.

You can run a flow by calling `run` or `arun` (async version of `run`) on the component:

```python
# Initialize component instance
instance = ECommercePrompt(user_id) # Some user_id

# Run the "styling_query" flow. Will return the result of the "styling_query" serve
# operation, and queue the "styling_query" update operation to run in the background.
rec = await instance.arun("styling_query", props={"event": "sightseeing in Santiago, Chile"})
```

After `rec` is returned, the `update` operation will run in the background and update the state of the component (for as long as the Python process is running). The state of the component instance is always committed to the key-value store after a flow is fully run, and is loaded from the key-value store when the component instance is initialized again.

Multiple clients can run flows on the same component instance, and the state of the component will be updated accordingly. Serve operations are run in parallel, while update operations are run sequentially in the order they are called. Motion maintains consistency by locking the state of the component while an update operation is running. Serve operations can run with old state while an update operation is running, so they are not blocked.

## Should I use Motion?

Motion is especially useful for LLM pipelines

- Need to update prompts based on new data (e.g., maintain a dynamic summary in the prompt)
- Want a Pythonic interface to build a distributed system of LLM application components

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
