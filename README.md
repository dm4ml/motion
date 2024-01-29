# Motion

[![motion](https://github.com/dm4ml/motion/workflows/motion/badge.svg)](https://github.com/dm4ml/motion/actions?query=workflow:"motion")
[![lint (via ruff)](https://github.com/dm4ml/motion/workflows/lint/badge.svg)](https://github.com/dm4ml/motion/actions?query=workflow:"lint")
[![docs](https://github.com/dm4ml/motion/workflows/docs/badge.svg)](https://github.com/dm4ml/motion/actions?query=workflow:"docs")
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![GitHub tag](https://img.shields.io/github/tag/dm4ml/motion?include_prereleases=&sort=semver&color=blue)](https://github.com/dm4ml/motion/releases/)
[![PyPI version](https://badge.fury.io/py/motion-python.svg?branch=main&kill_cache=1)](https://badge.fury.io/py/motion-python)

Motion is a system for defining and incrementally maintaining **reactive prompts** in Python.

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

## An Example Motion Component

It's hard to understand Motion without an example. In Motion, you define components, which are stateful objects that can be updated incrementally with new data. A component has an `init_state` method that initializes the state of the component, and any number of **flows**, where each flow consists of a `serve` operation (state read-only) and an `update` operation (can read and write state). These operations are arbitrary user-defined Python functions.

Here's an example of a component that recommends books to buy, personalized to each user:

```python
from motion import Component

BookRecommender = Component("BookRecommender")

@BookRecommender.init_state
def setup(user_demographics, liked_books):
    return {
        "user_demographics": user_demographics,
        "liked_books": liked_books,
        "recommended_books": [],
        "genres": [],
    }

@BookRecommender.serve("rec")
async def get_rec(state, props):
    genre_str = ", ".join(state["genres"]) if state["genres"] else ""
    rec = await llm(f"I liked {state['liked_books']} and {genre_str} genres. What book would you recommend me to read in the {props['specified_genre']} genre?")
    return rec

@BookRecommender.update("rec", discard_policy=DiscardPolicy.SECONDS, discard_after=86400) # If the update wasn't processed within 24 hours (due to backpressure), discard it
async def update_genres(state, props):
    recommended_books = state["recommended_books"] + props.serve_result
    all_books_positive_signal = state["liked_books"] + recommended_books
    new_genres = await llm(f"Update my list of preferred genres {state['genres']} based on my book collection: {all_books_positive_signal}")
    return {
        "recommended_books": recommended_books,
        "genres": new_genres"
    }

@BookRecommender.update("liked_book")
async def update_liked_books(state, props):
    all_books_positive_signal = state["liked_books"] + [props["liked_book"]]
    new_genres = await llm(f"Update my list of preferred genres {state['genres']} based on my book collection: {all_books_positive_signal}")
    return {"liked_books": props["liked_books"], "genres": new_genres}
```

In the above example, the `serve` operation recommends a book to the user based on a specified genre, and the `update` operation updates the context to be used in future recommendations (i.e., "rec" serve operations). `serve` operations execute first and cannot modify state, while `update` operations can modify state and execute after `serve` operations in the background.

You can run a flow by calling `run` or `arun` (async version of `run`) on the component:

```python
# Initialize component instance
book_recommender = BookRecommender("some_user_id", init_state_params={"user_demographics": "some_user_demographics", "liked_books": ["book1", "book2"]})

# Run the "rec" flow. Will return the result of the "rec" serve
# operation, and queue the "rec" update operation to run in the background.
rec = await book_recommender.arun("rec", props={"specified_genre": "fantasy"})

# Log a new liked book. There is no serve operation for the "liked_book" flow,
# so nothing is returned. The "liked_book" update operation is queued to run in
# the background.
book_recommender.arun("liked_book", props={"liked_book": "Harry Potter and the Deathly Hallows"})
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
