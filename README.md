# Motion

[![motion](https://github.com/dm4ml/motion/workflows/motion/badge.svg)](https://github.com/dm4ml/motion/actions?query=workflow:"motion")
[![lint (via ruff)](https://github.com/dm4ml/motion/workflows/lint/badge.svg)](https://github.com/dm4ml/motion/actions?query=workflow:"lint")
[![docs](https://github.com/dm4ml/motion/workflows/docs/badge.svg)](https://github.com/dm4ml/motion/actions?query=workflow:"docs")
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![GitHub tag](https://img.shields.io/github/tag/dm4ml/motion?include_prereleases=&sort=semver&color=blue)](https://github.com/dm4ml/motion/releases/)
[![PyPI version](https://badge.fury.io/py/motion-python.svg?branch=main&kill_cache=1)](https://badge.fury.io/py/motion-python)

Motion is a **Python framework** for incrementally maintaining prompts and other LLM pipeline state as new data arrives. Motion components are **stateful** and **incremental**, and can be run anywhere (e.g., in a notebook, in a serverless function, in a web app) because they are backed by a key-value store.

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

@BookRecommender.update("rec")
async def update_genres(state, props):
    recommended_books = state["recommended_books"] + props.serve_result
    all_books_positive_signal = state["liked_books"] + recommended_books
    new_genres = llm(f"Update my list of preferred genres {state['genres']} based on my book collection: {all_books_positive_signal}")
    return {
        "recommended_books": recommended_books,
        "genres": new_genres"
    }
```

In the above example, the `serve` operation recommends a book to the user based on a specified genre, and the `update` operation updates the context to be used in future recommendations (i.e., "rec" serve operations). `serve` operations execute first and cannot modify state, while `update` operations can modify state and execute after `serve` operations in the background.

You can run a flow by calling `run` or `arun` (async version of `run`) on the component:

```python
# Initialize component instance
book_recommender = BookRecommender("some_user_id", init_state_params={"user_demographics": "some_user_demographics", "liked_books": ["book1", "book2"]})

# Run the "rec" flow. Will return the result of the "rec" serve
# operation, and queue the "rec" update operation to run in the background.
rec = await book_recommender.arun("rec", {"specified_genre": "fantasy"})
```

After `rec` is returned, the `update` operation will run in the background and update the state of the component (for as long as the Python process is running). The state of the component instance is always committed to the key-value store after a flow is fully run, and is loaded from the key-value store when the component instance is initialized again.

Multiple clients can run flows on the same component instance, and the state of the component will be updated accordingly. Serve operations are run in parallel, while update operations are run sequentially in the order they are called. Motion maintains consistency by locking the state of the component while an update operation is running. Serve operations can run with old state while an update operation is running, so they are not blocked.

## Should I use Motion?

Motion is especially useful for applications that:

- Need to update prompts based on new data (e.g., maintain a dynamic list of examples in the prompt)
- Need to continually fine-tune models or do any online learning
- Want to maintain state across multiple requests
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
