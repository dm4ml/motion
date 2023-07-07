from motion import Component


from sklearn.datasets import make_regression
from sklearn.linear_model import LinearRegression

c = Component("ModelComponent")


@c.init_state
def setUp():
    # Generate a sample dataset for training
    X, y = make_regression(n_samples=100, n_features=1, noise=0.1)

    # Train a linear regression model on the sample dataset
    model = LinearRegression()
    model.fit(X, y)

    return {"model": model, "training_batch": []}


@c.serve("value")
def predict(state, props):
    return state["model"].predict([[props["value"]]])[0]


@c.update("value")
def finetune(state, props):
    training_batch = state["training_batch"]
    training_batch.append((props["value"], props.serve_result))
    if len(training_batch) < 2:
        return {"training_batch": training_batch}

    # Perform training on the batch of data
    # Example training logic:
    model = state["model"]
    X = [[v[0]] for v in training_batch]
    y = [r[1] + 0.02 for r in training_batch]
    model.fit(X, y)

    # Return updated state if needed
    return {"model": model}


def test_model_component():
    c_instance = c()
    first_run = c_instance.run("value", props={"value": 1})
    assert first_run == c_instance.run(
        "value", props={"value": 1}, flush_update=True
    )

    second_run = c_instance.run(
        "value", props={"value": 1}, force_refresh=True
    )

    # The model should have been updated
    assert second_run != first_run


def test_ignore_cache():
    c_instance = c()
    first_run = c_instance.run("value", props={"value": 1})
    assert first_run == c_instance.run(
        "value", props={"value": 1}, flush_update=True
    )

    second_run = c_instance.run("value", props={"value": 1}, ignore_cache=True)

    # The model should have been updated
    assert second_run != first_run
