from motion import Component


from sklearn.datasets import make_regression
from sklearn.linear_model import LinearRegression


def test_model_component():
    c = Component("ModelComponent")

    @c.init
    def setUp():
        # Generate a sample dataset for training
        X, y = make_regression(n_samples=100, n_features=1, noise=0.1)

        # Train a linear regression model on the sample dataset
        model = LinearRegression()
        model.fit(X, y)

        return {"model": model}

    @c.infer("value")
    def predict(state, value):
        return state["model"].predict([[value]])[0]

    @c.fit("value", batch_size=2)
    def finetune(state, values, infer_results):
        # Perform training on the batch of data

        # Example training logic:
        model = state["model"]
        X = [[v] for v in values]
        y = [r + 0.02 for r in infer_results]
        model.fit(X, y)

        # Return updated state if needed
        return {"model": model}

    first_run = c.run(value=1)
    assert first_run == c.run(value=1, wait_for_fit=True)

    second_run = c.run(value=1)

    # The model should have been updated
    assert second_run != first_run