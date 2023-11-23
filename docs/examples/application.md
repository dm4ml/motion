# Building a Motion Application

In this tutorial, we'll go through setting up an application of Motion components. We'll go through setting up a simple server with custom components and then demonstrate how to interact with it using Axios in a TypeScript environment.

## Prerequisites

- [Motion](/motion/getting-started/installation/)
- [FastAPI](https://fastapi.tiangolo.com/) -- installed automatically with Motion
- [uvicorn](https://www.uvicorn.org/) -- To serve the application

## Step 1: Define Some Components

We'll write two components: one representing a counter, and another representing a calculator. The counter will increment a count every time an operation is called. The calculator will add and subtract two numbers.

```python title="sample_components.py" linenums="1"
from motion import Component

Counter = Component("Counter")

@Counter.init_state
def setup():
    return {"count": 0}

@Counter.serve("increment")
def increment(state, props):
    return state["count"] + 1

@Counter.update("increment")
def update_count(state, props):
    return {"count": state["count"] + 1}

Calculator = Component("Calculator")

@Calculator.serve("add")
def add(state, props):
    return props["a"] + props["b"]

@Calculator.serve("subtract")
def subtract(state, props):
    return props["a"] - props["b"]
```

## Step 2: Create an Application

We'll create an application that serves the two components we just defined.

```python title="app.py" linenums="1"
# app.py
from motion import Application
from sample_components import Counter, Calculator
import uvicorn

# Create the Motion application with both components
motion_app = Application(components=[Counter, Calculator])
fastapi_app = motion_app.get_app()

# Run the application using Uvicorn
if __name__ == "__main__":
    # Print secret key
    print(motion_app.get_credentials())

    uvicorn.run(fastapi_app, host="0.0.0.0", port=8000)
```

In the script, we include a statement to print the secret key (which is useful if you don't specify your secret key and Motion automatically creates one for your application). Keep this key safe, as it is used to authenticate requests to the application.

To run the application, we can run `python app.py` in the terminal. We can also run `uvicorn app:fastapi_app --reload` to run the application with hot reloading.

One of the powerful features of FastAPI is its automatic generation of interactive API documentation. Once your server is running, you can access the API documentation by visiting `http://localhost:8000/docs` in your web browser. This documentation provides a detailed overview of all the available routes (i.e., the Counter and Calculator component dataflows) and allows you to directly test the API endpoints from the browser.

## Step 3: Interact with the Application

Suppose we are in TypeScript and want to interact with the application. We can use Axios to make requests to the application. First, install Axios in your web application:

```bash
npm install axios
```

Then, we can write a simple Typescript function to make requests to the application:

```typescript title="queryMotionApp.ts" linenums="1"
import axios from "axios";

const queryServer = async () => {
  const secretToken = "your_secret_key"; // Replace with the secret key from your Motion application

  try {
    // Increment the Counter
    const incrementResponse = await axios.post(
      "http://localhost:8000/Counter",
      {
        instance_id: "ts_testid",
        dataflow_key: "increment",
        props: {},
      },
      {
        headers: { Authorization: `Bearer ${secretToken}` },
      }
    );
    console.log("Counter Increment Result:", incrementResponse.data);

    // Perform an addition using the Calculator
    const addResponse = await axios.post(
      "http://localhost:8000/Calculator",
      {
        instance_id: "ts_testid",
        dataflow_key: "add",
        props: { a: 20, b: 10 },
      },
      {
        headers: { Authorization: `Bearer ${secretToken}` },
      }
    );
    console.log("Addition Result:", addResponse.data);
  } catch (error) {
    console.error("Error querying server:", error);
  }
};

queryServer();
```

Since the application is served as a REST API, we can also use any other HTTP client to interact with the application.
