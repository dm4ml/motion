# Component Dashboard

The dashboard is a web app that allows you to inspect and edit the states of your component instances. It is built with React and served with FastAPI.

## Running the Dashboard

The dashboard is exposed as a FastAPI app, available via importing `motion.dashboard`. For example, to run the dashboard on `localhost:8000`, you can run the following code:

```python
from motion.dashboard import dashboard_app

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(dashboard_app)
```

You can serve or deploy the dashboard app in any way you would serve a FastAPI app.

## Using the Dashboard

The dashboard allows you to inspect component states and edit state key-value pairs. You can only edit key-value pairs with string, int, float, bool, list, or dict values. The dashboard does not support editing more complex types like numpy arrays or pandas dataframes, but you can still inspect these types.

## VictoriaMetrics Integration

The dashboard also integrates with VictoriaMetrics, a time-series database. If you have a VictoriaMetrics instance running, you can configure Motion to log flow runs and visualize flow status in the dashboard. To do this, you can set the `MOTION_VICTORIAMETRICS_URL` environment variable to the URL of your VictoriaMetrics instance. For example, to use a VictoriaMetrics instance running on `localhost:8428`, you can add the following line to your .motionrc.yml file:

```yaml
MOTION_VICTORIAMETRICS_URL: "http://localhost:8428"
```
