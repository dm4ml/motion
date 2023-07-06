# Installing Motion

Motion is available on PyPI. Motion requires Python 3.8 or later. To install Motion, run the following command:

```bash
pip install motion-python
```

To verify motion is working as intended, run `motion` in your terminal. An usage explanation should be returned, as well as a list of CLI commands that can be executed.

## Setting up the database

Motion relies on Redis to store component state and metadata. You can install Redis [here](https://redis.io/download) and run it however you like, e.g., via [Docker](https://redis.io/docs/stack/get-started/install/docker/). You will need to configure the following environment variables:

- `MOTION_REDIS_HOST`: The host of the Redis server. Defaults to `localhost`.
- `MOTION_REDIS_PORT`: The port of the Redis server. Defaults to `6379`.
- `MOTION_REDIS_PASSWORD`: The password of the Redis server. Defaults to `None`.
- `MOTION_REDIS_DB`: The database of the Redis server. Defaults to `0`.

## (Optional) Installing from source

Motion is developed and maintained on Github. We use `poetry` to manage dependencies and build the package. To install Motion from source, run the following commands:

```bash
git clone https://github.com/dm4ml/motion
cd motion
make install
```

## (Optional) Component Visualization Tool

Check out the component visualization tool [here](https://dm4ml.github.io/motion-vis/).
