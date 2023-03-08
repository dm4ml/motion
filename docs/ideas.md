# ideas

Here are several ideas that we are interested in pursuing.

## Engineering

### 1. Integration with serverless to run remotely

We can integrate with `modal` to run remotely on a serverless platform. Additionally, `modal`'s support for cron jobs can be used to create cron-scheduled triggers (e.g., scraping).

### 2. DAG viewer

Implicitly, the user defines a DAG of triggers, where `shouldFit` and `shouldInfer` tell us which triggers are upstream of which other triggers. We can visualize this DAG in a web UI. Nodes are database keys and trigger operations, and edges are the dependencies between them.

### 3. Integration with `mlflow` and `wandb` to track experiments

Many times, users will want to try different experiments to see which one works best. We can integrate with `mlflow` and `wandb` to track experiments and compare them.

### 4. Incremental data ingestion

When data is ingested on a schedule, sometimes we may only want the incremental or new data to be ingested. How do we efficiently dedup the data and upsert it into the DB? Without blocking triggers from running?

## Research

### 1. Data validation for LLM model output

Sometimes, the user will want to impose structure on the output of the LLM model. For example, the user may want to ensure that the output is a valid date. How do we do this without running the LLM a billion times? We could look into a paper like [this](https://arxiv.org/pdf/2201.11227.pdf).

### 2. Monitoring the DB

We can summarize partitions of the DB and monitor the summaries if there are any anomalies in the data. When there is an anomaly, we can trigger an alert.

### 3. Asynchronous trigger state updates 

Allow `fit` to be called asynchronously. This will allow us to update the state in the background while the user is still using the trigger to make predictions. When a state is updated, we have to somehow let the DB know a new version of the state is available. This is a bit tricky because the DB is not aware of the trigger state. Moreover, some `infer` call should not switch state versions in the middle of inference. We can use a lock to ensure that only one `infer` call is running at a time, but this will slow down inference. 

We can also use a lock-free approach, where we have a version number that is incremented every time a new state is available. When `infer` is called, it will use the latest version of the state. This will allow us to update the state in the background while the user is still using the trigger to make predictions.

If we use a multiversioning approach, we will need to deprecate old versions. We can do this by having a TTL on the state. When the TTL expires, we can delete the old state. This will allow us to reclaim space in the DB.