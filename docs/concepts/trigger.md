# Triggers

In Motion, Triggers are how ML logic and transformations are executed.

## Trigger Definition

Triggers hold mutable state to execute operations on data in relations. For example, in a chatbot, a large language model (LLM) is some state that might change throughout the application's lifetime--say, when the user likes some LLM response that we want to fine-tune the chatbot on.

Triggers are fired on `cursor.set` calls for specific keys within relations. To define a trigger, you must subclass `motion.Trigger` and define initial state by implementing the `setUp` method:

```python
class Chatbot(motion.Trigger):
    
    def setUp(self, cursor):
        llm = OpenAIModel(...) # (1)
        return {"model": llm}
```

1.  Can be any large language model.


Within your implementation of `setUp`, you can use the `cursor` object to access data within relations and create state based on existing data. This is especially useful when stopping and restarting your Motion application. For example, imagine our trigger maintains an index of historical queries:

```python
class Retrieval(motion.Trigger):

    def setUp(self, cursor):
        llm = OpenAIModel(...)

        # Retrieve existing documents to query
        old_prompts = cursor.sql("SELECT prompt FROM Query WHERE prompt IS NOT NULL")
        if len(old_prompts) > 0:
            index = create_index(old_prompts) # (1)
            return {"index": index, "model": llm}
        
        return {"index": {}, "model": llm}
        
        
```

1.  `create_index` is some function that creates an index of documents.



!!! warning "`setUp` arguments and return value"

    The `setUp` method must accept `self` and `cursor` arguments and return a dictionary representing the initial state of the trigger. An empty dictionary can be returned if the trigger is stateless (e.g., a website scraper). Motion will error if the `setUp` method does not return a dictionary.

## Routing

Triggers are fired on `cursor.set` calls for specific keys within relations. To specify a trigger key and logic for what execute, you create a `Route`, which accepts the following arguments:

- `relation`: The relation to watch for changes.
- `key`: The key to watch for changes.
- `infer`: A method that runs as soon as there is a new value for the key in the relation. Runs synchronously (i.e., in the foreground). Can be `None`.
- `fit`: A method that runs after `infer` and is used to update the trigger state. Runs asynchronously (i.e., in the background). Can be `None`.

An example of a `Route` is shown below:

```python
motion.Route(
    relation="Query",
    key="prompt",
    infer=self.llm_infer,
    fit=self.fine_tune
)
```

Motion triggers must include a `routes` method that returns a list of `Route` objects. For example:

```python
class Chatbot(motion.Trigger):

    def setUp(self, cursor):
        ...

    def routes(self):
        return [
            motion.Route(
                relation="Query",
                key="prompt",
                infer=self.llm_infer,
                fit=self.fine_tune
            )
        ]
```

## Trigger Life Cycle

**On Creation**: Upon initialization or restart of a motion application, the `setUp` method is called to initialize the trigger state. Then, the `routes` method is called to initialize the user-defined routes for keys. 

**On Update**: When data is added or changed for a key in a relation that corresponds to a route, the route's `infer` method is called. After the  `infer` method is called, the `fit` method is called. Fit methods are queued and executed asynchronously in the background, first come first serve. This means another trigger of an `infer` method can be called before the enqueued `fit` method actually executes.

### Life Cycle Diagram

The following diagram shows how user-defined methods, the data store, and trigger state interact during the trigger life cycle:

``` mermaid
graph LR
  subgraph ide1 [User-defined Methods]
  A[setUp];
  E[routes];
  C[infer];
  D[fit];
  end
  subgraph ide2 [Motion-Managed Objects]
  B{state};
  F[(Data store)];
  end
  A --> B;
  B -->|used for| C;
  C --> D;
  D -->|updates| B;
  E --> C;
  E --> D;
  F -->|on change| E;
```

### Infer and Fit Methods

The `infer` and `fit` methods are the core of a trigger. The `infer` method is called when a key in a relation is updated, followed by the `fit` method. 

Both methods accept the following arguments:

- `cursor`: A `motion.Cursor` object that can be used to access and write data in relations.
- `triggered_by`: A named tuple with the following fields:
    - `relation`: The relation that was updated.
    - `identifier`: The identifier of the record that was updated.
    - `key`: The key that was updated.
    - `value`: The new value for the key.

The `infer` method is allowed to access the trigger state but _should not_ modify trigger state. This is because the `infer` method is designed to use existing state to transform a record (e.g., make a prediction). 

The `fit` method is allowed to access and modify the trigger state, as it is designed to update state (e.g., fine-tune a model). The `fit` method must return a dictionary representing any updates to the trigger state (or `{}` if there are no updates). Fit methods are queued and executed asynchronously in the background, first come first serve.

Each `Route` object can have either an `infer` method or a `fit` method (or both). This allows different routes to leverage the same shared state: for example, a route for the `prompt` key can trigger an `infer` method to make a completion using an LLM, while a route for the `feedback` key (i.e., someone liked the completion for a given prompt) can trigger a `fit` method to fine-tune the model.


!!! info "Allowed and disallowed operations for trigger methods"

    The `setUp` method must accept `self` and `cursor` arguments and return a dictionary representing the initial state of the trigger. An empty dictionary can be returned if the trigger is stateless (e.g., a website scraper). Motion will error if the `setUp` method does not return a dictionary.

    | Method Type | Cursor Operations Allowed? | Trigger State Reads Allowed? | Trigger State Writes Allowed? | Returns Dictionary? |
    |--------|----------------------------|-----------------------------|------------------------------|-------------------------|
    | `setUp` | :fontawesome-solid-check:  | N/A (state not available yet) | N/A (state not available yet) | :fontawesome-solid-check: |
    | `infer` | :fontawesome-solid-check: | :fontawesome-solid-check: | :fontawesome-solid-xmark: | :fontawesome-solid-xmark: |
    | `fit` | :fontawesome-solid-check: | :fontawesome-solid-check: | :fontawesome-solid-check: | :fontawesome-solid-check: |


## Example