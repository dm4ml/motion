# Triggers

In Motion, Triggers are how ML logic and transformations are executed.

## Trigger Definition

Triggers hold mutable state to execute operations on data in relations. For example, in a chatbot, a large language model (LLM) is some state that might change throughout the application's lifetime--say, when the user likes some LLM response that we want to fine-tune the chatbot on.

Triggers are fired on `cursor.set` calls for specific keys within relations. To define a trigger, you must subclass `motion.Trigger` and define initial state by implementing the `setUp` method:

```python
class Chatbot(motion.Trigger):
    
    def setUp(self, cursor):
        llm = OpenAIModel(...) # (1)!
        return {"model": llm}
```

1.  Can be any large language model.


Within your implementation of `setUp`, you can use the `cursor` object to access data within relations and create state based on existing data. This is especially useful when stopping and restarting your Motion application. For example, suppose our trigger maintains an index of historical queries that can be used to engineer a better prompt:

```python
class RetrChatbotieval(motion.Trigger):

    def setUp(self, cursor):
        llm = OpenAIModel(...)

        # Retrieve existing documents to query
        old_prompts = cursor.sql("SELECT prompt FROM Query WHERE prompt IS NOT NULL")
        if len(old_prompts) > 0:
            index = create_index(old_prompts) # (1)!
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
    fit=self.update_index
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
                fit=self.update_index
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

Putting it all together, we can create a Question & Answering Chatbot trigger that uses an LLM to make predictions on new prompts. For each vanilla prompt the trigger receieves, the trigger performs some prompt engineering by querying similar prompts from the data store and appending them to the prompt. The trigger then uses the LLM to make a completion for the prompt. The trigger then stores the prompt and completion in the data store. We will do this in the `infer` method.

To retrieve semantically similar prompts, the trigger maintains an index of historical prompts and completions. This index must be updated whenever a new prompt and completion are added to the data store. We will do this in the `fit` method.


```python
class QAChatbot(motion.Trigger):
    def setUp(self, cursor):
        llm = OpenAIModel(...)

        # Retrieve existing documents and completions to populate an index
        old_prompts_and_completions = cursor.sql(
            "SELECT prompt, completion FROM Query WHERE prompt IS NOT NULL AND completion IS NOT NULL"
        )  # (1)!
        if len(old_prompts) > 0:
            index = create_index(old_prompts_and_completions)  # (2)!
            return {"index": index, "model": llm}

        return {"index": {}, "model": llm}

    def routes(self):
        return [
            motion.Route(
                relation="Query",
                key="prompt",
                infer=self.llm_infer,
                fit=self.update_index,
            )
        ]

    def llm_infer(self, cursor, triggered_by):
        # Get the prompt from the record
        prompt = triggered_by.value

        # Search the index for similar prompts and completions
        similar_prompts_and_completions = retrieve_similar(
            self.state["index"], prompt
        )
        engineered_prompt = f"Here are some questions and answers:\n{similar_prompts_and_completions}\nAnswer this question:\n{prompt}"

        # Query the model for completions
        response = self.state["model"].query(
            engineered_prompt, num_completions=3
        )  # (3)!

        # Write the completions to the store
        for completion in response["completions"]:
            new_id = cursor.duplicate(
                triggered_by.relation, triggered_by.identifier
            )  # (4)!
            cursor.set(
                relation=triggered_by.relation,
                identifier=new_id,
                key_values={"llm_completion": completion},
            )
    
    def update_index(self, cursor, triggered_by):
        # Get the prompt and completions from the store
        prompts_and_completions = cursor.get(
            relation=triggered_by.relation,
            identifier=triggered_by.identifier,
            keys=["prompt", "llm_completion"],
            include_derived = True # (5)!
        )
        

        # Update the index
        new_index = update_index(
            self.state["index"], prompts_and_completions
        ) # (6)!

        return {"index": new_index}
```

1.  Returns dataframe with 3 columns: identifier, prompt, completion
2.  Some function that creates an index of prompts and completions, so we can use the index to prompt-engineer future completions
3.  Some function that queries the model for multiple completions, based on some engineered prompt
4.  Need to duplicate the existing record, so we can write multiple completions for a single query to the store
5.  Need to include derived identifiers in the `get` operation, so we can retrieve all prompts and completions for a given query
6.  Some function that updates the index with new prompts and completions


!!! info "Common trigger design patterns"

    - **State counters**: Sometimes, operations that you would want to run in a `fit` method might be better suited as batch operations (e.g., fine-tuning on a batch of examples). You can maintain a counter in the trigger state and increment the counter in the `fit` method--only running the batch operation when the counter reaches a certain threshold. See the FAQ for more details.
    - **Writing results of `infer`**: For a client or another process to read the result of an `infer` method, you must write the result to the store. To do this, call `cursor.set` for the relevant relation, identifier, and key-value pairs.
    - **Writing multiple results of `infer`**: If you want to write multiple results of an `infer` method, you must duplicate the record and write the results to the duplicated records. To do this, call `cursor.duplicate` for the relevant relation and identifier, and then call `cursor.set` for the duplicated identifier and key-value pairs (as in the example above).
    - **`infer`-only or `fit`-only routes**: Sometimes a route only needs to perform an `infer` or `fit` operation, but not both. In this case, you can set the `fit` or `infer` argument to `None` in the `Route` constructor. See the FAQ for more details.

## Frequently Asked Questions

### Are the `triggered_by` arguments passed to `infer` and `fit` methods the same?

Yes, the `triggered_by` arguments passed to `infer` and `fit` methods are the same. The `triggered_by` argument is a `TriggeredBy` object that contains the relation, identifier, key, and value that triggered the method, which can be accessed as attributes: `triggered_by.relation`, `triggered_by.identifier`, `triggered_by.key`, and `triggered_by.value`.

### Can I use the `cursor` object to access the trigger state?

No, trigger state is accessible only through the `self.state` attribute. The `cursor` object is only used to access the data store. You should not directly update the `self.state` attribute, but instead return a new state dictionary from the `fit` method.

### How do I maintain a counter in the trigger state?

Suppose, in our `QAChatbot` example above, we want to update the index only every 10 new prompts. We can maintain a counter in the trigger state, increment the counter in the `fit` method, and update the index every 10 new prompts:

```python
class QAChatbot(motion.Trigger):

    def setUp(self, cursor):
        ... # Same as above

        return {"index": index, "model": llm, "counter": 0}

    ... # Same routes, llm_infer as above

    def update_index(self, cursor, triggered_by):
        ... # Same as above

        # Increment the counter
        counter = self.state["counter"] + 1

        # Update the index every 10 new prompts
        if counter % 10 == 0:
            new_index = update_index(self.state["index"], prompts_and_completions)
            return {"index": new_index, "counter": counter}

        return {"counter": counter}

```

We'll use this design pattern more in the tutorials.

### `infer`-only and `fit`-only routes

Why would we want to set `fit` or `infer` to `None` in the `Route` constructor? Suppose we aren't interested in prompt engineering in our QAChatbot, and we only want to query the LLM for completions based on the vanilla prompt. Then, the `fit` method would be unnecessary, and we can set `fit` to `None` in the `Route` constructor:

```python
def routes(self):
    return [
        motion.Route(
            relation="Query",
            key="prompt",
            infer=self.llm_infer,
            fit=None,
        )
    ]
```

If we have another route that tells us when to fine-tune the LLM--say, a user liking a prompt-completion pair, we can set `infer` to `None` in the `Route` constructor:

```python
def routes(self):
    return [
        motion.Route(
            relation="Query",
            key="prompt",
            infer=None,
            fit=self.update_index,
        ),
        motion.Route(
            relation="Query",
            key="feedback",
            infer=None
            fit=self.fine_tune, # (1)!
        )
    ]
```

1.  Some method that fine-tunes the LLM on a batch of examples