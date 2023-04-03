# Triggers

In Motion, Triggers are how ML logic and transformations are executed.

## Trigger Definition

Triggers hold mutable state to execute operations on data in relations. For example, in a chatbot, a large language model (LLM) is some state that might change throughout the application's lifetime--say, when the user likes some LLM response that we want to fine-tune the chatbot on.

Triggers are fired on `cursor.set` calls for specific keys within relations. To define a trigger, you must subclass `motion.Trigger` and define initial state by implementing the `setUp` method:

```python
class Chatbot(motion.Trigger):
    
    def setUp(self, cursor):
        llm = OpenAIModel(...)
        return {"model": llm}
```

The `setUp` method must accept `self` and `cursor` arguments and return a dictionary representing the initial state of the trigger. An empty dictionary can be returned if the trigger is stateless (e.g., a website scraper). 

Within your implementation of `setUp`, you can use the `cursor` object to access data within relations and create state based on existing data. This is especially useful when stopping and restarting your Motion application. For example, imagine we have a trigger that queries an index of documents:

```python
class Retrieval(motion.Trigger):

    def setUp(self, cursor):
        # Retrieve existing documents to query
```

## Routing

## Trigger Life Cycle
