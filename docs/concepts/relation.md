# Relations

Relations are the primary way that data is stored in Motion.

## Defining a Relation

Relations follow a schema, or a list of key-type pairs. Relations in Motion must subclass `motion.Schema`. For example, suppose we are building a chatbot. We can define a relation that stores information about a user's query and resulting chatbot completion (i.e., response):

```python
class Query(motion.Schema):
    username: str
    prompt: str
    llm_completion: str
    llm_completion_score: float
    user_feedback: bool
```

Every _record_ in the Query relation can have a value for the keys (i.e., attributes) defined above, such as a `username`. When creating a new record, not all keys receive values upfront--some keys are the result of some computation. For example, `llm_completion` is the result of a call to a large language model API, and `user_feedback` might only be set if a user "likes" a completion for a query.

## Subclassing `motion.Schema`

Every relation defined as a subclass `motion.Schema` gets three additional keys: `identifier`, `create_at`, and `derived_id`. The `create_at` key is automatically set to the time at which a record with a new identifier is added to a relation.

The `derived_id` key is null for most records; however, in many cases, a developer will want to create many records based on a single record. For example, say we want our chatbot's ML model to return many possible completions for a prompt, and we want to store

## Allowed Types for Relation Keys

## Frequently Asked Questions

### Why am I forced to define so much structure in my data?

### What if I don't know all the relations and keys I want to have in my application?

### Is Motion a new database management system?

No. Motion does not support transactions, nor is there a recovery protocol. Motion's relations are in-memory and periodically checkpointed to disk once an hour, or on a user-defined schedule. Upon restart, the latest checkpoint is loaded. Motion's relations are stored in Apache Arrow dataframes.