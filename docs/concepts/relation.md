# Relations

A Motion application stores data in a data store, which comprises of different relations.

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

Every relation defined as a subclass `motion.Schema` gets three additional keys: 

- `identifier: str`
- `create_at: datetime.datetime`
- `derived_id: str`

The `identifier` key is a unique key--meaning, there cannot be multiple records with the same `identifier`. The `create_at` key is automatically set to the time at which a record with a new identifier is added to a relation.

The `derived_id` key is null for most records; however, in many cases, a developer will want to create many records based on a single record. For example, say we want our chatbot's ML model to return many possible completions for a single prompt. When the prompt is first added to the store, a single record with some identifier `i` be created. When each completion is generated and added to the store, a new record is created--with a different identifier but `derived_id` = `i`.

Motion provides utilities to retrieve all records for a given `identifier` or `derived_id`, which we will discuss later.

## Allowed Types for Relation Keys

For data validation purposes, Motion requires all keys to be typed. Supported types include:


- `int`
- `str`
- `float`
- `bool`
- `datetime.date`
- `datetime.datetime`
- `bytes` (commonly used to store preprocessed images)
- `typing.List` (commonly used to store embeddings)
- `typing.Dict`

Note that when defining a `typing.List` type, you must specify the type of an element within the list, i.e., `typing.List[float]`. Similarly, when defining a `typing.Dict` type, you must specify the key and value types, i.e., `typing.List[str, float]`.

Motion also supports enum types, which are defined as a Python `enum.Enum` subclass:

```python
from enum import Enum

class Source(Enum):
    ONLINE = "online"
    OFFLINE = "offline"
```

Then when defining a relation, you can use the enum type:

```python
class Query(motion.Schema):
    source: Source
    username: str
    prompt: str
    llm_completion: str
    llm_completion_score: float
    user_feedback: bool
```

## Frequently Asked Questions

### Why am I forced to define so much structure in my data?

Many errors in production ML applications stem from corrupted or invalid data, which is often hard to detect without some structure. Even for unstructured data like text and images, we find that defining a relation with its keys, rather than one big JSON object to store all the data, helps with basic data validation.

Motion's roadmap includes automatic data validation and monitoring, which significantly benefits from schema information. For more on our research in automatic data validation for ML pipelines, check out this paper.

### What if I don't know all the relations and keys I want to have in my application?

That is okay! You can add new keys in your relation definitions, even when your application is deployed. Motion's roadmap includes automatic schema migration.

### Is Motion a new database management system?

No. Motion does not support transactions, nor is there a recovery protocol. Motion's relations are in-memory and periodically checkpointed to disk once an hour, or on a user-defined schedule. Upon restart, the latest checkpoint is loaded. Motion's relations are stored in Apache Arrow dataframes.