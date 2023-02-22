# motion

A framework for building ML pipelines, using a trigger-based execution model.

## Sample Application

We'll be building a fashion search application, which will allow users to search for what clothes to buy based on a specific query. For our prototype, we'll use the following data sources (i.e., retailers):

* Nordstrom catalog
* Revolve catalog
* Bloomingdale's catalog

A catalog is a collection of (image, description, link) tuples.

Then, our ML pipeline will be composed of the following components:

* Query to idea generation (text to text, ML)
* Idea to image retrieval (text to image, ML)
* Image to product recommendation (map image back to product link, lookup)

The first pass of our pipeline will have no fine tuning. Then we will incorporate fine tuning into the idea to image retrieval component, by fine-tuning clip embeddings on a small dataset of (image, description) tuples.

## Motion Framework

The core ideas of Motion include the _store_ and _triggers_. The store is a key-value store that can be used to store data, and the triggers are used to execute components of the pipeline.

### Store

Stores are created using a name and list of schemas. The schema is a dataclass, where fields are annotated with types. For example, we might have schemas for our fashion search like this:

```python
import numpy as np
from dataclasses import dataclass
from enum import Enum

class Retailer(Enum):
    NORDSTROM = 0
    REVOLVE = 1
    BLOOMINGDALES = 2

class QuerySource(Enum):
    OFFLINE = 0
    ONLINE = 1


@dataclass
class QuerySchema:
    id: int
    ts: int
    src: QuerySource
    query: str
    text_suggestion: str
    text_suggestion_page: int
    img_id: int
    img_score: int
    img_id_page: int
    feedback: bool

@dataclass
class RetailerSchema:
    id: int
    ts: int
    retailer: Retailer
    img_url: str
    img_desc: str
    img_html: str
    img_summary: str
    img_embedding: np.ndarray
```

Each schema can be considered a table. Primary keys are the id and any keys with the word `page` in them. This is because often times we have multiple pages of results (e.g., a query for a red dress will give us 15 red dresses). We can then create the store like this:

```python
store = motion.create_store("fashion_search", schemas={"query": QuerySchema, "retailer": RetailerSchema})
```