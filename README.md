# motion

A framework for building ML pipelines, using a trigger-based execution model.

## Sample Application

We'll be building a fashion search application, which will allow users to search for what clothes to buy based on a specific query. For our prototype, we'll use the following data sources (i.e., vendors):

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

Stores are created using a name and schema. The schema is a dataclass, where fields are annotated with types.