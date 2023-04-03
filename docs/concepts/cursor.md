# Cursors

Within Motion applications, cursors are the primary way to access data in relations.

## Using a Cursor

You should not need to define cursor objects yourself. Cursor objects will be given to you when you write your ML logic within triggers (more in the next section).

The following cursor methods are most commonly used:

<!-- - `cursor.set(relation: str, identifier: str, key_values: typing.Dict[str, typing.Any])`
- `cursor.get(relation: str, identifier: str, keys: typing.List[str])`
- `cursor.mget(relation: str, identifiers: typing.List[str], keys: typing.List[str])`
- `cursor.duplicate(relation: str, identifier: str)`
- `cursor.sql(stmt: str)`

| Method      | Arguments                          | Returns | Description  |
| :---------- | :--------------------------------- | :----------- | :----------- |
| `set`       |  <ul><li>`relation: str`</li><li>`identifier: str`</li> <li>`key_values: Dict[str, Any]`</li> </ul> |  `identifier: str`|  Sets given key-value pairs for the given identifier in the given relation. If inserting a new record, can call `set` with `identifier = ""` and Motion will create an identifier. The identifier is returned. |
| `get`       | <ul><li>`relation: str`</li><li>`identifier: str`</li> <li>`keys: List[str]`</li> <li>(Optional) `include_derived: bool = True`</li> <li>(Optional) `as_df: bool = True`</li> </ul> | |
| `mget`      | | |
| `duplicate` | | |
| `sql`       | | | -->

::: motion.cursor.Cursor
    handler: python
    options:
      members:
        - set
        - get
        - mget
        - duplicate
        - sql
      heading_level: 3
      show_root_full_path: false
      show_root_toc_entry: false
      show_root_heading: false
      show_source: false

## Relational Access Method

Sometimes you will want to join data between two relations or perform more complicated queries. This can be done with a combination of `get`s; however, you may find it easier to use the relational access method (i.e., `cursor.sql`). The statement passed to `cursor.sql` should query the relation name as is, for example:

```python
cursor.sql("SELECT prompt, llm_completion FROM Query WHERE llm_completion IS NOT NULL")
```

By default, the `sql` method returns results as a pandas dataframe. The `sql` method will additionally return values for the `identifier` key if you do not include `identifier` in your SQL statement.

You can pass the optional argument `as_df = False` to `cursor.sql` if you want the results to be returned as a list of records, not a pandas dataframe.

Under the hood, Motion uses `duckdb` to query relations with an Apache Arrow scanner.

## Less-Commonly Used Cursor Methods

The following cursor methods are less commonly used:

::: motion.cursor.Cursor
    handler: python
    options:
      members:
        - getIdsForKey
        - waitForResults
      heading_level: 3
      show_root_full_path: false
      show_root_toc_entry: false
      show_root_heading: false
      show_source: false

## Frequently Asked Questions

### Why do I have to specify keywords for cursor function arguments?

When building applications in Motion, forcing yourself to think about every relation you are adding to or modifying can prevent against silent errors (e.g., accidentally adding an image to the wrong relation). We may remove the keyword specification requirements if people complain enough.

### What if two cursors want to edit a record for the same relation and identifier?

By default, writes to relations (i.e., `cursor.set`) require a lock to execute, so there will never be concurrent writes. Although Motion operations can execute concurrently, writes are processed first-come-first-serve.

Motion executes `cursor.sql` calls _without_ locking, so we recommend that you _do not_ execute writes in calls to `cursor.sql` (i.e., `INSERT INTO`).