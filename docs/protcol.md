# PEP 249 Compliant

Turu defines a simple protocol based on [PEP 249](https://peps.python.org/pep-0249/).

All Connection and Cursor class are derived from those protcols.

This tool is a simple wrapper for PEP 249 that allows you to specify and inspect the return type with the API you are familiar with.

In addition to `execute`/`executemany`, we provide `execute_map`/`executemany_map` with processing to map query results to a specified type.

```python
--8<-- "docs/data/turu_sqlite3_sample.py"
```

!!! note
    `typing.NamedDict`/`dataclasses.dataclass`/`pydantic.BaseModel` are available as types to map.

    But, for type validation, we reccomend using [pydantic](https://pydantic-docs.helpmanual.io/).


## Connection Pool

!!! todo
    Connection Pool may be implemented in the future, but has not yet been started.

    Reasons:

    - Turu is developed for use in data analysis workflows and has no incentive to pool connections.
    - Caching strategies exist at different layers, such as RDS Proxy.
    - [DbApi3](https://wiki.python.org/moin/DbApi3) is not yet in PEP.

## Connection

Connection can execute directly, but it is only reading the cursor internally.

```python
with connection.execute("select 1") as cursor:
    ...
```

Equivalent to the following.

```python
with connection.cursor().execute("select 1") as cursor:
    ...
```

It is recommended that you use this in conjunction with the `with` syntax and always close the cursor after using it.

## Cursor
Always assign a new Cursor as the return value of the `execute*` method in order to update a type hint that can be retrieved by Cursor.

```python
--8<-- "docs/data/turu_cursor_type_sample.py"
```
