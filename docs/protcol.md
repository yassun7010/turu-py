# PEP 249 Compliant

Turu defines a simple protocol based on [PEP 249](https://peps.python.org/pep-0249/).

All Connection and Cursor class are derived from those protcols.

This tool is a simple wrapper for PEP 249 that allows you to specify and inspect the return type with the API you are familiar with.

In addition to `execute`/`executemany`, we provide `execute_map`/`executemany_map` with processing to map query results to a specified type.

```python
import pydantic
import turu.sqlite3

class Row(pydantic.BaseModel):
    id: int

conn = turu.sqlite3.connect(":memory:")

with conn.execute_map(Row, "select 1") as cursor:
    assert cursor.fetchall() == [Row(id=1)]
```

!!! note
    `typing.NamedDict`/`dataclasses.dataclass`/`pydantic.BaseModel` are available as types to map.

    But, for type validation, we reccomend using [pydantic](https://pydantic-docs.helpmanual.io/).
