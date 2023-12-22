# Turu: Simple Database API for Typed Python

[![test](https://github.com/yassun7010/turu-py/actions/workflows/test-suite.yml/badge.svg)](https://github.com/yassun7010/turu-py/actions)
[![pypi package](https://badge.fury.io/py/turu.svg)](https://pypi.org/project/turu)


## Installation

```bash
pip install turu[snowflake]
```

## Why Turu?
SQL is a powerful language, but it has many dialects and is especially difficult to automatically test cloud-based SQL databases in a local environment.

Turu was developed as a simple tool to assist local development.
It provides a simple interface according to [PEP 249 – Python Database API Specification v2.0](https://peps.python.org/pep-0249/) and allows for easy recording of query results and injection mock data.

## Supprted Database

| Database   | Supported | Installation                  |
| ---------- | --------- | ----------------------------- |
| SQLite3    | Yes       | `pip install turu[sqlite3]`   |
| MySQL      | No        |  -                            |
| PostgreSQL | No        |  -                            |
| Snowflake  | Yes       | `pip install turu[snowflake]` |
| BigQuery   | Yes       | `pip install turu[bigquery]`  |

## Usage

### Basic Usage

```python
from pydantic import BaseModel


class Row(BaseModel):
    id: int
    name: str

connection = turu.sqlite3.connect("test.db")

with connection.execute_map(Row, "select 1, 'a'") as cursor:
    assert cursor.fetchone() == Row(id=1, name="a")
```

### Recording Usage

```python
import turu.sqlite3
from turu.core.record import record_as_csv

from pydantic import BaseModel


class Row(BaseModel):
    id: int
    name: str

connection = turu.sqlite3.connect("test.db")

with record_as_csv("test.csv", connection.execute_map(Row, "select 1, 'a'")) as cursor:
    assert cursor.fetchone() == Row(id=1, name="a")
```

### Testing Usage

```python
import turu.sqlite3

from pydantic import BaseModel


class Row(BaseModel):
    id: int
    name: str

expected1 = [Row(id=1, name="a"), Row(id=2, name="b")]
expected2 = [Row(id=3, name="c"), Row(id=4, name="d")]
expected3 = [Row(id=5, name="e"), Row(id=6, name="f")]

connection = turu.sqlite3.MockConnection()

(
    connection.chain()
    .inject_response(Row, expected1)
    .inject_response(Row, expected2)
    .inject_response(Row, expected3)
)

for expected in [expected1, expected2, expected3]:
    with connection.execute_map(Row, "select 1, 'a'") as cursor:
        assert cursor.fetchall() == expected
```
