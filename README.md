# Turu: Simple Database Client for Typed Python

<!-- --8<-- [start:badges] -->
[![docs](https://github.com/yassun7010/turu-py/actions/workflows/publish-mkdocs.yml/badge.svg)](https://yassun7010.github.io/turu-py/)
[![test](https://github.com/yassun7010/turu-py/actions/workflows/test-suite.yml/badge.svg)](https://github.com/yassun7010/turu-py/actions)
[![pypi package](https://badge.fury.io/py/turu.svg)](https://pypi.org/project/turu)
<!-- --8<-- [end:badges] -->

<p align="center">
    <img alt="logo" src="https://raw.githubusercontent.com/yassun7010/turu-py/main/docs/images/logo.svg" width="300" />
</p>

---

**Documentation**: <a href="https://yassun7010.github.io/turu-py/" target="_blank">https://yassun7010.github.io/turu-py/</a>

**Source Code**: <a href="https://github.com/yassun7010/turu-py" target="_blank">https://github.com/yassun7010/turu-py</a>

---

## Installation

```bash
pip install "turu[snowflake]"
```

<!-- --8<-- [start:why_turu] -->
## Why Turu?
SQL is a powerful language, but it has many dialects, and Cloud Native Databases are especially difficult to test automatically in a local environment.

Turu was developed as a simple tool to assist local development.
It provides a simple interface according to [PEP 249 – Python Database API Specification v2.0](https://peps.python.org/pep-0249/) and allows for easy recording of query results and injection mock data.
<!-- --8<-- [end:why_turu] -->

<!-- --8<-- [start:features] -->
## Features

- :rocket: **Simple** - Turu is a simple database api wrapper of [PEP 249](https://peps.python.org/pep-0249/).
- :bulb: **Type Hint**  - Full support for type hints.
- :zap: **Async/Await** - Async/Await supports.
- :test_tube: **Recoed and Mock** - Record and mock database queries for testing.
<!-- --8<-- [end:features] -->

<!-- --8<-- [start:adapters] -->
## Supprted Database

| Database   | Sync Support | Async Support | Installation                    |
| ---------- | ------------ | ------------- | ------------------------------- |
| SQLite3    | Yes          | Yes           | `pip install "turu[sqlite3]"`   |
| MySQL      | Yes          | Yes           | `pip install "turu[mysql]"`     |
| PostgreSQL | Yes          | Yes           | `pip install "turu[postgres]"`  |
| Snowflake  | Yes          | Yes           | `pip install "turu[snowflake]"` |
| BigQuery   | Yes          | No            | `pip install "turu[bigquery]"`  |
<!-- --8<-- [end:adapters] -->

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

## Testing

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

## Recording and Testing

Your Production Code

```python
import os

import turu.sqlite3
from turu.core.record import record_to_csv

from your_package.data import RECORD_DIR
from your_package.schema import Row


def do_something(connection: turu.sqlite3.Connection):
    with record_to_csv(
        RECORD_DIR / "test.csv",
        connection.execute_map(Row, "select 1, 'a'"),
        enable=os.environ.get("ENABLE_RECORDING"),
        limit=100,
    ) as cursor:
        ... # Your logic
```

Your Test Code

```python
import turu.sqlite3

from your_package.data import RECORD_DIR
from your_package.schema import Row


def test_do_something(connection: turu.sqlite3.MockConnection):
    connection.inject_response_from_csv(Row, RECORD_DIR / "test.csv")

    assert do_something(connection) is None
```
