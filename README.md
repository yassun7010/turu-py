# Turu: Simple Database API for Typed Python

<!-- --8<-- [start:badges] -->
[![docs](https://github.com/yassun7010/turu-py/actions/workflows/publish-mkdocs.yml/badge.svg)](https://yassun7010.github.io/turu-py/)
[![test](https://github.com/yassun7010/turu-py/actions/workflows/test-suite.yml/badge.svg)](https://github.com/yassun7010/turu-py/actions)
[![pypi package](https://badge.fury.io/py/turu.svg)](https://pypi.org/project/turu)
<!-- --8<-- [end:badges] -->

<p align="center">
    <img alt="logo" src="./docs/images/logo.svg" width="300" />
</p>

---

**Documentation**: <a href="https://yassun7010.github.io/turu-py/" target="_blank">https://yassun7010.github.io/turu-py/</a>

**Source Code**: <a href="https://github.com/yassun7010/turu-py" target="_blank">https://github.com/yassun7010/turu-py</a>

---

## Installation

```bash
pip install turu[snowflake]
```

<!-- --8<-- [start:why_turu] -->
## Why Turu?
SQL is a powerful language, but it has many dialects and is especially difficult to automatically test cloud-based SQL databases in a local environment.

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

## Supprted Database

| Database   | Sync Support | Async Support | Installation                  |
| ---------- | ------------ | ------------- | ----------------------------- |
| SQLite3    | Yes          | Yes           | `pip install turu[sqlite3]`   |
| MySQL      | No           | No            |  -                            |
| PostgreSQL | No           | No            |  -                            |
| Snowflake  | Yes          | Yes           | `pip install turu[snowflake]` |
| BigQuery   | Yes          | No            | `pip install turu[bigquery]`  |

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
