# PostgreSQL Adapter

!!! tip
    `turu-postgres` depends on [psycopg](https://pypi.org/project/psycopg/).

## Installation

```bash
pip install "turu[postgres]"
```

## Usage

### Basic Usage

```python
--8<-- "docs/data/turu_postgres_sample.py"
```

### Parameters Usage
#### [format style](https://peps.python.org/pep-0249/#paramstyle)

```python
--8<-- "docs/data/turu_postgres_sample_format_params.py"
```

!!! warning
    The variables placeholder must always be a `%s`, even if a different placeholder (such as a `%d` for integers or `%f` for floats) may look more appropriate for the type.

### Keyword Parameters Usage
#### [pyformat style](https://peps.python.org/pep-0249/#paramstyle)

```python
--8<-- "docs/data/turu_postgres_sample_pyformat_params.py"
```
