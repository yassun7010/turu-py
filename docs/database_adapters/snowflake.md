# :snowflake: Snowflake

!!! tip
    `turu-snowflake` depends on [snowflake-connector-python](https://pypi.org/project/snowflake-connector-python/).

## Installation

```bash
pip install "turu[snowflake]"
```

## Usage
### Basic Usage

```python
--8<-- "docs/data/turu_snowflake_sample.py"
```

### Parameters Usage
#### [format style](https://peps.python.org/pep-0249/#paramstyle)

```python
--8<-- "docs/data/turu_snowflake_sample_format_params.py"
```

!!! warning
    The variables placeholder must always be a `%s`, even if a different placeholder (such as a `%d` for integers or `%f` for floats) may look more appropriate for the type.

<!-- #### [qmark style](https://peps.python.org/pep-0249/#paramstyle)

```python
--8<-- "docs/data/turu_snowflake_sample_qmark_params.py"
```

#### [numeric style](https://peps.python.org/pep-0249/#paramstyle)

```python
--8<-- "docs/data/turu_snowflake_sample_numeric_params.py"
```

!!! warning
    `qmark` and `numeric` styles have some points to note. Please refer to [the official document](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-example#qmark-or-numeric-binding) for details. -->

### Keyword Parameters Usage
#### [pyformat style](https://peps.python.org/pep-0249/#paramstyle)

```python
--8<-- "docs/data/turu_snowflake_sample_pyformat_params.py"
```

### Use methods

`turu.snowflake.Cursor` supports `use_*` methods to set options.

- use_warehouse
- use_database
- use_schema
- use_role


```python
--8<-- "docs/data/turu_snowflake_sample_use_cursor.py"
```

!!! tip
    `use_*` methods are not supported in `turu.snowflake.Connection`.
    Settings for connection creation should be specified in the constructor.

!!! tip
    `AsyncCursor.use_*` methods are not async.
    This is intentionally done in sync because of the short processing time of those methods and the deteriorating readability caused by method chaining.
