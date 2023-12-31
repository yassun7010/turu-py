# BigQuery

!!! tip
    `turu-bigquery` depends on [google-cloud-bigquery](https://pypi.org/project/google-cloud-bigquery/).

!!! todo
    `turu-bigquery` does not support `async` yet.
    [bigquery](https://pypi.org/project/google-cloud-bigquery/) does not officially support async.

## Installation

```bash
pip install "turu[bigquery]"
```

## Usage
### Basic Usage

```python
--8<-- "docs/data/turu_bigquery_sample.py"
```

### Parameters Usage
#### [format style](https://peps.python.org/pep-0249/#paramstyle)

```python
--8<-- "docs/data/turu_bigquery_sample_format_params.py"
```

!!! warning
    [Official documentation](https://cloud.google.com/bigquery/docs/parameterized-queries#bigquery-query-params-python) says that "BigQuery supports qmark parameters", but it is not.

### Keyword Parameters Usage
#### [named style](https://peps.python.org/pep-0249/#paramstyle)

```python
--8<-- "docs/data/turu_bigquery_sample_pyformat_params.py"
```

!!! info
    [bigquery](https://pypi.org/project/google-cloud-bigquery/) supports "Providing explicit type information". Please see more information in [official documentation](https://cloud.google.com/python/docs/reference/bigquery/2.19.0/dbapi#providing-explicit-type-information).
