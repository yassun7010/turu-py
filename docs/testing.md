# Testing

## Response Injection
Turu supports `MockConnection` for all of the database adapters.

`MockConnection` has an `inject_response` method that allows you to write automated tests by injecting the return value corresponding to the Row type specified in the `Cursor.execute_map` / `Cursor.executemany_map`.

```python
--8<-- "README.md:inject_response"
```

!!! tip
    The `MockConnection.chain` method is used to make method chains more readable.
    It improves code readability when using [black](https://pypi.org/project/black/) formatter.

For queries that do not require a return value, such as INSERT,
`MockConnection.inject_response` can be injected as `None`.

```python title="production_code.py"
def do_something(connection: turu.sqlite3.Connection):
    with connection.execute("insert ...") as cursor:
        ...

    with connection.execute_map(Row, "select ...") as cursor:
        ...

```

```python title="test_code.py"
def test_do_something():
    connection = turu.sqlite3.MockConnection()

    # Indicates the use of the `execute` method call.
    connection.inject_response(None)

    # Indicates the use of the `execute_map` method call.
    connection.inject_response(Row)

    do_something(connection)
```

## Recording & Testing

In the production code, the actual rows can be recorded to a csv file using the `record_as_csv` method.

Recording on/off can be controlled with the `enable` option (default is `True`).

```python title="production_code.py"
--8<-- "README.md:recording"
```

In the test code, the recorded csv is available using the `MockConnection.inject_response_from_csv` method.

```python title="test_code.py"
--8<-- "README.md:testing"
```
