# Testing

## Response Injection
Turu supports `MockConnection` for all of the database adapters.

`MockConnection` has an `inject_response` method that allows you to write automated tests by injecting the return value corresponding to the Row type specified in the `Cursor.execute_map` / `Cursor.executemany_map`.

```python
--8<-- "README.md:inject_response"
```

!!! tip
    The `MockConnection.chain` method is used to make method chains more readable.
    It improves code readability when using black formatter.

For queries that do not require a return value, such as INSERT,
`MockConnection.inject_response` can be injected as `None`.

```python
# The production code
def do_something(connection: turu.sqlite3.Connection):
    with connection.execute("select 1, 'a'") as cursor:
        ... # Your logic

# The test code
def test_do_something():
    connection = turu.sqlite3.MockConnection()
    connection.inject_response(None)
    do_something(connection)
```

## Recording & Testing

In the production code, the actual rows can be recorded to a csv file using the `record_as_csv` method.

Recording on/off can be controlled with the `enable` option (default is `True`).

```python
--8<-- "README.md:recording"
```

In the test code, the recorded csv is available using the `MockConnection.inject_response_from_csv` method.

```python
--8<-- "README.md:testing"
```
