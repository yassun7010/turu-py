# Testing

## Response Injection
Turu supports `MockConnection` for all of the database adapters.

`MockConnection` has an `inject_response` method that allows you to write automated tests by injecting the return value corresponding to the Row type specified in the `Cursor.execute_map` / `Cursor.executemany_map`.

```python
--8<-- "docs/data/turu_testing.py"
```

!!! tip
    The `MockConnection.chain` method is used to make method chains more readable.
    It improves code readability when using [black](https://pypi.org/project/black/) formatter.

For queries that do not require a return value, such as INSERT,
`MockConnection.inject_response` can be injected as `None`.

```python
--8<-- "docs/data/turu_testing_response.py"
```

## Operation Injection

How can I teach `MockConnection` about operations that do not have a return value, such as `INSERT`, `UPDATE`, and `DELETE`?

For this purpose, `Cursor.execute_with_tag` and `MockConnection.inject_operation_with_tag` are provided.

By injecting a tag instead of a return value, MockConnection can determine the type of operation and test whether the calls are made in the intended order.

```python
--8<-- "docs/data/turu_testing_operation_with_tag.py"

```

## Recording & Testing

In the production code, the actual rows can be recorded to a csv file using the `record_to_csv` method.

Recording on/off can be controlled with the `enable` option (default is `True`).

In the test code, the recorded csv is available using the `MockConnection.inject_response_from_csv` method.

```python
--8<-- "docs/data/turu_recording_and_testing.py"
```
