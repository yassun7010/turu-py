# Testing

## Response Injection
Turu supports `MockConnection` for all of the database adapters.

`MockConnection` has an `inject_response` method that allows you to write automated tests by injecting the return value corresponding to the Row type specified in the `execute_map` / `executemany_map`.

```python
--8<-- "README.md:inject_response"
```

## Recording & Testing

In the production code, the actual rows can be recorded to a csv file using the `record_as_csv` method.

For production use, recording can be controlled with the `enable` option (default is `True`).

```python
--8<-- "README.md:recording"
```

In the test code, the recorded csv is available using the `inject_response_from_csv` method.

```python
--8<-- "README.md:testing"
```
