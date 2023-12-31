# Test

## Response Injection
Turu supports `MockConnection` for all of the database adapters.

`MockConnection` has an `inject_response` method that allows you to write automated tests by injecting the return value corresponding to the Row type specified in the `execute_map` / `executemany_map`.

```python
--8<-- "README.md:inject_response"
```

## Recording & Testing

Using `record_as_csv`, the actual rows can be recorded to a csv file.

For production use, recording can be controlled with the `enable` option (default is `True`).

```python
--8<-- "README.md:recording"
```

The recorded csv is available in `inject_response_from_csv` method.

```python
--8<-- "README.md:testing"
```
