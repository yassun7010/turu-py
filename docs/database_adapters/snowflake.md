# :snowflake: Snowflake

## use options

`turu.snowflake.Cursor` supports `use_*` methods to set options.

- use_warehouse
- use_database
- use_schema
- use_role


```python
import turu.snowflake

connection = turu.snowflake.connect(
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    database=os.environ["SNOWFLAKE_DATABASE"],
    schema=os.environ["SNOWFLAKE_SCHEMA"],
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    role=os.environ["SNOWFLAKE_ROLE"],
)

with connection.cursor().use_warehouse("YOUR_WAREHOUSE") as cursor:
    cursor.execute("SELECT CURRENT_WAREHOUSE()")
    print(cursor.fetchone()[0])  # WAREHOUSE_NAME
```
