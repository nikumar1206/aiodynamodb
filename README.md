# aiodynamodb

Async DynamoDB client + lightweight model layer built on `aioboto3` and Pydantic v2.

## Features

- Async API for common DynamoDB operations.
- Pydantic models for validation and typed data access.
- Table mapping via `@table(...)` decorator.
- Query pagination with typed results.
- Optional conditional writes/deletes.
- Transactional reads/writes (`transact_get` / `transact_write`).
- Helpers to create/delete tables from model metadata.

## Requirements

- Python `>=3.12`
- AWS credentials/region configured in your environment (or mocked for tests).

## Installation

```bash
pip install aiodynamodb
```

For local development in this repo:

```bash
make install-dev
```

## Quickstart

```python
import asyncio
from aiodynamodb import DynamoDB, DynamoModel, table


@table("users", hash_key="user_id")
class User(DynamoModel):
    user_id: str
    name: str
    email: str | None = None


async def main() -> None:
    db = DynamoDB()

    await db.create_table(User)

    user = User(user_id="u1", name="Alice", email="alice@example.com")
    await db.put(user)

    fetched = await db.get(User, hash_key="u1")
    print(fetched)

    await db.delete_table(User)


asyncio.run(main())
```

## Defining Models

Use `DynamoModel` + `@table(...)`:

```python
from aiodynamodb import DynamoModel, table


@table("orders", hash_key="order_id", range_key="created_at")
class Order(DynamoModel):
    order_id: str
    created_at: str
    total: int
```

Decorator arguments:

- `name`: DynamoDB table name
- `hash_key`: partition key field name
- `range_key`: optional sort key field name

### Indexes (GSI and LSI)

Define indexes on the model via the `indexes` argument to `@table(...)`.

```python
from aiodynamodb import DynamoModel, table
from aiodynamodb.models import GSI, LSI


order_gsi = GSI(
    name="order_gsi",
    hash_key="order_id",
    range_key="total",
)

order_lsi = LSI(
    name="order_lsi",
    range_key="total",
)


@table(
    "orders",
    hash_key="order_id",
    range_key="created_at",
    indexes=[order_gsi, order_lsi],
)
class Order(DynamoModel):
    order_id: str
    created_at: str
    total: int
```

Query a GSI:

```python
from boto3.dynamodb.conditions import Key

async for page in db.query(
    Order,
    index_name="order_gsi",
    key_condition_expression=Key("order_id").eq("o1"),
):
    print(page.items)
```

Query an LSI:

```python
from boto3.dynamodb.conditions import Attr, Key

async for page in db.query(
    Order,
    index_name="order_lsi",
    key_condition_expression=Key("order_id").eq("o1"),
    filter_expression=Attr("total").gte(200),
):
    print(page.items)
```

## Client API

Instantiate once and reuse:

```python
from aiodynamodb import DynamoDB

db = DynamoDB()
```

### `put`

Insert or overwrite an item:

```python
await db.put(User(user_id="u1", name="Alice"))
```

With conditional expression:

```python
from boto3.dynamodb.conditions import Attr

await db.put(
    User(user_id="u1", name="Alice"),
    condition_expression=Attr("user_id").not_exists(),
)
```

### `get`

Get one item by key:

```python
user = await db.get(User, hash_key="u1")
```

Composite key lookup:

```python
order = await db.get(Order, hash_key="o1", range_key="2026-01-01T00:00:00")
```

Returns `None` if not found.

### `delete`

Delete an item:

```python
await db.delete(User(user_id="u1", name="Alice"))
```

With condition:

```python
from boto3.dynamodb.conditions import Attr

await db.delete(
    User(user_id="u1", name="Alice"),
    condition_expression=Attr("user_id").exists(),
)
```

### `query`

`query` is an async generator yielding paginated `QueryResult[T]` values.

```python
from boto3.dynamodb.conditions import Key

async for page in db.query(
    Order,
    key_condition_expression=Key("order_id").eq("o1"),
    limit=25,
    scan_index_forward=False,
):
    for item in page.items:
        print(item)
    if page.last_evaluated_key is None:
        break
```

Important arguments:

- `index_name`: query a specific index
- `key_condition_expression`: required for most query patterns
- `filter_expression`: post-key filtering
- `exclusive_start_key`: continue from a previous page
- `consistent_read`: strongly consistent reads (where supported)

### `create_table` / `delete_table`

Create a table from model metadata:

```python
await db.create_table(User)
```

Delete table:

```python
await db.delete_table(User)
```

`create_table` also accepts optional DynamoDB settings such as:

- `billing_mode`
- `provisioned_throughput`
- `tags`
- `table_class`

Global and local secondary indexes are taken from model metadata
(`@table(..., indexes=[...])`).

### `transact_write`

Atomically execute up to 100 write operations across one or more tables:

```python
from boto3.dynamodb.conditions import Attr
from aiodynamodb import TransactConditionCheck, TransactDelete, TransactPut

await db.transact_write(
    [
        TransactPut(User(user_id="u1", name="Alice")),
        TransactConditionCheck(User, hash_key="u1", condition_expression=Attr("user_id").exists()),
        TransactDelete(User, hash_key="u2"),
    ],
    client_request_token="req-123",
)
```

### `transact_get`

Atomically read up to 100 items and get typed results back in request order:

```python
from aiodynamodb import TransactGet

items = await db.transact_get(
    [
        TransactGet(User, hash_key="u1"),
        TransactGet(User, hash_key="u2"),
    ]
)
```

### `exceptions`

Access boto3 DynamoDB exception classes:

```python
ex = await db.exceptions
```

Example:

```python
import pytest
from boto3.dynamodb.conditions import Attr

ex = await db.exceptions
with pytest.raises(ex.ConditionalCheckFailedException):
    await db.put(
        User(user_id="u1", name="Alice"),
        condition_expression=Attr("user_id").exists(),
    )
```

## Running Tests

```bash
make test
```

This project uses `pytest`, `pytest-asyncio`, and `aiomoto` for mocked AWS tests.

## Project Status

Current API is intentionally small and focused on:

- model mapping
- basic CRUD
- query pagination
- table lifecycle helpers

If you need additional DynamoDB operations, extend the client with the same typed model pattern.
