# aiodynamodb

Async DynamoDB client + lightweight model layer built on `aioboto3` and Pydantic v2.

[![PyPI](https://img.shields.io/pypi/v/aiodynamodb)](https://pypi.org/project/aiodynamodb/)
[![Python](https://img.shields.io/pypi/pyversions/aiodynamodb)](https://pypi.org/project/aiodynamodb/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## AI Disclaimer
Some code and content in this repository was created with the assistance of AI tools. All code is reviewed thoroughly.

## Features

- Fully async API built on `aioboto3`
- Pydantic v2 models for validation and typed data access
- Table mapping via `@table(...)` decorator with GSI/LSI support
- Persistent connection pooling via context manager
- Query pagination with typed results
- Conditional writes/deletes
- Transactional reads/writes (`transact_get` / `transact_write`)
- Batch reads/writes (`batch_get` / `batch_write`)
- Table lifecycle helpers (`create_table` / `delete_table`)

## Requirements

- Python `>=3.12`
- AWS credentials configured via environment or IAM (standard boto3 credential chain)

## Installation

```bash
pip install aiodynamodb
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
    async with DynamoDB() as db:
        await db.create_table(User)

        await db.put(User(user_id="u1", name="Alice", email="alice@example.com"))

        user = await db.get(User, hash_key="u1")
        print(user)  # User(user_id='u1', name='Alice', email='alice@example.com')


asyncio.run(main())
```

## Connection Pooling

`DynamoDB()` initialises lazily — connections open on the first operation. Use it as an async context manager to pre-warm connections and reuse them across all operations:

```python
# Recommended: pre-warm and hold connections for the lifetime of your app
async with DynamoDB() as db:
    await db.put(...)
    await db.get(...)

# Also valid: connections open lazily per-operation (higher latency on first call)
db = DynamoDB()
await db.put(...)
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

```python
from aiodynamodb import DynamoModel, table
from aiodynamodb.models import GSI, LSI


order_gsi = GSI(name="order_gsi", hash_key="order_id", range_key="total")
order_lsi = LSI(name="order_lsi", range_key="total")


@table("orders", hash_key="order_id", range_key="created_at", indexes=[order_gsi, order_lsi])
class Order(DynamoModel):
    order_id: str
    created_at: str
    total: int
```

Query a GSI:

```python
from boto3.dynamodb.conditions import Key

async for page in db.query(Order, index_name="order_gsi", key_condition_expression=Key("order_id").eq("o1")):
    print(page.items)
```

## Client API

### `put`

Insert or overwrite an item:

```python
await db.put(User(user_id="u1", name="Alice"))
```

With a condition:

```python
from boto3.dynamodb.conditions import Attr

await db.put(
    User(user_id="u1", name="Alice"),
    condition_expression=Attr("user_id").not_exists(),
)
```

### `get`

Get one item by key — returns `None` if not found:

```python
user = await db.get(User, hash_key="u1")
order = await db.get(Order, hash_key="o1", range_key="2026-01-01T00:00:00")
```

Project selected attributes:

```python
from aiodynamodb import ProjectionAttr

user = await db.get(User, hash_key="u1", projection_expression=[ProjectionAttr("name")])
```

### `delete`

```python
await db.delete(User(user_id="u1", name="Alice"))
```

With a condition:

```python
await db.delete(User(user_id="u1", name="Alice"), condition_expression=Attr("user_id").exists())
```

### `update`

Update fields by key using `UpdateAttr`:

```python
from aiodynamodb import UpdateAttr

updated = await db.update(
    User,
    hash_key="u1",
    update_expression={UpdateAttr("name").set("Bob")},
    return_values="ALL_NEW",
)
```

Supported actions: `.set(value)`, `.remove()`, `.add(value)`, `.delete(value)`. Nested paths and list indexing are supported:

```python
update_expression={UpdateAttr("basket.items[1].qty").set(9)}
```

### `query`

Async generator yielding paginated `QueryResult[T]`:

```python
from boto3.dynamodb.conditions import Key

async for page in db.query(Order, key_condition_expression=Key("order_id").eq("o1"), limit=25):
    for item in page.items:
        print(item)
    if page.last_evaluated_key is None:
        break
```

Key arguments:

| Argument | Description |
|---|---|
| `index_name` | Query a GSI or LSI |
| `key_condition_expression` | Key filter (required) |
| `filter_expression` | Post-key attribute filter |
| `exclusive_start_key` | Pagination cursor |
| `consistent_read` | Strongly consistent reads |
| `projection_expression` | Project specific attributes |

### `transact_write`

Atomically execute up to 100 write operations:

```python
from aiodynamodb import TransactConditionCheck, TransactDelete, TransactPut

await db.transact_write([
    TransactPut(User(user_id="u1", name="Alice")),
    TransactConditionCheck(User, hash_key="u2", condition_expression=Attr("user_id").exists()),
    TransactDelete(User, hash_key="u3"),
])
```

### `transact_get`

Atomically read up to 100 items, returned in request order:

```python
from aiodynamodb import TransactGet

items = await db.transact_get([
    TransactGet(User, hash_key="u1"),
    TransactGet(User, hash_key="u2"),
])
```

### `batch_write`

Write or delete multiple items (up to 25 per call):

```python
from aiodynamodb import BatchDelete, BatchPut

result = await db.batch_write([
    BatchPut(User(user_id="u1", name="Alice")),
    BatchDelete(User, hash_key="u2"),
])
print(result.unprocessed_items)
```

### `batch_get`

Read multiple items (up to 100 keys per call):

```python
from aiodynamodb import BatchGet

result = await db.batch_get([BatchGet(User, hash_key="u1"), BatchGet(User, hash_key="u2")])
print(result.items[User])
```

### `create_table` / `delete_table`

Create or delete a table using model metadata. GSI/LSI definitions are included automatically:

```python
await db.create_table(User)
await db.delete_table(User)
```

`create_table` also accepts `billing_mode`, `provisioned_throughput`, `tags`, and `table_class`.

### Exceptions

Access typed DynamoDB exception classes:

```python
ex = await db.exceptions()

try:
    await db.put(User(user_id="u1", name="Alice"), condition_expression=Attr("user_id").exists())
except ex.ConditionalCheckFailedException:
    print("item already exists")
```

## Running Tests

```bash
make test
```

Uses `pytest`, `pytest-asyncio`, and `aiomoto` for mocked AWS tests.