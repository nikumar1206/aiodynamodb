# Batch Operations

Batch operations let you read or write multiple items in a single request, which is more efficient than individual `get`/`put`/`delete` calls.

## batch_write

Write up to **25 operations** per call using DynamoDB `batch_write_item`. Supports puts and deletes.

```python
from aiodynamodb import BatchDelete, BatchPut

result = await db.batch_write([
    BatchPut(User(user_id="u1", name="Alice")),
    BatchPut(User(user_id="u2", name="Bob")),
    BatchDelete(User, hash_key="u3"),
])
print(result.unprocessed_items)  # {} if all succeeded
```

### Unprocessed items

DynamoDB may return some items as unprocessed due to throttling. Check `result.unprocessed_items` and retry:

```python
result = await db.batch_write([...])
if result.unprocessed_items:
    # retry unprocessed_items — handle as needed
    print("some items were not processed:", result.unprocessed_items)
```

### `BatchPut`

```python
BatchPut(item=User(user_id="u1", name="Alice"))
```

| Field | Type | Description |
|---|---|---|
| `item` | `DynamoModel` | Model instance to write |

### `BatchDelete`

```python
BatchDelete(User, hash_key="u1")
BatchDelete(Order, hash_key="o1", range_key="2026-01-01T00:00:00")
```

| Field | Type | Description |
|---|---|---|
| `model` | `type[T]` | Model class |
| `hash_key` | `KeyT` | Partition key value |
| `range_key` | `KeyT | None` | Sort key value (optional) |

### `batch_write` parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `operations` | `list[BatchPut | BatchDelete]` | — | Operations to execute |
| `return_consumed_capacity` | `bool` | `False` | Include consumed capacity |
| `return_item_collection_metrics` | `bool` | `False` | Include item collection metrics |

## batch_get

Read up to **100 items** per call using DynamoDB `batch_get_item`. Results are grouped by model type.

```python
from aiodynamodb import BatchGet, ProjectionAttr

result = await db.batch_get([
    BatchGet(User, hash_key="u1"),
    BatchGet(User, hash_key="u2", projection_expression=[ProjectionAttr("user_id")]),
    BatchGet(Order, hash_key="o1", range_key="2026-01-01T00:00:00"),
])

users = result.items[User]  # list[User]
orders = result.items[Order]  # list[Order]
print(result.unprocessed_keys)  # {} if all succeeded
```

Items are returned in an **unordered** dict grouped by model class — the order within each group matches the order DynamoDB returns them, which may differ from the request order.

### `BatchGet`

| Field | Type | Default | Description |
|---|---|---|---|
| `model` | `type[T]` | — | Model class |
| `hash_key` | `KeyT` | — | Partition key value |
| `range_key` | `KeyT | None` | `None` | Sort key value (optional) |
| `consistent_read` | `bool` | `False` | Strongly consistent read |
| `projection_expression` | `list[ProjectionAttr] | None` | `None` | Fields to project |

> **Note:** `consistent_read` must be the same for all requests targeting the same table. Mixed values raise `ValueError`.

### `batch_get` parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `requests` | `list[BatchGet]` | — | Items to read |
| `return_consumed_capacity` | `bool` | `False` | Include consumed capacity |

### Unprocessed keys

Similar to `batch_write`, DynamoDB may not process all keys due to throttling:

```python
result = await db.batch_get([...])
if result.unprocessed_keys:
    # retry unprocessed_keys
    print("unprocessed:", result.unprocessed_keys)
```
