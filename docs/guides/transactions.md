# Transactions

DynamoDB transactions allow you to perform multiple reads or writes atomically — all succeed or all fail together.

## transact_write

Execute up to **100 write operations** atomically across one or more tables.

```python
from boto3.dynamodb.conditions import Attr
from aiodynamodb import TransactConditionCheck, TransactDelete, TransactPut

await db.transact_write(
    [
        TransactPut(User(user_id="u1", name="Alice")),
        TransactConditionCheck(
            User,
            hash_key="u2",
            condition_expression=Attr("user_id").exists(),
        ),
        TransactDelete(User, hash_key="u3"),
    ],
    client_request_token="req-123",
)
```

### Supported write operations

#### `TransactPut`

Puts (insert or replace) an item. Optionally guarded by a condition.

```python
from aiodynamodb import TransactPut

TransactPut(item=User(user_id="u1", name="Alice"))

# With condition
TransactPut(
    item=User(user_id="u1", name="Alice"),
    condition_expression=Attr("user_id").not_exists(),
)
```

#### `TransactDelete`

Deletes an item by key.

```python
from aiodynamodb import TransactDelete

TransactDelete(User, hash_key="u1")

# With range key
TransactDelete(Order, hash_key="o1", range_key="2026-01-01T00:00:00")

# With condition
TransactDelete(User, hash_key="u1", condition_expression=Attr("status").eq("inactive"))
```

#### `TransactConditionCheck`

Asserts a condition on an item without modifying it. Useful to enforce invariants within a transaction.

```python
from aiodynamodb import TransactConditionCheck

TransactConditionCheck(
    User,
    hash_key="u1",
    condition_expression=Attr("balance").gte(100),
)
```

#### `TransactUpdate`

Updates an item within a transaction.

```python
from aiodynamodb import TransactUpdate, UpdateAttr

TransactUpdate(
    User,
    hash_key="u1",
    update_expression={UpdateAttr("login_count").add(1)},
)
```

### `transact_write` parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `operations` | `list[...]` | — | List of `TransactPut`, `TransactDelete`, `TransactConditionCheck`, or `TransactUpdate` |
| `client_request_token` | `str | None` | `None` | Idempotency token (deduplicated within 10 minutes) |
| `return_consumed_capacity` | `bool` | `False` | Include consumed capacity |
| `return_item_collection_metrics` | `bool` | `False` | Include item collection metrics |

## transact_get

Read up to **100 items atomically** across one or more tables. Results come back in request order.

```python
from aiodynamodb import ProjectionAttr, TransactGet

items = await db.transact_get([
    TransactGet(User, hash_key="u1"),
    TransactGet(User, hash_key="u2", projection_expression=[ProjectionAttr("user_id")]),
    TransactGet(Order, hash_key="o1", range_key="2026-01-01T00:00:00"),
])

user_1 = items[0]  # User | None
user_2 = items[1]  # User | None
order = items[2]  # Order | None
```

`transact_get_items` is always **strongly consistent** — there is no per-item consistency setting in the DynamoDB API.

### `TransactGet` fields

| Field | Type | Description |
|---|---|---|
| `model` | `type[T]` | `DynamoModel` subclass |
| `hash_key` | `KeyT` | Partition key value |
| `range_key` | `KeyT | None` | Sort key value (optional) |
| `projection_expression` | `list[ProjectionAttr] | None` | Fields to project |

### `transact_get` parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `requests` | `list[TransactGet]` | — | Items to read |
| `return_consumed_capacity` | `bool` | `False` | Include consumed capacity |
