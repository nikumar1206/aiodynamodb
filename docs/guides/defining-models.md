# Defining Models

## Overview

Every DynamoDB table maps to a `DynamoModel` subclass — a Pydantic `BaseModel` with table metadata attached via the `@table()` decorator. You get Pydantic validation, type hints, and serialization for free.

## `@table()` decorator

```python
from aiodynamodb import DynamoModel, table


@table("users", hash_key="user_id")
class User(DynamoModel):
    user_id: str
    name: str
    email: str | None = None
```

### Parameters

| Parameter | Type | Description |
|---|---|---|
| `name` | `str` | DynamoDB table name |
| `hash_key` | `str` | Field name used as the partition key |
| `range_key` | `str | None` | Field name used as the sort key (optional) |
| `indexes` | `list[GSI | LSI] | None` | Secondary indexes (optional) |

## Composite key tables

```python
@table("orders", hash_key="order_id", range_key="created_at")
class Order(DynamoModel):
    order_id: str
    created_at: str
    total: int
```

With a `range_key`, operations that accept a key require both `hash_key` and `range_key`.

## Supported key types

The `hash_key` and `range_key` fields can be typed as:

| Python type | DynamoDB type |
|---|---|
| `str` | String (S) |
| `int` | Number (N) |
| `float` | Number (N) |
| `bytes` | Binary (B) |
| `datetime` | String (S) — ISO format |
| `Timestamp` | Number (N) — Unix seconds |
| `TimestampMillis` | Number (N) — Unix milliseconds |
| `TimestampMicros` | Number (N) — Unix microseconds |
| `TimestampNanos` | Number (N) — Unix nanoseconds |

See [Custom Types](../guides/custom-types.md) for timestamp and JSON field details.

## Field types

Any Pydantic-compatible field type works for non-key fields. Some examples:

```python
from datetime import datetime
from aiodynamodb import DynamoModel, table
from aiodynamodb.custom_types import Timestamp, JSONStr
from pydantic import BaseModel


class Address(BaseModel):
    street: str
    city: str


@table("profiles", hash_key="profile_id")
class Profile(DynamoModel):
    profile_id: str
    created_at: Timestamp  # stored as Unix seconds integer
    address: Address  # stored as a DynamoDB Map
    tags: list[str]  # stored as a DynamoDB List
    metadata: JSONStr[Address]  # stored as a JSON string in DynamoDB
    score: float  # stored as Number; float → Decimal handled automatically
```

## What `@table` does

The decorator attaches a `Meta` class variable (`TableMeta`) to your model:

```python
User.Meta.table_name  # "users"
User.Meta.hash_key  # "user_id"
User.Meta.range_key  # None
User.Meta.global_secondary_indexes  # {}
User.Meta.local_secondary_indexes  # {}
```

It also computes `_has_float_fields` once at decoration time — a cached flag used to skip the float → Decimal conversion traversal for models that contain no float fields, improving serialization performance.

## Serialization

`DynamoModel` has two serialization paths:

- `to_dynamo()` — serializes to DynamoDB AttributeValue objects (wire format, used by transact/batch operations)
- `to_dynamo_compatible()` — serializes to Python dicts with `float` → `Decimal` coercion (used by table-level resource operations)
- `from_dynamo(raw)` — deserializes from AttributeValue objects back to a model instance

These are called internally by the client — you rarely need to invoke them directly.
