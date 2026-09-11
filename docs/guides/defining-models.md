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
| `IntEnum` subclasses | Number (N) |
| `StrEnum` subclasses | String (S) |
| `Literal[...]` | Type inferred from its values; all must map to the same supported DynamoDB key type |
| `Timestamp` | Number (N) — Unix seconds |
| `TimestampMillis` | Number (N) — Unix milliseconds |
| `TimestampMicros` | Number (N) — Unix microseconds |
| `TimestampNanos` | Number (N) — Unix nanoseconds |

See [Custom Types](../guides/custom-types.md) for timestamp and JSON field details.

Literal keys work with both decorator arguments and key annotations, for example
`pk: HashKey[Literal["user"]]` and `sk: RangeKey[Literal[1, 2]]` (import
`Literal` from `typing`). They also work as secondary index keys. Mixed DynamoDB
types such as `Literal["user", 1]`, and unsupported values such as booleans or
`None`, are rejected during table creation. Pydantic retains the literal constraints
when validating model instances.

Enum key fields should inherit from `enum.IntEnum` or `enum.StrEnum`:

```python
from enum import IntEnum

from aiodynamodb import DynamoModel, HashKey, RangeKey, table


class UserType(IntEnum):
    customer = 1
    admin = 2


@table("user_versions")
class UserVersion(DynamoModel):
    user_id: HashKey[str]
    user_type: RangeKey[UserType]
    name: str


await db.put(UserVersion(user_id="u1", user_type=UserType.admin, name="Alice"))
user = await db.get(UserVersion, hash_key="u1", range_key=UserType.admin)
```

## Field types

Non-key fields can use values that resolve to DynamoDB-compatible types during
Pydantic's Python-mode serialization:

| Python type | DynamoDB type |
|---|---|
| `None` | Null (NULL) |
| `bool` | Boolean (BOOL) |
| `str` | String (S) |
| `int`, `float`, `Decimal` | Number (N) |
| `bytes` | Binary (B) |
| `datetime` | ISO-8601 String (S) |
| `Enum` | The recursively serialized enum value |
| `list`, `tuple` | List (L) |
| Non-empty homogeneous `set`, `frozenset` | String, Number, or Binary Set (SS/NS/BS) |
| `dict[str, T]`, nested Pydantic models | Map (M) |

DynamoDB map keys must be strings. Sets cannot be empty and must contain only
strings, numbers, or bytes of a single type.

Other Pydantic-compatible types need a Python-mode `PlainSerializer` that
returns one of the supported values above. A serializer configured only with
`when_used="json"` is not applied because DynamoDB models are dumped in Python
mode.

Some examples:

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

## Serialization

`DynamoModel` has two serialization paths:

- `to_dynamo()` — serializes to DynamoDB AttributeValue objects (wire format, used by transact/batch operations)
- `to_dynamo_compatible()` — recursively normalizes values to DynamoDB-compatible Python types (used by table-level resource operations)
- `from_dynamo(raw)` — deserializes from AttributeValue objects back to a model instance

These are called internally by the client — you rarely need to invoke them directly.
