# Custom Types

`aiodynamodb` ships several Pydantic type annotations in `aiodynamodb.custom_types` that control how values are serialized to and from DynamoDB.

## Timestamp types

All timestamp types annotate a `datetime` field. They differ only in the integer precision stored in DynamoDB as a Number (N).

| Type | DynamoDB storage | Exact conversion |
|---|---|---|
| `Timestamp` | Unix seconds (int) | Truncate microseconds since epoch ÷ 1,000,000 |
| `TimestampMillis` | Unix milliseconds (int) | Truncate microseconds since epoch ÷ 1,000 |
| `TimestampMicros` | Unix microseconds (int) | Microseconds since epoch |
| `TimestampNanos` | Unix nanoseconds (int) | Microseconds since epoch × 1,000 |

The serializers use integer arithmetic internally, avoiding the rounding loss
that occurs when a floating-point Unix timestamp is scaled to nanoseconds.
Timezone-aware datetimes are recommended. As with `datetime.timestamp()`, a
naive datetime is interpreted in the system's local timezone.

### Usage

```python
from datetime import datetime
from aiodynamodb import DynamoModel, table
from aiodynamodb.custom_types import Timestamp, TimestampMillis


@table("events", hash_key="event_id", range_key="occurred_at")
class Event(DynamoModel):
    event_id: str
    occurred_at: Timestamp  # stored as integer seconds in DynamoDB
    processed_at: TimestampMillis | None = None


event = Event(
    event_id="e1",
    occurred_at=datetime(2026, 1, 1, 12, 0, 0),  # stored as 1767254400
    processed_at=None,
)
await db.put(event)
```

### As key types

Timestamp types work as `hash_key` or `range_key` fields:

```python
@table("events", hash_key="event_id", range_key="occurred_at")
class Event(DynamoModel):
    event_id: str
    occurred_at: Timestamp
```

When you call `db.get()` or `db.update()` with a `datetime` as the key value, the client serializes it to the correct integer automatically.

## JSONStr

`JSONStr[T]` stores a Pydantic model as a **JSON string** in DynamoDB (a String/S attribute), and deserializes it back transparently on read.

```python
from pydantic import BaseModel
from aiodynamodb.custom_types import JSONStr


class Address(BaseModel):
    street: str
    city: str


@table("profiles", hash_key="profile_id")
class Profile(DynamoModel):
    profile_id: str
    address: JSONStr[Address]  # stored as '{"street":"...","city":"..."}'
```

Use this when you want to store nested structured data as a string (for example, to fit within a field that has a string constraint elsewhere, or to avoid DynamoDB Map overhead for deeply nested structures).

## KeyT

`KeyT` is the union of all types accepted as `hash_key` and `range_key` values in client method calls:

```python
type KeyT = int | str | Timestamp | TimestampMillis | TimestampMicros | TimestampNanos | datetime | IntEnum | StrEnum
```

You don't use `KeyT` in model field annotations — it's the type used by `db.get()`, `db.update()`, `db.delete()`, etc. for their key parameters.

## Enum keys

`IntEnum` and `StrEnum` subclasses can be used as `HashKey` or `RangeKey` field types. `IntEnum` values are stored as DynamoDB Number (N) attributes, and `StrEnum` values are stored as String (S) attributes.

```python
from enum import StrEnum

from aiodynamodb import DynamoModel, HashKey, table


class AccountStatus(StrEnum):
    active = "active"
    disabled = "disabled"


@table("accounts")
class Account(DynamoModel):
    status: HashKey[AccountStatus]
    name: str


await db.put(Account(status=AccountStatus.active, name="Alice"))
account = await db.get(Account, hash_key=AccountStatus.active)
```

Plain `Enum` subclasses are also supported in non-key fields, including nested
models and condition/filter expressions. Their values are recursively converted
to DynamoDB-compatible values. Primary and secondary key enums must still use
`IntEnum` or `StrEnum`, because DynamoDB key attributes can only be String,
Number, or Binary values.

## ReturnValues

`ReturnValues` is a literal type for the `return_values` parameter of `db.update()`:

```python
type ReturnValues = Literal["NONE", "ALL_OLD", "UPDATED_OLD", "ALL_NEW", "UPDATED_NEW"]
```

## float and Decimal

DynamoDB requires `Decimal` for numeric values, not Python `float`. `aiodynamodb` handles this automatically:

- At `@table()` decoration time, the decorator inspects the model's fields and sets `_has_float_fields = True` if any field (recursively, including nested models) has a `float` annotation.
- When serializing with `to_dynamo_compatible()`, if `_has_float_fields` is `True`, all `float` values are recursively cast to `Decimal(str(value))` before being handed to boto3.
- If no float fields are present, this traversal is skipped entirely for performance.

This means you can use `float` naturally in your models without thinking about `Decimal`:

```python
@table("products", hash_key="product_id")
class Product(DynamoModel):
    product_id: str
    price: float  # float → Decimal handled automatically on write
    weight: float
```
