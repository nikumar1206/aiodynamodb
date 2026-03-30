# Custom Types

**Module:** `aiodynamodb.custom_types`
**Import:** `from aiodynamodb.custom_types import Timestamp, TimestampMillis, ...`

## Timestamp types

Pydantic type annotations for `datetime` fields that control how datetimes are serialized as integers in DynamoDB.

### `Timestamp`

```python
type Timestamp = Annotated[datetime, PlainSerializer(lambda d: int(d.timestamp()))]
```

Serializes a `datetime` to Unix **seconds** (integer). Stored as DynamoDB Number (N).

### `TimestampMillis`

```python
type TimestampMillis = Annotated[datetime, PlainSerializer(lambda d: int(d.timestamp() * 1_000))]
```

Serializes a `datetime` to Unix **milliseconds** (integer). Stored as DynamoDB Number (N).

### `TimestampMicros`

```python
type TimestampMicros = Annotated[datetime, PlainSerializer(lambda d: int(d.timestamp() * 1_000_000))]
```

Serializes a `datetime` to Unix **microseconds** (integer). Stored as DynamoDB Number (N).

### `TimestampNanos`

```python
type TimestampNanos = Annotated[datetime, PlainSerializer(lambda d: int(d.timestamp() * 1_000_000_000))]
```

Serializes a `datetime` to Unix **nanoseconds** (integer). Stored as DynamoDB Number (N).

---

## `JSONStr[T]`

```python
type JSONStr[T: BaseModel] = Annotated[
    T,
    PlainSerializer(lambda v: v.model_dump_json()),
    BeforeValidator(lambda v: json.loads(v) if isinstance(v, str) else v),
]
```

Stores a Pydantic model `T` as a JSON **string** in DynamoDB (String/S attribute). On read, the JSON string is parsed back into `T` transparently.

```python
class Address(BaseModel):
    street: str
    city: str


@table("profiles", hash_key="profile_id")
class Profile(DynamoModel):
    profile_id: str
    address: JSONStr[Address]  # stored as '{"street":"...","city":"..."}'
```

---

## `KeyT`

```python
type KeyT = int | str | Timestamp | TimestampMillis | TimestampMicros | TimestampNanos | datetime
```

Union type accepted by client method key parameters (`hash_key`, `range_key` arguments to `get()`, `update()`, `delete()`, and operation dataclasses). Not intended for use in model field annotations.

---

## `ReturnValues`

```python
type ReturnValues = Literal["NONE", "ALL_OLD", "UPDATED_OLD", "ALL_NEW", "UPDATED_NEW"]
```

Valid values for the `return_values` parameter of `db.update()`.

| Value | Returns |
|---|---|
| `"NONE"` | Nothing (default DynamoDB behavior) |
| `"ALL_OLD"` | All attributes before the update |
| `"UPDATED_OLD"` | Only updated attributes before the update |
| `"ALL_NEW"` | All attributes after the update |
| `"UPDATED_NEW"` | Only updated attributes after the update |
