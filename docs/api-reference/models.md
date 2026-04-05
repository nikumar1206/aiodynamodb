# Models

**Module:** `aiodynamodb.models`
**Import:** `from aiodynamodb import DynamoModel, HashKey, RangeKey, table`

## `DynamoModel`

Base class for all table-mapped models. Inherits from Pydantic `BaseModel`.

```python
class DynamoModel(BaseModel):
    Meta: ClassVar[TableMeta]  # set by @table()
    _has_float_fields: ClassVar[bool]  # set by @table(), used for float→Decimal optimization
```

### Methods

#### `to_dynamo() -> dict[str, Any]`

Serialize all fields to DynamoDB AttributeValue wire format (used by transact/batch operations).

#### `to_dynamo_compatible() -> dict[str, Any]`

Serialize all fields to Python dict with `float` → `Decimal` coercion (used by table resource operations). Skips the coercion traversal if `_has_float_fields` is `False`.

#### `from_dynamo(raw: dict) -> Self` (classmethod)

Deserialize from DynamoDB AttributeValue format back to a model instance.

---

## `@table()`

```python
def table(
    name: str,
    *,
    indexes: list[GSI | LSI] | None = None,
    # Legacy (still supported for backward compatibility):
    hash_key: str | None = None,
    range_key: str | None = None,
) -> Callable
```

Decorator that attaches DynamoDB table metadata to a `DynamoModel` subclass. Sets `cls.Meta` and computes `cls._has_float_fields`.

The recommended way to declare keys is with `HashKey[T]` and `RangeKey[T]` field annotations:

```python
@table("users")
class User(DynamoModel):
    user_id: HashKey[str]
    name: str
```

The legacy `hash_key="field"` / `range_key="field"` keyword arguments still work for backward compatibility.

Index names must be unique within each index type.

---

## `TableMeta`

```python
@dataclass
class TableMeta:
    table_name: str
    hash_key: str
    range_key: str | None = None
    global_secondary_indexes: dict[str, GSI] = field(default_factory=dict)
    local_secondary_indexes: dict[str, LSI] = field(default_factory=dict)
```

Attached as `Model.Meta` by `@table()`. Accessed internally by the client for all operations.

---

## `GSI`

```python
@dataclass
class GSI:
    name: str
    hash_key: str
    range_key: str | None = None
    projection: ProjectionTypeType = "ALL"
    non_key_attributes: list[str] | None = None
    provisioned_throughput: ProvisionedThroughputTypeDef | None = None
    on_demand_throughput: OnDemandThroughputTypeDef | None = None
    warm_throughput: WarmThroughputTypeDef | None = None
```

Global Secondary Index definition. Pass instances to `indexes` in `@table()`.

---

## `LSI`

```python
@dataclass
class LSI:
    name: str
    range_key: str
    projection: ProjectionTypeType = "ALL"
    non_key_attributes: list[str] | None = None
```

Local Secondary Index definition. LSIs share the table's partition key.

---

## `QueryResult[T]`

```python
@dataclass
class QueryResult[T: DynamoModel]:
    items: list[T]
    last_evaluated_key: dict[str, Any] | None
```

One page yielded by `db.query()`. `last_evaluated_key` is `None` on the final page.

---

## Transaction operation types

### `TransactGet[T]`

```python
@dataclass(frozen=True)
class TransactGet[T: DynamoModel]:
    model: type[T]
    hash_key: KeyT
    range_key: KeyT | None = None
    projection_expression: list[ProjectionAttr] | None = None
```

### `TransactPut[T]`

```python
@dataclass(frozen=True)
class TransactPut[T: DynamoModel]:
    item: T
    condition_expression: ConditionBase | None = None

    @property
    def model(self) -> type[T]: ...
```

### `TransactDelete[T]`

```python
@dataclass(frozen=True)
class TransactDelete[T: DynamoModel]:
    model: type[T]
    hash_key: KeyT
    range_key: KeyT | None = None
    condition_expression: ConditionBase | None = None
```

### `TransactConditionCheck[T]`

```python
@dataclass(frozen=True)
class TransactConditionCheck[T: DynamoModel]:
    model: type[T]
    hash_key: KeyT
    condition_expression: ConditionBase  # required (no default)
    range_key: KeyT | None = None
```

### `TransactUpdate[T]`

```python
@dataclass(frozen=True)
class TransactUpdate[T: DynamoModel]:
    model: type[T]
    hash_key: KeyT
    update_expression: set[UpdateAttr]
    range_key: KeyT | None = None
    condition_expression: ConditionBase | None = None
```

---

## Batch operation types

### `BatchGet[T]`

```python
@dataclass(frozen=True)
class BatchGet[T: DynamoModel]:
    model: type[T]
    hash_key: KeyT
    range_key: KeyT | None = None
    consistent_read: bool = False
    projection_expression: list[ProjectionAttr] | None = None
```

### `BatchPut[T]`

```python
@dataclass(frozen=True)
class BatchPut[T: DynamoModel]:
    item: T

    @property
    def model(self) -> type[T]: ...
```

### `BatchDelete[T]`

```python
@dataclass(frozen=True)
class BatchDelete[T: DynamoModel]:
    model: type[T]
    hash_key: KeyT
    range_key: KeyT | None = None
```

---

## Result types

### `BatchGetResult[T]`

```python
@dataclass
class BatchGetResult[T: DynamoModel]:
    items: dict[type[T], list[T]]
    unprocessed_keys: dict[str, Any]
```

Results grouped by model type. Check `unprocessed_keys` and retry if non-empty.

### `BatchWriteResult`

```python
@dataclass
class BatchWriteResult:
    unprocessed_items: dict[str, list[WriteRequestOutputTypeDef]]
```

Check `unprocessed_items` and retry if non-empty.
