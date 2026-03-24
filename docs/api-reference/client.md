# DynamoDB Client

**Module:** `aiodynamodb.client`
**Import:** `from aiodynamodb import DynamoDB`

## Class: `DynamoDB`

Async DynamoDB client for working with `DynamoModel` entities. Maps model metadata to DynamoDB table operations and returns validated model instances for reads and queries.

> **Not thread-safe.** `DynamoDB` uses `asyncio.Lock` internally for connection management. Do not share a single instance across threads or use it in synchronous contexts. Create one instance per async application context.

### Constructor

```python
DynamoDB(
    session: aioboto3.Session | None = None,
    hash_key_types: dict[Any, str] = ...,
)
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `session` | `aioboto3.Session \| None` | `None` | Optional aioboto3 session. A new one is created if omitted. |
| `hash_key_types` | `dict[Any, str]` | Built-in map | Mapping from Python type to DynamoDB attribute type (`"S"`, `"N"`, `"B"`). Override to add custom key types. |

### Context manager

```python
async with DynamoDB() as db:
    ...
```

Calling `__aenter__` opens and holds the underlying resource and client connections. `__aexit__` calls `close()`.

### `close()`

```python
await db.close()
```

Releases held connections and clears the table cache. Call this when not using the context manager pattern.

---

## Methods

### `put`

```python
async def put(
    self,
    item: DynamoModel,
    *,
    condition_expression: ConditionBase | None = None,
) -> None
```

Insert or replace an item. Raises `ConditionalCheckFailedException` if the condition fails.

| Parameter | Description |
|---|---|
| `item` | Model instance to persist |
| `condition_expression` | Optional boto3 condition expression |

---

### `get`

```python
async def get(
    self,
    model: type[T],
    *,
    hash_key: KeyT,
    range_key: KeyT | None = None,
    consistent_reads: bool = False,
    projection_expression: list[ProjectionAttr] | None = None,
) -> T | None
```

Fetch a single item by primary key. Returns `None` if not found.

| Parameter | Description |
|---|---|
| `model` | `DynamoModel` subclass |
| `hash_key` | Partition key value |
| `range_key` | Sort key value (required if table has a range key) |
| `consistent_reads` | Strongly consistent read |
| `projection_expression` | List of `ProjectionAttr` paths to project |

---

### `delete`

```python
async def delete(
    self,
    item: DynamoModel,
    *,
    condition_expression: ConditionBase | None = None,
) -> None
```

Delete an item. The key is extracted from the model instance. Raises `ConditionalCheckFailedException` if the condition fails.

---

### `update`

```python
async def update(
    self,
    model: type[T],
    *,
    hash_key: KeyT,
    update_expression: set[UpdateAttr],
    range_key: KeyT | None = None,
    condition_expression: ConditionBase | None = None,
    return_values: ReturnValues | None = None,
) -> T | None
```

Update an item by key. Returns a validated model when `return_values` causes DynamoDB to return attributes; otherwise `None`.

| Parameter | Description |
|---|---|
| `model` | `DynamoModel` subclass |
| `hash_key` | Partition key value |
| `update_expression` | Set of `UpdateAttr` actions |
| `range_key` | Sort key value (optional) |
| `condition_expression` | Optional condition |
| `return_values` | `"NONE"`, `"ALL_OLD"`, `"UPDATED_OLD"`, `"ALL_NEW"`, or `"UPDATED_NEW"` |

---

### `query`

```python
async def query(
    self,
    model: type[T],
    *,
    index_name: str | None = None,
    limit: int | None = None,
    key_condition_expression: ConditionBase | None = None,
    filter_expression: ConditionBase | None = None,
    exclusive_start_key: dict | None = None,
    return_consumed_capacity: bool = False,
    consistent_read: bool = False,
    scan_index_forward: bool = True,
    projection_expression: list[ProjectionAttr] | None = None,
) -> AsyncIterator[QueryResult[T]]
```

Async generator. Yields `QueryResult[T]` pages. Automatically follows `LastEvaluatedKey` to fetch all pages.

---

### `transact_get`

```python
async def transact_get(
    self,
    requests: list[TransactGet[T]],
    *,
    return_consumed_capacity: bool = False,
) -> list[T | None]
```

Atomically read up to 100 items. Results are in request order. Always strongly consistent.

---

### `transact_write`

```python
async def transact_write(
    self,
    operations: list[TransactPut | TransactDelete | TransactConditionCheck | TransactUpdate],
    *,
    client_request_token: str | None = None,
    return_consumed_capacity: bool = False,
    return_item_collection_metrics: bool = False,
) -> TransactWriteItemsOutputTypeDef
```

Atomically execute up to 100 write operations.

---

### `batch_get`

```python
async def batch_get(
    self,
    requests: list[BatchGet[DynamoModel]],
    *,
    return_consumed_capacity: bool = False,
) -> BatchGetResult
```

Fetch up to 100 items. Results are grouped by model type in `BatchGetResult.items`.

---

### `batch_write`

```python
async def batch_write(
    self,
    operations: list[BatchPut[DynamoModel] | BatchDelete[DynamoModel]],
    *,
    return_consumed_capacity: bool = False,
    return_item_collection_metrics: bool = False,
) -> BatchWriteResult
```

Write up to 25 items per call.

---

### `create_table`

```python
async def create_table(
    self,
    model: type[T],
    *,
    billing_mode: BillingModeType = "PAY_PER_REQUEST",
    provisioned_throughput: ProvisionedThroughputTypeDef | None = None,
    tags: list[TagTypeDef] | None = None,
    table_class: TableClassType | None = None,
) -> CreateTableOutputTypeDef
```

Create the DynamoDB table from model metadata, including all GSIs and LSIs.

---

### `create_global_table`

```python
async def create_global_table(
    self,
    model: type[T],
    *,
    regions: list[str],
) -> CreateGlobalTableOutputTypeDef
```

Create a global table (multi-region replication) from an existing table.

---

### `delete_table`

```python
async def delete_table(self, model: type[T]) -> DeleteTableOutputTypeDef
```

Delete the table associated with a model.

---

### `exceptions`

```python
async def exceptions(self) -> Exceptions
```

Return the boto3 DynamoDB exception namespace. Cached after first call. Use to catch typed DynamoDB errors.

```python
ex = await db.exceptions()
try:
    await db.put(item, condition_expression=...)
except ex.ConditionalCheckFailedException:
    ...
```

## Connection management internals

`DynamoDB` lazily opens and holds two connections under async locks:

- A DynamoDB **resource** (used for table-level operations: `put`, `get`, `delete`, `update`, `query`)
- A DynamoDB **client** (used for client-level operations: `transact_*`, `batch_*`, `create_table`, `delete_table`)

Both are opened on first use (or eagerly when using the context manager). A table object cache (`_table_cache`) avoids repeated `resource.Table(name)` calls.
