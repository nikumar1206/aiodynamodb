# CRUD Operations

## put

Insert or overwrite an item. If an item with the same primary key already exists, it is replaced.

```python
await db.put(User(user_id="u1", name="Alice", email="alice@example.com"))
```

### Conditional put

Guard the write with a condition expression. The put fails (raises `ConditionalCheckFailedException`) if the condition is not met.

```python
from boto3.dynamodb.conditions import Attr

# Only insert if the item does not already exist
await db.put(
    User(user_id="u1", name="Alice"),
    condition_expression=Attr("user_id").not_exists(),
)
```

**Signature:**

```python
async def put(
    self,
    item: DynamoModel,
    *,
    condition_expression: ConditionBase | None = None,
) -> None
```

## get

Fetch a single item by its primary key. Returns `None` if not found.

```python
user = await db.get(User, hash_key="u1")
if user is None:
    print("not found")
```

With a composite key:

```python
order = await db.get(Order, hash_key="o1", range_key="2026-01-01T00:00:00")
```

### Projection

Fetch only specific fields using `ProjectionAttr`:

```python
from aiodynamodb import ProjectionAttr

user = await db.get(
    User,
    hash_key="u1",
    projection_expression=[ProjectionAttr("user_id"), ProjectionAttr("name")],
)
```

See [Projections](../guides/projections.md) for details.

### Consistent reads

```python
user = await db.get(User, hash_key="u1", consistent_reads=True)
```

**Signature:**

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

## delete

Delete an item by primary key.

```python
await db.delete(User, hash_key="u1")
```

With a composite key:

```python
await db.delete(Order, hash_key="o1", range_key="2026-01-01")
```

### Conditional delete

```python
from boto3.dynamodb.conditions import Attr

await db.delete(
    User,
    hash_key="u1",
    condition_expression=Attr("user_id").exists(),
)
```

**Signature:**

```python
async def delete(
    self,
    model: type[T],
    *,
    hash_key: KeyT,
    range_key: KeyT | None = None,
    condition_expression: ConditionBase | None = None,
) -> None
```

## Exception handling

Condition expression failures raise `ConditionalCheckFailedException`. Access exception classes via `db.exceptions()`:

```python
ex = await db.exceptions()

try:
    await db.put(
        User(user_id="u1", name="Alice"),
        condition_expression=Attr("user_id").not_exists(),
    )
except ex.ConditionalCheckFailedException:
    print("item already exists")
```

See [Exceptions](../guides/exceptions.md) for the full error handling guide.
