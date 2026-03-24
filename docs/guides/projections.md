# Projections

Projections let you fetch only specific attributes from DynamoDB instead of the full item, reducing network transfer and read costs.

## ProjectionAttr

`ProjectionAttr` is a path descriptor for a DynamoDB attribute. Pass a list of them as `projection_expression` to `get()`, `query()`, `transact_get()`, or `batch_get()`.

```python
from aiodynamodb import ProjectionAttr

user = await db.get(
    User,
    hash_key="u1",
    projection_expression=[ProjectionAttr("user_id"), ProjectionAttr("name")],
)
```

## Nested paths

`ProjectionAttr` supports dot notation and list indexing:

```python
ProjectionAttr("address.city")
ProjectionAttr("basket.items[0].qty")
```

## With get

```python
from aiodynamodb import ProjectionAttr

user = await db.get(
    User,
    hash_key="u1",
    projection_expression=[ProjectionAttr("user_id"), ProjectionAttr("email")],
)
# user.name will be None since it was not projected
```

## With query

```python
async for page in db.query(
    Order,
    key_condition_expression=Key("order_id").eq("o1"),
    projection_expression=[ProjectionAttr("order_id"), ProjectionAttr("total")],
):
    for item in page.items:
        print(item.total)
```

## With transact_get

```python
from aiodynamodb import TransactGet, ProjectionAttr

items = await db.transact_get([
    TransactGet(
        User,
        hash_key="u1",
        projection_expression=[ProjectionAttr("user_id"), ProjectionAttr("name")],
    ),
])
```

## With batch_get

```python
from aiodynamodb import BatchGet, ProjectionAttr

result = await db.batch_get([
    BatchGet(
        User,
        hash_key="u1",
        projection_expression=[ProjectionAttr("user_id")],
    ),
])
```

## Important notes

- Projected items will have `None` for any field not included in the projection. Pydantic validation still runs on the returned data, so fields with no default will be missing from the model if not projected — plan your projections to match your model's required fields or use `Optional` types.
- DynamoDB uses expression attribute name placeholders internally to avoid conflicts with reserved words — `ProjectionAttr` handles this automatically.
