# Update Operations

`db.update()` modifies an existing item by key and optionally returns the updated attributes.

## Basic usage

```python
from aiodynamodb import UpdateAttr

updated = await db.update(
    User,
    hash_key="u1",
    update_expression={UpdateAttr("name").set("Bob")},
    return_values="ALL_NEW",
)
print(updated)  # User(user_id='u1', name='Bob', ...)
```

**Signature:**

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

`update_expression` is a `set` of `UpdateAttr` actions. Returns a validated model instance when `return_values` causes DynamoDB to return attributes; otherwise returns `None`.

## Actions

`UpdateAttr` supports four DynamoDB update actions.

### SET

Set a field to a value:

```python
{UpdateAttr("name").set("Alice Smith")}
```

### REMOVE

Remove a field entirely:

```python
{UpdateAttr("email").remove()}
```

### ADD

Add a number to a numeric field, or add elements to a set:

```python
{UpdateAttr("login_count").add(1)}
```

### DELETE

Remove elements from a set field:

```python
{UpdateAttr("roles").delete({"admin"})}
```

## Multiple actions in one call

Pass multiple `UpdateAttr` instances in the same set:

```python
updated = await db.update(
    User,
    hash_key="u1",
    update_expression={
        UpdateAttr("name").set("Alice Smith"),
        UpdateAttr("email").remove(),
        UpdateAttr("login_count").add(1),
    },
    return_values="ALL_NEW",
)
```

## Nested paths

Use dot notation for nested fields, and `[n]` for list index access:

```python
# Set a nested field
{UpdateAttr("address.city").set("New York")}

# Set a list element by index
{UpdateAttr("basket.items[1].qty").set(9)}
```

Full example:

```python
from datetime import datetime


@table("orders", hash_key="order_id", range_key="created_at")
class ComplexOrder(DynamoModel):
    order_id: str
    created_at: str
    basket: dict  # contains {"items": [{"qty": int, ...}, ...]}


created_at = "2026-01-01T00:00:00"

updated = await db.update(
    ComplexOrder,
    hash_key="o1",
    range_key=created_at,
    update_expression={UpdateAttr("basket.items[1].qty").set(9)},
    return_values="ALL_NEW",
)
```

## return_values

| Value | DynamoDB behavior |
|---|---|
| `"NONE"` | Return nothing (default) |
| `"ALL_OLD"` | Return all attributes before the update |
| `"UPDATED_OLD"` | Return only updated attributes before the update |
| `"ALL_NEW"` | Return all attributes after the update |
| `"UPDATED_NEW"` | Return only updated attributes after the update |

When `return_values` is omitted or `"NONE"`, `update()` returns `None`. For any other value, it returns a validated model instance built from the attributes DynamoDB returns.

## Conditional update

```python
from boto3.dynamodb.conditions import Attr

updated = await db.update(
    User,
    hash_key="u1",
    update_expression={UpdateAttr("name").set("Alice Smith")},
    condition_expression=Attr("name").eq("Alice"),
    return_values="ALL_NEW",
)
```

If the condition fails, `ConditionalCheckFailedException` is raised. See [Exceptions](../guides/exceptions.md).
