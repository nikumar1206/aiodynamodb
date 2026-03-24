# Updates

**Module:** `aiodynamodb.updates`
**Import:** `from aiodynamodb import UpdateAttr`

## `UpdateAttr`

A DynamoDB update attribute path. Inherits from boto3 `AttributeBase` so placeholder handling reuses the same builder machinery as condition expressions.

Instantiate with a field path string, then chain one action method:

```python
UpdateAttr("name").set("Alice")
UpdateAttr("email").remove()
UpdateAttr("login_count").add(1)
UpdateAttr("roles").delete({"admin"})
```

### Action methods

#### `.set(value: Any) -> UpdateAttr`

Set the attribute to a value.

```python
UpdateAttr("name").set("Alice Smith")
UpdateAttr("address.city").set("New York")
UpdateAttr("basket.items[1].qty").set(9)
```

#### `.remove() -> UpdateAttr`

Remove the attribute entirely (equivalent to DynamoDB `REMOVE`).

```python
UpdateAttr("email").remove()
```

#### `.add(value: Any) -> UpdateAttr`

Add a number to a numeric attribute, or add elements to a DynamoDB set.

```python
UpdateAttr("login_count").add(1)
UpdateAttr("tags").add({"new-tag"})
```

#### `.delete(value: Any) -> UpdateAttr`

Remove elements from a DynamoDB set attribute.

```python
UpdateAttr("roles").delete({"admin"})
```

### Path syntax

| Syntax | Example | Description |
|---|---|---|
| Top-level field | `UpdateAttr("name")` | Direct attribute access |
| Nested field | `UpdateAttr("address.city")` | Dot notation for nested maps |
| List element | `UpdateAttr("items[0]")` | Zero-based list index |
| Nested in list | `UpdateAttr("basket.items[1].qty")` | Combined path |

### Hashing

`UpdateAttr` instances are used in `set[UpdateAttr]`, so they are hashable. The hash is based on `(action_type, attribute_name, frozen_value)`.

---

## `Action`

```python
class Action(Enum):
    SET = "SET"
    REMOVE = "REMOVE"
    ADD = "ADD"
    DELETE = "DELETE"
```

The action type set on an `UpdateAttr` after calling one of its action methods.

---

## `UpdateExpressionBuilder`

Internal class used by the client to compile a `set[UpdateAttr]` into a DynamoDB `UpdateExpression` string with `ExpressionAttributeNames` and `ExpressionAttributeValues`.

You do not need to use this directly — it is invoked internally by `db.update()` and `TransactUpdate`.
