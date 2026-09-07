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

#### `.set(value: Any, *, if_not_exists: bool = False) -> UpdateAttr`

Set the attribute to a value. `set(None)` removes the attribute. With
`if_not_exists=True` the value is written only when the attribute is absent
(`SET path = if_not_exists(path, :value)`).

```python
UpdateAttr("name").set("Alice Smith")
UpdateAttr("address.city").set("New York")
UpdateAttr("basket.items[1].qty").set(9)
UpdateAttr("created_at").set(now, if_not_exists=True)
```

#### `.append(value: list[Any], *, if_not_exists: bool = True) -> UpdateAttr`

Append elements to a list using `SET path = list_append(path, value)`.
Pass a list, even when appending a single element. By default a missing list is
treated as empty (`list_append(if_not_exists(path, :empty), value)`), so the
first append creates it. Pass `if_not_exists=False` to require the list to
already exist.

```python
UpdateAttr("basket.items").append([Item(qty=1, price=2.5, name="new")])
UpdateAttr("basket.items").append([item], if_not_exists=False)
```

#### `.prepend(value: list[Any], *, if_not_exists: bool = True) -> UpdateAttr`

Prepend elements to a list using `SET path = list_append(value, path)`. Same
semantics as `.append()` otherwise.

```python
UpdateAttr("basket.items").prepend([Item(qty=1, price=2.5, name="first")])
```

#### `.remove(index: int | None = None) -> UpdateAttr`

Remove the attribute entirely (equivalent to DynamoDB `REMOVE`).

```python
UpdateAttr("email").remove()
UpdateAttr("basket.items").remove(1)
UpdateAttr("basket.items[1]").remove()  # equivalent indexed removal
```

List elements are removed by zero-based, non-negative index, not by value.
Subsequent elements shift down. Omitting the index removes the whole attribute.
Passing an index when the path already ends in one (`UpdateAttr("items[0]").remove(1)`)
raises `ValueError`.

#### `.add(value: int | float | Decimal | Set[Any]) -> UpdateAttr`

Add a number to a numeric attribute, or add elements to a DynamoDB set (`set` or `frozenset`).
List operands are rejected; use `.append([...])` for lists.

```python
UpdateAttr("login_count").add(1)
UpdateAttr("tags").add({"new-tag"})
```

#### `.delete(value: Set[Any]) -> UpdateAttr`

Remove elements from a DynamoDB set attribute (`set` or `frozenset`).
List operands are rejected; use `.remove(index)` for list elements.

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

Traversing a list field without an index (`"basket.items.qty"`) raises
`ValueError`; the index must be explicit. The same rule applies to
`ProjectionAttr` and condition/filter `Attr` paths.

### Path uniqueness

DynamoDB allows each document path to appear only once per update, and
rejects overlapping paths (`items` together with `items[0]`, or `basket`
together with `basket.total`). `UpdateExpressionBuilder` validates this
up-front and raises `ValueError` naming both paths.

### Hashing

`UpdateAttr` instances are hashable so they can be passed in a `set`, though a `list` is preferred for deterministic clause order. The hash is based on the action type, attribute path, list-operation flags, and a frozen copy of the value.

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
Values map 1:1 to DynamoDB update clauses; `.append()` and `.prepend()` are
`SET` actions that compile to `list_append`.

---

## `UpdateExpressionBuilder`

Internal class used by the client to compile a collection of `UpdateAttr` into a DynamoDB `UpdateExpression` string with `ExpressionAttributeNames` and `ExpressionAttributeValues`.

You do not need to use this directly — it is invoked internally by `db.update()` and `TransactUpdate`.
