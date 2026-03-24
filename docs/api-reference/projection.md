# Projection

**Module:** `aiodynamodb.projection`
**Import:** `from aiodynamodb import ProjectionAttr`

## `ProjectionAttr`

A DynamoDB projection attribute path. Inherits from boto3 `AttributeBase`.

```python
ProjectionAttr("user_id")
ProjectionAttr("address.city")
ProjectionAttr("basket.items[0].qty")
```

Pass a list of `ProjectionAttr` instances as `projection_expression` to:
- `db.get()`
- `db.query()`
- `db.transact_get()` (via `TransactGet`)
- `db.batch_get()` (via `BatchGet`)

### Path syntax

Supports dot notation for nested maps and `[n]` for list indexing:

| Syntax | Example |
|---|---|
| Top-level | `ProjectionAttr("name")` |
| Nested | `ProjectionAttr("address.city")` |
| List element | `ProjectionAttr("items[0]")` |
| Combined | `ProjectionAttr("basket.items[1].qty")` |

---

## `ProjectionExpressionArg`

Type alias:

```python
type ProjectionExpressionArg = list[ProjectionAttr]
```

This is the type accepted by all `projection_expression` parameters.

---

## `ProjectionExpressionBuilder`

Internal class that compiles a list of `ProjectionAttr` into a DynamoDB `ProjectionExpression` string with `ExpressionAttributeNames` for reserved word escaping.

Used internally by the client — you do not call this directly.

### `BuiltProjectionExpression`

```python
@dataclass
class BuiltProjectionExpression:
    projection_expression: str
    expression_attribute_names: dict[str, str]
```

The compiled output of `ProjectionExpressionBuilder`. Contains the expression string and the placeholder-to-name mapping.
