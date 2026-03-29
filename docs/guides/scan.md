# Scan Operations

`db.scan()` is an **async generator** that yields paginated `QueryResult[T]` objects by reading every item in a table (or index). Unlike `query()`, it does not require a key condition.

> **Use sparingly on large tables.** Scan reads every item and charges for all consumed capacity, even when a `filter_expression` is applied.

## Basic scan

```python
async for page in db.scan(User):
    for item in page.items:
        print(item)
```

## Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `model` | `type[T]` | — | `DynamoModel` subclass to scan |
| `index_name` | `str \| None` | `None` | Name of a GSI or LSI to scan |
| `filter_expression` | `ConditionBase \| None` | `None` | Attribute filter applied after the scan read |
| `limit` | `int \| None` | `None` | Max items to evaluate per page |
| `exclusive_start_key` | `dict \| None` | `None` | Pagination token from a previous page |
| `consistent_read` | `bool` | `False` | Strongly consistent reads (not supported on GSIs) |
| `projection_expression` | `list[ProjectionAttr] \| None` | `None` | Project specific fields |
| `return_consumed_capacity` | `bool` | `False` | Include consumed capacity in response |

## Filtering

`filter_expression` is applied **after** DynamoDB reads each item — it reduces what is returned but not what is billed.

```python
from boto3.dynamodb.conditions import Attr

async for page in db.scan(Order, filter_expression=Attr("total").gte(100)):
    print(page.items)
```

## Pagination

`scan()` automatically follows `LastEvaluatedKey` across pages:

```python
all_items = []
async for page in db.scan(User):
    all_items.extend(page.items)
```

Take one page and save a cursor for the next request:

```python
async for page in db.scan(User, limit=25):
    items = page.items
    cursor = page.last_evaluated_key  # None on the last page
    break

# resume later
async for page in db.scan(User, limit=25, exclusive_start_key=cursor):
    ...
    break
```

## Scanning an index

```python
async for page in db.scan(Order, index_name="order_gsi"):
    print(page.items)
```

## Projections

```python
from aiodynamodb import ProjectionAttr

async for page in db.scan(User, projection_expression=[ProjectionAttr("user_id"), ProjectionAttr("name")]):
    print(page.items)
```

See [Projections](projections.md) for details.
