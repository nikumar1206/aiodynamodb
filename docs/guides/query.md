# Query Operations

`db.query()` is an **async generator** that yields paginated `QueryResult[T]` objects. Each page contains a list of validated model instances and an optional pagination token.

## Basic query

```python
from boto3.dynamodb.conditions import Key

async for page in db.query(
    Order,
    key_condition_expression=Key("order_id").eq("o1"),
):
    for item in page.items:
        print(item)
```

## Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `model` | `type[T]` | — | `DynamoModel` subclass to query |
| `key_condition_expression` | `ConditionBase \| None` | `None` | Key condition (required for most queries) |
| `index_name` | `str \| None` | `None` | Name of a GSI or LSI to query |
| `filter_expression` | `ConditionBase \| None` | `None` | Post-key filter applied after key lookup |
| `limit` | `int \| None` | `None` | Max items to evaluate per page |
| `exclusive_start_key` | `dict \| None` | `None` | Pagination token from a previous page |
| `consistent_read` | `bool` | `False` | Strongly consistent reads |
| `scan_index_forward` | `bool` | `True` | Sort ascending (`True`) or descending (`False`) |
| `projection_expression` | `list[ProjectionAttr] \| None` | `None` | Project specific fields |
| `return_consumed_capacity` | `bool` | `False` | Include consumed capacity in response |

## Pagination patterns

### Stream all items

`query()` automatically fetches subsequent pages using `LastEvaluatedKey`. The generator handles pagination for you:

```python
async for page in db.query(Order, key_condition_expression=Key("order_id").eq("o1")):
    for item in page.items:
        process(item)
# loop exits naturally when there are no more pages
```

### Collect all items at once

```python
all_items = []
async for page in db.query(Order, key_condition_expression=Key("order_id").eq("o1")):
    all_items.extend(page.items)
```

### Single page with a continuation token

`last_evaluated_key` is `None` when there are no more pages:

```python
async for page in db.query(Order, key_condition_expression=Key("order_id").eq("o1"), limit=25):
    items = page.items
    cursor = page.last_evaluated_key  # None on the last page
    break  # take one page, save cursor for next request
```

Resume from a cursor:

```python
async for page in db.query(
    Order,
    key_condition_expression=Key("order_id").eq("o1"),
    limit=25,
    exclusive_start_key=cursor,
):
    items = page.items
    break
```

## Filtering

`filter_expression` is applied **after** the key lookup, meaning DynamoDB still reads and charges for all key-matched items. It does not replace the key condition.

```python
from boto3.dynamodb.conditions import Attr, Key

async for page in db.query(
    Order,
    key_condition_expression=Key("order_id").eq("o1"),
    filter_expression=Attr("total").gte(100),
):
    print(page.items)
```

## Sort order

```python
# Descending (newest first)
async for page in db.query(
    Order,
    key_condition_expression=Key("order_id").eq("o1"),
    scan_index_forward=False,
):
    print(page.items)
```

## Querying an index

```python
from boto3.dynamodb.conditions import Key

async for page in db.query(
    Order,
    index_name="order_by_status",
    key_condition_expression=Key("status").eq("pending"),
):
    print(page.items)
```

See [Indexes](../guides/indexes.md) for index definition.

## Projections

```python
from aiodynamodb import ProjectionAttr

async for page in db.query(
    Order,
    key_condition_expression=Key("order_id").eq("o1"),
    projection_expression=[ProjectionAttr("order_id"), ProjectionAttr("total")],
):
    print(page.items)
```

See [Projections](../guides/projections.md) for details.

## QueryResult

Each page yielded by `query()` is a `QueryResult[T]`:

| Field | Type | Description |
|---|---|---|
| `items` | `list[T]` | Validated model instances for this page |
| `last_evaluated_key` | `dict \| None` | Pagination token; `None` on the last page |
