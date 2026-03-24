# Indexes

DynamoDB supports two types of secondary indexes: Global Secondary Indexes (GSI) and Local Secondary Indexes (LSI). Both are defined via the `indexes` argument to `@table()`.

## Global Secondary Index (GSI)

A GSI lets you query on a different partition key than the table's primary key.

```python
from aiodynamodb import DynamoModel, table
from aiodynamodb.models import GSI


order_by_status = GSI(
    name="order_by_status",
    hash_key="status",
    range_key="created_at",
)


@table(
    "orders",
    hash_key="order_id",
    range_key="created_at",
    indexes=[order_by_status],
)
class Order(DynamoModel):
    order_id: str
    created_at: str
    status: str
    total: int
```

### GSI parameters

| Parameter | Type | Description |
|---|---|---|
| `name` | `str` | Index name (must be unique among GSIs on this table) |
| `hash_key` | `str` | Partition key field for the index |
| `range_key` | `str \| None` | Sort key field for the index (optional) |
| `projection` | `str` | `"ALL"` (default), `"KEYS_ONLY"`, or `"INCLUDE"` |
| `non_key_attributes` | `list[str] \| None` | Projected attributes when `projection="INCLUDE"` |
| `provisioned_throughput` | `ProvisionedThroughputTypeDef \| None` | Throughput for provisioned mode |
| `on_demand_throughput` | `OnDemandThroughputTypeDef \| None` | On-demand throughput configuration |
| `warm_throughput` | `WarmThroughputTypeDef \| None` | Warm throughput configuration |

## Local Secondary Index (LSI)

An LSI shares the table's partition key but uses a different sort key. It must be created at table creation time.

```python
from aiodynamodb.models import LSI


order_lsi = LSI(
    name="order_by_total",
    range_key="total",
)


@table(
    "orders",
    hash_key="order_id",
    range_key="created_at",
    indexes=[order_lsi],
)
class Order(DynamoModel):
    order_id: str
    created_at: str
    total: int
```

### LSI parameters

| Parameter | Type | Description |
|---|---|---|
| `name` | `str` | Index name (must be unique among LSIs on this table) |
| `range_key` | `str` | Sort key field for the index |
| `projection` | `str` | `"ALL"` (default), `"KEYS_ONLY"`, or `"INCLUDE"` |
| `non_key_attributes` | `list[str] \| None` | Projected attributes when `projection="INCLUDE"` |

## Combining GSI and LSI

```python
from aiodynamodb.models import GSI, LSI

order_gsi = GSI(name="order_gsi", hash_key="order_id", range_key="total")
order_lsi = LSI(name="order_lsi", range_key="total")


@table(
    "orders",
    hash_key="order_id",
    range_key="created_at",
    indexes=[order_gsi, order_lsi],
)
class Order(DynamoModel):
    order_id: str
    created_at: str
    total: int
```

Index names must be unique within each index type (GSI names must be unique, LSI names must be unique).

## Querying an index

Pass `index_name` to `db.query()`:

```python
from boto3.dynamodb.conditions import Key

# Query a GSI
async for page in db.query(
    Order,
    index_name="order_gsi",
    key_condition_expression=Key("order_id").eq("o1"),
):
    for item in page.items:
        print(item)
```

```python
from boto3.dynamodb.conditions import Attr, Key

# Query an LSI with a filter
async for page in db.query(
    Order,
    index_name="order_lsi",
    key_condition_expression=Key("order_id").eq("o1"),
    filter_expression=Attr("total").gte(200),
):
    print(page.items)
```

See the [Query guide](../guides/query.md) for the full `query()` API.
