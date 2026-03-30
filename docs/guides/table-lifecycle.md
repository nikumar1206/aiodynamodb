# Table Lifecycle

`DynamoDB` provides helpers to create and delete tables directly from model metadata, which is especially useful for tests and local development.

## create_table

Creates the DynamoDB table for a model. All key schema, attribute definitions, and index configurations are derived from `model.Meta`.

```python
await db.create_table(User)
```

With options:

```python
await db.create_table(
    Order,
    billing_mode="PROVISIONED",
    provisioned_throughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    tags=[{"Key": "env", "Value": "production"}],
    table_class="STANDARD_INFREQUENT_ACCESS",
)
```

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `model` | `type[T]` | — | `DynamoModel` subclass |
| `billing_mode` | `BillingModeType` | `"PAY_PER_REQUEST"` | `"PAY_PER_REQUEST"` or `"PROVISIONED"` |
| `provisioned_throughput` | `ProvisionedThroughputTypeDef | None` | `None` | Required when `billing_mode="PROVISIONED"` |
| `tags` | `list[TagTypeDef] | None` | `None` | Tags to attach to the table |
| `table_class` | `TableClassType | None` | `None` | `"STANDARD"` or `"STANDARD_INFREQUENT_ACCESS"` |

### What gets created

- Table with `hash_key` (and `range_key` if defined)
- All GSIs defined in `model.Meta.global_secondary_indexes`
- All LSIs defined in `model.Meta.local_secondary_indexes`
- All required attribute definitions inferred from key field types

### Supported key field types

| Python type | DynamoDB attribute type |
|---|---|
| `str`, `datetime` | String (S) |
| `int`, `float`, `Timestamp`, `TimestampMillis`, `TimestampMicros`, `TimestampNanos` | Number (N) |
| `bytes` | Binary (B) |

## create_global_table

Create a global table (multi-region replication) from an existing table:

```python
response = await db.create_global_table(
    User,
    regions=["us-east-1", "eu-west-1"],
)
```

> Note: The table must already exist in the primary region before calling this.

## delete_table

Delete the table associated with a model:

```python
await db.delete_table(User)
```

Returns the raw DynamoDB `delete_table` response.

## Usage in tests

`create_table` and `delete_table` are commonly used in test fixtures alongside `mock_dynamodb()`:

```python
from aiodynamodb.testing import mock_dynamodb

async with mock_dynamodb(User, Order) as db:
    # tables are created automatically for all provided models
    await db.put(User(user_id="u1", name="Alice"))
```

See [Testing](../guides/testing.md) for the full testing guide.
