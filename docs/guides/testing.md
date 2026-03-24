# Testing

`aiodynamodb` ships a `mock_dynamodb()` async context manager that starts an in-memory mock of AWS DynamoDB, creates tables for your models, and yields a ready-to-use `DynamoDB` client — no real AWS account needed.

## Install the testing extra

```bash
pip install aiodynamodb[testing]
```

Or with uv:

```bash
uv add --optional testing aiodynamodb
```

This installs `aiomoto` and `moto[dynamodb]`.

## mock_dynamodb

```python
from aiodynamodb.testing import mock_dynamodb

async with mock_dynamodb(User, Order) as db:
    await db.put(User(user_id="u1", name="Alice"))
    user = await db.get(User, hash_key="u1")
    assert user is not None
```

### What it does

1. Patches AWS environment variables with fake credentials (`AWS_ACCESS_KEY_ID=testing`, etc.)
2. Starts the `aiomoto` mock for DynamoDB
3. Creates tables for all provided model classes by calling `db.create_table(model)` for each
4. Yields the `DynamoDB` client
5. Tears down the mock on exit

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `*models` | `type[DynamoModel]` | — | Model classes whose tables should be created |
| `patch_env` | `bool` | `True` | Patch AWS env vars with fake credentials. Set `False` if you manage credentials yourself. |

## pytest setup

Install `pytest-asyncio`:

```bash
pip install pytest pytest-asyncio
```

Configure `asyncio_mode = "auto"` in `pyproject.toml` (already done in this project):

```toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
asyncio_default_fixture_loop_scope = "function"
```

## Writing tests

### Basic test

```python
import pytest
from aiodynamodb import DynamoModel, table
from aiodynamodb.testing import mock_dynamodb


@table("users", hash_key="user_id")
class User(DynamoModel):
    user_id: str
    name: str


async def test_put_and_get():
    async with mock_dynamodb(User) as db:
        await db.put(User(user_id="u1", name="Alice"))
        result = await db.get(User, hash_key="u1")
        assert result is not None
        assert result.name == "Alice"
```

### Using a fixture

```python
import pytest
from aiodynamodb import DynamoDB
from aiodynamodb.testing import mock_dynamodb


@pytest.fixture
async def db():
    async with mock_dynamodb(User, Order) as client:
        yield client


async def test_something(db: DynamoDB):
    await db.put(User(user_id="u1", name="Alice"))
    ...
```

### Testing condition expressions

```python
import pytest
from boto3.dynamodb.conditions import Attr


async def test_conditional_put_fails():
    async with mock_dynamodb(User) as db:
        ex = await db.exceptions()
        await db.put(User(user_id="u1", name="Alice"))

        with pytest.raises(ex.ConditionalCheckFailedException):
            await db.put(
                User(user_id="u1", name="Bob"),
                condition_expression=Attr("user_id").not_exists(),
            )
```

### Testing queries

```python
async def test_query_pagination():
    async with mock_dynamodb(Order) as db:
        for i in range(5):
            await db.put(Order(order_id="o1", created_at=f"2026-01-0{i+1}", total=i * 10))

        from boto3.dynamodb.conditions import Key

        all_items = []
        async for page in db.query(
            Order,
            key_condition_expression=Key("order_id").eq("o1"),
        ):
            all_items.extend(page.items)

        assert len(all_items) == 5
```

## What aiomoto mocks

`aiomoto` intercepts all `aioboto3` network calls and emulates DynamoDB in memory. This includes:

- `put_item`, `get_item`, `delete_item`, `update_item`
- `query`, `scan`
- `transact_get_items`, `transact_write_items`
- `batch_get_item`, `batch_write_item`
- `create_table`, `delete_table`

All DynamoDB behavior (conditions, projections, indexes, pagination) is emulated — but it is not 100% identical to the real service. For full fidelity, use localstack or a real DynamoDB table in integration tests.
