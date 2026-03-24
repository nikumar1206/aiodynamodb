# aiodynamodb

**Async DynamoDB ORM with Pydantic.** Define your tables as Pydantic models, run fully async DynamoDB operations, and get typed results back — no boilerplate.

```bash
pip install aiodynamodb
```

---

## Features

- **Fully async** — built on `aioboto3`, designed for async Python applications
- **Pydantic v2 models** — validation, type hints, and serialization out of the box
- **`@table()` decorator** — attach table metadata to your model class in one line
- **GSI and LSI support** — define and query secondary indexes declaratively
- **Query pagination** — `query()` is an async generator yielding typed pages
- **Conditional operations** — guard puts, deletes, and updates with condition expressions
- **Transactional reads/writes** — up to 100 operations atomically
- **Batch reads/writes** — efficient multi-item operations
- **Table lifecycle helpers** — `create_table` / `delete_table` from model metadata
- **Built-in test support** — `mock_dynamodb()` context manager via aiomoto

## Requirements

- Python 3.12+
- AWS credentials configured in your environment (or use `mock_dynamodb()` in tests)

## Quick example

```python
import asyncio
from aiodynamodb import DynamoDB, DynamoModel, table


@table("users", hash_key="user_id")
class User(DynamoModel):
    user_id: str
    name: str
    email: str | None = None


async def main() -> None:
    async with DynamoDB() as db:
        await db.create_table(User)
        await db.put(User(user_id="u1", name="Alice", email="alice@example.com"))

        user = await db.get(User, hash_key="u1")
        print(user)  # User(user_id='u1', name='Alice', email='alice@example.com')


asyncio.run(main())
```

> **Not thread-safe.** `DynamoDB` holds internal async locks and connection state. Create one instance per async context — do not share across threads or use in synchronous code.
