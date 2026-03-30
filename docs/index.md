# aiodynamodb

**Interact with DynamoDB asynchronously in a Pythonic-interface.** Define your tables as Pydantic models, run fully async DynamoDB operations, and get typed results back.

```bash
pip install aiodynamodb
```

---

## Features

- **Fully async** — built on `aioboto3` and designed for async Python applications (like FastAPI!).
- **Pydantic** — DynamoDB tables are represented as Pydantic models, which handles data validation and serialization.
- **Fully Typed** - All public and private methods are typed and mypy-compliant.
- **Light and Performant** — Most overhead is due to network or data 
- **Supports most DynamoDB APIs** - create/delete/query/scan table, get/put/delete item. Supports filter expressions, GSI, LSIs, and much more! If something isn't supported, drop a Github issue or a PR <3!
- **Built-in test support** — `mock_dynamodb()` context manager via aiomoto

## Requirements

- Python 3.12+

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

> **Not thread-safe.** `DynamoDB` holds internal async locks and connection state. Create one instance per async context and do not share across threads.
> **Avoid managing infra.** AiodynamoDB can definitely create/delete your Dynamo tables, but we would like to warn against creating infrastructure in application code. This is much better done through dedicated IaaC such as Terraform. Please only use the `create_table` or `delete_table` APIs for testing purposes.
