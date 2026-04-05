# aiodynamodb

Async DynamoDB client + lightweight model layer built on `aioboto3` and Pydantic v2.

[![PyPI](https://img.shields.io/pypi/v/aiodynamodb)](https://pypi.org/project/aiodynamodb/)
[![Python](https://img.shields.io/pypi/pyversions/aiodynamodb)](https://pypi.org/project/aiodynamodb/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## AI Disclaimer
Some code and content in this repository was created with the assistance of AI tools. All code is reviewed thoroughly.

## Features

- Fully async API built on `aioboto3`
- Pydantic v2 models for validation and typed data access
- Table mapping via `@table(...)` decorator with GSI/LSI support
- Persistent connection pooling via context manager
- Query and scan with typed, paginated results
- Conditional writes/deletes, transactions, and batch operations

## Installation

```bash
pip install aiodynamodb
```

## Quickstart

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
        print(user)


asyncio.run(main())
```

## Documentation

Full documentation is available at **[aiodynamodb.com](https://aiodynamodb.com)**, including:

- [Getting Started](https://aiodynamodb.com/getting-started/)
- [Guides](https://aiodynamodb.com/guides/) — CRUD, queries, scans, transactions, batch ops, custom types, and more
- [API Reference](https://aiodynamodb.com/api-reference/)

## Running Tests

```bash
make test
```
