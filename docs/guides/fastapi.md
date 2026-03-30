# FastAPI Integration

This guide shows how to use **`aiodynamodb`** with **FastAPI** in an idiomatic, async-safe way, including dependency injection and type-safe models.

## Installation

```bash
pip install fastapi uvicorn aioboto3 aiodynamodb pydantic
```

## Define DynamoDB Models

```python
from aiodynamodb import DynamoModel, table


@table(name="foo", hash_key="id")
class User(DynamoModel):
    id: int
    name: str
    email: str
```

## Setup a Reusable DynamoDB Dependency

```python
from aioboto3 import Session
from aiodynamodb import DynamoDB
from fastapi import Depends
from typing import Annotated


async def get_db():
    async with DynamoDB(session=Session()) as db:
        yield db


DynamoDI = Annotated[DynamoDB, Depends(get_db)]
```

## CRUD Example

```python
from fastapi import FastAPI, HTTPException

app = FastAPI(title="aiodynamodb + fastapi")


@app.get("/users/{user_id}", response_model=User)
async def get_user(user_id: int, db: DynamoDI):
    user = await db.get(User, hash_key=user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user


@app.post("/users/", response_model=User)
async def create_user(user: User, db: DynamoDI):
    await db.put(user)
    return user
```

## Notes & Best Practices

- Avoid using `db = DynamoDB(session=Session())` without `async with` unless you manage `.close()` manually.
- `Annotated[DynamoDB, Depends(get_db)]` makes DI reusable across multiple endpoints.
- Returning `User` as `response_model` ensures FastAPI validates and serializes DynamoDB items correctly.
- If `db.get(...)` returns `None`, return a 404. For condition failures, see [Exceptions](./exceptions.md).

## Advanced: Using Secondary Indexes

```python
user = await db.query(
    User,
    index_name="email-index",
    key_condition_expression="email = :email",
    expression_values={":email": "alice@example.com"},
)
```
