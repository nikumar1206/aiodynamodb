# Quickstart

## Define a model

Every table maps to a `DynamoModel` subclass decorated with `@table`:

```python
from aiodynamodb import DynamoModel, HashKey, table


@table("users")
class User(DynamoModel):
    user_id: HashKey[str]
    name: str
    email: str | None = None
```
Under the hood `DynamoModel` is a small abstraction over pydantic's `BaseModel`. The `@table` decorator adds some additional ClassVars to support DynamoDB functionality. The `HashKey[str]` annotation marks `user_id` as the partition key.

## Create a client

Instantiate `DynamoDB` and use it as an async context manager to manage connection lifetime cleanly:

```python
from aiodynamodb import DynamoDB

async with DynamoDB() as db:
    ...
```

You can also instantiate it standalone and call `await db.close()` when done:

```python
db = DynamoDB()
# ... use db ...
await db.close()
```

## First operations

```python
import asyncio
from aiodynamodb import DynamoDB, DynamoModel, HashKey, table


@table("users")
class User(DynamoModel):
    user_id: HashKey[str]
    name: str
    email: str | None = None


async def main() -> None:
    async with DynamoDB() as db:
        # Create the table (idempotent in real DynamoDB if it already exists)
        await db.create_table(User)

        # Write an item
        await db.put(User(user_id="u1", name="Alice", email="alice@example.com"))

        # Read it back
        user = await db.get(User, hash_key="u1")
        print(user)

        # Update a field
        from aiodynamodb import UpdateAttr

        updated = await db.update(
            User,
            hash_key="u1",
            update_expression={UpdateAttr("name").set("Alice Smith")},
            return_values="ALL_NEW",
        )
        print(updated)

        # Delete the item
        await db.delete(User(user_id="u1", name="Alice Smith"))


asyncio.run(main())
```

## Next steps

- [Defining Models](../guides/defining-models.md) — full model and decorator API
- [CRUD operations](../guides/crud.md) — put, get, delete with conditions
- [Querying](../guides/query.md) — pagination, filters, projections
- [Testing](../guides/testing.md) — write tests with mocked DynamoDB
