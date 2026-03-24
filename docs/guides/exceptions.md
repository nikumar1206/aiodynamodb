# Exception Handling

## Accessing exception classes

DynamoDB exception classes are accessed via `db.exceptions()`. This is async because the exceptions namespace is fetched lazily from the underlying boto3 client.

```python
ex = await db.exceptions()
```

`ex` gives you the full boto3 DynamoDB exceptions namespace. Exception classes are cached after the first call.

## ConditionalCheckFailedException

The most common exception — raised when a condition expression on a `put`, `delete`, `update`, or transactional operation is not satisfied.

```python
from boto3.dynamodb.conditions import Attr

ex = await db.exceptions()

try:
    await db.put(
        User(user_id="u1", name="Alice"),
        condition_expression=Attr("user_id").not_exists(),
    )
except ex.ConditionalCheckFailedException:
    print("item already exists")
```

## TransactionCanceledException

Raised when a `transact_write` or `transact_get` is cancelled. This can happen because:

- A condition check in the transaction failed
- A conflict with another concurrent transaction
- Provisioned throughput exceeded

```python
ex = await db.exceptions()

try:
    await db.transact_write([
        TransactPut(User(user_id="u1", name="Alice"), condition_expression=Attr("user_id").not_exists()),
        TransactPut(Order(order_id="o1", created_at="2026-01-01", total=100)),
    ])
except ex.TransactionCanceledException as e:
    print("transaction cancelled:", e)
```

## ResourceNotFoundException

Raised when you try to operate on a table that does not exist.

```python
ex = await db.exceptions()

try:
    await db.get(User, hash_key="u1")
except ex.ResourceNotFoundException:
    print("table does not exist — did you call create_table?")
```

## ProvisionedThroughputExceededException

Raised when you exceed your table's provisioned read or write capacity.

```python
ex = await db.exceptions()

try:
    await db.put(user)
except ex.ProvisionedThroughputExceededException:
    # implement backoff / retry
    ...
```

## Pattern: fetch exceptions once

Fetch and store the exceptions namespace once per client lifetime:

```python
async with DynamoDB() as db:
    ex = await db.exceptions()

    try:
        await db.put(user, condition_expression=Attr("user_id").not_exists())
    except ex.ConditionalCheckFailedException:
        pass
```

## Using in pytest

```python
import pytest
from boto3.dynamodb.conditions import Attr
from aiodynamodb.testing import mock_dynamodb


async def test_duplicate_put():
    async with mock_dynamodb(User) as db:
        ex = await db.exceptions()
        await db.put(User(user_id="u1", name="Alice"))

        with pytest.raises(ex.ConditionalCheckFailedException):
            await db.put(
                User(user_id="u1", name="Bob"),
                condition_expression=Attr("user_id").not_exists(),
            )
```
