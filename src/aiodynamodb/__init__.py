"""
aiodynamodb - Async DynamoDB ORM with Pydantic.

An async-first DynamoDB ORM built on aioboto3 and Pydantic v2.
Note, not threadsafe.

Example:
    from aiodynamodb import table, DynamoDB, DynamoModel

    @table("users", hash_key="user_id")
    class User(DynamoModel):
        user_id: str
        name: str
        email: str | None = None

    async def main():
        db = DynamoDB()

        user = User(user_id="user_123", name="Alice", email="alice@example.com")
        await db.put(user)

        fetched = await db.get(User, hash_key="user_123")
        print(fetched.name)
"""

from importlib.metadata import PackageNotFoundError, version

from aiodynamodb import custom_types
from aiodynamodb.client import (
    DynamoDB,
)
from aiodynamodb.custom_types import ReturnValues
from aiodynamodb.models import (
    BatchDelete,
    BatchGet,
    BatchGetResult,
    BatchPut,
    BatchWriteResult,
    DynamoModel,
    TableMeta,
    TransactConditionCheck,
    TransactDelete,
    TransactGet,
    TransactPut,
    TransactUpdate,
    table,
)
from aiodynamodb.projection import ProjectionAttr
from aiodynamodb.updates import UpdateAttr

try:
    __version__ = version("aiodynamodb")
except PackageNotFoundError:
    __version__ = "0.0.0.dev0"

VERSION = __version__

__all__ = [
    "DynamoDB",
    "DynamoModel",
    "TableMeta",
    "BatchGet",
    "BatchPut",
    "BatchDelete",
    "BatchGetResult",
    "BatchWriteResult",
    "TransactGet",
    "TransactPut",
    "TransactDelete",
    "TransactConditionCheck",
    "TransactUpdate",
    "ProjectionAttr",
    "UpdateAttr",
    "table",
    "VERSION",
    "custom_types",
    "ReturnValues",
]
