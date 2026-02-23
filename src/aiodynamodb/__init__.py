"""
aiodynamodb - Async DynamoDB ORM with Pydantic.

A modern, async-first DynamoDB ORM built on aioboto3 and Pydantic v2.

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

from aiodynamodb.client import DynamoDB, Key, Page
from aiodynamodb.models import (
    AttributeValue,
    ComparisonOperator,
    ConditionalOperator,
    ConsumedCapacity,
    DynamoModel,
    QueryInput,
    QueryOutput,
    ReturnConsumedCapacity,
    Select,
    TableMeta,
    table,
)

try:
    __version__ = version("aiodynamodb")
except PackageNotFoundError:
    __version__ = "0.0.0.dev0"

VERSION = __version__

__all__ = [
    "DynamoDB",
    "DynamoModel",
    "Key",
    "TableMeta",
    "Page",
    "AttributeValue",
    "ComparisonOperator",
    "ConditionalOperator",
    "ConsumedCapacity",
    "QueryInput",
    "QueryOutput",
    "ReturnConsumedCapacity",
    "Select",
    "table",
    "VERSION",
]
