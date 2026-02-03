"""
aiodynamodb - Async DynamoDB ORM with Pydantic.

A modern, async-first DynamoDB ORM built on aioboto3 and Pydantic v2.

Example:
    import aioboto3
    from aiodynamodb import Model, table, init, HashKey, RangeKey, DynamoDateTime
    from datetime import datetime

    @table("users")
    class User(Model):
        user_id: HashKey[str]
        created_at: RangeKey[DynamoDateTime]
        name: str
        email: str | None = None

    async def main():
        session = aioboto3.Session()
        await init(session=session, models=[User])

        # Create and save
        user = User(
            user_id="user_123",
            created_at=datetime.now(),
            name="Alice",
            email="alice@example.com"
        )
        await user.save()

        # Retrieve
        user = await User.get(hash_key="user_123", range_key=user.created_at)

        # Query
        async for u in User.query(hash_key="user_123"):
            print(u.name)

        # Delete
        await user.delete()
"""

from importlib.metadata import PackageNotFoundError, version

from aiodynamodb._init import close, init
from aiodynamodb.exceptions import (
    AioDynamoDBError,
    ConditionalCheckFailedError,
    ItemNotFoundError,
    NotInitializedError,
    TableNotFoundError,
    TransactionCancelledError,
    ValidationError,
)
from aiodynamodb.fields import (
    GSIHashKey,
    GSIRangeKey,
    HashKey,
    LSIRangeKey,
    RangeKey,
)
from aiodynamodb.model import Model, TableConfig, table
from aiodynamodb.operations import AsyncQuery, BatchWriteResult
from aiodynamodb.serializers import (
    DynamoDate,
    DynamoDateTime,
    DynamoDecimal,
    DynamoTime,
    DynamoUUID,
    enum_serializer,
)

try:
    __version__ = version("aiodynamodb")
except PackageNotFoundError:
    __version__ = "0.0.0.dev0"

__all__ = [
    # Core
    "Model",
    "table",
    "TableConfig",
    "init",
    "close",
    # Keys
    "HashKey",
    "RangeKey",
    # Indexes
    "GSIHashKey",
    "GSIRangeKey",
    "LSIRangeKey",
    # Serializers
    "DynamoDateTime",
    "DynamoDate",
    "DynamoTime",
    "DynamoDecimal",
    "DynamoUUID",
    "enum_serializer",
    # Operations
    "AsyncQuery",
    "BatchWriteResult",
    # Exceptions
    "AioDynamoDBError",
    "NotInitializedError",
    "ItemNotFoundError",
    "TableNotFoundError",
    "ConditionalCheckFailedError",
    "TransactionCancelledError",
    "ValidationError",
]
