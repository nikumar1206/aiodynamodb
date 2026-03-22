import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, nullcontext
from unittest.mock import patch

from aiodynamodb.client import DynamoDB
from aiodynamodb.models import DynamoModel

_FAKE_AWS_CREDENTIALS = {
    "AWS_ACCESS_KEY_ID": "testing",
    "AWS_SECRET_ACCESS_KEY": "testing",
    "AWS_SECURITY_TOKEN": "testing",
    "AWS_SESSION_TOKEN": "testing",
    "AWS_DEFAULT_REGION": "us-east-1",
}


@asynccontextmanager
async def mock_dynamodb(
    *models: type[DynamoModel],
    patch_env: bool = True,
) -> AsyncGenerator[DynamoDB]:
    """Async context manager that provides a mocked DynamoDB instance for testing.

    Starts the aiomoto mock, creates tables for all provided models, and yields
    a ready-to-use ``DynamoDB`` instance.

    Requires the ``testing`` optional dependency:
        pip install aiodynamodb[testing]

    Args:
        *models: ``DynamoModel`` subclasses whose tables should be created.
        patch_env: When ``True`` (default), overrides AWS environment variables
            with fake credentials for the duration of the context. Set to
            ``False`` if you manage credentials yourself.

    Example:
        async with mock_dynamodb(User, Order) as db:
            await db.put(User(user_id="u1", name="Alice"))
            fetched = await db.get(User, hash_key="u1")
    """
    try:
        from aiomoto import mock_aws
    except ImportError as e:
        raise ImportError("mock_dynamodb requires the 'testing' extra: pip install aiodynamodb[testing]") from e

    with patch.dict(os.environ, _FAKE_AWS_CREDENTIALS) if patch_env else nullcontext():
        async with mock_aws():
            db = DynamoDB()
            for model in models:
                await db.create_table(model)
            yield db
