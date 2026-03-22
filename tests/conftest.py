from collections.abc import AsyncGenerator

import pytest

from aiodynamodb import DynamoDB
from aiodynamodb.testing import mock_dynamodb
from tests.entities import ComplexOrder, Order, User


@pytest.fixture
async def db() -> AsyncGenerator[DynamoDB]:
    async with mock_dynamodb(User, Order, ComplexOrder) as db:
        yield db
