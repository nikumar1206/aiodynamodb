from collections.abc import AsyncGenerator

import pytest

from aiodynamodb import DynamoDB
from aiodynamodb.testing import mock_dynamodb
from tests.unit.entities import ComplexOrder, Order, StatusUser, User, UserType, UserVersion


@pytest.fixture
async def db() -> AsyncGenerator[DynamoDB]:
    async with mock_dynamodb(User, Order, ComplexOrder, UserType, UserVersion, StatusUser) as db:
        yield db
