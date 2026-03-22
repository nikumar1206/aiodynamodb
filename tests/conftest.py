from collections.abc import AsyncGenerator

import pytest

from aiodynamodb import DynamoDB
from aiodynamodb.testing import mock_dynamodb
from tests.entities import ComplexOrder, Order, User


@pytest.fixture
async def dynamo_resource() -> AsyncGenerator[DynamoDB]:
    async with mock_dynamodb() as db:
        yield db


@pytest.fixture
async def users_table(dynamo_resource):
    await dynamo_resource.create_table(User)
    return dynamo_resource


@pytest.fixture
async def orders_table(dynamo_resource):
    await dynamo_resource.create_table(Order)
    return dynamo_resource


@pytest.fixture
async def complex_order_table(dynamo_resource):
    await dynamo_resource.create_table(ComplexOrder)
    return dynamo_resource
