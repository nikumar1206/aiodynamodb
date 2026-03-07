import os

import pytest
from aiomoto import mock_aws

from aiodynamodb import DynamoDB
from tests.entities import ComplexOrder, Order, User


@pytest.fixture(autouse=True)
def _aws_credentials():
    # this ensures no AWS calls are ever made.
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture
async def dynamo_resource() -> DynamoDB:
    """Yield a mocked DynamoDB resource for table creation."""
    async with mock_aws():
        yield DynamoDB()


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
