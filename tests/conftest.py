import os

import aioboto3
import pytest
from aiomoto import mock_aws


@pytest.fixture(autouse=True)
def _aws_credentials():
    # this ensures no AWS calls are ever made.
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture
async def dynamo_resource():
    """Yield a mocked DynamoDB resource for table creation."""
    async with mock_aws():
        session = aioboto3.Session()
        async with session.resource("dynamodb", region_name="us-east-1") as resource:
            yield resource


@pytest.fixture
async def users_table(dynamo_resource):
    await dynamo_resource.create_table(
        TableName="users",
        KeySchema=[{"AttributeName": "user_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "user_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    return dynamo_resource


@pytest.fixture
async def orders_table(dynamo_resource):
    await dynamo_resource.create_table(
        TableName="orders",
        KeySchema=[
            {"AttributeName": "order_id", "KeyType": "HASH"},
            {"AttributeName": "created_at", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "order_id", "AttributeType": "S"},
            {"AttributeName": "created_at", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    return dynamo_resource
