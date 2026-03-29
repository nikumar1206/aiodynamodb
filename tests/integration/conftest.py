"""
Integration test fixtures — targets a real LocalStack DynamoDB endpoint.

Set AWS_ENDPOINT_URL=http://localhost:4566 to point at LocalStack.
The workflow sets this via env; for local runs export it yourself or start
LocalStack and let the default below kick in.
"""

import os
from collections.abc import AsyncGenerator

import pytest

from aiodynamodb import DynamoDB, DynamoModel, table
from aiodynamodb.models import GSI, LSI

os.environ.setdefault("AWS_ENDPOINT_URL", "http://localhost:4566")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")


# ---------------------------------------------------------------------------
# Shared models
# ---------------------------------------------------------------------------


@table("it_users", hash_key="user_id")
class User(DynamoModel):
    user_id: str
    name: str
    age: int | None = None
    email: str | None = None
    active: bool = True


status_gsi = GSI(name="status_idx", hash_key="status", range_key="total")
created_lsi = LSI(name="created_idx", range_key="created_at")


@table("it_orders", hash_key="order_id", range_key="created_at", indexes=[status_gsi, created_lsi])
class Order(DynamoModel):
    order_id: str
    created_at: str
    total: int
    status: str = "pending"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def db() -> AsyncGenerator[DynamoDB]:
    """Fresh tables for every test; torn down afterward."""
    async with DynamoDB() as client:
        await client.create_table(User)
        await client.create_table(Order)
        try:
            yield client
        finally:
            for model in (User, Order):
                try:
                    await client.delete_table(model)
                except Exception:
                    pass
