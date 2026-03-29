"""
Integration test fixtures — targets a real LocalStack DynamoDB endpoint.

Set AWS_ENDPOINT_URL=http://localhost:4566 to point at LocalStack.
The workflow sets this via env; for local runs export it yourself or start
LocalStack and let the default below kick in.
"""

import contextlib
import os
from collections.abc import AsyncGenerator

import pytest
from pydantic import BaseModel as PydanticModel

from aiodynamodb import DynamoDB, DynamoModel, table
from aiodynamodb.custom_types import JSONStr, Timestamp
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


# Event — LSI range key (priority: int) differs from table range key (timestamp: str),
# which lets tests verify the LSI is actually being used via key conditions on priority.
event_priority_lsi = LSI(name="priority_idx", range_key="priority")


@table("it_events", hash_key="event_id", range_key="timestamp", indexes=[event_priority_lsi])
class Event(DynamoModel):
    event_id: str
    timestamp: str
    priority: int
    message: str = ""


# Product — category is optional so items without it are excluded from the GSI,
# allowing sparse-index behaviour to be tested.
product_category_gsi = GSI(name="category_idx", hash_key="category")


@table("it_products", hash_key="product_id", indexes=[product_category_gsi])
class Product(DynamoModel):
    product_id: str
    name: str
    category: str | None = None


# Metadata nested model used as a JSONStr field inside TypedRecord.
class Metadata(PydanticModel):
    version: int
    tags: list[str]


@table("it_typed_records", hash_key="record_id")
class TypedRecord(DynamoModel):
    record_id: str
    created_at: Timestamp  # datetime stored as unix-seconds integer
    metadata: JSONStr[Metadata]  # Pydantic model stored as a JSON string


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def db() -> AsyncGenerator[DynamoDB]:
    """Fresh User + Order tables for every test; torn down afterward."""
    async with DynamoDB() as client:
        await client.create_table(User)
        await client.create_table(Order)
        try:
            yield client
        finally:
            for model in (User, Order):
                with contextlib.suppress(Exception):
                    await client.delete_table(model)


@pytest.fixture
async def db_event() -> AsyncGenerator[DynamoDB]:
    """Fresh Event table — used by tests that exercise LSI behaviour."""
    async with DynamoDB() as client:
        await client.create_table(Event)
        try:
            yield client
        finally:
            with contextlib.suppress(Exception):
                await client.delete_table(Event)


@pytest.fixture
async def db_product() -> AsyncGenerator[DynamoDB]:
    """Fresh Product table — used by tests that exercise sparse GSI behaviour."""
    async with DynamoDB() as client:
        await client.create_table(Product)
        try:
            yield client
        finally:
            with contextlib.suppress(Exception):
                await client.delete_table(Product)


@pytest.fixture
async def db_typed() -> AsyncGenerator[DynamoDB]:
    """Fresh TypedRecord table — used by custom-type serialisation tests."""
    async with DynamoDB() as client:
        await client.create_table(TypedRecord)
        try:
            yield client
        finally:
            with contextlib.suppress(Exception):
                await client.delete_table(TypedRecord)
