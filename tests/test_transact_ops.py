from datetime import datetime

import pytest
from boto3.dynamodb.conditions import Attr
from pydantic_core import TzInfo

from aiodynamodb import (
    DynamoDB,
    DynamoModel,
    ProjectionAttr,
    TransactConditionCheck,
    TransactDelete,
    TransactGet,
    TransactPut,
    TransactUpdate,
    UpdateAttr,
    table,
)
from aiodynamodb.custom_types import Timestamp
from tests.entities import Basket, ComplexOrder, Item, User


async def test_transact_write_applies_put_delete_and_condition(users_table):
    db = users_table

    await db.put(User(user_id="u1", name="Alice"))
    await db.put(User(user_id="u2", name="Bob"))

    await db.transact_write(
        [
            TransactPut(User(user_id="u3", name="Carol")),
            TransactDelete(User, hash_key="u2"),
            TransactConditionCheck(User, hash_key="u1", condition_expression=Attr("user_id").exists()),
        ],
        client_request_token="req-token",
        return_consumed_capacity=True,
        return_item_collection_metrics=True,
    )

    assert await db.get(User, hash_key="u1") == User(user_id="u1", name="Alice", email=None)
    assert await db.get(User, hash_key="u2") is None
    assert await db.get(User, hash_key="u3") == User(user_id="u3", name="Carol", email=None)


async def test_transact_write_condition_check_requires_expression():
    db = DynamoDB()
    with pytest.raises(TypeError):
        await db.transact_write([TransactConditionCheck(User, hash_key="u1")])


async def test_transact_get_parses_models_and_serializes_custom_keys(dynamo_resource):
    db = dynamo_resource
    await db.create_table(User)
    await db.create_table(ComplexOrder)

    basket = Basket(items=[Item(qty=1, price=10.9, name="foo")])
    await db.put(User(user_id="u1", name="Alice", email="alice@example.com"))
    await db.put(
        ComplexOrder(
            order_id="o1",
            created_at=datetime(2020, 1, 1, tzinfo=TzInfo(0)),
            total=100,
            basket=basket,
        )
    )

    results = await db.transact_get(
        [
            TransactGet(User, hash_key="u1"),
            TransactGet(
                ComplexOrder,
                hash_key="o1",
                range_key=datetime(2020, 1, 1, tzinfo=TzInfo(0)),
            ),
            TransactGet(User, hash_key="missing"),
        ],
        return_consumed_capacity=True,
    )

    assert results == [
        User(user_id="u1", name="Alice", email="alice@example.com"),
        ComplexOrder(
            order_id="o1",
            created_at=datetime(2020, 1, 1, tzinfo=TzInfo(0)),
            total=100,
            basket=basket,
        ),
        None,
    ]


async def test_transact_get_accepts_projection_expression(dynamo_resource):
    db = dynamo_resource
    await db.create_table(User)
    await db.put(User(user_id="u1", name="Alice", email="alice@example.com"))

    results = await db.transact_get(
        [
            TransactGet(
                User,
                hash_key="u1",
                projection_expression=[ProjectionAttr("user_id"), ProjectionAttr("name")],
            )
        ],
        cast=False,
    )

    assert results[0] is not None
    assert results[0]["user_id"] == "u1"
    assert results[0]["name"] == "Alice"


async def test_transact_write_supports_update_operation(complex_order_table):
    db = complex_order_table
    basket = Basket(items=[Item(qty=1, price=10.9, name="foo")])
    await db.put(
        ComplexOrder(
            order_id="o1",
            created_at=datetime(2020, 1, 1, tzinfo=TzInfo(0)),
            total=100,
            basket=basket,
        )
    )

    await db.transact_write([
        TransactUpdate(
            ComplexOrder,
            hash_key="o1",
            range_key=datetime(2020, 1, 1, tzinfo=TzInfo(0)),
            update_expression={UpdateAttr("total").set(250)},
            condition_expression=Attr("total").gte(100),
        )
    ])

    updated = await db.get(
        ComplexOrder,
        hash_key="o1",
        range_key=datetime(2020, 1, 1, tzinfo=TzInfo(0)),
    )
    assert updated is not None
    assert updated.total == 250


async def test_transact_write_update_serializes_timestamp_fields(dynamo_resource):
    @table("transact_update_events", hash_key="event_id")
    class Event(DynamoModel):
        event_id: str
        processed_at: Timestamp | None = None

    db = dynamo_resource
    await db.create_table(Event)
    await db.put(Event(event_id="e1"))

    ts = datetime(2020, 1, 1, tzinfo=TzInfo(0))
    await db.transact_write([
        TransactUpdate(
            Event,
            hash_key="e1",
            update_expression={UpdateAttr("processed_at").set(ts)},
        )
    ])

    updated = await db.get(Event, hash_key="e1")
    assert updated == Event(event_id="e1", processed_at=ts)


async def test_transact_write_update_supports_nested_field_paths(complex_order_table):
    db = complex_order_table
    basket = Basket(items=[Item(qty=1, price=10.9, name="foo")])
    created_at = datetime(2020, 1, 1, tzinfo=TzInfo(0))
    await db.put(
        ComplexOrder(
            order_id="o1",
            created_at=created_at,
            total=100,
            basket=basket,
        )
    )

    await db.transact_write([
        TransactUpdate(
            ComplexOrder,
            hash_key="o1",
            range_key=created_at,
            update_expression={UpdateAttr("basket.items.qty").set(8)},
        )
    ])

    updated = await db.get(ComplexOrder, hash_key="o1", range_key=created_at)
    assert updated is not None
    assert updated.basket.items[0].qty == 8
