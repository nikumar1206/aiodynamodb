from datetime import datetime

from pydantic_core import TzInfo

from aiodynamodb import DynamoDB, DynamoModel, UpdateAttr, table
from aiodynamodb.custom_types import Timestamp
from tests.entities import Basket, ComplexOrder, Item, User


async def test_update_supports_high_level_update_expression(users_table):
    db = DynamoDB()

    await db.put(User(user_id="u1", name="Alice", email="alice@example.com"))

    updated = await db.update(
        User,
        hash_key="u1",
        update_expression={UpdateAttr("name").set("Bob")},
        return_values="ALL_NEW",
    )

    assert updated == User(user_id="u1", name="Bob", email="alice@example.com")


async def test_update_serializes_timestamp_fields(dynamo_resource):
    @table("update_events", hash_key="event_id")
    class Event(DynamoModel):
        event_id: str
        processed_at: Timestamp | None = None

    db = dynamo_resource
    await db.create_table(Event)
    await db.put(Event(event_id="e1"))

    ts = datetime(2020, 1, 1, tzinfo=TzInfo(0))
    updated = await db.update(
        Event,
        hash_key="e1",
        update_expression={UpdateAttr("processed_at").set(ts)},
        return_values="ALL_NEW",
    )

    assert updated == Event(event_id="e1", processed_at=ts)


async def test_update_supports_nested_field_paths(complex_order_table):
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

    updated = await db.update(
        ComplexOrder,
        hash_key="o1",
        range_key=created_at,
        update_expression={UpdateAttr("basket.items.qty").set(7)},
        return_values="ALL_NEW",
    )

    assert updated is not None
    assert updated.basket.items[0].qty == 7


async def test_update_can_return_raw_item(users_table):
    db = DynamoDB()

    await db.put(User(user_id="u1", name="Alice", email="alice@example.com"))

    updated = await db.update(
        User,
        hash_key="u1",
        update_expression={UpdateAttr("name").set("Bob")},
        return_values="ALL_NEW",
        cast=False,
    )

    assert updated == {
        "user_id": "u1",
        "name": "Bob",
        "email": "alice@example.com",
    }


async def test_update_returns_none_without_return_values(users_table):
    db = DynamoDB()

    await db.put(User(user_id="u1", name="Alice", email="alice@example.com"))

    updated = await db.update(
        User,
        hash_key="u1",
        update_expression={UpdateAttr("name").set("Bob")},
    )

    assert updated is None
