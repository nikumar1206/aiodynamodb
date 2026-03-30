from datetime import datetime

import pytest
from botocore.exceptions import ClientError
from pydantic_core import TzInfo

from aiodynamodb import DynamoModel, UpdateAttr, table
from aiodynamodb.custom_types import Timestamp
from tests.unit.entities import Basket, ComplexOrder, Item, User


async def test_update_supports_high_level_update_expression(db):
    await db.put(User(user_id="u1", name="Alice", email="alice@example.com"))

    updated = await db.update(
        User,
        hash_key="u1",
        update_expression={UpdateAttr("name").set("Bob")},
        return_values="ALL_NEW",
    )

    assert updated == User(user_id="u1", name="Bob", email="alice@example.com")


async def test_update_serializes_timestamp_fields(db):
    @table("update_events", hash_key="event_id")
    class Event(DynamoModel):
        event_id: str
        processed_at: Timestamp | None = None

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


async def test_update_supports_nested_field_paths(db):
    basket = Basket(items=[Item(qty=1, price=10.9, name="foo")])
    created_at = datetime(2020, 1, 1, tzinfo=TzInfo(0))
    await db.put(ComplexOrder(order_id="o1", created_at=created_at, total=100, basket=basket))

    updated = await db.update(
        ComplexOrder,
        hash_key="o1",
        range_key=created_at,
        update_expression={UpdateAttr("basket.items.qty").set(7)},
        return_values="ALL_NEW",
    )

    assert updated is not None
    assert updated.basket.items[0].qty == 7


async def test_update_supports_atomic_counter_increment(db):
    @table("counter_values", hash_key="counter_id")
    class Counter(DynamoModel):
        counter_id: str
        value: int = 0

    await db.create_table(Counter)
    await db.put(Counter(counter_id="c1", value=0))

    first = await db.update(
        Counter,
        hash_key="c1",
        update_expression={UpdateAttr("value").add(2)},
        return_values="ALL_NEW",
    )
    second = await db.update(
        Counter,
        hash_key="c1",
        update_expression={UpdateAttr("value").add(3)},
        return_values="ALL_NEW",
    )

    assert first == Counter(counter_id="c1", value=2)
    assert second == Counter(counter_id="c1", value=5)


async def test_update_supports_specific_indexed_list_element(db):
    basket = Basket(items=[Item(qty=1, price=10.9, name="foo"), Item(qty=2, price=5.5, name="bar")])
    created_at = datetime(2020, 1, 1, tzinfo=TzInfo(0))
    await db.put(ComplexOrder(order_id="o1", created_at=created_at, total=100, basket=basket))

    updated = await db.update(
        ComplexOrder,
        hash_key="o1",
        range_key=created_at,
        update_expression={UpdateAttr("basket.items[1].qty").set(9)},
        return_values="ALL_NEW",
    )

    assert updated is not None
    assert updated.basket.items[0].qty == 1
    assert updated.basket.items[1].qty == 9


async def test_update_returns_model_instance(db):
    await db.put(User(user_id="u1", name="Alice", email="alice@example.com"))

    updated = await db.update(
        User,
        hash_key="u1",
        update_expression={UpdateAttr("name").set("Bob")},
        return_values="ALL_NEW",
    )

    assert updated == User(user_id="u1", name="Bob", email="alice@example.com")


async def test_update_returns_none_without_return_values(db):
    await db.put(User(user_id="u1", name="Alice", email="alice@example.com"))

    updated = await db.update(
        User,
        hash_key="u1",
        update_expression={UpdateAttr("name").set("Bob")},
    )

    assert updated is None


async def test_update_supports_remove_add_and_delete_actions(db):
    @table("counter_users", hash_key="user_id")
    class CounterUser(DynamoModel):
        user_id: str
        score: int = 0
        tags: set[str] | None = None
        email: str | None = None

    await db.create_table(CounterUser)
    await db.put(CounterUser(user_id="u1", score=1, email="alice@example.com"))

    updated = await db.update(
        CounterUser,
        hash_key="u1",
        update_expression={
            UpdateAttr("score").add(3),
            UpdateAttr("email").remove(),
        },
        return_values="ALL_NEW",
    )

    assert updated == CounterUser(user_id="u1", score=4, tags=None, email=None)

    with pytest.raises(ClientError, match="ValidationException"):
        await db.update(
            CounterUser,
            hash_key="u1",
            update_expression={UpdateAttr("tags").add({"a", "b"})},
            return_values="ALL_NEW",
        )

    with pytest.raises(ClientError, match="ValidationException"):
        await db.update(
            CounterUser,
            hash_key="u1",
            update_expression={UpdateAttr("tags").delete({"b"})},
            return_values="ALL_NEW",
        )
