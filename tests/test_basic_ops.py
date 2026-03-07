from datetime import datetime

import pytest
from boto3.dynamodb.conditions import Attr, Key
from pydantic_core import TzInfo

from aiodynamodb import DynamoDB
from tests.entities import Basket, ComplexOrder, Item, Order, User


async def test_put_and_get(users_table):
    db = DynamoDB()

    user = User(user_id="u1", name="Alice", email="alice@example.com")
    await db.put(user)

    fetched = await db.get(User, hash_key="u1")
    assert fetched is not None
    assert fetched.model_dump() == {
        "user_id": "u1",
        "name": "Alice",
        "email": "alice@example.com",
    }


async def test_put_with_condition(users_table):
    db = DynamoDB()

    user = User(user_id="u1", name="Alice", email="alice@example.com")
    ex = await db.exceptions
    with pytest.raises(ex.ConditionalCheckFailedException):
        await db.put(user, condition_expression=Attr("user_id").exists())

    fetched = await db.get(User, hash_key="u1")
    assert fetched is None
    await db.put(user, condition_expression=Attr("user_id").not_exists())
    fetched = await db.get(User, hash_key="u1")
    assert fetched == User(user_id="u1", name="Alice", email="alice@example.com")


async def test_get_nonexistent(users_table):
    db = DynamoDB()

    fetched = await db.get(User, hash_key="does-not-exist")
    assert fetched is None


async def test_put_overwrites(users_table):
    db = DynamoDB()

    await db.put(User(user_id="u1", name="Alice"))
    await db.put(User(user_id="u1", name="Bob"))

    fetched = await db.get(User, hash_key="u1")
    assert fetched is not None
    assert fetched.model_dump() == {
        "user_id": "u1",
        "name": "Bob",
        "email": None,
    }


async def test_composite_key_put_and_get(orders_table):
    db = DynamoDB()

    order = Order(order_id="o1", created_at="2026-01-01T00:00:00", total=100)
    await db.put(order)

    fetched = await db.get(Order, hash_key="o1", range_key="2026-01-01T00:00:00")
    assert fetched is not None
    assert fetched.model_dump() == {
        "order_id": "o1",
        "created_at": "2026-01-01T00:00:00",
        "total": 100,
    }


async def test_composite_key_different_range_keys(orders_table):
    db = DynamoDB()

    await db.put(Order(order_id="o1", created_at="2026-01-01", total=100))
    await db.put(Order(order_id="o1", created_at="2026-01-02", total=200))

    first = await db.get(Order, hash_key="o1", range_key="2026-01-01")
    second = await db.get(Order, hash_key="o1", range_key="2026-01-02")

    assert first is not None
    assert first.model_dump() == {
        "order_id": "o1",
        "created_at": "2026-01-01",
        "total": 100,
    }
    assert second is not None
    assert second.model_dump() == {
        "order_id": "o1",
        "created_at": "2026-01-02",
        "total": 200,
    }


async def test_query_returns_paginated_results(orders_table):
    db = DynamoDB()

    await db.put(Order(order_id="o1", created_at="2026-01-01", total=100))
    await db.put(Order(order_id="o1", created_at="2026-01-02", total=200))
    await db.put(Order(order_id="o1", created_at="2026-01-03", total=300))
    await db.put(Order(order_id="o2", created_at="2026-01-01", total=999))

    pages = []
    async for page in db.query(
            Order,
            key_condition_expression=Key("order_id").eq("o1"),
            limit=2,
            scan_index_forward=True,
    ):
        pages.append(page)

    assert len(pages) == 2
    assert pages[0].last_evaluated_key is not None
    assert pages[1].last_evaluated_key is None
    assert [item.created_at for page in pages for item in page.items] == [
        "2026-01-01",
        "2026-01-02",
        "2026-01-03",
    ]


async def test_query_supports_exclusive_start_key(orders_table):
    db = DynamoDB()

    await db.put(Order(order_id="o1", created_at="2026-01-01", total=100))
    await db.put(Order(order_id="o1", created_at="2026-01-02", total=200))
    await db.put(Order(order_id="o1", created_at="2026-01-03", total=300))

    first_page = None
    async for page in db.query(
            Order,
            key_condition_expression=Key("order_id").eq("o1"),
            limit=1,
            scan_index_forward=True,
    ):
        first_page = page
        break

    assert first_page is not None
    assert first_page.last_evaluated_key is not None
    assert [item.created_at for item in first_page.items] == ["2026-01-01"]

    remaining = []
    async for page in db.query(
            Order,
            key_condition_expression=Key("order_id").eq("o1"),
            exclusive_start_key=first_page.last_evaluated_key,
            scan_index_forward=True,
    ):
        remaining.extend(page.items)

    assert [item.created_at for item in remaining] == ["2026-01-02", "2026-01-03"]


async def test_query_applies_filter_expression(orders_table):
    db = DynamoDB()

    await db.put(Order(order_id="o1", created_at="2026-01-01", total=100))
    await db.put(Order(order_id="o1", created_at="2026-01-02", total=200))
    await db.put(Order(order_id="o1", created_at="2026-01-03", total=300))

    filtered = []
    async for page in db.query(
            Order,
            key_condition_expression=Key("order_id").eq("o1"),
            filter_expression=Attr("total").gte(200),
            scan_index_forward=True,
    ):
        filtered.extend(page.items)

    assert [item.total for item in filtered] == [200, 300]


async def test_query_index(orders_table):
    db = DynamoDB()

    await db.put(Order(order_id="o1", created_at="2026-01-01", total=100))
    await db.put(Order(order_id="o1", created_at="2026-01-02", total=200))
    await db.put(Order(order_id="o1", created_at="2026-01-03", total=300))

    filtered = []
    async for page in db.query(
            Order,
            index_name="order_gsi",
            key_condition_expression=Key("order_id").eq("o1"),
            filter_expression=Attr("total").gte(200),
            scan_index_forward=True,
    ):
        filtered.extend(page.items)

    assert [item.total for item in filtered] == [200, 300]


async def test_query_lsi_index(orders_table):
    db = DynamoDB()

    await db.put(Order(order_id="o1", created_at="2026-01-01", total=100))
    await db.put(Order(order_id="o1", created_at="2026-01-02", total=200))
    await db.put(Order(order_id="o1", created_at="2026-01-03", total=300))

    filtered = []
    async for page in db.query(
            Order,
            index_name="order_lsi",
            key_condition_expression=Key("order_id").eq("o1"),
            filter_expression=Attr("total").gte(200),
            scan_index_forward=True,
    ):
        filtered.extend(page.items)

    assert [item.total for item in filtered] == [200, 300]


async def test_complex_item(complex_order_table):
    db = DynamoDB()
    basket = Basket(items=[Item(qty=1, price=10.9, name="foo")])
    await db.put(
        ComplexOrder(
            order_id="o1",
            created_at=datetime(2020, 1, 1, tzinfo=TzInfo(0)),
            total=100,
            basket=basket
        )
    )
    await db.put(
        ComplexOrder(
            order_id="o1",
            created_at=datetime(2020, 1, 2, tzinfo=TzInfo(0)),
            total=200,
            basket=basket
        )
    )
    await db.put(
        ComplexOrder(
            order_id="o1",
            created_at=datetime(2020, 1, 3, tzinfo=TzInfo(0)),
            total=300,
            basket=basket
        )
    )

    filtered = []
    async for page in db.query(
            ComplexOrder,
            index_name="order_lsi",
            key_condition_expression=Key("order_id").eq("o1"),
            filter_expression=Attr("total").gte(200),
            scan_index_forward=True,
    ):
        filtered.extend(page.items)

    assert filtered == [
        ComplexOrder(
            order_id='o1',
            created_at=datetime(2020, 1, 2, tzinfo=TzInfo(0)),
            total=200,
            basket=Basket(
                items=[
                    Item(qty=1, price=10.9, name='foo')
                ]
            )
        ),
        ComplexOrder(
            order_id='o1',
            created_at=datetime(2020, 1, 3, tzinfo=TzInfo(0)),
            total=300,
                     basket=Basket(
                         items=[
                             Item(qty=1, price=10.9, name='foo')
                         ]
                     )
        )
    ]
