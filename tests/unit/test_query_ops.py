from datetime import datetime

from boto3.dynamodb.conditions import Attr, Key
from pydantic_core import TzInfo

from aiodynamodb import ProjectionAttr
from tests.unit.entities import Basket, ComplexOrder, Item, Order, UserType, UserTypeT, UserVersion


async def test_query_returns_paginated_results(db):
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


async def test_query_supports_exclusive_start_key(db):
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


async def test_query_applies_filter_expression(db):
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


async def test_query_index(db):
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


async def test_query_lsi_index(db):
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


async def test_complex_item(db):
    basket = Basket(items=[Item(qty=1, price=10.9, name="foo")])
    await db.put(
        ComplexOrder(order_id="o1", created_at=datetime(2020, 1, 1, tzinfo=TzInfo()), total=100, basket=basket)
    )
    await db.put(
        ComplexOrder(order_id="o1", created_at=datetime(2020, 1, 2, tzinfo=TzInfo()), total=200, basket=basket)
    )
    await db.put(
        ComplexOrder(order_id="o1", created_at=datetime(2020, 1, 3, tzinfo=TzInfo()), total=300, basket=basket)
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
            order_id="o1",
            created_at=datetime(2020, 1, 2, tzinfo=TzInfo()),
            total=200,
            basket=Basket(items=[Item(qty=1, price=10.9, name="foo")]),
        ),
        ComplexOrder(
            order_id="o1",
            created_at=datetime(2020, 1, 3, tzinfo=TzInfo()),
            total=300,
            basket=Basket(items=[Item(qty=1, price=10.9, name="foo")]),
        ),
    ]


async def test_query_serializes_custom_key_condition_values(db):
    basket = Basket(items=[Item(qty=1, price=10.9, name="foo")])

    await db.put(
        ComplexOrder(order_id="o1", created_at=datetime(2020, 1, 1, tzinfo=TzInfo()), total=100, basket=basket)
    )
    await db.put(
        ComplexOrder(order_id="o1", created_at=datetime(2020, 1, 2, tzinfo=TzInfo()), total=200, basket=basket)
    )
    await db.put(
        ComplexOrder(order_id="o1", created_at=datetime(2020, 1, 3, tzinfo=TzInfo()), total=300, basket=basket)
    )

    filtered = []
    async for page in db.query(
        ComplexOrder,
        key_condition_expression=(
            Key("order_id").eq("o1") & Key("created_at").gte(datetime(2020, 1, 2, tzinfo=TzInfo()))
        ),
        scan_index_forward=True,
    ):
        filtered.extend(page.items)

    assert [item.total for item in filtered] == [200, 300]


async def test_query_returns_model_instances(db):
    await db.put(Order(order_id="o1", created_at="2026-01-01", total=100))
    await db.put(Order(order_id="o1", created_at="2026-01-02", total=200))

    pages = []
    async for page in db.query(
        Order,
        key_condition_expression=Key("order_id").eq("o1"),
        scan_index_forward=True,
    ):
        pages.append(page)

    assert [item.created_at for page in pages for item in page.items] == [
        "2026-01-01",
        "2026-01-02",
    ]


async def test_query_supports_projection_expression_with_filter(db):
    basket = Basket(items=[Item(qty=1, price=10.9, name="foo")])

    await db.put(
        ComplexOrder(order_id="o1", created_at=datetime(2020, 1, 1, tzinfo=TzInfo()), total=100, basket=basket)
    )

    projected = []
    async for page in db.query(
        ComplexOrder,
        key_condition_expression=Key("order_id").eq("o1"),
        filter_expression=Attr("basket.items.qty").eq(1),
    ):
        projected.extend(page.items)

    assert len(projected) == 1
    assert projected[0].order_id == "o1"


async def test_deep_filter(db):
    basket = Basket(items=[Item(qty=1, price=10.9, name="foo")])
    basket2 = Basket(items=[Item(qty=2, price=10.9, name="foo")])

    await db.put(
        ComplexOrder(order_id="o1", created_at=datetime(2020, 1, 1, tzinfo=TzInfo()), total=100, basket=basket)
    )
    await db.put(
        ComplexOrder(order_id="o1", created_at=datetime(2020, 1, 2, tzinfo=TzInfo()), total=200, basket=basket)
    )
    await db.put(
        ComplexOrder(order_id="o1", created_at=datetime(2020, 1, 3, tzinfo=TzInfo()), total=300, basket=basket2)
    )

    filtered = []
    async for page in db.query(
        ComplexOrder,
        key_condition_expression=Key("order_id").eq("o1"),
        filter_expression=Attr("basket.items.qty").gt(1),
        scan_index_forward=True,
    ):
        filtered.extend(page.items)

    assert [item.total for item in filtered] == [300]


async def test_query_supports_enum_hash_key_condition(db):
    await db.put(UserType(user_type=UserTypeT.bar, name="Alice"))
    await db.put(UserType(user_type=UserTypeT.foo, name="Bob"))

    items = [
        item
        async for page in db.query(UserType, key_condition_expression=Key("user_type").eq(UserTypeT.bar))
        for item in page.items
    ]

    assert items == [UserType(user_type=UserTypeT.bar, name="Alice")]


async def test_query_supports_enum_range_key_condition_and_projection(db):
    await db.put(UserVersion(user_id="u1", user_type=UserTypeT.foo, name="Bob"))
    await db.put(UserVersion(user_id="u1", user_type=UserTypeT.bar, name="Alice"))
    await db.put(UserVersion(user_id="u2", user_type=UserTypeT.bar, name="Carol"))

    items = [
        item
        async for page in db.query(
            UserVersion,
            key_condition_expression=Key("user_id").eq("u1") & Key("user_type").gte(UserTypeT.foo),
            projection_expression=[ProjectionAttr("user_id"), ProjectionAttr("user_type")],
            scan_index_forward=True,
        )
        for item in page.items
    ]

    assert len(items) == 2
    assert [item.user_type for item in items] == [UserTypeT.foo, UserTypeT.bar]
    assert all(not hasattr(item, "name") for item in items)
