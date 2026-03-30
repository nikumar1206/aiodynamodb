from boto3.dynamodb.conditions import Attr, Key

from tests.integration.conftest import Order, User


async def test_query_returns_items_for_partition(db):
    for i in range(5):
        await db.put(Order(order_id="o1", created_at=f"2026-01-{i + 1:02d}", total=i * 100))
    await db.put(Order(order_id="o2", created_at="2026-01-01", total=999))

    items = [
        item async for page in db.query(Order, key_condition_expression=Key("order_id").eq("o1")) for item in page.items
    ]

    assert len(items) == 5
    assert all(i.order_id == "o1" for i in items)


async def test_query_empty_partition_returns_nothing(db):
    items = [
        item
        async for page in db.query(Order, key_condition_expression=Key("order_id").eq("nonexistent"))
        for item in page.items
    ]
    assert items == []


async def test_query_sort_ascending(db):
    for i in range(5):
        await db.put(Order(order_id="o1", created_at=f"2026-01-{i + 1:02d}", total=i))

    items = [
        item
        async for page in db.query(Order, key_condition_expression=Key("order_id").eq("o1"), scan_index_forward=True)
        for item in page.items
    ]

    dates = [i.created_at for i in items]
    assert dates == sorted(dates)


async def test_query_sort_descending(db):
    for i in range(5):
        await db.put(Order(order_id="o1", created_at=f"2026-01-{i + 1:02d}", total=i))

    items = [
        item
        async for page in db.query(Order, key_condition_expression=Key("order_id").eq("o1"), scan_index_forward=False)
        for item in page.items
    ]

    dates = [i.created_at for i in items]
    assert dates == sorted(dates, reverse=True)


async def test_query_range_key_condition(db):
    for i in range(5):
        await db.put(Order(order_id="o1", created_at=f"2026-01-{i + 1:02d}", total=i))

    items = [
        item
        async for page in db.query(
            Order,
            key_condition_expression=Key("order_id").eq("o1") & Key("created_at").gte("2026-01-03"),
        )
        for item in page.items
    ]

    assert len(items) == 3
    assert all(i.created_at >= "2026-01-03" for i in items)


async def test_query_filter_expression_post_key(db):
    for i in range(5):
        await db.put(Order(order_id="o1", created_at=f"2026-01-{i + 1:02d}", total=i * 100))

    items = [
        item
        async for page in db.query(
            Order,
            key_condition_expression=Key("order_id").eq("o1"),
            filter_expression=Attr("total").gte(200),
        )
        for item in page.items
    ]

    assert len(items) == 3
    assert all(i.total >= 200 for i in items)


async def test_query_consistent_read(db):
    await db.put(Order(order_id="o1", created_at="2026-01-01", total=100))
    items = [
        item
        async for page in db.query(Order, key_condition_expression=Key("order_id").eq("o1"), consistent_read=True)
        for item in page.items
    ]
    assert len(items) == 1


async def test_scan_returns_all_items(db):
    for i in range(5):
        await db.put(User(user_id=f"u{i}", name=f"User{i}"))

    items = [item async for page in db.scan(User) for item in page.items]
    assert len(items) == 5
    assert {u.user_id for u in items} == {f"u{i}" for i in range(5)}


async def test_scan_empty_table(db):
    items = [item async for page in db.scan(User) for item in page.items]
    assert items == []


async def test_scan_with_filter_expression(db):
    await db.put(User(user_id="u1", name="Alice", active=True))
    await db.put(User(user_id="u2", name="Bob", active=False))
    await db.put(User(user_id="u3", name="Carol", active=True))

    items = [item async for page in db.scan(User, filter_expression=Attr("active").eq(True)) for item in page.items]

    assert len(items) == 2
    assert all(i.active for i in items)


async def test_scan_consistent_read(db):
    await db.put(User(user_id="u1", name="Alice"))
    items = [item async for page in db.scan(User, consistent_read=True) for item in page.items]
    assert len(items) == 1
