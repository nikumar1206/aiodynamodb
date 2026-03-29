from boto3.dynamodb.conditions import Attr, Key

from tests.integration.conftest import Order


async def test_gsi_query_by_hash_key(db):
    await db.put(Order(order_id="o1", created_at="2026-01-01", total=100, status="shipped"))
    await db.put(Order(order_id="o2", created_at="2026-01-01", total=200, status="pending"))
    await db.put(Order(order_id="o3", created_at="2026-01-02", total=150, status="shipped"))

    items = [
        item
        async for page in db.query(
            Order,
            index_name="status_idx",
            key_condition_expression=Key("status").eq("shipped"),
        )
        for item in page.items
    ]

    assert len(items) == 2
    assert {i.order_id for i in items} == {"o1", "o3"}
    assert all(i.status == "shipped" for i in items)


async def test_gsi_query_with_range_key_condition(db):
    await db.put(Order(order_id="o1", created_at="2026-01-01", total=50, status="shipped"))
    await db.put(Order(order_id="o2", created_at="2026-01-01", total=200, status="shipped"))
    await db.put(Order(order_id="o3", created_at="2026-01-02", total=300, status="shipped"))

    items = [
        item
        async for page in db.query(
            Order,
            index_name="status_idx",
            key_condition_expression=Key("status").eq("shipped") & Key("total").gte(100),
        )
        for item in page.items
    ]

    assert len(items) == 2
    assert all(i.total >= 100 for i in items)


async def test_gsi_query_returns_nothing_for_missing_value(db):
    await db.put(Order(order_id="o1", created_at="2026-01-01", total=100, status="pending"))

    items = [
        item
        async for page in db.query(
            Order,
            index_name="status_idx",
            key_condition_expression=Key("status").eq("shipped"),
        )
        for item in page.items
    ]

    assert items == []


async def test_gsi_query_with_filter(db):
    await db.put(Order(order_id="o1", created_at="2026-01-01", total=100, status="shipped"))
    await db.put(Order(order_id="o2", created_at="2026-01-02", total=500, status="shipped"))

    items = [
        item
        async for page in db.query(
            Order,
            index_name="status_idx",
            key_condition_expression=Key("status").eq("shipped"),
            filter_expression=Attr("total").lt(200),
        )
        for item in page.items
    ]

    assert len(items) == 1
    assert items[0].order_id == "o1"


async def test_lsi_query(db):
    await db.put(Order(order_id="o1", created_at="2026-01-01", total=100))
    await db.put(Order(order_id="o1", created_at="2026-01-02", total=50))
    await db.put(Order(order_id="o1", created_at="2026-01-03", total=200))

    items = [
        item
        async for page in db.query(
            Order,
            index_name="created_idx",
            key_condition_expression=Key("order_id").eq("o1"),
            scan_index_forward=True,
        )
        for item in page.items
    ]

    assert len(items) == 3
    dates = [i.created_at for i in items]
    assert dates == sorted(dates)


async def test_lsi_query_consistent_read(db):
    await db.put(Order(order_id="o1", created_at="2026-01-01", total=100))

    items = [
        item
        async for page in db.query(
            Order,
            index_name="created_idx",
            key_condition_expression=Key("order_id").eq("o1"),
            consistent_read=True,
        )
        for item in page.items
    ]

    assert len(items) == 1


async def test_gsi_scan(db):
    await db.put(Order(order_id="o1", created_at="2026-01-01", total=100, status="shipped"))
    await db.put(Order(order_id="o2", created_at="2026-01-01", total=200, status="pending"))

    items = [item async for page in db.scan(Order, index_name="status_idx") for item in page.items]

    assert len(items) == 2
    assert {i.status for i in items} == {"shipped", "pending"}
