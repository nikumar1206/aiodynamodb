from boto3.dynamodb.conditions import Key

from aiodynamodb import BatchPut, TransactPut
from tests.integration.conftest import Event, Order, Product


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

    # total is the GSI range key — must be in key_condition_expression, not filter_expression
    items = [
        item
        async for page in db.query(
            Order,
            index_name="status_idx",
            key_condition_expression=Key("status").eq("shipped") & Key("total").lt(200),
        )
        for item in page.items
    ]

    assert len(items) == 1
    assert items[0].order_id == "o1"


async def test_lsi_query(db_event):
    # priority is the LSI range key; timestamp is the table range key.
    # The key condition on priority is only satisfiable via the LSI — querying
    # the base table with Key("priority").gte(...) would be rejected by DynamoDB.
    await db_event.put(Event(event_id="e1", timestamp="2026-01-01T10:00:00", priority=1, message="low"))
    await db_event.put(Event(event_id="e1", timestamp="2026-01-02T10:00:00", priority=5, message="high"))
    await db_event.put(Event(event_id="e1", timestamp="2026-01-03T10:00:00", priority=3, message="medium"))
    await db_event.put(Event(event_id="e2", timestamp="2026-01-01T10:00:00", priority=10, message="other"))

    items = [
        item
        async for page in db_event.query(
            Event,
            index_name="priority_idx",
            key_condition_expression=Key("event_id").eq("e1") & Key("priority").gte(3),
        )
        for item in page.items
    ]

    assert len(items) == 2
    assert all(i.priority >= 3 for i in items)
    assert all(i.event_id == "e1" for i in items)
    assert {i.message for i in items} == {"high", "medium"}


async def test_lsi_query_consistent_read(db_event):
    await db_event.put(Event(event_id="e1", timestamp="2026-01-01T10:00:00", priority=1))

    items = [
        item
        async for page in db_event.query(
            Event,
            index_name="priority_idx",
            key_condition_expression=Key("event_id").eq("e1"),
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


async def test_sparse_gsi_excludes_items_without_key(db_product):
    # p1 and p3 have a category and appear in the GSI; p2 does not and must be absent.
    await db_product.put(Product(product_id="p1", name="Widget", category="electronics"))
    await db_product.put(Product(product_id="p2", name="Unknown"))
    await db_product.put(Product(product_id="p3", name="Gadget", category="electronics"))

    items = [
        item
        async for page in db_product.query(
            Product,
            index_name="category_idx",
            key_condition_expression=Key("category").eq("electronics"),
        )
        for item in page.items
    ]

    assert len(items) == 2
    assert {i.product_id for i in items} == {"p1", "p3"}


async def test_sparse_gsi_scan_excludes_items_without_key(db_product):
    await db_product.put(Product(product_id="p1", name="Widget", category="electronics"))
    await db_product.put(Product(product_id="p2", name="Unknown"))
    await db_product.put(Product(product_id="p3", name="Gadget", category="books"))

    items = [item async for page in db_product.scan(Product, index_name="category_idx") for item in page.items]

    assert len(items) == 2
    assert {i.product_id for i in items} == {"p1", "p3"}


async def test_batch_write_sparse_gsi(db_product):
    """Items written via batch_write with a None GSI key must be excluded from the index."""
    await db_product.batch_write([
        BatchPut(Product(product_id="p1", name="Widget", category="electronics")),
        BatchPut(Product(product_id="p2", name="Unknown")),  # no category — must be absent from GSI
    ])

    items = [
        item
        async for page in db_product.query(
            Product,
            index_name="category_idx",
            key_condition_expression=Key("category").eq("electronics"),
        )
        for item in page.items
    ]

    assert len(items) == 1
    assert items[0].product_id == "p1"


async def test_transact_put_sparse_gsi(db_product):
    """Items written via transact_write with a None GSI key must be excluded from the index."""
    await db_product.transact_write([
        TransactPut(Product(product_id="p1", name="Widget", category="electronics")),
        TransactPut(Product(product_id="p2", name="Unknown")),  # no category — must be absent from GSI
    ])

    items = [
        item
        async for page in db_product.query(
            Product,
            index_name="category_idx",
            key_condition_expression=Key("category").eq("electronics"),
        )
        for item in page.items
    ]

    assert len(items) == 1
    assert items[0].product_id == "p1"
