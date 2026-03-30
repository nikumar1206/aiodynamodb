from boto3.dynamodb.conditions import Attr

from aiodynamodb import ProjectionAttr
from tests.unit.entities import Order, User


async def test_scan_returns_all_items(db):
    await db.put(User(user_id="u1", name="Alice"))
    await db.put(User(user_id="u2", name="Bob"))
    await db.put(User(user_id="u3", name="Carol"))

    items = [item async for page in db.scan(User) for item in page.items]

    assert len(items) == 3
    assert {u.user_id for u in items} == {"u1", "u2", "u3"}


async def test_scan_empty_table(db):
    items = [item async for page in db.scan(User) for item in page.items]
    assert items == []


async def test_scan_with_filter_expression(db):
    await db.put(Order(order_id="o1", created_at="2026-01-01", total=100))
    await db.put(Order(order_id="o2", created_at="2026-01-01", total=500))
    await db.put(Order(order_id="o3", created_at="2026-01-01", total=200))

    items = [item async for page in db.scan(Order, filter_expression=Attr("total").gte(300)) for item in page.items]

    assert len(items) == 1
    assert items[0].order_id == "o2"


async def test_scan_pagination(db):
    for i in range(5):
        await db.put(User(user_id=f"u{i}", name=f"User{i}"))

    pages = [page async for page in db.scan(User, limit=2)]

    # each page respects the limit; all items are returned across pages
    assert all(len(p.items) <= 2 for p in pages)
    all_ids = {item.user_id for page in pages for item in page.items}
    assert all_ids == {f"u{i}" for i in range(5)}
    # all pages except the last have a pagination cursor
    assert pages[-1].last_evaluated_key is None


async def test_scan_with_projection(db):
    await db.put(User(user_id="u1", name="Alice", email="alice@example.com"))

    items = [
        item
        async for page in db.scan(User, projection_expression=[ProjectionAttr("user_id"), ProjectionAttr("name")])
        for item in page.items
    ]

    assert len(items) == 1
    assert items[0].user_id == "u1"
    assert items[0].name == "Alice"
    assert items[0].email is None


async def test_scan_index(db):
    await db.put(Order(order_id="o1", created_at="2026-01-01", total=100))
    await db.put(Order(order_id="o2", created_at="2026-01-02", total=100))
    await db.put(Order(order_id="o3", created_at="2026-01-01", total=999))

    items = [
        item
        async for page in db.scan(Order, index_name="order_gsi", filter_expression=Attr("total").eq(100))
        for item in page.items
    ]

    assert len(items) == 2
    assert {i.order_id for i in items} == {"o1", "o2"}
