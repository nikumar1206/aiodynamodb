from boto3.dynamodb.conditions import Attr, Key

from aiodynamodb import ProjectionAttr
from tests.integration.conftest import Order, User


async def test_get_projection_returns_only_requested_fields(db):
    await db.put(User(user_id="u1", name="Alice", age=30, email="alice@example.com"))

    user = await db.get(
        User,
        hash_key="u1",
        projection_expression=[ProjectionAttr("user_id"), ProjectionAttr("name")],
    )

    assert user.user_id == "u1"
    assert user.name == "Alice"
    assert user.age is None
    assert user.email is None


async def test_get_projection_with_optional_field(db):
    await db.put(User(user_id="u1", name="Alice", age=30))

    user = await db.get(
        User,
        hash_key="u1",
        projection_expression=[ProjectionAttr("user_id"), ProjectionAttr("name"), ProjectionAttr("age")],
    )

    assert user.age == 30
    assert user.email is None  # not projected, falls back to default


async def test_query_projection(db):
    await db.put(Order(order_id="o1", created_at="2026-01-01", total=100, status="shipped"))

    items = [
        item
        async for page in db.query(
            Order,
            key_condition_expression=Key("order_id").eq("o1"),
            projection_expression=[ProjectionAttr("order_id"), ProjectionAttr("created_at"), ProjectionAttr("total")],
        )
        for item in page.items
    ]

    assert len(items) == 1
    assert items[0].order_id == "o1"
    assert items[0].total == 100
    assert items[0].status == "pending"  # not projected — falls back to model default


async def test_scan_projection(db):
    await db.put(User(user_id="u1", name="Alice", age=30, email="alice@example.com"))
    await db.put(User(user_id="u2", name="Bob", age=25, email="bob@example.com"))

    items = [
        item
        async for page in db.scan(
            User,
            projection_expression=[ProjectionAttr("user_id"), ProjectionAttr("age")],
        )
        for item in page.items
    ]

    assert len(items) == 2
    assert all(i.age is not None for i in items)
    assert all(i.email is None for i in items)  # not projected


async def test_get_projection_composite_key(db):
    await db.put(Order(order_id="o1", created_at="2026-01-01", total=500, status="shipped"))

    order = await db.get(
        Order,
        hash_key="o1",
        range_key="2026-01-01",
        projection_expression=[ProjectionAttr("order_id"), ProjectionAttr("created_at"), ProjectionAttr("status")],
    )

    assert order.order_id == "o1"
    assert order.status == "shipped"
    assert order.total == 0  # not projected — falls back to model default (int default is 0 via Pydantic)


async def test_transact_get_with_projection(db):
    from aiodynamodb import TransactGet

    await db.put(User(user_id="u1", name="Alice", age=30, email="alice@example.com"))

    results = await db.transact_get([
        TransactGet(User, hash_key="u1", projection_expression=[ProjectionAttr("user_id"), ProjectionAttr("name")]),
    ])

    assert results[0].user_id == "u1"
    assert results[0].name == "Alice"
    assert results[0].age is None
    assert results[0].email is None


async def test_projection_numeric_field_type_coercion(db):
    """Projected numeric fields must come back as int, not Decimal."""
    await db.put(User(user_id="u1", name="Alice", age=30))

    user = await db.get(
        User,
        hash_key="u1",
        projection_expression=[ProjectionAttr("user_id"), ProjectionAttr("age")],
    )

    assert user.age == 30
    assert isinstance(user.age, int)


async def test_query_projection_with_filter(db):
    """Projection and filter in the same query must not produce ExpressionAttributeNames conflicts."""
    await db.put(Order(order_id="o1", created_at="2026-01-01", total=100, status="shipped"))
    await db.put(Order(order_id="o1", created_at="2026-01-02", total=200, status="pending"))
    await db.put(Order(order_id="o1", created_at="2026-01-03", total=50, status="shipped"))

    items = [
        item
        async for page in db.query(
            Order,
            key_condition_expression=Key("order_id").eq("o1"),
            filter_expression=Attr("status").eq("shipped"),
            projection_expression=[ProjectionAttr("order_id"), ProjectionAttr("created_at"), ProjectionAttr("total")],
        )
        for item in page.items
    ]

    assert len(items) == 2
    assert {i.created_at for i in items} == {"2026-01-01", "2026-01-03"}
    assert all(i.total is not None for i in items)
    assert all(i.status == "pending" for i in items)  # not projected — falls back to model default


async def test_scan_projection_with_filter(db):
    """Projection and filter in the same scan must not produce ExpressionAttributeNames conflicts."""
    await db.put(User(user_id="u1", name="Alice", age=30))
    await db.put(User(user_id="u2", name="Bob", age=25))
    await db.put(User(user_id="u3", name="Carol", age=17))

    items = [
        item
        async for page in db.scan(
            User,
            filter_expression=Attr("age").gte(18),
            projection_expression=[ProjectionAttr("user_id"), ProjectionAttr("age")],
        )
        for item in page.items
    ]

    assert len(items) == 2
    assert all(i.age >= 18 for i in items)
    assert all(isinstance(i.age, int) for i in items)
    assert all(i.email is None for i in items)  # not projected — falls back to default
