from aiodynamodb import DynamoDB, DynamoModel, table


@table("users", hash_key="user_id")
class User(DynamoModel):
    user_id: str
    name: str
    email: str | None = None


@table("orders", hash_key="order_id", range_key="created_at")
class Order(DynamoModel):
    order_id: str
    created_at: str
    total: int


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
