from tests.integration.conftest import Order, User


async def test_put_and_get(db):
    user = User(user_id="u1", name="Alice", age=30, email="alice@example.com")
    await db.put(user)
    fetched = await db.get(User, hash_key="u1")
    assert fetched == user


async def test_get_returns_none_for_missing_item(db):
    assert await db.get(User, hash_key="nonexistent") is None


async def test_put_overwrites_existing(db):
    await db.put(User(user_id="u1", name="Alice"))
    await db.put(User(user_id="u1", name="Bob"))
    fetched = await db.get(User, hash_key="u1")
    assert fetched.name == "Bob"


async def test_delete_removes_item(db):
    await db.put(User(user_id="u1", name="Alice"))
    await db.delete(User(user_id="u1", name="Alice"))
    assert await db.get(User, hash_key="u1") is None


async def test_delete_nonexistent_is_noop(db):
    # DynamoDB delete is idempotent — no error on missing item
    await db.delete(User(user_id="ghost", name="Ghost"))


async def test_put_and_get_composite_key(db):
    order = Order(order_id="o1", created_at="2026-01-01", total=100, status="shipped")
    await db.put(order)
    fetched = await db.get(Order, hash_key="o1", range_key="2026-01-01")
    assert fetched == order


async def test_get_wrong_range_key_returns_none(db):
    await db.put(Order(order_id="o1", created_at="2026-01-01", total=100))
    assert await db.get(Order, hash_key="o1", range_key="2026-12-31") is None


async def test_multiple_range_keys_same_hash(db):
    for i in range(1, 4):
        await db.put(Order(order_id="o1", created_at=f"2026-01-{i:02d}", total=i * 100))

    for i in range(1, 4):
        fetched = await db.get(Order, hash_key="o1", range_key=f"2026-01-{i:02d}")
        assert fetched is not None
        assert fetched.total == i * 100


async def test_all_optional_fields_roundtrip(db):
    user = User(user_id="u1", name="Alice", age=42, email="a@b.com", active=False)
    await db.put(user)
    fetched = await db.get(User, hash_key="u1")
    assert fetched.age == 42
    assert fetched.email == "a@b.com"
    assert fetched.active is False


async def test_none_optional_field_roundtrip(db):
    await db.put(User(user_id="u1", name="Alice", email=None))
    fetched = await db.get(User, hash_key="u1")
    assert fetched.email is None


async def test_consistent_read(db):
    await db.put(User(user_id="u1", name="Alice"))
    fetched = await db.get(User, hash_key="u1", consistent_reads=True)
    assert fetched is not None
    assert fetched.name == "Alice"


async def test_delete_composite_key(db):
    # Two orders sharing the same hash key but different range keys.
    await db.put(Order(order_id="o1", created_at="2026-01-01", total=100))
    await db.put(Order(order_id="o1", created_at="2026-01-02", total=200))

    # Delete only the first one; the second must remain untouched.
    await db.delete(Order(order_id="o1", created_at="2026-01-01", total=100))

    assert await db.get(Order, hash_key="o1", range_key="2026-01-01") is None
    assert await db.get(Order, hash_key="o1", range_key="2026-01-02") is not None
