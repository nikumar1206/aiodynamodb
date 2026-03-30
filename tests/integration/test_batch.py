from aiodynamodb import BatchDelete, BatchGet, BatchPut, ProjectionAttr
from tests.integration.conftest import User


async def test_batch_write_puts_items(db):
    await db.batch_write([BatchPut(User(user_id=f"u{i}", name=f"User{i}")) for i in range(5)])
    for i in range(5):
        assert (await db.get(User, hash_key=f"u{i}")).name == f"User{i}"


async def test_batch_write_deletes_items(db):
    for i in range(3):
        await db.put(User(user_id=f"u{i}", name=f"User{i}"))
    await db.batch_write([BatchDelete(User, hash_key=f"u{i}") for i in range(3)])
    for i in range(3):
        assert await db.get(User, hash_key=f"u{i}") is None


async def test_batch_write_mixed_put_and_delete(db):
    await db.put(User(user_id="u0", name="ToDelete"))
    await db.put(User(user_id="u1", name="ToKeep"))

    await db.batch_write([
        BatchPut(User(user_id="u2", name="NewItem")),
        BatchDelete(User, hash_key="u0"),
    ])

    assert await db.get(User, hash_key="u0") is None
    assert await db.get(User, hash_key="u1") is not None
    assert await db.get(User, hash_key="u2") is not None


async def test_batch_write_result_has_no_unprocessed_items(db):
    result = await db.batch_write([BatchPut(User(user_id="u1", name="Alice"))])
    assert result.unprocessed_items == {}


async def test_batch_get_all_items(db):
    for i in range(5):
        await db.put(User(user_id=f"u{i}", name=f"User{i}"))

    result = await db.batch_get([BatchGet(User, hash_key=f"u{i}") for i in range(5)])

    assert len(result.items[User]) == 5
    assert {u.user_id for u in result.items[User]} == {f"u{i}" for i in range(5)}
    assert result.unprocessed_keys == {}


async def test_batch_get_partial_miss(db):
    # DynamoDB silently omits missing keys — no error raised and they do NOT
    # appear in unprocessed_keys (that field is for throttled keys only).
    await db.put(User(user_id="u1", name="Alice"))

    result = await db.batch_get([
        BatchGet(User, hash_key="u1"),
        BatchGet(User, hash_key="missing"),
    ])

    assert len(result.items[User]) == 1
    assert result.items[User][0].user_id == "u1"
    assert result.unprocessed_keys == {}


async def test_batch_get_all_missing(db):
    result = await db.batch_get([BatchGet(User, hash_key="x"), BatchGet(User, hash_key="y")])
    assert result.items.get(User, []) == []


async def test_batch_get_with_projection(db):
    await db.put(User(user_id="u1", name="Alice", age=30, email="a@b.com"))

    from aiodynamodb import ProjectionAttr

    result = await db.batch_get([
        BatchGet(User, hash_key="u1", projection_expression=[ProjectionAttr("user_id"), ProjectionAttr("name")]),
    ])

    user = result.items[User][0]
    assert user.user_id == "u1"
    assert user.name == "Alice"
    assert user.age is None  # not projected
    assert user.email is None  # not projected


async def test_batch_get_projection_omits_required_field(db):
    """batch_get with a projection that excludes a required field must not crash."""
    await db.put(User(user_id="u1", name="Alice", age=30))

    result = await db.batch_get([
        BatchGet(User, hash_key="u1", projection_expression=[ProjectionAttr("user_id"), ProjectionAttr("age")]),
    ])

    users = result.items[User]
    assert len(users) == 1
    assert users[0].user_id == "u1"
    assert users[0].age == 30
    assert isinstance(users[0].age, int)  # must be int, not Decimal
    assert users[0].email is None  # not projected — falls back to default


async def test_batch_write_25_items(db):
    """Boundary: 25 items is the DynamoDB batch write limit."""
    await db.batch_write([BatchPut(User(user_id=f"u{i:02d}", name=f"User{i}")) for i in range(25)])
    result = await db.batch_get([BatchGet(User, hash_key=f"u{i:02d}") for i in range(25)])
    assert len(result.items[User]) == 25
