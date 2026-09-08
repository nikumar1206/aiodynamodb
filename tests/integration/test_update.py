import pytest
from boto3.dynamodb.conditions import Attr

from aiodynamodb import UpdateAttr
from tests.integration.conftest import Order, User, UserType, UserTypeT, UserVersion


async def test_update_set_none_removes_attribute(db):
    """UpdateAttr.set(None) must behave like .remove(), not store a NULL."""
    await db.put(User(user_id="u1", name="Alice", email="alice@example.com"))
    await db.update(User, hash_key="u1", update_expression={UpdateAttr("email").set(None)})

    fetched = await db.get(User, hash_key="u1")
    assert fetched.email is None

    # The attribute must be absent, not NULL — Attr.exists() should fail.
    ex = await db.exceptions()
    with pytest.raises(ex.ConditionalCheckFailedException):
        await db.put(
            User(user_id="u1", name="Alice"),
            condition_expression=Attr("email").exists(),
        )


async def test_update_returns_updated_new(db):
    """UPDATED_NEW returns only the changed attributes; absent fields fall back to defaults."""
    await db.put(User(user_id="u1", name="Alice", age=20))
    updated = await db.update(
        User,
        hash_key="u1",
        update_expression={UpdateAttr("name").set("Bob")},
        return_values="UPDATED_NEW",
    )
    assert updated is not None
    assert updated.name == "Bob"
    assert updated.age is None  # not in UPDATED_NEW response — falls back to default


async def test_update_returns_updated_old(db):
    """UPDATED_OLD returns only the pre-update values of changed attributes."""
    await db.put(User(user_id="u1", name="Alice", age=20))
    old = await db.update(
        User,
        hash_key="u1",
        update_expression={UpdateAttr("name").set("Bob")},
        return_values="UPDATED_OLD",
    )
    assert old is not None
    assert old.name == "Alice"
    assert old.age is None  # not in UPDATED_OLD response — falls back to default


async def test_update_set_single_field(db):
    await db.put(User(user_id="u1", name="Alice"))
    await db.update(User, hash_key="u1", update_expression={UpdateAttr("name").set("Bob")})
    assert (await db.get(User, hash_key="u1")).name == "Bob"


async def test_update_set_multiple_fields(db):
    await db.put(User(user_id="u1", name="Alice", age=20))
    await db.update(
        User,
        hash_key="u1",
        update_expression={UpdateAttr("name").set("Bob"), UpdateAttr("age").set(30)},
    )
    fetched = await db.get(User, hash_key="u1")
    assert fetched.name == "Bob"
    assert fetched.age == 30


async def test_update_returns_all_new(db):
    await db.put(User(user_id="u1", name="Alice", age=20))
    updated = await db.update(
        User,
        hash_key="u1",
        update_expression={UpdateAttr("name").set("Bob")},
        return_values="ALL_NEW",
    )
    assert updated is not None
    assert updated.name == "Bob"
    assert updated.age == 20  # unchanged field present in ALL_NEW


async def test_update_returns_none_without_return_values(db):
    await db.put(User(user_id="u1", name="Alice"))
    result = await db.update(
        User,
        hash_key="u1",
        update_expression={UpdateAttr("name").set("Bob")},
    )
    assert result is None


async def test_update_add_creates_attribute(db):
    """ADD on a non-existent numeric attribute initialises it to the given value."""
    await db.put(User(user_id="u1", name="Alice"))
    await db.update(User, hash_key="u1", update_expression={UpdateAttr("age").add(1)})
    assert (await db.get(User, hash_key="u1")).age == 1


async def test_update_add_increments_existing(db):
    await db.put(User(user_id="u1", name="Alice", age=5))
    await db.update(User, hash_key="u1", update_expression={UpdateAttr("age").add(3)})
    assert (await db.get(User, hash_key="u1")).age == 8


async def test_update_remove_optional_field(db):
    await db.put(User(user_id="u1", name="Alice", email="alice@example.com"))
    await db.update(User, hash_key="u1", update_expression={UpdateAttr("email").remove()})
    assert (await db.get(User, hash_key="u1")).email is None


async def test_update_set_on_nonexistent_item_creates_it(db):
    """SET on a non-existent item creates a new item with the key + updated fields."""
    updated = await db.update(
        User,
        hash_key="u99",
        update_expression={UpdateAttr("name").set("Ghost")},
        return_values="ALL_NEW",
    )
    assert updated is not None
    assert updated.user_id == "u99"
    assert updated.name == "Ghost"


async def test_update_composite_key(db):
    await db.put(Order(order_id="o1", created_at="2026-01-01", total=100))
    await db.update(
        Order,
        hash_key="o1",
        range_key="2026-01-01",
        update_expression={UpdateAttr("total").set(999)},
    )
    fetched = await db.get(Order, hash_key="o1", range_key="2026-01-01")
    assert fetched.total == 999


async def test_update_with_condition_passes(db):
    await db.put(User(user_id="u1", name="Alice", age=25))
    await db.update(
        User,
        hash_key="u1",
        update_expression={UpdateAttr("name").set("Bob")},
        condition_expression=Attr("age").gte(18),
    )
    assert (await db.get(User, hash_key="u1")).name == "Bob"


async def test_update_with_condition_fails(db):
    await db.put(User(user_id="u1", name="Alice", age=16))
    ex = await db.exceptions()
    with pytest.raises(ex.ConditionalCheckFailedException):
        await db.update(
            User,
            hash_key="u1",
            update_expression={UpdateAttr("name").set("Bob")},
            condition_expression=Attr("age").gte(18),
        )
    assert (await db.get(User, hash_key="u1")).name == "Alice"


async def test_update_set_and_remove_in_one_call(db):
    await db.put(User(user_id="u1", name="Alice", age=30, email="alice@example.com"))
    await db.update(
        User,
        hash_key="u1",
        update_expression={UpdateAttr("name").set("Bob"), UpdateAttr("email").remove()},
    )
    fetched = await db.get(User, hash_key="u1")
    assert fetched.name == "Bob"
    assert fetched.email is None
    assert fetched.age == 30  # untouched


async def test_update_returns_all_old(db):
    await db.put(User(user_id="u1", name="Alice", age=20, email="alice@example.com"))
    old = await db.update(
        User,
        hash_key="u1",
        update_expression={UpdateAttr("name").set("Bob"), UpdateAttr("age").set(99)},
        return_values="ALL_OLD",
    )
    assert old is not None
    assert old.name == "Alice"
    assert old.age == 20
    assert old.email == "alice@example.com"
    # Confirm the update was still applied.
    current = await db.get(User, hash_key="u1")
    assert current.name == "Bob"
    assert current.age == 99


async def test_update_supports_enum_hash_key(db):
    await db.put(UserType(user_type=UserTypeT.bar, name="Alice"))

    updated = await db.update(
        UserType,
        hash_key=UserTypeT.bar,
        update_expression={UpdateAttr("name").set("Alice Updated")},
        return_values="ALL_NEW",
    )

    assert updated == UserType(user_type=UserTypeT.bar, name="Alice Updated")


async def test_update_supports_enum_range_key(db):
    await db.put(UserVersion(user_id="u1", user_type=UserTypeT.foo, name="Bob"))

    updated = await db.update(
        UserVersion,
        hash_key="u1",
        range_key=UserTypeT.foo,
        update_expression={UpdateAttr("name").set("Bob Updated")},
        return_values="ALL_NEW",
    )

    assert updated == UserVersion(user_id="u1", user_type=UserTypeT.foo, name="Bob Updated")


async def test_update_list_append_prepend_and_remove_by_index(db):
    """list_append/if_not_exists and indexed REMOVE against a real DynamoDB endpoint."""
    from pydantic import BaseModel as PydanticModel

    from aiodynamodb import DynamoModel, HashKey, table

    class Line(PydanticModel):
        sku: str
        note: str | None = None

    @table("it_list_ops")
    class Cart(DynamoModel):
        cart_id: HashKey[str]
        lines: list[Line] | None = None

    await db.create_table(Cart)
    try:
        await db.put(Cart(cart_id="c1"))

        # First append creates the list; None fields are omitted, matching put().
        created = await db.update(
            Cart,
            hash_key="c1",
            update_expression={UpdateAttr("lines").append([Line(sku="a")])},
            return_values="ALL_NEW",
        )
        assert created is not None and created.lines == [Line(sku="a")]

        prepended = await db.update(
            Cart,
            hash_key="c1",
            update_expression={UpdateAttr("lines").prepend([Line(sku="z", note="first")])},
            return_values="ALL_NEW",
        )
        assert prepended is not None and [line.sku for line in prepended.lines] == ["z", "a"]

        removed = await db.update(
            Cart,
            hash_key="c1",
            update_expression={UpdateAttr("lines").remove(0)},
            return_values="ALL_NEW",
        )
        assert removed is not None and removed.lines == [Line(sku="a")]

        # Strict mode: DynamoDB rejects list_append on a missing attribute.
        await db.put(Cart(cart_id="c2"))
        ex = await db.exceptions()
        with pytest.raises(ex.ClientError):
            await db.update(
                Cart,
                hash_key="c2",
                update_expression={UpdateAttr("lines").append([Line(sku="a")], if_not_exists=False)},
            )
    finally:
        await db.delete_table(Cart)
