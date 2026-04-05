import pytest
from boto3.dynamodb.conditions import Attr

from aiodynamodb import UpdateAttr
from tests.integration.conftest import User


async def test_put_condition_not_exists_succeeds(db):
    await db.put(
        User(user_id="u1", name="Alice"),
        condition_expression=Attr("user_id").not_exists(),
    )
    assert (await db.get(User, hash_key="u1")).name == "Alice"


async def test_put_condition_not_exists_fails_on_duplicate(db):
    await db.put(User(user_id="u1", name="Alice"))
    ex = await db.exceptions()
    with pytest.raises(ex.ConditionalCheckFailedException):
        await db.put(
            User(user_id="u1", name="Bob"),
            condition_expression=Attr("user_id").not_exists(),
        )
    # original item must be untouched
    assert (await db.get(User, hash_key="u1")).name == "Alice"


async def test_put_condition_attribute_equals(db):
    await db.put(User(user_id="u1", name="Alice", age=30))
    ex = await db.exceptions()
    with pytest.raises(ex.ConditionalCheckFailedException):
        await db.put(
            User(user_id="u1", name="Bob"),
            condition_expression=Attr("age").eq(99),
        )


async def test_delete_condition_passes(db):
    await db.put(User(user_id="u1", name="Alice", age=30))
    await db.delete(
        User,
        hash_key="u1",
        condition_expression=Attr("age").eq(30),
    )
    assert await db.get(User, hash_key="u1") is None


async def test_delete_condition_fails_item_survives(db):
    await db.put(User(user_id="u1", name="Alice", age=30))
    ex = await db.exceptions()
    with pytest.raises(ex.ConditionalCheckFailedException):
        await db.delete(
            User,
            hash_key="u1",
            condition_expression=Attr("age").eq(99),
        )
    assert await db.get(User, hash_key="u1") is not None


async def test_update_condition_passes(db):
    await db.put(User(user_id="u1", name="Alice", age=30))
    await db.update(
        User,
        hash_key="u1",
        update_expression={UpdateAttr("name").set("Bob")},
        condition_expression=Attr("age").gte(18),
    )
    assert (await db.get(User, hash_key="u1")).name == "Bob"


async def test_update_condition_fails_item_unchanged(db):
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


async def test_condition_attribute_exists(db):
    await db.put(User(user_id="u1", name="Alice"))
    # email is None — Attr.exists() checks whether the attribute is present at all
    ex = await db.exceptions()
    with pytest.raises(ex.ConditionalCheckFailedException):
        await db.put(
            User(user_id="u1", name="Bob"),
            condition_expression=Attr("email").exists(),
        )


async def test_condition_begins_with(db):
    await db.put(User(user_id="u1", name="Alice"))
    ex = await db.exceptions()
    with pytest.raises(ex.ConditionalCheckFailedException):
        await db.put(
            User(user_id="u1", name="Bob"),
            condition_expression=Attr("name").begins_with("Z"),
        )


async def test_condition_between(db):
    await db.put(User(user_id="u1", name="Alice", age=25))
    await db.put(
        User(user_id="u1", name="Alice Updated"),
        condition_expression=Attr("age").between(18, 30),
    )
    assert (await db.get(User, hash_key="u1")).name == "Alice Updated"
