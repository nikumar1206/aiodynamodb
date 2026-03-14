from aiodynamodb import DynamoDB, UpdateAttr
from tests.entities import User


async def test_update_supports_high_level_update_expression(users_table):
    db = DynamoDB()

    await db.put(User(user_id="u1", name="Alice", email="alice@example.com"))

    updated = await db.update(
        User,
        hash_key="u1",
        update_expression={UpdateAttr("name").set("Bob")},
        return_values="ALL_NEW",
    )

    assert updated == User(user_id="u1", name="Bob", email="alice@example.com")
