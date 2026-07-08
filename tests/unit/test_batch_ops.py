from datetime import datetime

import pytest
from pydantic_core import TzInfo

from aiodynamodb import BatchDelete, BatchGet, BatchPut, ProjectionAttr
from tests.unit.entities import Basket, ComplexOrder, Item, User, UserType, UserTypeT, UserVersion


async def test_batch_write_applies_put_and_delete(db):
    await db.put(User(user_id="u2", name="Bob"))

    result = await db.batch_write(
        [
            BatchPut(User(user_id="u1", name="Alice")),
            BatchDelete(User, hash_key="u2"),
        ],
        return_consumed_capacity=True,
        return_item_collection_metrics=True,
    )

    assert result.unprocessed_items == {}
    assert await db.get(User, hash_key="u1") == User(user_id="u1", name="Alice", email=None)
    assert await db.get(User, hash_key="u2") is None


async def test_batch_get_groups_requests_and_parses_typed_models(db):
    basket = Basket(items=[Item(qty=1, price=10.9, name="foo")])
    await db.put(User(user_id="u1", name="Alice", email="alice@example.com"))
    await db.put(
        ComplexOrder(
            order_id="o1",
            created_at=datetime(2020, 1, 1, tzinfo=TzInfo()),
            total=100,
            basket=basket,
        )
    )

    result = await db.batch_get(
        [
            BatchGet(User, hash_key="u1", projection_expression=[ProjectionAttr("user_id"), ProjectionAttr("name")]),
            BatchGet(
                ComplexOrder, hash_key="o1", range_key=datetime(2020, 1, 1, tzinfo=TzInfo()), consistent_read=True
            ),
        ],
        return_consumed_capacity=True,
    )

    assert result.items[User] == [User(user_id="u1", name="Alice", email=None)]
    assert result.items[ComplexOrder] == [
        ComplexOrder(
            order_id="o1",
            created_at=datetime(2020, 1, 1, tzinfo=TzInfo()),
            total=100,
            basket=basket,
        )
    ]
    assert result.unprocessed_keys == {}


async def test_batch_get_rejects_conflicting_projection_for_same_table(db):
    with pytest.raises(ValueError):
        await db.batch_get([
            BatchGet(User, hash_key="u1", projection_expression=[ProjectionAttr("user_id")]),
            BatchGet(User, hash_key="u2", projection_expression=[ProjectionAttr("name")]),
        ])


async def test_batch_get_returns_model_instances(db):
    await db.put(User(user_id="u1", name="Alice", email="alice@example.com"))

    result = await db.batch_get([BatchGet(User, hash_key="u1")])

    assert result.items[User] == [User(user_id="u1", name="Alice", email="alice@example.com")]


async def test_batch_write_supports_enum_keys(db):
    await db.batch_write([
        BatchPut(UserType(user_type=UserTypeT.bar, name="Alice")),
        BatchPut(UserVersion(user_id="u1", user_type=UserTypeT.foo, name="Bob")),
    ])

    await db.batch_write([
        BatchDelete(UserType, hash_key=UserTypeT.bar),
        BatchDelete(UserVersion, hash_key="u1", range_key=UserTypeT.foo),
    ])

    assert await db.get(UserType, hash_key=UserTypeT.bar) is None
    assert await db.get(UserVersion, hash_key="u1", range_key=UserTypeT.foo) is None


async def test_batch_get_supports_enum_keys_and_projection(db):
    await db.put(UserType(user_type=UserTypeT.bar, name="Alice"))
    await db.put(UserVersion(user_id="u1", user_type=UserTypeT.foo, name="Bob"))

    result = await db.batch_get([
        BatchGet(
            UserType,
            hash_key=UserTypeT.bar,
            projection_expression=[ProjectionAttr("user_type"), ProjectionAttr("name")],
        ),
        BatchGet(
            UserVersion,
            hash_key="u1",
            range_key=UserTypeT.foo,
            projection_expression=[ProjectionAttr("user_id"), ProjectionAttr("user_type")],
        ),
    ])

    assert result.items[UserType] == [UserType(user_type=UserTypeT.bar, name="Alice")]
    assert result.items[UserVersion][0].user_id == "u1"
    assert result.items[UserVersion][0].user_type is UserTypeT.foo
    assert not hasattr(result.items[UserVersion][0], "name")
