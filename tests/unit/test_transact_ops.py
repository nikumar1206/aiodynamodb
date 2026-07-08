from datetime import datetime

import pytest
from boto3.dynamodb.conditions import Attr
from pydantic_core import TzInfo

from aiodynamodb import (
    DynamoDB,
    DynamoModel,
    HashKey,
    ProjectionAttr,
    TransactConditionCheck,
    TransactDelete,
    TransactGet,
    TransactPut,
    TransactUpdate,
    UpdateAttr,
    table,
)
from aiodynamodb.custom_types import Timestamp
from tests.unit.entities import Basket, ComplexOrder, Item, User, UserType, UserTypeT, UserVersion


async def test_transact_write_applies_put_delete_and_condition(db):
    await db.put(User(user_id="u1", name="Alice"))
    await db.put(User(user_id="u2", name="Bob"))

    await db.transact_write(
        [
            TransactPut(User(user_id="u3", name="Carol")),
            TransactDelete(User, hash_key="u2"),
            TransactConditionCheck(User, hash_key="u1", condition_expression=Attr("user_id").exists()),
        ],
        client_request_token="req-token",
        return_consumed_capacity=True,
        return_item_collection_metrics=True,
    )

    assert await db.get(User, hash_key="u1") == User(user_id="u1", name="Alice", email=None)
    assert await db.get(User, hash_key="u2") is None
    assert await db.get(User, hash_key="u3") == User(user_id="u3", name="Carol", email=None)


async def test_transact_write_condition_check_requires_expression(db):
    with pytest.raises(TypeError):
        await db.transact_write([TransactConditionCheck(User, hash_key="u1")])


async def test_transact_get_parses_models_and_serializes_custom_keys(db):
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

    results = await db.transact_get(
        [
            TransactGet(User, hash_key="u1"),
            TransactGet(ComplexOrder, hash_key="o1", range_key=datetime(2020, 1, 1, tzinfo=TzInfo())),
            TransactGet(User, hash_key="missing"),
        ],
        return_consumed_capacity=True,
    )

    assert results == [
        User(user_id="u1", name="Alice", email="alice@example.com"),
        ComplexOrder(
            order_id="o1",
            created_at=datetime(2020, 1, 1, tzinfo=TzInfo()),
            total=100,
            basket=basket,
        ),
        None,
    ]


async def test_transact_get_accepts_projection_expression(db: DynamoDB):
    await db.put(User(user_id="u1", name="Alice", email="alice@example.com"))

    results = await db.transact_get([
        TransactGet(
            User,
            hash_key="u1",
            projection_expression=[ProjectionAttr("user_id"), ProjectionAttr("name")],
        )
    ])

    user_1 = results[0]
    assert user_1 is not None
    assert user_1.user_id == "u1"
    assert user_1.name == "Alice"


async def test_transact_write_supports_update_operation(db):
    basket = Basket(items=[Item(qty=1, price=10.9, name="foo")])
    await db.put(
        ComplexOrder(order_id="o1", created_at=datetime(2020, 1, 1, tzinfo=TzInfo()), total=100, basket=basket)
    )

    await db.transact_write([
        TransactUpdate(
            ComplexOrder,
            hash_key="o1",
            range_key=datetime(2020, 1, 1, tzinfo=TzInfo()),
            update_expression={UpdateAttr("total").set(250)},
            condition_expression=Attr("total").gte(100),
        )
    ])

    updated = await db.get(ComplexOrder, hash_key="o1", range_key=datetime(2020, 1, 1, tzinfo=TzInfo()))
    assert updated is not None
    assert updated.total == 250


async def test_transact_write_update_serializes_timestamp_fields(db):
    @table("transact_update_events")
    class Event(DynamoModel):
        event_id: HashKey[str]
        processed_at: Timestamp | None = None

    await db.create_table(Event)
    await db.put(Event(event_id="e1"))

    ts = datetime(2020, 1, 1, tzinfo=TzInfo())
    await db.transact_write([
        TransactUpdate(
            Event,
            hash_key="e1",
            update_expression={UpdateAttr("processed_at").set(ts)},
        )
    ])

    updated = await db.get(Event, hash_key="e1")
    assert updated == Event(event_id="e1", processed_at=ts)


async def test_transact_write_update_supports_nested_field_paths(db):
    basket = Basket(items=[Item(qty=1, price=10.9, name="foo")])
    created_at = datetime(2020, 1, 1, tzinfo=TzInfo())
    await db.put(ComplexOrder(order_id="o1", created_at=created_at, total=100, basket=basket))

    await db.transact_write([
        TransactUpdate(
            ComplexOrder,
            hash_key="o1",
            range_key=created_at,
            update_expression={UpdateAttr("basket.items.qty").set(8)},
        )
    ])

    updated = await db.get(ComplexOrder, hash_key="o1", range_key=created_at)
    assert updated is not None
    assert updated.basket.items[0].qty == 8


async def test_transact_get_supports_enum_keys_and_projection(db):
    await db.put(UserType(user_type=UserTypeT.bar, name="Alice"))
    await db.put(UserVersion(user_id="u1", user_type=UserTypeT.foo, name="Bob"))

    results = await db.transact_get([
        TransactGet(
            UserType,
            hash_key=UserTypeT.bar,
            projection_expression=[ProjectionAttr("user_type"), ProjectionAttr("name")],
        ),
        TransactGet(
            UserVersion,
            hash_key="u1",
            range_key=UserTypeT.foo,
            projection_expression=[ProjectionAttr("user_id"), ProjectionAttr("user_type")],
        ),
    ])

    assert results[0] == UserType(user_type=UserTypeT.bar, name="Alice")
    assert results[1] is not None
    assert results[1].user_id == "u1"
    assert results[1].user_type is UserTypeT.foo


async def test_transact_write_supports_enum_key_operations(db):
    await db.put(UserType(user_type=UserTypeT.foo, name="Existing"))
    await db.put(UserVersion(user_id="u1", user_type=UserTypeT.foo, name="Bob"))

    await db.transact_write([
        TransactPut(UserType(user_type=UserTypeT.bar, name="Alice")),
        TransactConditionCheck(
            UserType,
            hash_key=UserTypeT.foo,
            condition_expression=Attr("name").exists(),
        ),
        TransactUpdate(
            UserVersion,
            hash_key="u1",
            range_key=UserTypeT.foo,
            update_expression={UpdateAttr("name").set("Bob Updated")},
        ),
    ])

    await db.transact_write([
        TransactDelete(UserType, hash_key=UserTypeT.bar),
    ])

    updated = await db.get(UserVersion, hash_key="u1", range_key=UserTypeT.foo)
    assert updated == UserVersion(user_id="u1", user_type=UserTypeT.foo, name="Bob Updated")
    assert await db.get(UserType, hash_key=UserTypeT.bar) is None
