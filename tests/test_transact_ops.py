from datetime import datetime

import pytest
from boto3.dynamodb.conditions import Attr
from pydantic_core import TzInfo

from aiodynamodb import (
    DynamoDB,
    TransactConditionCheck,
    TransactDelete,
    TransactGet,
    TransactPut,
    TransactUpdate,
    UpdateAttr,
)
from tests.entities import Basket, ComplexOrder, Item, User


async def test_transact_write_applies_put_delete_and_condition(users_table):
    db = users_table

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


async def test_transact_write_condition_check_requires_expression():
    db = DynamoDB()
    with pytest.raises(TypeError):
        await db.transact_write([TransactConditionCheck(User, hash_key="u1")])


async def test_transact_get_parses_models_and_serializes_custom_keys(dynamo_resource):
    db = dynamo_resource
    await db.create_table(User)
    await db.create_table(ComplexOrder)

    basket = Basket(items=[Item(qty=1, price=10.9, name="foo")])
    await db.put(User(user_id="u1", name="Alice", email="alice@example.com"))
    await db.put(
        ComplexOrder(
            order_id="o1",
            created_at=datetime(2020, 1, 1, tzinfo=TzInfo(0)),
            total=100,
            basket=basket,
        )
    )

    results = await db.transact_get(
        [
            TransactGet(User, hash_key="u1"),
            TransactGet(
                ComplexOrder,
                hash_key="o1",
                range_key=datetime(2020, 1, 1, tzinfo=TzInfo(0)),
                consistent_read=True,
            ),
            TransactGet(User, hash_key="missing"),
        ],
        return_consumed_capacity=True,
    )

    assert results == [
        User(user_id="u1", name="Alice", email="alice@example.com"),
        ComplexOrder(
            order_id="o1",
            created_at=datetime(2020, 1, 1, tzinfo=TzInfo(0)),
            total=100,
            basket=basket,
        ),
        None,
    ]


async def test_transact_write_supports_update_operation(complex_order_table):
    db = complex_order_table
    basket = Basket(items=[Item(qty=1, price=10.9, name="foo")])
    await db.put(
        ComplexOrder(
            order_id="o1",
            created_at=datetime(2020, 1, 1, tzinfo=TzInfo(0)),
            total=100,
            basket=basket,
        )
    )

    await db.transact_write(
        [
            TransactUpdate(
                ComplexOrder,
                hash_key="o1",
                range_key=datetime(2020, 1, 1, tzinfo=TzInfo(0)),
                update_expression={UpdateAttr("total").set(250)},
                condition_expression=Attr("total").gte(100),
            )
        ]
    )

    updated = await db.get(
        ComplexOrder,
        hash_key="o1",
        range_key=datetime(2020, 1, 1, tzinfo=TzInfo(0)),
    )
    assert updated is not None
    assert updated.total == 250
