from datetime import datetime

import pytest
from pydantic_core import TzInfo

from aiodynamodb import BatchDelete, BatchGet, BatchPut, DynamoDB
from tests.entities import Basket, ComplexOrder, Item, User


async def test_batch_write_applies_put_and_delete(users_table):
    db = users_table
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


async def test_batch_get_groups_requests_and_parses_typed_models(dynamo_resource):
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

    result = await db.batch_get(
        [
            BatchGet(User, hash_key="u1", projection_expression="user_id, #n", expression_attribute_names={"#n": "name"}),
            BatchGet(ComplexOrder, hash_key="o1", range_key=datetime(2020, 1, 1, tzinfo=TzInfo(0)), consistent_read=True),
        ],
        return_consumed_capacity=True,
    )

    assert result.items[User] == [User(user_id="u1", name="Alice", email=None)]
    assert result.items[ComplexOrder] == [
        ComplexOrder(
            order_id="o1",
            created_at=datetime(2020, 1, 1, tzinfo=TzInfo(0)),
            total=100,
            basket=basket,
        )
    ]
    assert result.unprocessed_keys == {}


async def test_batch_get_rejects_conflicting_projection_for_same_table():
    db = DynamoDB()

    with pytest.raises(ValueError):
        await db.batch_get(
            [
                BatchGet(User, hash_key="u1", projection_expression="user_id"),
                BatchGet(User, hash_key="u2", projection_expression="name"),
            ]
        )
