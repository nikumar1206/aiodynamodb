from datetime import datetime

import pytest
from boto3.dynamodb.conditions import Attr
from pydantic_core import TzInfo

from aiodynamodb import DynamoDB, DynamoModel, ProjectionAttr, table
from tests.entities import Basket, ComplexOrder, Item, Order, User


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


async def test_put_with_condition(users_table):
    db = DynamoDB()

    user = User(user_id="u1", name="Alice", email="alice@example.com")
    ex = await db.exceptions
    with pytest.raises(ex.ConditionalCheckFailedException):
        await db.put(user, condition_expression=Attr("user_id").exists())

    fetched = await db.get(User, hash_key="u1")
    assert fetched is None
    await db.put(user, condition_expression=Attr("user_id").not_exists())
    fetched = await db.get(User, hash_key="u1")
    assert fetched == User(user_id="u1", name="Alice", email="alice@example.com")


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


async def test_get_supports_projection_expression_model(users_table):
    db = DynamoDB()

    await db.put(User(user_id="u1", name="Alice", email="alice@example.com"))

    fetched = await db.get(
        User,
        hash_key="u1",
        projection_expression=[ProjectionAttr("user_id"), ProjectionAttr("name")],
        cast=False,
    )

    assert fetched == {
        "user_id": "u1",
        "name": "Alice",
    }


async def test_get_supports_projection_expression_single_field(users_table):
    db = DynamoDB()

    await db.put(User(user_id="u1", name="Alice", email="alice@example.com"))

    fetched = await db.get(
        User,
        hash_key="u1",
        projection_expression=[ProjectionAttr("user_id")],
        cast=False,
    )

    assert fetched == {"user_id": "u1"}


async def test_get_supports_specific_list_element(complex_order_table):
    db = DynamoDB()
    basket = Basket(items=[Item(qty=1, price=10.9, name="foo"), Item(qty=2, price=5.5, name="bar")])

    created_at = datetime(2026, 1, 1, tzinfo=TzInfo(0))
    await db.put(
        ComplexOrder(
            order_id="o1",
            created_at=created_at,
            total=100,
            basket=basket,
        )
    )

    fetched = await db.get(ComplexOrder, hash_key="o1", range_key=created_at)

    assert fetched is not None
    assert fetched.basket.items[1].name == "bar"


async def test_get_supports_specific_set_member(dynamo_resource):
    @table("tagged_users", hash_key="user_id")
    class TaggedUser(DynamoModel):
        user_id: str
        tags: set[str]

    db = dynamo_resource
    await db.create_table(TaggedUser)
    await db.put(TaggedUser(user_id="u1", tags={"alpha", "beta"}))

    fetched = await db.get(TaggedUser, hash_key="u1")

    assert fetched is not None
    assert "beta" in fetched.tags
