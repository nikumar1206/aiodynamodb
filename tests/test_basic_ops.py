from datetime import datetime

import pytest
from boto3.dynamodb.conditions import Attr, Key
from pydantic import BaseModel
from pydantic_core import TzInfo
from types_aiobotocore_dynamodb import DynamoDBClient

from aiodynamodb import (
    BatchDelete,
    BatchGet,
    BatchPut,
    DynamoDB,
    DynamoModel,
    TransactConditionCheck,
    TransactDelete,
    TransactGet,
    TransactPut,
    TransactUpdate,
    UpdateAttr,
    table,
)
from aiodynamodb.custom_types import JSONStr, Timestamp, TimestampMillis
from tests.entities import Basket, ComplexOrder, Item, Order, User


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
                update_expression="SET #tot = :new_total",
                condition_expression=Attr("total").gte(100),
                expression_attribute_names={"#tot": "total"},
                expression_attribute_values={":new_total": 250},
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


async def test_transact_write_update_rejects_conflicting_expression_attribute_names():
    db = DynamoDB()

    with pytest.raises(ValueError):
        await db.transact_write(
            [
                TransactUpdate(
                    User,
                    hash_key="u1",
                    update_expression="SET #n = :name",
                    condition_expression=Attr("name").exists(),
                    expression_attribute_names={"#n0": "user_id"},
                    expression_attribute_values={":name": "Alice"},
                )
            ]
        )


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


async def test_query_returns_paginated_results(orders_table):
    db = DynamoDB()

    await db.put(Order(order_id="o1", created_at="2026-01-01", total=100))
    await db.put(Order(order_id="o1", created_at="2026-01-02", total=200))
    await db.put(Order(order_id="o1", created_at="2026-01-03", total=300))
    await db.put(Order(order_id="o2", created_at="2026-01-01", total=999))

    pages = []
    async for page in db.query(
            Order,
            key_condition_expression=Key("order_id").eq("o1"),
            limit=2,
            scan_index_forward=True,
    ):
        pages.append(page)

    assert len(pages) == 2
    assert pages[0].last_evaluated_key is not None
    assert pages[1].last_evaluated_key is None
    assert [item.created_at for page in pages for item in page.items] == [
        "2026-01-01",
        "2026-01-02",
        "2026-01-03",
    ]


async def test_query_supports_exclusive_start_key(orders_table):
    db = DynamoDB()

    await db.put(Order(order_id="o1", created_at="2026-01-01", total=100))
    await db.put(Order(order_id="o1", created_at="2026-01-02", total=200))
    await db.put(Order(order_id="o1", created_at="2026-01-03", total=300))

    first_page = None
    async for page in db.query(
            Order,
            key_condition_expression=Key("order_id").eq("o1"),
            limit=1,
            scan_index_forward=True,
    ):
        first_page = page
        break

    assert first_page is not None
    assert first_page.last_evaluated_key is not None
    assert [item.created_at for item in first_page.items] == ["2026-01-01"]

    remaining = []
    async for page in db.query(
            Order,
            key_condition_expression=Key("order_id").eq("o1"),
            exclusive_start_key=first_page.last_evaluated_key,
            scan_index_forward=True,
    ):
        remaining.extend(page.items)

    assert [item.created_at for item in remaining] == ["2026-01-02", "2026-01-03"]


async def test_query_applies_filter_expression(orders_table):
    db = DynamoDB()

    await db.put(Order(order_id="o1", created_at="2026-01-01", total=100))
    await db.put(Order(order_id="o1", created_at="2026-01-02", total=200))
    await db.put(Order(order_id="o1", created_at="2026-01-03", total=300))

    filtered = []
    async for page in db.query(
            Order,
            key_condition_expression=Key("order_id").eq("o1"),
            filter_expression=Attr("total").gte(200),
            scan_index_forward=True,
    ):
        filtered.extend(page.items)

    assert [item.total for item in filtered] == [200, 300]


async def test_query_index(orders_table):
    db = DynamoDB()

    await db.put(Order(order_id="o1", created_at="2026-01-01", total=100))
    await db.put(Order(order_id="o1", created_at="2026-01-02", total=200))
    await db.put(Order(order_id="o1", created_at="2026-01-03", total=300))

    filtered = []
    async for page in db.query(
            Order,
            index_name="order_gsi",
            key_condition_expression=Key("order_id").eq("o1"),
            filter_expression=Attr("total").gte(200),
            scan_index_forward=True,
    ):
        filtered.extend(page.items)

    assert [item.total for item in filtered] == [200, 300]


async def test_query_lsi_index(orders_table):
    db = DynamoDB()

    await db.put(Order(order_id="o1", created_at="2026-01-01", total=100))
    await db.put(Order(order_id="o1", created_at="2026-01-02", total=200))
    await db.put(Order(order_id="o1", created_at="2026-01-03", total=300))

    filtered = []
    async for page in db.query(
            Order,
            index_name="order_lsi",
            key_condition_expression=Key("order_id").eq("o1"),
            filter_expression=Attr("total").gte(200),
            scan_index_forward=True,
    ):
        filtered.extend(page.items)

    assert [item.total for item in filtered] == [200, 300]


async def test_complex_item(complex_order_table):
    db = DynamoDB()
    basket = Basket(items=[Item(qty=1, price=10.9, name="foo")])
    await db.put(
        ComplexOrder(
            order_id="o1",
            created_at=datetime(2020, 1, 1, tzinfo=TzInfo(0)),
            total=100,
            basket=basket
        )
    )
    await db.put(
        ComplexOrder(
            order_id="o1",
            created_at=datetime(2020, 1, 2, tzinfo=TzInfo(0)),
            total=200,
            basket=basket
        )
    )
    await db.put(
        ComplexOrder(
            order_id="o1",
            created_at=datetime(2020, 1, 3, tzinfo=TzInfo(0)),
            total=300,
            basket=basket
        )
    )

    filtered = []
    async for page in db.query(
            ComplexOrder,
            index_name="order_lsi",
            key_condition_expression=Key("order_id").eq("o1"),
            filter_expression=Attr("total").gte(200),
            scan_index_forward=True,
    ):
        filtered.extend(page.items)

    assert filtered == [
        ComplexOrder(
            order_id='o1',
            created_at=datetime(2020, 1, 2, tzinfo=TzInfo(0)),
            total=200,
            basket=Basket(
                items=[
                    Item(qty=1, price=10.9, name='foo')
                ]
            )
        ),
        ComplexOrder(
            order_id='o1',
            created_at=datetime(2020, 1, 3, tzinfo=TzInfo(0)),
            total=300,
            basket=Basket(
                items=[
                    Item(qty=1, price=10.9, name='foo')
                ]
            )
        )
    ]


async def test_query_serializes_custom_key_condition_values(complex_order_table):
    db = DynamoDB()
    basket = Basket(items=[Item(qty=1, price=10.9, name="foo")])

    await db.put(
        ComplexOrder(
            order_id="o1",
            created_at=datetime(2020, 1, 1, tzinfo=TzInfo(0)),
            total=100,
            basket=basket,
        )
    )
    await db.put(
        ComplexOrder(
            order_id="o1",
            created_at=datetime(2020, 1, 2, tzinfo=TzInfo(0)),
            total=200,
            basket=basket,
        )
    )
    await db.put(
        ComplexOrder(
            order_id="o1",
            created_at=datetime(2020, 1, 3, tzinfo=TzInfo(0)),
            total=300,
            basket=basket,
        )
    )

    filtered = []
    async for page in db.query(
            ComplexOrder,
            key_condition_expression=(
                    Key("order_id").eq("o1")
                    & Key("created_at").gte(datetime(2020, 1, 2, tzinfo=TzInfo(0)))
            ),
            scan_index_forward=True,
    ):
        filtered.extend(page.items)

    assert [item.total for item in filtered] == [200, 300]


async def test_deep_filter(complex_order_table):
    db = DynamoDB()
    basket = Basket(items=[Item(qty=1, price=10.9, name="foo")])
    basket2 = Basket(items=[Item(qty=2, price=10.9, name="foo")])

    await db.put(
        ComplexOrder(
            order_id="o1",
            created_at=datetime(2020, 1, 1, tzinfo=TzInfo(0)),
            total=100,
            basket=basket,
        )
    )
    await db.put(
        ComplexOrder(
            order_id="o1",
            created_at=datetime(2020, 1, 2, tzinfo=TzInfo(0)),
            total=200,
            basket=basket,
        )
    )
    await db.put(
        ComplexOrder(
            order_id="o1",
            created_at=datetime(2020, 1, 3, tzinfo=TzInfo(0)),
            total=300,
            basket=basket2,
        )
    )

    filtered = []
    async for page in db.query(
            ComplexOrder,
            key_condition_expression=(
                    Key("order_id").eq("o1")
            ),
            filter_expression=Attr("basket.items.qty").gt(1),
            scan_index_forward=True,
    ):
        filtered.extend(page.items)

    assert [item.total for item in filtered] == [300]


async def test_items_are_stored_in_the_correct_raw_format(complex_order_table):
    class JsonData(BaseModel):
        f1: bool
        f2: str

    @table("complex", hash_key="order_id", range_key="created_at")
    class Complex(DynamoModel):
        order_id: str
        created_at: Timestamp
        created_at_milli: TimestampMillis
        json_str: JSONStr[JsonData]
        total: int
        basket: Basket

    db = DynamoDB()
    await db.create_table(Complex)
    basket = Basket(items=[Item(qty=1, price=10.9, name="foo")])
    await db.put(
        Complex(
            order_id="o1",
            created_at=datetime(2020, 1, 3, tzinfo=TzInfo(0)),
            created_at_milli=datetime(2020, 1, 3, microsecond=1000, tzinfo=TzInfo(0)),
            total=300,
            json_str=JsonData(f1=False, f2="test"),
            basket=basket
        )
    )

    c: DynamoDBClient
    async with db._client() as c:
        meta = Complex.Meta
        key = {
            meta.hash_key: {"S": "o1"},
            meta.range_key: {"N": str(int(datetime(2020, 1, 3, tzinfo=TzInfo(0)).timestamp()))}
        }

        actual = await c.get_item(TableName=meta.table_name, Key=key)

    expected_item = {
        'basket': {
            'M': {
                'items': {
                    'L': [
                        {
                            'M': {
                                'name': {'S': 'foo'},
                                'price': {'N': '10.9'},
                                'qty': {'N': '1'}
                            }
                        }
                    ]
                }
            }
        },
        'created_at': {'N': '1578009600'},
        'order_id': {'S': 'o1'},
        'total': {'N': '300'},
        'created_at_milli': {'N': '1578009600001'},
        'json_str': {'S': '{"f1":false,"f2":"test"}'},
    }
    assert actual["Item"] == expected_item

    actual_item = await db.get(
        Complex,
        hash_key="o1",
        range_key=datetime(2020, 1, 3, tzinfo=TzInfo(0))
    )

    assert actual_item == Complex(
        order_id='o1',
        created_at=datetime(2020, 1, 3, tzinfo=TzInfo(0)),
        created_at_milli=datetime(2020, 1, 3, microsecond=1000, tzinfo=TzInfo(0)),
        json_str=JsonData(f1=False, f2='test'),
        total=300,
        basket=Basket(items=[Item(qty=1, price=10.9, name='foo')])
    )
