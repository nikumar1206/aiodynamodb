from contextlib import asynccontextmanager
from datetime import datetime
from unittest.mock import AsyncMock

import pytest
from boto3.dynamodb.conditions import Attr, Key
from pydantic import BaseModel
from pydantic_core import TzInfo
from types_aiobotocore_dynamodb import DynamoDBClient

from aiodynamodb import (
    DynamoDB,
    DynamoModel,
    TransactConditionCheck,
    TransactDelete,
    TransactGet,
    TransactPut,
    table,
)
from aiodynamodb.custom_types import JSONStr, Timestamp, TimestampMillis
from tests.entities import Basket, ComplexOrder, Item, Order, User


async def test_create_global_table_uses_model_table_name_and_regions():
    db = DynamoDB()
    mock_client = AsyncMock()
    expected = {"GlobalTableDescription": {"GlobalTableName": Order.Meta.table_name}}
    mock_client.create_global_table.return_value = expected

    @asynccontextmanager
    async def fake_client():
        yield mock_client

    db._client = fake_client  # type: ignore[method-assign]

    resp = await db.create_global_table(Order, regions=["us-east-1", "eu-west-1"])

    assert resp == expected
    mock_client.create_global_table.assert_awaited_once_with(
        GlobalTableName=Order.Meta.table_name,
        ReplicationGroup=[{"RegionName": "us-east-1"}, {"RegionName": "eu-west-1"}],
    )


async def test_create_global_table_with_no_regions_sends_empty_replication_group():
    db = DynamoDB()
    mock_client = AsyncMock()
    mock_client.create_global_table.return_value = {
        "GlobalTableDescription": {"GlobalTableName": Order.Meta.table_name}}

    @asynccontextmanager
    async def fake_client():
        yield mock_client

    db._client = fake_client  # type: ignore[method-assign]

    await db.create_global_table(Order, regions=[])

    mock_client.create_global_table.assert_awaited_once_with(
        GlobalTableName=Order.Meta.table_name,
        ReplicationGroup=[],
    )


async def test_transact_write_builds_expected_requests():
    db = DynamoDB()
    mock_client = AsyncMock()
    mock_client.transact_write_items.return_value = {"ConsumedCapacity": []}

    @asynccontextmanager
    async def fake_client():
        yield mock_client

    db._client = fake_client  # type: ignore[method-assign]

    await db.transact_write(
        [
            TransactPut(User(user_id="u1", name="Alice")),
            TransactDelete(User, hash_key="u2"),
            TransactConditionCheck(User, hash_key="u1", condition_expression=Attr("user_id").exists()),
        ],
        client_request_token="req-token",
        return_consumed_capacity=True,
        return_item_collection_metrics=True,
    )

    kwargs = mock_client.transact_write_items.await_args.kwargs
    assert kwargs["ClientRequestToken"] == "req-token"
    assert kwargs["ReturnConsumedCapacity"] == "TOTAL"
    assert kwargs["ReturnItemCollectionMetrics"] == "SIZE"
    assert len(kwargs["TransactItems"]) == 3

    put_item = kwargs["TransactItems"][0]["Put"]
    assert put_item["TableName"] == User.Meta.table_name
    assert put_item["Item"] == {"user_id": "u1", "name": "Alice", "email": None}

    delete_item = kwargs["TransactItems"][1]["Delete"]
    assert delete_item["TableName"] == User.Meta.table_name
    assert delete_item["Key"] == {"user_id": "u2"}

    condition_item = kwargs["TransactItems"][2]["ConditionCheck"]
    assert condition_item["TableName"] == User.Meta.table_name
    assert condition_item["Key"] == {"user_id": "u1"}
    assert "ConditionExpression" in condition_item
    assert condition_item["ExpressionAttributeNames"]
    assert "ExpressionAttributeValues" not in condition_item


async def test_transact_write_condition_check_requires_expression():
    db = DynamoDB()
    with pytest.raises(TypeError):
        await db.transact_write([TransactConditionCheck(User, hash_key="u1")])


async def test_transact_get_parses_models_and_serializes_custom_keys():
    db = DynamoDB()
    mock_client = AsyncMock()
    mock_client.transact_get_items.return_value = {
        "Responses": [
            {"Item": {"user_id": "u1", "name": "Alice", "email": "alice@example.com"}},
            {},
        ]
    }

    @asynccontextmanager
    async def fake_client():
        yield mock_client

    db._client = fake_client  # type: ignore[method-assign]

    results = await db.transact_get(
        [
            TransactGet(User, hash_key="u1"),
            TransactGet(
                ComplexOrder,
                hash_key="o1",
                range_key=datetime(2020, 1, 1, tzinfo=TzInfo(0)),
                consistent_read=True,
            ),
        ],
        return_consumed_capacity=True,
    )

    assert results == [User(user_id="u1", name="Alice", email="alice@example.com"), None]

    kwargs = mock_client.transact_get_items.await_args.kwargs
    assert kwargs["ReturnConsumedCapacity"] == "TOTAL"
    assert kwargs["TransactItems"][0]["Get"]["Key"] == {"user_id": "u1"}
    assert kwargs["TransactItems"][1]["Get"]["Key"] == {"order_id": "o1", "created_at": 1577836800}
    assert kwargs["TransactItems"][1]["Get"]["ConsistentRead"] is True


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
