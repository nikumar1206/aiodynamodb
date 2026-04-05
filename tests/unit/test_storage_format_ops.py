from datetime import datetime

from pydantic import BaseModel
from pydantic_core import TzInfo
from types_aiobotocore_dynamodb import DynamoDBClient

from aiodynamodb import DynamoModel, HashKey, RangeKey, table
from aiodynamodb.custom_types import JSONStr, Timestamp, TimestampMillis
from tests.unit.entities import Basket, Item


async def test_items_are_stored_in_the_correct_raw_format(db):
    class JsonData(BaseModel):
        f1: bool
        f2: str

    @table("complex")
    class Complex(DynamoModel):
        order_id: HashKey[str]
        created_at: RangeKey[Timestamp]
        created_at_milli: TimestampMillis
        json_str: JSONStr[JsonData]
        total: int
        basket: Basket

    await db.create_table(Complex)
    basket = Basket(items=[Item(qty=1, price=10.9, name="foo")])
    await db.put(
        Complex(
            order_id="o1",
            created_at=datetime(2020, 1, 3, tzinfo=TzInfo()),
            created_at_milli=datetime(2020, 1, 3, microsecond=1000, tzinfo=TzInfo()),
            total=300,
            json_str=JsonData(f1=False, f2="test"),
            basket=basket,
        )
    )

    c: DynamoDBClient
    async with db._client() as c:
        meta = Complex.Meta
        key = {
            meta.hash_key: {"S": "o1"},
            meta.range_key: {"N": str(int(datetime(2020, 1, 3, tzinfo=TzInfo()).timestamp()))},
        }

        actual = await c.get_item(TableName=meta.table_name, Key=key)

    expected_item = {
        "basket": {"M": {"items": {"L": [{"M": {"name": {"S": "foo"}, "price": {"N": "10.9"}, "qty": {"N": "1"}}}]}}},
        "created_at": {"N": "1578009600"},
        "order_id": {"S": "o1"},
        "total": {"N": "300"},
        "created_at_milli": {"N": "1578009600001"},
        "json_str": {"S": '{"f1":false,"f2":"test"}'},
    }
    assert actual["Item"] == expected_item

    actual_item = await db.get(Complex, hash_key="o1", range_key=datetime(2020, 1, 3, tzinfo=TzInfo()))

    assert actual_item == Complex(
        order_id="o1",
        created_at=datetime(2020, 1, 3, tzinfo=TzInfo()),
        created_at_milli=datetime(2020, 1, 3, microsecond=1000, tzinfo=TzInfo()),
        json_str=JsonData(f1=False, f2="test"),
        total=300,
        basket=Basket(items=[Item(qty=1, price=10.9, name="foo")]),
    )
