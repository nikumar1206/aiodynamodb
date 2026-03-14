from datetime import UTC, datetime
from decimal import Decimal

from pydantic import BaseModel

from aiodynamodb._serializers import DESERIALIZER, SERIALIZER, _serialize_custom_attribute
from aiodynamodb.custom_types import Timestamp


def test_serializer_casts_float_to_decimal():
    serialized = SERIALIZER._to_dynamo(12.34)
    assert serialized == {"N": "12.34"}
    assert DESERIALIZER._to_dynamo(serialized) == Decimal("12.34")


def test_serializer_casts_nested_floats_to_decimal():
    payload = {"amount": 1.5, "items": [2.25, {"tax": 0.1}]}
    serialized = SERIALIZER._to_dynamo(payload)
    assert serialized["M"]["amount"] == {"N": "1.5"}
    assert serialized["M"]["items"]["L"][0] == {"N": "2.25"}
    assert serialized["M"]["items"]["L"][1]["M"]["tax"] == {"N": "0.1"}


def test_serialize_custom_attribute_supports_nested_model_paths():
    class BazModel(BaseModel):
        baz: Timestamp

    class BarModel(BaseModel):
        bar: BazModel

    class FooModel(BaseModel):
        foo: BarModel

    value = datetime(2020, 1, 1, tzinfo=UTC)
    serialized = _serialize_custom_attribute(FooModel, "foo.bar.baz", value)
    assert serialized == int(value.timestamp())


def test_serialize_custom_attribute_supports_indexed_nested_paths():
    class ItemModel(BaseModel):
        baz: Timestamp

    class FooModel(BaseModel):
        foo: list[ItemModel]

    value = datetime(2020, 1, 1, tzinfo=UTC)
    serialized = _serialize_custom_attribute(FooModel, "foo[0].baz", value)
    assert serialized == int(value.timestamp())
