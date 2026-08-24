from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Annotated

import pytest
from boto3.dynamodb.conditions import Attr
from pydantic import BaseModel

from aiodynamodb import DynamoModel, HashKey, table
from aiodynamodb._serializers import (
    DESERIALIZER,
    SERIALIZER,
    _extract_nested_model,
    _resolve_key_annotation,
    _serialize_custom_attribute,
    _to_dynamo_compatible,
)
from aiodynamodb._util import _condition_expressions
from aiodynamodb.custom_types import Timestamp, TimestampMicros, TimestampMillis, TimestampNanos


@pytest.mark.parametrize(
    ("annotation", "expected"),
    [
        (str, str),
        (int, int),
        (str | None, str),
        (int | None, int),
        # Annotated path — broken by str(origin) == "typing.Annotated" comparison
        (Annotated[datetime, "some_metadata"], datetime),
        # Annotated inside Optional, as with `Timestamp | None`
        (Annotated[datetime, "meta"] | None, datetime),  # type: ignore
        # set[str] should not be unwrapped — valid DynamoDB set type
        (set[str], set[str]),
    ],
)
def test_resolve_key_annotation(annotation, expected):
    assert _resolve_key_annotation(annotation) == expected


@pytest.mark.parametrize(
    ("annotation", "expected"),
    [
        (str, None),
        (int, None),
        (Annotated[str, "meta"], None),
    ],
)
def test_extract_nested_model_returns_none_for_primitives(annotation, expected):
    assert _extract_nested_model(annotation) is expected


def test_extract_nested_model_unwraps_annotated():
    class Inner(BaseModel):
        x: int

    annotated = Annotated[Inner, "meta"]
    assert _extract_nested_model(annotated) is Inner


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


def test_serializer_recursively_normalizes_enum_values():
    class Payload(Enum):
        value = (1.5, datetime(2026, 1, 1, tzinfo=UTC))

    assert SERIALIZER._to_dynamo(Payload.value) == {"L": [{"N": "1.5"}, {"S": "2026-01-01T00:00:00+00:00"}]}


def test_serializer_handles_float_mixin_enum_before_float():
    class Ratio(float, Enum):
        half = 0.5

    assert SERIALIZER._to_dynamo(Ratio.half) == {"N": "0.5"}


def test_nested_generic_enum_roundtrips_and_normalizes_in_conditions():
    class Status(Enum):
        active = "active"

    class Metadata(BaseModel):
        status: Status

    @table("enum_condition_orders")
    class Order(DynamoModel):
        order_id: HashKey[str]
        metadata: Metadata

    order = Order(order_id="o1", metadata=Metadata(status=Status.active))
    raw = order.to_dynamo()
    condition = _condition_expressions(Order, Attr("metadata.status").eq(Status.active))

    assert raw["metadata"] == {"M": {"status": {"S": "active"}}}
    assert Order.from_dynamo(raw) == order
    assert condition["ExpressionAttributeValues"] == {":v0": "active"}


def test_serializer_rejects_non_string_map_keys():
    with pytest.raises(TypeError, match="DynamoDB map keys must be strings"):
        _to_dynamo_compatible({1: "one"})


def test_serializer_normalizes_frozen_numeric_sets():
    serialized = SERIALIZER._to_dynamo(frozenset({1.5, 2.5}))

    assert set(serialized["NS"]) == {"1.5", "2.5"}


@pytest.mark.parametrize("value", [set(), frozenset()])
def test_serializer_rejects_empty_sets(value):
    with pytest.raises(ValueError, match="DynamoDB does not support empty sets"):
        _to_dynamo_compatible(value)


@pytest.mark.parametrize("value", [{"one", 2}, {True}])
def test_serializer_rejects_invalid_dynamo_sets(value):
    with pytest.raises(TypeError, match="DynamoDB sets must contain only"):
        _to_dynamo_compatible(value)


def test_timestamp_serializers_use_exact_integer_arithmetic():
    class Event(BaseModel):
        seconds: Timestamp
        milliseconds: TimestampMillis
        microseconds: TimestampMicros
        nanoseconds: TimestampNanos

    value = datetime(2026, 1, 1, microsecond=1, tzinfo=UTC)
    dumped = Event(seconds=value, milliseconds=value, microseconds=value, nanoseconds=value).model_dump()

    assert dumped == {
        "seconds": 1_767_225_600,
        "milliseconds": 1_767_225_600_000,
        "microseconds": 1_767_225_600_000_001,
        "nanoseconds": 1_767_225_600_000_001_000,
    }


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
