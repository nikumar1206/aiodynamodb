from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated

import pytest
from pydantic import BaseModel

from aiodynamodb._serializers import (
    DESERIALIZER,
    SERIALIZER,
    _extract_nested_model,
    _resolve_key_annotation,
    _serialize_custom_attribute,
)
from aiodynamodb.custom_types import Timestamp


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
