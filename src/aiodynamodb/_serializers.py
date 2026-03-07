from __future__ import annotations

from decimal import Decimal
from typing import Any

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer


def _cast_float_to_decimal(value: Any) -> Any:
    """Recursively cast ``float`` values to ``Decimal`` for DynamoDB numbers."""
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, list):
        return [_cast_float_to_decimal(v) for v in value]
    if isinstance(value, tuple):
        return [_cast_float_to_decimal(v) for v in value]
    if isinstance(value, set):
        return {_cast_float_to_decimal(v) for v in value}
    if isinstance(value, dict):
        return {k: _cast_float_to_decimal(v) for k, v in value.items()}
    return value


def to_dynamo_compatible(value: Any) -> Any:
    """Convert python values into forms accepted by boto DynamoDB serializers."""
    return _cast_float_to_decimal(value)


class DynamoSerializer:
    """Wrapper around boto3 serializer with float-to-Decimal coercion."""

    def __init__(self):
        self._serializer = TypeSerializer()

    def _to_dynamo(self, value: Any) -> dict[str, Any]:
        return self._serializer.serialize(_cast_float_to_decimal(value))

    def serialize(self, value: Any) -> dict[str, Any]:
        return self._to_dynamo(value)


class DynamoDeserializer:
    """Wrapper around boto3 deserializer."""

    def __init__(self):
        self._deserializer = TypeDeserializer()

    def _to_dynamo(self, value: dict[str, Any]) -> Any:
        return self._deserializer.deserialize(value)

    def deserialize(self, value: dict[str, Any]) -> Any:
        return self._to_dynamo(value)


SERIALIZER = DynamoSerializer()
DESERIALIZER = DynamoDeserializer()
