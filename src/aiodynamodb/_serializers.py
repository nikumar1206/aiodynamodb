from __future__ import annotations

from decimal import Decimal
from typing import Any

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from pydantic import TypeAdapter, BaseModel
from aiodynamodb.custom_types import KeyT

from aiodynamodb._util import _resolve_key_annotation


def _serilize_dynamo_primitives(value: Any) -> Any:
    """Recursively cast ``float`` values to ``Decimal`` for DynamoDB numbers."""
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, list):
        return [_serilize_dynamo_primitives(v) for v in value]
    if isinstance(value, tuple):
        return [_serilize_dynamo_primitives(v) for v in value]
    if isinstance(value, set):
        return {_serilize_dynamo_primitives(v) for v in value}
    if isinstance(value, dict):
        return {k: _serilize_dynamo_primitives(v) for k, v in value.items()}
    return value


def to_dynamo_compatible(value: Any) -> Any:
    """Convert python values into forms accepted by boto DynamoDB serializers."""
    return _serilize_dynamo_primitives(value)


class DynamoSerializer:
    """Wrapper around boto3 serializer with float-to-Decimal coercion."""

    def __init__(self):
        self._serializer = TypeSerializer()

    def _to_dynamo(self, value: Any) -> dict[str, Any]:
        return self._serializer.serialize(_serilize_dynamo_primitives(value))

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


def _serialize_custom_attribute(model: BaseModel, field_name: str, field_value: KeyT) -> str | int:
    """Function to correctly serialize custom types to the expected dynamo value.

    This is auto applied on hash and range key on get() but must be manually applied in query
    """
    key_type = _resolve_key_annotation(model.model_fields[field_name].annotation)
    serialized = TypeAdapter(key_type).serializer.to_python(field_value)
    return serialized


SERIALIZER = DynamoSerializer()
DESERIALIZER = DynamoDeserializer()
