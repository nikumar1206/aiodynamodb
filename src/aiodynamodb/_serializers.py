from __future__ import annotations

from decimal import Decimal
from typing import Any, get_args, get_origin

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from pydantic import BaseModel, TypeAdapter

from aiodynamodb.custom_types import KeyT


def _resolve_key_annotation(annotation: Any) -> type:
    """Resolve optional/union annotations to a concrete key type."""
    origin = get_origin(annotation)
    if origin is None:
        return annotation
    args = [arg for arg in get_args(annotation) if arg is not type(None)]
    if len(args) == 1:
        return _resolve_key_annotation(args[0])
    return annotation

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
    current_model: type[BaseModel] = model
    key_type: Any = None
    fields = [part for part in field_name.split(".") if part]

    for index, raw_field in enumerate(fields):
        field = raw_field.split("[", 1)[0]
        if field not in current_model.model_fields:
            raise KeyError(f"Unknown field '{field}' in path '{field_name}' for model {current_model.__name__}")
        key_type = _resolve_key_annotation(current_model.model_fields[field].annotation)
        if index == len(fields) - 1:
            break
        nested_model = _extract_nested_model(key_type)
        if nested_model is None:
            raise TypeError(f"Field path '{field_name}' is not a nested model path")
        current_model = nested_model

    if key_type is None:
        raise ValueError(f"Invalid field path '{field_name}'")
    serialized = TypeAdapter(key_type).serializer.to_python(field_value)
    return serialized


def _extract_nested_model(annotation: Any) -> type[BaseModel] | None:
    """Return nested BaseModel type from an annotation when present."""
    resolved = _resolve_key_annotation(annotation)
    origin = get_origin(resolved)

    if origin is None:
        if isinstance(resolved, type) and issubclass(resolved, BaseModel):
            return resolved
        return None

    if origin is list or origin is set or origin is tuple:
        args = [arg for arg in get_args(resolved) if arg is not Ellipsis]
        if args:
            return _extract_nested_model(args[0])
        return None

    if origin is dict:
        args = get_args(resolved)
        if len(args) == 2:
            return _extract_nested_model(args[1])
        return None

    if str(origin) == "typing.Annotated":
        args = get_args(resolved)
        if args:
            return _extract_nested_model(args[0])
        return None

    return None


SERIALIZER = DynamoSerializer()
DESERIALIZER = DynamoDeserializer()
