import typing
from decimal import Decimal
from typing import Any, cast, get_args, get_origin

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from pydantic import BaseModel, TypeAdapter

from aiodynamodb.custom_types import KeyT


def _model_has_float_fields(model: type[BaseModel]) -> bool:
    """Return True if any field in the model (recursively) has a float annotation.

    Called once at @table decoration time and cached as a ClassVar so that
    to_dynamo_compatible can skip the float→Decimal traversal for models
    that never contain float values.
    """
    for field_info in model.model_fields.values():
        ann = field_info.annotation
        args = get_args(ann) if get_origin(ann) is not None else (ann,)
        for arg in args:
            if arg is float:
                return True
            if isinstance(arg, type) and issubclass(arg, BaseModel) and _model_has_float_fields(arg):
                return True
    return False


def _resolve_key_annotation(annotation: Any) -> type:
    """Resolve optional/union annotations to a concrete key type."""
    origin = get_origin(annotation)
    if origin is None:
        return annotation
    # Preserve container types like ``set[str]`` while unwrapping Annotated metadata.
    if origin == typing.Annotated:
        annotated_args = get_args(annotation)
        if annotated_args:
            return _resolve_key_annotation(annotated_args[0])
    union_args = [arg for arg in get_args(annotation) if arg is not type(None)]
    if len(union_args) == 1 and len(union_args) != len(get_args(annotation)):
        return _resolve_key_annotation(union_args[0])
    return annotation


def _serialize_dynamo_primitives(value: Any) -> Any:
    """Recursively cast ``float`` values to ``Decimal`` for DynamoDB numbers."""
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, list):
        return [_serialize_dynamo_primitives(v) for v in value]
    if isinstance(value, tuple):
        return [_serialize_dynamo_primitives(v) for v in value]
    if isinstance(value, set):
        return {_serialize_dynamo_primitives(v) for v in value}
    if isinstance(value, dict):
        return {k: _serialize_dynamo_primitives(v) for k, v in value.items()}
    return value


def _to_dynamo_compatible(value: Any) -> Any:
    """Convert python values into forms accepted by boto DynamoDB serializers."""
    return _serialize_dynamo_primitives(value)


class DynamoSerializer:
    """Wrapper around boto3 serializer with float-to-Decimal coercion."""

    def __init__(self):
        self._serializer = TypeSerializer()

    def _to_dynamo(self, value: Any) -> dict[str, Any]:
        return self._serializer.serialize(_serialize_dynamo_primitives(value))  # type: ignore[return-value]

    def serialize(self, value: Any) -> dict[str, Any]:
        return self._to_dynamo(value)


class DynamoDeserializer:
    """Wrapper around boto3 deserializer."""

    def __init__(self):
        self._deserializer = TypeDeserializer()

    def _to_dynamo(self, value: dict[str, Any]) -> Any:
        return self._deserializer.deserialize(value)  # type: ignore[arg-type]

    def deserialize(self, value: dict[str, Any]) -> Any:
        return self._to_dynamo(value)


_type_adapter_cache: dict[tuple[type[BaseModel], str], TypeAdapter[Any]] = {}


def _serialize_custom_attribute(model: type[BaseModel], field_name: str, field_value: KeyT) -> str | int:
    """Function to correctly serialize custom types to the expected dynamo value.

    This is auto applied on hash and range key on get() but must be manually applied in query
    """
    cache_key = (model, field_name)
    adapter = _type_adapter_cache.get(cache_key)
    if adapter is None:
        current_model = model
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
        adapter = TypeAdapter(key_type)
        _type_adapter_cache[cache_key] = adapter

    return cast(str | int, adapter.serializer.to_python(field_value))


def _extract_nested_model(annotation: Any) -> type[BaseModel] | None:
    """Return nested BaseModel type from an annotation when present."""
    resolved = _resolve_key_annotation(annotation)
    origin = get_origin(resolved)

    if origin is None:
        if isinstance(resolved, type) and issubclass(resolved, BaseModel):
            return resolved
        return None

    if origin is list or origin is set or origin is tuple:
        inner = [arg for arg in get_args(resolved) if arg is not Ellipsis]
        if inner:
            return _extract_nested_model(inner[0])
        return None

    if origin is dict:
        dict_args = get_args(resolved)
        if len(dict_args) == 2:
            return _extract_nested_model(dict_args[1])
        return None

    if origin == typing.Annotated:
        annotated_args = get_args(resolved)
        if annotated_args:
            return _extract_nested_model(annotated_args[0])
        return None

    return None


SERIALIZER = DynamoSerializer()
DESERIALIZER = DynamoDeserializer()
