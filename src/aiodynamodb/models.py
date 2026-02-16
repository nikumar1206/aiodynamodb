from typing import Any, Literal, Callable

from pydantic import BaseModel

from boto3.dynamodb.types import TypeSerializer, TypeDeserializer
from pydantic.main import IncEx
import json

SERIALIZER = TypeSerializer()
DESERIALIZER = TypeDeserializer()


class DynamoBaseModel(BaseModel):

    def model_dump(
            self,
            *,
            mode: Literal['json', 'python'] | str = 'python',
            include: IncEx | None = None,
            exclude: IncEx | None = None,
            context: Any | None = None,
            by_alias: bool | None = None,
            exclude_unset: bool = False,
            exclude_defaults: bool = False,
            exclude_none: bool = False,
            exclude_computed_fields: bool = False,
            round_trip: bool = False,
            warnings: bool | Literal['none', 'warn', 'error'] = True,
            fallback: Callable[[Any], Any] | None = None,
            serialize_as_any: bool = False,
    ):
        _serielized = super().model_dump(
            mode,
            include,
            exclude,
            context,
            by_alias,
            exclude_unset,
            exclude_defaults,
            exclude_none,
            exclude_computed_fields,
            round_trip,
            warnings,
            fallback,
            serialize_as_any,
        )
        return {k: SERIALIZER.serialize(v) for k, v in _serielized.items()}

    @classmethod
    def model_validate(
            cls,
            obj: dict[str, Any],
            *,
            strict: bool | None = None,
            extra: ExtraValues | None = None,
            from_attributes: bool | None = None,
            context: Any | None = None,
            by_alias: bool | None = None,
            by_name: bool | None = None,
    ):
        return {k: DESERIALIZER.deserialize(v) for k, v in obj}

