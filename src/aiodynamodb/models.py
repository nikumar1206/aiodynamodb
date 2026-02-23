from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, ClassVar, Self

from boto3.dynamodb.conditions import Key
from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_pascal

from aiodynamodb._serializers import SERIALIZER, DESERIALIZER


@dataclass
class TableMeta:
    table_name: str
    hash_key: str
    range_key: str | None = None
    indexes: dict[str, Any] = field(default_factory=dict)


class DynamoModel(BaseModel):
    """Base for models decorated with @table."""

    Meta: ClassVar[TableMeta]

    def to_dynamo(self) -> dict[str, Any]:
        dumped = self.model_dump(mode="json")
        return {k: SERIALIZER.serialize(v) for k, v in dumped.items()}

    @classmethod
    def from_dynamo(cls, raw: dict[str, Any]) -> Self:
        dynamo_dict = {k: DESERIALIZER.serialize(v) for k, v in raw.items()}
        return cls.model_validate(dynamo_dict)


def table(name: str, hash_key: str, range_key: str | None = None):
    """Decorator that attaches DynamoDB table metadata to a Pydantic model.

    Usage:
        @table("users", hash_key="user_id")
        class User(DynamoModel):
            user_id: str
            name: str
    """

    def decorator[T: DynamoModel](cls: type[T]) -> type[T]:
        cls.Meta = TableMeta(
            table_name=name,
            hash_key=hash_key,
            range_key=range_key,
        )
        return cls

    return decorator


@dataclass
class QueryResult[T: DynamoModel]:
    items: list[T]
    last_evaluated_key: dict[str, Any] | None
