from dataclasses import dataclass, field
from typing import Any, ClassVar

from pydantic import BaseModel


@dataclass
class TableMeta:
    table_name: str
    hash_key: str
    range_key: str | None = None
    region: str = "us-east-1"
    indexes: dict[str, Any] = field(default_factory=dict)


class DynamoModel(BaseModel):
    """Base for models decorated with @table."""

    Meta: ClassVar[TableMeta]


def table(name: str, hash_key: str, range_key: str | None = None, region: str = "us-east-1"):
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
            region=region,
        )
        return cls

    return decorator
