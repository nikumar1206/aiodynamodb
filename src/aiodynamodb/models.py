from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, ClassVar, Self

from boto3.dynamodb.conditions import ConditionBase
from pydantic import BaseModel
from types_aiobotocore_dynamodb.literals import ProjectionTypeType
from types_aiobotocore_dynamodb.type_defs import (
    GlobalSecondaryIndexUnionTypeDef,
    LocalSecondaryIndexTypeDef,
    OnDemandThroughputTypeDef,
    ProvisionedThroughputTypeDef,
    WarmThroughputTypeDef,
)

from aiodynamodb._serializers import DESERIALIZER, SERIALIZER
from aiodynamodb.custom_types import KeyT
from aiodynamodb.updates import UpdateAttr


@dataclass
class GSI:
    """Global secondary index definition used in ``@table(..., indexes=[...])``."""

    name: str
    hash_key: str
    range_key: str | None = None
    projection: ProjectionTypeType = "ALL"
    non_key_attributes: list[str] | None = None
    provisioned_throughput: None | ProvisionedThroughputTypeDef = None
    on_demand_throughput: None | OnDemandThroughputTypeDef = None
    warm_throughput: None | WarmThroughputTypeDef = None

    def to_dynamo(self) -> GlobalSecondaryIndexUnionTypeDef:
        """Serialize this GSI definition to DynamoDB ``create_table`` format.

        Returns:
            A ``GlobalSecondaryIndexes`` entry compatible with
            ``CreateTable``.
        """
        _dict: GlobalSecondaryIndexUnionTypeDef = {
            "IndexName": self.name,
            "KeySchema": [
                {"AttributeName": self.hash_key, "KeyType": "HASH"},
            ],
            "Projection": {"ProjectionType": self.projection},
        }
        if self.provisioned_throughput:
            _dict["ProvisionedThroughputTypeDef"] = self.provisioned_throughput
        if self.range_key:
            _dict["KeySchema"].append({"AttributeName": self.range_key, "KeyType": "RANGE"})
        if self.non_key_attributes:
            _dict["Projection"]["NonKeyAttributes"] = self.non_key_attributes
        if self.provisioned_throughput:
            _dict["ProvisionedThroughput"] = self.provisioned_throughput
        if self.on_demand_throughput:
            _dict["OnDemandThroughput"] = self.on_demand_throughput
        if self.warm_throughput:
            _dict["WarmThroughput"] = self.warm_throughput
        return _dict


@dataclass
class LSI:
    """Local secondary index definition used in ``@table(..., indexes=[...])``."""

    name: str
    range_key: str
    projection: ProjectionTypeType = "ALL"
    non_key_attributes: list[str] | None = None

    def to_dynamo(self, hash_key: str) -> LocalSecondaryIndexTypeDef:
        """Serialize this LSI definition to DynamoDB ``create_table`` format.

        Args:
            hash_key: Table hash key name required in every LSI key schema.

        Returns:
            A ``LocalSecondaryIndexes`` entry compatible with ``CreateTable``.
        """
        _dict: LocalSecondaryIndexTypeDef = {
            "IndexName": self.name,
            "KeySchema": [
                {"AttributeName": hash_key, "KeyType": "HASH"},
                {"AttributeName": self.range_key, "KeyType": "RANGE"},
            ],
            "Projection": {"ProjectionType": self.projection},
        }
        if self.non_key_attributes:
            _dict["Projection"]["NonKeyAttributes"] = self.non_key_attributes
        return _dict


@dataclass
class TableMeta:
    """Model-to-table metadata attached by the ``@table`` decorator."""

    table_name: str
    hash_key: str
    range_key: str | None = None
    global_secondary_indexes: dict[str, GSI] = field(default_factory=dict)
    local_secondary_indexes: dict[str, LSI] = field(default_factory=dict)


class DynamoModel(BaseModel):
    """Base for models decorated with @table."""

    Meta: ClassVar[TableMeta]

    def to_dynamo(self) -> dict[str, Any]:
        """Serialize model fields to DynamoDB AttributeValue objects."""
        dumped = self.model_dump(mode="json")
        return {k: SERIALIZER._to_dynamo(v) for k, v in dumped.items()}

    @classmethod
    def from_dynamo(cls, raw: dict[str, Any]) -> Self:
        """Deserialize DynamoDB AttributeValue objects into a model instance."""
        dynamo_dict = {k: DESERIALIZER._to_dynamo(v) for k, v in raw.items()}
        return cls.model_validate(dynamo_dict)


def table(name: str, hash_key: str, range_key: str | None = None, indexes: list[GSI | LSI] | None = None):
    """Decorator that attaches DynamoDB table metadata to a Pydantic model.

    Usage:
        @table("users", hash_key="user_id")
        class User(DynamoModel):
            user_id: str
            name: str

    Args:
        name: DynamoDB table name.
        hash_key: Partition key field name.
        range_key: Optional sort key field name.
        indexes: Optional list of ``GSI`` and ``LSI`` metadata objects.
            Names must be unique per index type.
    """

    def decorator[T: DynamoModel](cls: type[T]) -> type[T]:
        idxs = indexes or []
        _validate_index_names(idxs, GSI)
        _validate_index_names(idxs, LSI)
        cls.Meta = TableMeta(
            table_name=name,
            hash_key=hash_key,
            range_key=range_key,
            global_secondary_indexes={i.name: i for i in idxs if isinstance(i, GSI)},
            local_secondary_indexes={i.name: i for i in idxs if isinstance(i, LSI)},
        )
        return cls

    def _validate_index_names(_indexes: list[LSI | GSI], index_type: type[GSI | LSI]):
        """Ensure index names are unique within a single index type."""
        index_names = [i.name for i in _indexes if isinstance(i, index_type)]
        if len(index_names) != len(set(index_names)):
            raise ValueError("Index names must be unique")

    return decorator


@dataclass
class QueryResult[T: DynamoModel]:
    """One page of typed query results."""

    items: list[T]
    last_evaluated_key: dict[str, Any] | None


@dataclass
class Page[T]:
    """Represents one page of results from a paginated DynamoDB operation."""

    items: list[T]
    last_evaluated_key: dict | None = None


@dataclass(frozen=True)
class TransactGet[T: DynamoModel]:
    """Single item read request used by ``transact_get``."""

    model: type[T]
    hash_key: KeyT
    range_key: KeyT | None = None
    consistent_read: bool = False
    projection_expression: str | None = None
    expression_attribute_names: dict[str, str] | None = None


@dataclass(frozen=True)
class TransactPut[T: DynamoModel]:
    """Put operation used by ``transact_write``."""

    item: T
    condition_expression: ConditionBase | None = None

    @property
    def model(self) -> T:
        return type(self.item)


@dataclass(frozen=True)
class TransactDelete[T: DynamoModel]:
    """Delete operation used by ``transact_write``."""

    model: type[T]
    hash_key: KeyT
    range_key: KeyT | None = None
    condition_expression: ConditionBase | None = None


@dataclass(frozen=True)
class TransactConditionCheck[T: DynamoModel]:
    """Condition-check operation used by ``transact_write``."""

    model: type[T]
    hash_key: KeyT
    condition_expression: ConditionBase
    range_key: KeyT | None = None


@dataclass(frozen=True)
class TransactUpdate[T: DynamoModel]:
    """Update operation used by ``transact_write``."""

    model: type[T]
    hash_key: KeyT
    update_expression: set[UpdateAttr]
    range_key: KeyT | None = None
    condition_expression: ConditionBase | None = None


@dataclass(frozen=True)
class BatchGet[T: DynamoModel]:
    """Single item read request used by ``batch_get``."""

    model: type[T]
    hash_key: KeyT
    range_key: KeyT | None = None
    consistent_read: bool = False
    projection_expression: str | None = None
    expression_attribute_names: dict[str, str] | None = None


@dataclass(frozen=True)
class BatchPut[T: DynamoModel]:
    """Put operation used by ``batch_write``."""

    item: T

    @property
    def model(self) -> T:
        return type(self.item)


@dataclass(frozen=True)
class BatchDelete[T: DynamoModel]:
    """Delete operation used by ``batch_write``."""

    model: type[T]
    hash_key: KeyT
    range_key: KeyT | None = None


@dataclass
class BatchGetResult:
    """Typed result returned by ``batch_get``."""

    items: dict[type[DynamoModel], list[DynamoModel]]
    unprocessed_keys: dict[str, Any]


@dataclass
class BatchWriteResult:
    """Result returned by ``batch_write``."""

    unprocessed_items: dict[str, list[dict[str, Any]]]
