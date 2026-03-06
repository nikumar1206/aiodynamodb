from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING, Any, get_args, get_origin

import aioboto3
from boto3.dynamodb.conditions import Attr, ConditionBase, Key
from types_aiobotocore_dynamodb import DynamoDBServiceResource
from types_aiobotocore_dynamodb.literals import BillingModeType, TableClassType
from types_aiobotocore_dynamodb.type_defs import (
    CreateTableInputTypeDef,
    CreateTableOutputTypeDef,
    DeleteTableOutputTypeDef,
    ProvisionedThroughputTypeDef,
    TableAttributeValueTypeDef,
)

if TYPE_CHECKING:
    from types_aiobotocore_dynamodb.client import DynamoDBClient
    from types_aiobotocore_dynamodb.service_resource import Table

from aiodynamodb.models import DynamoModel, QueryResult

type KeyT = int | str

_KEY_TO_TYPE = {str: "S", bytes: "B", int: "N", float: "N"}


def _resolve_key_annotation(annotation: Any) -> type:
    """Resolve optional/union annotations to a concrete key type."""
    origin = get_origin(annotation)
    if origin is None:
        return annotation
    args = [arg for arg in get_args(annotation) if arg is not type(None)]
    if len(args) == 1:
        return _resolve_key_annotation(args[0])
    return annotation


@dataclass
class Page[T]:
    """Represents one page of results from a paginated DynamoDB operation."""

    items: list[T]
    last_evaluated_key: dict | None = None


class DynamoDB:
    """Async DynamoDB client for working with ``DynamoModel`` entities.

    The client maps model metadata from ``model.Meta`` to DynamoDB table
    operations and returns validated model instances for reads/queries.
    """

    def __init__(self, session: aioboto3.Session | None = None):
        """Create a client instance.

        Args:
            session: Optional ``aioboto3`` session. If omitted, a new session is created.
        """
        self._session = session or aioboto3.Session()

    @cached_property
    async def exceptions(self):
        """Return the boto3 DynamoDB exception namespace for error handling."""
        client: DynamoDBClient
        async with self._session.client("dynamodb") as client:
            return client.exceptions

    async def put[T: DynamoModel](self, item: T, *, condition_expression: ConditionBase = None) -> None:
        """Insert or replace an item in DynamoDB.

        Args:
            item: The model instance to persist.
            condition_expression: Optional conditional expression for guarded
                writes.
        """
        args = {}
        if condition_expression:
            args["ConditionExpression"] = condition_expression
        async with self._resource() as resource:
            table: Table = await resource.Table(item.Meta.table_name)
            await table.put_item(Item=item.model_dump(), **args)

    async def delete[T: DynamoModel](self, item: T, *, condition_expression: ConditionBase = None) -> None:
        """Delete an item from DynamoDB.

        Args:
            item: The model instance that identifies the row to remove.
            condition_expression: Optional conditional expression that must match
                for the delete to succeed.
        """
        args = {}
        if condition_expression:
            args["ConditionExpression"] = condition_expression
        async with self._resource() as resource:
            table: Table = await resource.Table(item.Meta.table_name)
            await table.delete_item(Item=item.model_dump(), **args)

    async def get[T: DynamoModel](
        self,
        model: type[T],
        *,
        hash_key: KeyT,
        range_key: KeyT | None = None,
        consistent_reads: bool = False,
        attributes_to_get: list[str] | None = None,
    ) -> T | None:
        """Get a single item by primary key.

        Args:
            model: ``DynamoModel`` subclass mapped to the target table.
            hash_key: Partition key value.
            range_key: Sort key value, when the table defines one.
            consistent_reads: Whether to use strongly consistent reads.
            attributes_to_get: Optional legacy projection list of attribute names.

        Returns:
            Parsed model instance when found, otherwise ``None``.
        """
        meta = model.Meta
        key = {meta.hash_key: hash_key}
        if meta.range_key and range_key is not None:
            key[meta.range_key] = range_key

        args = dict(
            Key=key,
            ConsistentRead=consistent_reads,
        )
        if attributes_to_get:
            args["AttributesToGet"] = attributes_to_get

        async with self._resource() as resource:
            table: Table = await resource.Table(meta.table_name)
            resp = await table.get_item(**args)
            item = resp.get("Item")
            if item is None:
                return None
            return model.model_validate(item)

    async def query[T: DynamoModel](
        self,
        model: type[T],
        *,
        index_name: str | None = None,
        limit: int | None = None,
        key_condition_expression: Key | None = None,
        filter_expression: Attr | None = None,
        exclusive_start_key: dict[str, TableAttributeValueTypeDef] | None = None,
        return_consumed_capacity=False,
        consistent_read: bool = False,
        scan_index_forward=False,
    ) -> AsyncIterator[QueryResult[T]]:
        """Query items and yield paginated results.

        Args:
            model: ``DynamoModel`` subclass mapped to the target table.
            index_name: Optional index name to query.
            limit: Maximum number of items to evaluate per page.
            key_condition_expression: Key condition expression for the query.
            filter_expression: Optional post-key filter expression.
            exclusive_start_key: Pagination token from a previous page.
            return_consumed_capacity: Include consumed capacity information
                (`"TOTAL"` in DynamoDB request).
            consistent_read: Whether to use strongly consistent reads.
            scan_index_forward: Sort ascending when ``True``, descending when
                ``False``.

        Yields:
            ``QueryResult`` pages containing parsed model instances.
        """
        meta = model.Meta

        query_args: dict[str, Any] = {
            "ScanIndexForward": scan_index_forward,
            "ConsistentRead": consistent_read,
        }
        if index_name is not None:
            query_args["IndexName"] = index_name
        if limit is not None:
            query_args["Limit"] = limit
        if key_condition_expression is not None:
            query_args["KeyConditionExpression"] = key_condition_expression
        if filter_expression is not None:
            query_args["FilterExpression"] = filter_expression
        if exclusive_start_key is not None:
            query_args["ExclusiveStartKey"] = exclusive_start_key
        if return_consumed_capacity:
            query_args["ReturnConsumedCapacity"] = "TOTAL"

        async with self._resource() as resource:
            table: Table = await resource.Table(meta.table_name)

            while True:
                page = await table.query(**query_args)
                yield QueryResult(
                    items=[model.model_validate(item) for item in page.get("Items", [])],
                    last_evaluated_key=page.get("LastEvaluatedKey"),
                )
                if "LastEvaluatedKey" not in page:
                    break
                query_args["ExclusiveStartKey"] = page["LastEvaluatedKey"]

    async def create_table[T: DynamoModel](
        self,
        model: type[T],
        *,
        billing_mode: BillingModeType = "PAY_PER_REQUEST",
        provisioned_throughput: ProvisionedThroughputTypeDef | None = None,
        tags: list[dict[str, str]] | None = None,
        table_class: TableClassType | None = None,
    ) -> CreateTableOutputTypeDef:
        """Create a table for a ``DynamoModel`` definition.

        Args:
            model: ``DynamoModel`` subclass containing table metadata.
            billing_mode: DynamoDB billing mode.
            provisioned_throughput: Throughput settings for provisioned mode.
            tags: Optional table tags.
            table_class: Optional table storage class.

        Notes:
            Global secondary indexes are taken from
            ``model.Meta.global_secondary_indexes``.

        Returns:
            Raw ``create_table`` response from the DynamoDB API.
        """
        meta = model.Meta
        key_schema = [{"AttributeName": meta.hash_key, "KeyType": "HASH"}]
        attribute_types: dict[str, str] = {}

        def _add_attribute(field_name: str) -> None:
            annotation = _resolve_key_annotation(model.model_fields[field_name].annotation)
            if annotation not in _KEY_TO_TYPE:
                raise TypeError(
                    f"Unsupported key type for field '{field_name}': {annotation!r}. "
                    f"Supported types are: {tuple(_KEY_TO_TYPE)}"
                )
            attribute_types[field_name] = _KEY_TO_TYPE[annotation]

        _add_attribute(meta.hash_key)
        if meta.range_key:
            key_schema.append({"AttributeName": meta.range_key, "KeyType": "RANGE"})
            _add_attribute(meta.range_key)

        request: CreateTableInputTypeDef = {
            "TableName": meta.table_name,
            "KeySchema": key_schema,
            "BillingMode": billing_mode,
        }
        if provisioned_throughput is not None:
            request["ProvisionedThroughput"] = provisioned_throughput

        if meta.global_secondary_indexes:
            request["GlobalSecondaryIndexes"] = [i.to_dynamo() for i in meta.global_secondary_indexes.values()]
            for index in request["GlobalSecondaryIndexes"]:
                for key in index.get("KeySchema", []):
                    _add_attribute(key["AttributeName"])

        if meta.local_secondary_indexes:
            request["LocalSecondaryIndexes"] = [
                i.to_dynamo(meta.hash_key) for i in meta.local_secondary_indexes.values()
            ]
            for index in request["LocalSecondaryIndexes"]:
                for key in index.get("KeySchema", []):
                    _add_attribute(key["AttributeName"])
        if tags:
            request["Tags"] = tags
        if table_class:
            request["TableClass"] = table_class
        request["AttributeDefinitions"] = [
            {"AttributeName": name, "AttributeType": attr_type} for name, attr_type in sorted(attribute_types.items())
        ]

        client: DynamoDBClient
        async with self._client() as client:
            return await client.create_table(**request)

    async def delete_table[T: DynamoModel](self, model: type[T]) -> DeleteTableOutputTypeDef:
        """Delete the table associated with a ``DynamoModel``.

        Returns:
            Raw ``delete_table`` response from the DynamoDB API.
        """
        meta = model.Meta
        client: DynamoDBClient
        async with self._client() as client:
            return await client.delete_table(TableName=meta.table_name)

    @asynccontextmanager
    async def _resource(self) -> AsyncIterator[DynamoDBServiceResource]:
        resource: DynamoDBServiceResource
        async with self._session.resource("dynamodb") as resource:
            yield resource

    @asynccontextmanager
    async def _client(self) -> AsyncIterator[DynamoDBClient]:
        client: DynamoDBClient
        async with self._session.client("dynamodb") as client:
            yield client
