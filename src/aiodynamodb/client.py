from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from typing import TYPE_CHECKING, Any

import aioboto3
from types_aiobotocore_dynamodb import DynamoDBServiceResource
from types_aiobotocore_dynamodb.literals import BillingModeType, TableClassType
from types_aiobotocore_dynamodb.type_defs import (
    CreateGlobalTableInputTypeDef,
    CreateGlobalTableOutputTypeDef,
    CreateTableInputTypeDef,
    CreateTableOutputTypeDef,
    DeleteTableOutputTypeDef,
    ProvisionedThroughputTypeDef,
    TableAttributeValueTypeDef,
)

from aiodynamodb._util import _resolve_key_annotation

if TYPE_CHECKING:
    from types_aiobotocore_dynamodb.client import DynamoDBClient
    from types_aiobotocore_dynamodb.service_resource import Table

from boto3.dynamodb.conditions import Attr, ConditionBase, Key

from aiodynamodb._serializers import _serialize_custom_attribute, to_dynamo_compatible
from aiodynamodb.conditions import CustomConditionExpressionBuilder
from aiodynamodb.custom_types import KeyT, Timestamp, TimestampMicros, TimestampMillis, TimestampNanos
from aiodynamodb.models import DynamoModel, QueryResult

_KEY_TO_TYPE = {
    str: "S",
    bytes: "B",
    int: "N",
    datetime: "S",
    float: "N",
    Timestamp: "N",
    TimestampMillis: "N",
    TimestampMicros: "N",
    TimestampNanos: "N"
}


@dataclass
class Page[T]:
    """Represents one page of results from a paginated DynamoDB operation."""

    items: list[T]
    last_evaluated_key: dict | None = None


def _build_condition_expression(
        model: type[DynamoModel],
        expression: ConditionBase | None,
        *,
        is_key_condition: bool = False,
        builder: CustomConditionExpressionBuilder | None = None,
) -> tuple[str | ConditionBase, dict[str, str], dict[str, Any]] | None:
    if expression is None:
        return None
    if not isinstance(expression, ConditionBase):
        return expression, {}, {}
    builder = builder or CustomConditionExpressionBuilder(model)
    built = builder.build_expression(expression, is_key_condition=is_key_condition)
    return (
        built.condition_expression,
        built.attribute_name_placeholders,
        built.attribute_value_placeholders,
    )


class DynamoDB:
    """Async DynamoDB client for working with ``DynamoModel`` entities.

    The client maps model metadata from ``model.Meta`` to DynamoDB table
    operations and returns validated model instances for reads/queries.
    """

    def __init__(self, session: aioboto3.Session | None = None, hask_key_types: dict[Any, str] = _KEY_TO_TYPE):
        """Create a client instance.

        Args:
            session: Optional ``aioboto3`` session. If omitted, a new session is created.
        """
        self._session = session or aioboto3.Session()
        self.hask_key_types = hask_key_types

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
            built = _build_condition_expression(type(item), condition_expression)
            if built is not None:
                condition, names, values = built
                args["ConditionExpression"] = condition
                if names:
                    args["ExpressionAttributeNames"] = names
                if values:
                    args["ExpressionAttributeValues"] = values
        async with self._resource() as resource:
            table: Table = await resource.Table(item.Meta.table_name)
            await table.put_item(Item=to_dynamo_compatible(item.model_dump()), **args)

    async def delete[T: DynamoModel](self, item: T, *, condition_expression: ConditionBase = None) -> None:
        """Delete an item from DynamoDB.

        Args:
            item: The model instance that identifies the row to remove.
            condition_expression: Optional conditional expression that must match
                for the delete to succeed.
        """
        args = {}
        if condition_expression:
            built = _build_condition_expression(type(item), condition_expression)
            if built is not None:
                condition, names, values = built
                args["ConditionExpression"] = condition
                if names:
                    args["ExpressionAttributeNames"] = names
                if values:
                    args["ExpressionAttributeValues"] = values
        async with self._resource() as resource:
            table: Table = await resource.Table(item.Meta.table_name)
            await table.delete_item(Item=to_dynamo_compatible(item.model_dump()), **args)

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
        key = {meta.hash_key: _serialize_custom_attribute(model, meta.hash_key, hash_key)}
        if meta.range_key and range_key is not None:
            serialized = _serialize_custom_attribute(model, meta.range_key, range_key)
            key[meta.range_key] = serialized

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
        condition_builder = CustomConditionExpressionBuilder(model)
        if key_condition_expression is not None:
            built = _build_condition_expression(
                model,
                key_condition_expression,
                is_key_condition=True,
                builder=condition_builder,
            )
            if built is not None:
                condition, names, values = built
                query_args["KeyConditionExpression"] = condition
                if names:
                    query_args["ExpressionAttributeNames"] = names
                if values:
                    query_args["ExpressionAttributeValues"] = values
        if filter_expression is not None:
            built = _build_condition_expression(
                model,
                filter_expression,
                builder=condition_builder,
            )
            if built is not None:
                condition, names, values = built
                query_args["FilterExpression"] = condition
                if names:
                    existing_names = query_args.get("ExpressionAttributeNames", {})
                    existing_names.update(names)
                    query_args["ExpressionAttributeNames"] = existing_names
                if values:
                    existing_values = query_args.get("ExpressionAttributeValues", {})
                    existing_values.update(values)
                    query_args["ExpressionAttributeValues"] = existing_values
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
            if annotation not in self.hask_key_types:
                raise TypeError(
                    f"Unsupported key type for field '{field_name}': {annotation!r}. "
                    f"Supported types are: {tuple(self.hask_key_types)}"
                )
            attribute_types[field_name] = self.hask_key_types[annotation]

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

    async def create_global_table[T: DynamoModel](
            self,
            model: type[T],
            *,
            regions: list[str]
    ) -> CreateGlobalTableOutputTypeDef:
        """Creates a global table from an existing table.

        Args:
            model: ``DynamoModel`` subclass containing table metadata.
            regions: the list of replica regions

        Returns:
            Raw ``create_global_table`` response from the DynamoDB API.
        """
        meta = model.Meta

        request: CreateGlobalTableInputTypeDef = {
            "GlobalTableName": meta.table_name,
            "ReplicationGroup": [{"RegionName": r} for r in regions]
        }

        client: DynamoDBClient
        async with self._client() as client:
            return await client.create_global_table(**request)

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
