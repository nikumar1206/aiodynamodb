from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import aioboto3
from boto3.dynamodb.conditions import Key, Attr
from types_aiobotocore_dynamodb.literals import BillingModeType, TableClassType
from types_aiobotocore_dynamodb.type_defs import (
    TableAttributeValueTypeDef,
    ProvisionedThroughputTypeDef,
    GlobalSecondaryIndexUnionTypeDef,
    LocalSecondaryIndexTypeDef,
)

if TYPE_CHECKING:
    from types_aiobotocore_dynamodb.service_resource import Table
    from types_aiobotocore_dynamodb.client import DynamoDBClient

from aiodynamodb.models import DynamoModel, QueryResult

type KeyT = int | str

_KEY_TO_TYPE = {str: "S", bytes: "B", int: "N", float: "N"}


@dataclass
class Page[T]:
    items: list[T]
    last_evaluated_key: dict | None = None


class DynamoDB:
    def __init__(self, session: aioboto3.Session | None = None):
        self._session = session or aioboto3.Session()

    @asynccontextmanager
    async def _resource(self):
        async with self._session.resource("dynamodb") as resource:
            yield resource

    @asynccontextmanager
    async def _client(self):
        async with self._session.client("dynamodb") as client:
            yield client

    async def put[T: DynamoModel](self, item: T) -> None:
        async with self._resource() as resource:
            table: Table = await resource.Table(item.Meta.table_name)
            await table.put_item(Item=item.model_dump())

    async def get[T: DynamoModel](
        self, model: type[T], *, hash_key: KeyT, range_key: KeyT | None = None, consistent_reads: bool = False
    ) -> T | None:
        meta = model.Meta
        key = {meta.hash_key: hash_key}
        if meta.range_key and range_key is not None:
            key[meta.range_key] = range_key

        async with self._resource() as resource:
            table: Table = await resource.Table(meta.table_name)
            resp = await table.get_item(Key=key, ConsistentReads=consistent_reads)
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
    ) -> QueryResult[T]:
        meta = model.Meta

        client: DynamoDBClient
        async with self._client() as client:
            paginator = client.get_paginator("query")

            query_args = dict(
                TableName=meta.table_name,
                IndexName=index_name,
                Limit=limit,
                ScanIndexForward=scan_index_forward,
                ExclusiveStartKey=exclusive_start_key,
                ReturnConsumedCapacity=return_consumed_capacity,
                FilterExpression=filter_expression,
                KeyConditionExpression=key_condition_expression,
                ConsistentRead=consistent_read,
            )
            async for page in paginator.paginate(**query_args):
                yield QueryResult(
                    items=[model.model_validate(i) for i in page.items], last_evaluated_key=page.last_evaluated_key
                )

    async def create_table[T: DynamoModel](
        self,
        model: type[T],
        *,
        billing_mode: BillingModeType = "PAY_PER_REQUEST",
        provisioned_throughput: ProvisionedThroughputTypeDef | None = None,
        global_secondary_indexes: list[GlobalSecondaryIndexUnionTypeDef] | None = None,
        local_secondary_indexes: list[LocalSecondaryIndexTypeDef] | None = None,
        tags: list[dict[str, str]] | None = None,
        table_class: TableClassType | None = None,
    ) -> dict[str, Any]:
        meta = model.Meta
        key_schema = [{"AttributeName": meta.hash_key, "KeyType": "HASH"}]
        _hash_type = model.__fields__[meta.hash_key].annotation
        attribute_definitions = {meta.hash_key: _KEY_TO_TYPE[_hash_type]}
        if meta.range_key:
            key_schema.append({"AttributeName": meta.range_key, "KeyType": "RANGE"})
            _range_type = model.__fields__[meta.range_key].annotation
            attribute_definitions[meta.range_key] = _KEY_TO_TYPE[_range_type]

        request: dict[str, Any] = {
            "TableName": meta.table_name,
            "AttributeDefinitions": attribute_definitions,
            "KeySchema": key_schema,
            "BillingMode": billing_mode,
        }
        if provisioned_throughput is not None:
            request["ProvisionedThroughput"] = provisioned_throughput
        if global_secondary_indexes:
            request["GlobalSecondaryIndexes"] = global_secondary_indexes
        if local_secondary_indexes:
            request["LocalSecondaryIndexes"] = local_secondary_indexes
        if tags:
            request["Tags"] = tags
        if table_class:
            request["TableClass"] = table_class

        async with self._client() as client:
            return await client.create_table(**request)
