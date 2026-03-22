from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Literal, assert_never, cast

import aioboto3
from boto3.dynamodb.conditions import ConditionBase
from types_aiobotocore_dynamodb import DynamoDBServiceResource
from types_aiobotocore_dynamodb.client import DynamoDBClient, Exceptions
from types_aiobotocore_dynamodb.literals import BillingModeType, TableClassType
from types_aiobotocore_dynamodb.service_resource import Table
from types_aiobotocore_dynamodb.type_defs import (
    AttributeDefinitionTypeDef,
    CreateGlobalTableInputTypeDef,
    CreateGlobalTableOutputTypeDef,
    CreateTableInputTypeDef,
    CreateTableOutputTypeDef,
    DeleteTableOutputTypeDef,
    KeySchemaElementTypeDef,
    LocalSecondaryIndexTypeDef,
    ProvisionedThroughputTypeDef,
    TableAttributeValueTypeDef,
    TagTypeDef,
    TransactWriteItemsOutputTypeDef,
)

from aiodynamodb._serializers import (
    SERIALIZER,
    _resolve_key_annotation,
    _serialize_custom_attribute,
    _to_dynamo_compatible,
)
from aiodynamodb._util import (
    ConditionExpression,
    _add_filter_expressions,
    _condition_expressions,
    _key_condition_expressions,
    _projection_expression,
)
from aiodynamodb.conditions import CustomConditionExpressionBuilder
from aiodynamodb.custom_types import KeyT, ReturnValues, Timestamp, TimestampMicros, TimestampMillis, TimestampNanos
from aiodynamodb.models import (
    BatchDelete,
    BatchGet,
    BatchGetResult,
    BatchPut,
    BatchWriteResult,
    DynamoModel,
    QueryResult,
    Raw,
    TransactConditionCheck,
    TransactDelete,
    TransactGet,
    TransactPut,
    TransactUpdate,
)
from aiodynamodb.projection import ProjectionExpressionArg
from aiodynamodb.updates import UpdateAttr, UpdateExpressionBuilder

_KEY_TO_TYPE = {
    str: "S",
    bytes: "B",
    int: "N",
    datetime: "S",
    float: "N",
    Timestamp: "N",
    TimestampMillis: "N",
    TimestampMicros: "N",
    TimestampNanos: "N",
}

type TransactWriteOperation = (
    TransactPut[DynamoModel]
    | TransactDelete[DynamoModel]
    | TransactConditionCheck[DynamoModel]
    | TransactUpdate[DynamoModel]
)
type BatchWriteOperation = BatchPut[DynamoModel] | BatchDelete[DynamoModel]


def _to_model[T: DynamoModel](item: Raw, model: type[T], _is_raw_dynamo: bool = False) -> T:
    if _is_raw_dynamo:
        return model.from_dynamo(item)
    return model.model_validate(item)


def _merge_expression_attribute_names(
    existing: dict[str, str] | None,
    incoming: dict[str, str] | None,
) -> dict[str, str] | None:
    """Merge name placeholders and reject conflicting placeholder reuse.

    DynamoDB requires a placeholder like ``#n0`` to always refer to the same
    attribute name within a single request. This helper preserves existing
    mappings, adds new ones, and raises ``ValueError`` if the same placeholder
    is bound to a different attribute.
    """
    if not existing:
        return dict(incoming) if incoming else None
    if not incoming:
        return existing

    merged = dict(existing)
    for key, value in incoming.items():
        current = merged.get(key)
        if current is not None and current != value:
            raise ValueError(f"Conflicting expression_attribute_names value for placeholder '{key}'.")
        merged[key] = value
    return merged


class DynamoDB:
    """Async DynamoDB client for working with ``DynamoModel`` entities.

    The client maps model metadata from ``model.Meta`` to DynamoDB table
    operations and returns validated model instances for reads/queries.
    """

    def __init__(self, session: aioboto3.Session | None = None, hash_key_types: dict[Any, str] = _KEY_TO_TYPE):
        """Create a client instance.

        Args:
            session: Optional ``aioboto3`` session. If omitted, a new session is created.
        """
        self._session = session or aioboto3.Session()
        self.hash_key_types = hash_key_types
        self._exceptions: Exceptions | None = None

    async def exceptions(self):
        """Return the boto3 DynamoDB exception namespace for error handling."""
        if self._exceptions is None:
            client: DynamoDBClient
            async with self._session.client("dynamodb") as client:
                self._exceptions = client.exceptions
        return self._exceptions

    async def put(self, item: DynamoModel, *, condition_expression: ConditionBase | None = None) -> None:
        """Insert or replace an item in DynamoDB.

        Args:
            item: The model instance to persist.
            condition_expression: Optional conditional expression for guarded
                writes.
        """
        args = _condition_expressions(type(item), condition_expression)
        async with self._resource() as resource:
            table: Table = await resource.Table(item.Meta.table_name)
            await table.put_item(Item=item.to_dynamo_compatible(), **args)

    async def delete(self, item: DynamoModel, *, condition_expression: ConditionBase | None = None) -> None:
        """Delete an item from DynamoDB.

        Args:
            item: The model instance that identifies the row to remove.
            condition_expression: Optional conditional expression that must match
                for the delete to succeed.
        """
        meta = type(item).Meta
        key = _build_key(
            type(item),
            hash_key=getattr(item, meta.hash_key),
            range_key=getattr(item, meta.range_key) if meta.range_key else None,
        )
        args = _condition_expressions(type(item), condition_expression)
        async with self._resource() as resource:
            table: Table = await resource.Table(meta.table_name)
            await table.delete_item(Key=key, **args)

    async def update[T: DynamoModel](
        self,
        model: type[T],
        *,
        hash_key: KeyT,
        update_expression: set[UpdateAttr],
        range_key: KeyT | None = None,
        condition_expression: ConditionBase | None = None,
        return_values: ReturnValues | None = None,
    ) -> T | None:
        """Update an item by key and optionally return updated attributes.

        Args:
            model: ``DynamoModel`` subclass mapped to the target table.
            hash_key: Partition key value.
            update_expression: Set of ``UpdateAttr(...)`` actions describing the
                update to apply.
            range_key: Sort key value, when the table defines one.
            condition_expression: Optional conditional expression.
            return_values: Optional DynamoDB return mode (for example,
                ``"ALL_NEW"``). When omitted, DynamoDB default behavior applies.

        Returns:
            Validated model instance when DynamoDB returns ``Attributes``;
            otherwise ``None``.
        """
        args: dict[str, Any] = {
            "Key": _build_key(model, hash_key=hash_key, range_key=range_key),
        }
        if return_values is not None:
            args["ReturnValues"] = return_values

        # global builder to avoid name conflicts
        condition_builder = UpdateExpressionBuilder(model)

        condition_payload = _condition_expressions(model, condition_expression, builder=condition_builder)
        args.update(condition_payload)
        built = condition_builder.build_update_expression(update_expression)

        args["UpdateExpression"] = built.update_expression
        args["ExpressionAttributeNames"] = _merge_expression_attribute_names(
            args.get("ExpressionAttributeNames"),
            _to_dynamo_compatible(built.expression_attribute_names),
        )
        args["ExpressionAttributeValues"] = args.get("ExpressionAttributeValues", {}) | _to_dynamo_compatible(
            built.expression_attribute_values
        )

        async with self._resource() as resource:
            table: Table = await resource.Table(model.Meta.table_name)
            response = await table.update_item(**args)

        item = response.get("Attributes")
        if not item:
            return None
        return _to_model(item, model)

    async def get[T: DynamoModel](
        self,
        model: type[T],
        *,
        hash_key: KeyT,
        range_key: KeyT | None = None,
        consistent_reads: bool = False,
        projection_expression: ProjectionExpressionArg | None = None,
    ) -> T | None:
        """Get a single item by primary key.

        Args:
            model: ``DynamoModel`` subclass mapped to the target table.
            hash_key: Partition key value.
            range_key: Sort key value, when the table defines one.
            consistent_reads: Whether to use strongly consistent reads.
            projection_expression: Optional list of ``ProjectionAttr(...)``
                paths to project.

        Returns:
            Validated model instance when found, otherwise ``None``.
        """
        meta = model.Meta
        key = {meta.hash_key: _serialize_custom_attribute(model, meta.hash_key, hash_key)}
        if meta.range_key and range_key is not None:
            serialized = _serialize_custom_attribute(model, meta.range_key, range_key)
            key[meta.range_key] = serialized

        args: dict[str, Any] = {
            "Key": key,
            "ConsistentRead": consistent_reads,
        }
        args.update(_projection_expression(model, projection_expression))

        async with self._resource() as resource:
            table: Table = await resource.Table(meta.table_name)
            resp = await table.get_item(**args)
            item = resp.get("Item")
            if item is None:
                return None
        return _to_model(item, model)

    async def query[T: DynamoModel](
        self,
        model: type[T],
        *,
        index_name: str | None = None,
        limit: int | None = None,
        key_condition_expression: ConditionBase | None = None,
        filter_expression: ConditionBase | None = None,
        exclusive_start_key: dict[str, TableAttributeValueTypeDef] | None = None,
        return_consumed_capacity=False,
        consistent_read: bool = False,
        scan_index_forward=True,
        projection_expression: ProjectionExpressionArg | None = None,
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
            projection_expression: Optional list of ``ProjectionAttr(...)``
                paths to project.

        Yields:
            ``QueryResult`` pages containing validated model instances.
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

        # we need the stateful builder here as we set 2 conditions
        condition_builder = CustomConditionExpressionBuilder(model)

        dynamo_key_condition = _key_condition_expressions(
            model,
            key_condition_expression,
            builder=condition_builder,
        )
        query_args.update(dynamo_key_condition)

        # this updates args in place
        _add_filter_expressions(
            model,
            filter_expression,
            query_args=query_args,
            builder=condition_builder,
        )
        projection_payload = _projection_expression(model, projection_expression)
        if projection_payload:
            query_args["ProjectionExpression"] = projection_payload["ProjectionExpression"]
            merged_names = _merge_expression_attribute_names(
                query_args.get("ExpressionAttributeNames"),
                projection_payload.get("ExpressionAttributeNames"),
            )
            if merged_names:
                query_args["ExpressionAttributeNames"] = merged_names

        if exclusive_start_key is not None:
            query_args["ExclusiveStartKey"] = exclusive_start_key
        if return_consumed_capacity:
            query_args["ReturnConsumedCapacity"] = "TOTAL"

        async with self._resource() as resource:
            table: Table = await resource.Table(meta.table_name)

            while True:
                page = await table.query(**query_args)
                yield QueryResult(
                    items=[_to_model(item, model) for item in page.get("Items", [])],
                    last_evaluated_key=page.get("LastEvaluatedKey"),
                )
                if "LastEvaluatedKey" not in page:
                    break
                query_args["ExclusiveStartKey"] = page["LastEvaluatedKey"]

    async def transact_get[T: DynamoModel](
        self, requests: list[TransactGet[T]], *, return_consumed_capacity=False
    ) -> list[T | None]:
        """Read up to 100 items atomically across one or more tables.

        Args:
            requests: Ordered list of transaction get requests.
            return_consumed_capacity: Include consumed capacity information
                (`"TOTAL"` in DynamoDB request).

        Returns:
            Ordered list of validated model instances or ``None`` for missing
            items, in request order.
        """
        transact_items = []
        for request in requests:
            get_item: dict[str, Any] = {
                "TableName": request.model.Meta.table_name,
                "Key": _build_dynamo_key(request.model, hash_key=request.hash_key, range_key=request.range_key),
            }
            get_item.update(_projection_expression(request.model, request.projection_expression))
            transact_items.append({"Get": get_item})

        args: dict[str, Any] = {"TransactItems": transact_items}
        if return_consumed_capacity:
            args["ReturnConsumedCapacity"] = "TOTAL"

        client: DynamoDBClient
        async with self._client() as client:
            response = await client.transact_get_items(**args)

        items = response.get("Responses", [])
        results: list[T | None] = []
        for request, item_response in zip(requests, items, strict=False):
            item = item_response.get("Item")
            if item is None:
                results.append(None)
                continue
            results.append(_to_model(item, request.model, True))
        if len(results) < len(requests):
            results.extend([None] * (len(requests) - len(results)))
        return results

    async def transact_write(
        self,
        operations: list[TransactWriteOperation],
        *,
        client_request_token: str | None = None,
        return_consumed_capacity=False,
        return_item_collection_metrics=False,
    ) -> TransactWriteItemsOutputTypeDef:
        """Execute up to 100 transactional write operations atomically.

        Supported operations are ``TransactPut``, ``TransactDelete``,
        ``TransactConditionCheck``, and ``TransactUpdate``.
        """
        transact_items: list[dict[str, Any]] = []
        for operation in operations:
            match operation:
                case TransactPut(condition_expression=condition_expression) as p:
                    put_item: dict[str, Any] = {
                        "TableName": p.model.Meta.table_name,
                        "Item": p.item.to_dynamo(),
                    }
                    dynamo_condition = _condition_expressions_for_client(p.model, condition_expression)
                    put_item.update(dynamo_condition)
                    transact_items.append({"Put": put_item})

                case TransactDelete(
                    model=model, hash_key=hash_key, range_key=range_key, condition_expression=condition_expression
                ):
                    delete_item: dict[str, Any] = {
                        "TableName": model.Meta.table_name,
                        "Key": _build_dynamo_key(model, hash_key=hash_key, range_key=range_key),
                    }
                    dynamo_condition = _condition_expressions_for_client(model, condition_expression)
                    delete_item.update(dynamo_condition)
                    transact_items.append({"Delete": delete_item})
                    continue

                case TransactConditionCheck(
                    model=model, hash_key=hash_key, range_key=range_key, condition_expression=condition_expression
                ):
                    condition_item: dict[str, Any] = {
                        "TableName": model.Meta.table_name,
                        "Key": _build_dynamo_key(model, hash_key=hash_key, range_key=range_key),
                    }
                    dynamo_condition = _condition_expressions_for_client(model, condition_expression)
                    condition_item.update(dynamo_condition)
                    transact_items.append({"ConditionCheck": condition_item})

                case TransactUpdate(
                    model=model,
                    hash_key=hash_key,
                    range_key=range_key,
                    update_expression=update_expression,
                    condition_expression=condition_expression,
                ):
                    update_item: dict[str, Any] = {
                        "TableName": model.Meta.table_name,
                        "Key": _build_dynamo_key(model, hash_key=hash_key, range_key=range_key),
                    }

                    # global builder to avoid name conflicts
                    condition_builder = UpdateExpressionBuilder(model)
                    condition_payload = _condition_expressions(model, condition_expression, builder=condition_builder)
                    update_item.update(condition_payload)
                    built = condition_builder.build_update_expression(update_expression)

                    update_item["UpdateExpression"] = built.update_expression
                    update_item["ExpressionAttributeNames"] = _merge_expression_attribute_names(
                        update_item.get("ExpressionAttributeNames"),
                        built.expression_attribute_names,
                    )
                    # merge items
                    update_item["ExpressionAttributeValues"] = (
                        update_item.get("ExpressionAttributeValues", {}) | built.expression_attribute_values
                    )

                    update_item["ExpressionAttributeValues"] = _to_dynamo_expression_values(
                        update_item["ExpressionAttributeValues"]
                    )

                    transact_items.append({"Update": update_item})

                case _ as impossible:
                    assert_never(impossible)

        args: dict[str, Any] = {"TransactItems": transact_items}
        if client_request_token is not None:
            args["ClientRequestToken"] = client_request_token
        if return_consumed_capacity:
            args["ReturnConsumedCapacity"] = "TOTAL"
        if return_item_collection_metrics:
            args["ReturnItemCollectionMetrics"] = "SIZE"

        client: DynamoDBClient
        async with self._client() as client:
            return await client.transact_write_items(**args)

    async def batch_get(
        self,
        requests: list[BatchGet[DynamoModel]],
        *,
        return_consumed_capacity=False,
    ) -> BatchGetResult:
        """Fetch up to 100 items using DynamoDB ``batch_get_item``.

        Results are grouped by model type and include unprocessed keys from
        DynamoDB when throttling occurs.

        Args:
            requests: Ordered list of batch get requests.
            return_consumed_capacity: Include consumed capacity information
                (`"TOTAL"` in DynamoDB request).
        """
        table_to_model: dict[str, type[DynamoModel]] = {}
        request_items: dict[str, dict[str, Any]] = {}
        for request in requests:
            table_name = request.model.Meta.table_name
            table_to_model[table_name] = request.model
            table_entry = request_items.setdefault(table_name, {"Keys": []})
            table_entry["Keys"].append(
                _build_dynamo_key(request.model, hash_key=request.hash_key, range_key=request.range_key)
            )

            if request.consistent_read:
                existing = table_entry.get("ConsistentRead")
                if existing is False:
                    raise ValueError(f"Conflicting consistent_read values for table '{table_name}'.")
                table_entry["ConsistentRead"] = True
            if request.projection_expression is not None:
                projection_payload = _projection_expression(
                    request.model,
                    request.projection_expression,
                )
                existing = table_entry.get("ProjectionExpression")
                if existing is not None and existing != projection_payload["ProjectionExpression"]:
                    raise ValueError(f"Conflicting projection_expression values for table '{table_name}'.")
                table_entry["ProjectionExpression"] = projection_payload["ProjectionExpression"]

                merged_names = _merge_expression_attribute_names(
                    table_entry.get("ExpressionAttributeNames"),
                    projection_payload.get("ExpressionAttributeNames"),
                )
                if merged_names:
                    table_entry["ExpressionAttributeNames"] = merged_names

        args: dict[str, Any] = {"RequestItems": request_items}
        if return_consumed_capacity:
            args["ReturnConsumedCapacity"] = "TOTAL"

        client: DynamoDBClient
        async with self._client() as client:
            response = await client.batch_get_item(**args)

        parsed_items: dict[type[DynamoModel], list[DynamoModel]] = {}
        for table_name, items in response.get("Responses", {}).items():
            model = table_to_model.get(table_name)
            if model is None:
                continue
            parsed_items[model] = [_to_model(item, model, True) for item in items]
        return BatchGetResult(
            items=parsed_items,
            unprocessed_keys=response.get("UnprocessedKeys", {}),
        )

    async def batch_write(
        self,
        operations: list[BatchWriteOperation],
        *,
        return_consumed_capacity=False,
        return_item_collection_metrics=False,
    ) -> BatchWriteResult:
        """Write up to 25 items per request using DynamoDB ``batch_write_item``."""

        request_items: dict[str, list[dict[str, Any]]] = {}
        for operation in operations:
            match operation:
                case BatchPut(item=item) as p:
                    request_items.setdefault(p.model.Meta.table_name, []).append({
                        "PutRequest": {"Item": item.to_dynamo()}
                    })
                case BatchDelete(model=model, hash_key=hash_key, range_key=range_key):
                    request_items.setdefault(model.Meta.table_name, []).append({
                        "DeleteRequest": {"Key": _build_dynamo_key(model, hash_key=hash_key, range_key=range_key)}
                    })
                case _ as impossible:
                    assert_never(impossible)

        args: dict[str, Any] = {"RequestItems": request_items}
        if return_consumed_capacity:
            args["ReturnConsumedCapacity"] = "TOTAL"
        if return_item_collection_metrics:
            args["ReturnItemCollectionMetrics"] = "SIZE"

        client: DynamoDBClient
        async with self._client() as client:
            response = await client.batch_write_item(**args)

        return BatchWriteResult(unprocessed_items=response.get("UnprocessedItems", {}))

    async def create_table[T: DynamoModel](
        self,
        model: type[T],
        *,
        billing_mode: BillingModeType = "PAY_PER_REQUEST",
        provisioned_throughput: ProvisionedThroughputTypeDef | None = None,
        tags: list[TagTypeDef] | None = None,
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
        attribute_types: dict[str, Literal["B", "N", "S"]] = {}

        def _add_attribute(field_name: str) -> None:
            annotation = _resolve_key_annotation(model.model_fields[field_name].annotation)
            if annotation not in self.hash_key_types:
                raise TypeError(
                    f"Unsupported key type for field '{field_name}': {annotation!r}. "
                    f"Supported types are: {tuple(self.hash_key_types)}"
                )
            attribute_types[field_name] = cast(Literal["B", "N", "S"], self.hash_key_types[annotation])

        key_schema: list[KeySchemaElementTypeDef] = [{"AttributeName": meta.hash_key, "KeyType": "HASH"}]
        _add_attribute(meta.hash_key)
        if meta.range_key:
            key_schema.append({"AttributeName": meta.range_key, "KeyType": "RANGE"})
            _add_attribute(meta.range_key)

        gsi_list = [i.to_dynamo() for i in meta.global_secondary_indexes.values()]
        for gsi_index in gsi_list:
            for key in gsi_index.get("KeySchema", []):
                _add_attribute(key["AttributeName"])

        lsi_list: list[LocalSecondaryIndexTypeDef] = [
            i.to_dynamo(meta.hash_key) for i in meta.local_secondary_indexes.values()
        ]
        for lsi_index in lsi_list:
            for key in lsi_index.get("KeySchema", []):
                _add_attribute(key["AttributeName"])

        attribute_definitions: list[AttributeDefinitionTypeDef] = [
            {"AttributeName": name, "AttributeType": attr_type} for name, attr_type in sorted(attribute_types.items())
        ]

        request: CreateTableInputTypeDef = {
            "TableName": meta.table_name,
            "KeySchema": key_schema,
            "BillingMode": billing_mode,
            "AttributeDefinitions": attribute_definitions,
        }
        if provisioned_throughput is not None:
            request["ProvisionedThroughput"] = provisioned_throughput
        if gsi_list:
            request["GlobalSecondaryIndexes"] = gsi_list
        if lsi_list:
            request["LocalSecondaryIndexes"] = lsi_list
        if tags:
            request["Tags"] = tags
        if table_class:
            request["TableClass"] = table_class

        client: DynamoDBClient
        async with self._client() as client:
            return await client.create_table(**request)

    async def create_global_table[T: DynamoModel](
        self, model: type[T], *, regions: list[str]
    ) -> CreateGlobalTableOutputTypeDef:
        """Create a global table from an existing table.

        Args:
            model: ``DynamoModel`` subclass containing table metadata.
            regions: Replica regions to attach to the global table.

        Returns:
            Raw ``create_global_table`` response from the DynamoDB API.
        """
        meta = model.Meta

        request: CreateGlobalTableInputTypeDef = {
            "GlobalTableName": meta.table_name,
            "ReplicationGroup": [{"RegionName": r} for r in regions],
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


def _build_key(model: type[DynamoModel], *, hash_key: KeyT, range_key: KeyT | None = None) -> dict[str, Any]:
    meta = model.Meta
    key = {meta.hash_key: _serialize_custom_attribute(model, meta.hash_key, hash_key)}
    if meta.range_key and range_key is not None:
        key[meta.range_key] = _serialize_custom_attribute(model, meta.range_key, range_key)
    return key


def _build_dynamo_key(model: type[DynamoModel], *, hash_key: KeyT, range_key: KeyT | None = None) -> dict[str, Any]:
    key = _build_key(model, hash_key=hash_key, range_key=range_key)
    return {k: SERIALIZER._to_dynamo(v) for k, v in key.items()}


def _to_dynamo_expression_values(values: dict[str, Any]) -> dict[str, Any]:
    serialized = _to_dynamo_compatible(values)
    return {k: SERIALIZER._to_dynamo(v) for k, v in serialized.items()}


def _condition_expressions_for_client(
    model: type[DynamoModel],
    expression: ConditionBase | None,
    *,
    builder: CustomConditionExpressionBuilder | None = None,
) -> ConditionExpression:
    payload = _condition_expressions(model, expression, builder=builder)
    if "ExpressionAttributeValues" in payload:
        payload["ExpressionAttributeValues"] = _to_dynamo_expression_values(payload["ExpressionAttributeValues"])
    return payload


def _merge_expression_attributes(
    left: dict[str, Any],
    right: dict[str, Any],
    *,
    kind: str,
) -> dict[str, Any]:
    merged = dict(left)
    for key, value in right.items():
        if key in merged and merged[key] != value:
            raise ValueError(f"Conflicting expression attribute {kind} for placeholder '{key}'.")
        merged[key] = value
    return merged
