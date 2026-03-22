"""Internal helpers for building DynamoDB expression payloads.

These functions convert high-level condition and projection inputs into the
request fragments expected by boto/aioboto3. They are shared across read and
write APIs so placeholder handling stays consistent.
"""

from typing import Any, TypedDict

from boto3.dynamodb.conditions import ConditionBase
from pydantic import BaseModel

from aiodynamodb.conditions import CustomConditionExpressionBuilder
from aiodynamodb.models import DynamoModel
from aiodynamodb.projection import ProjectionExpressionArg, ProjectionExpressionBuilder


class ConditionExpression(TypedDict, total=False):
    """Payload fragment for DynamoDB ``ConditionExpression`` requests."""

    ConditionExpression: str
    ExpressionAttributeNames: dict[str, str]
    ExpressionAttributeValues: dict[str, Any]


class KeyConditionExpression(TypedDict, total=False):
    """Payload fragment for DynamoDB ``KeyConditionExpression`` requests."""

    KeyConditionExpression: str
    ExpressionAttributeNames: dict[str, str]
    ExpressionAttributeValues: dict[str, Any]


class FilterExpression(TypedDict, total=False):
    """Payload fragment for DynamoDB ``FilterExpression`` requests."""

    FilterExpression: str
    ExpressionAttributeNames: dict[str, str]
    ExpressionAttributeValues: dict[str, Any]


class ProjectionExpression(TypedDict, total=False):
    """Payload fragment for DynamoDB ``ProjectionExpression`` requests."""

    ProjectionExpression: str
    ExpressionAttributeNames: dict[str, str]


def _build_condition_expression(
    model: type[BaseModel],
    expression: ConditionBase | None,
    *,
    is_key_condition: bool = False,
    custom_builder: CustomConditionExpressionBuilder | None = None,
    # needed as this is a statefiul object with autoincrementing values!
) -> tuple[str | None, dict[str, str] | None, dict[str, Any] | None]:
    """Build the raw expression string plus placeholder maps.

    When ``expression`` is already a plain string, it is passed through and no
    placeholders are generated. Otherwise the custom builder expands attribute
    paths and serializes values based on the model schema.
    """
    if expression is None:
        return None, None, None
    if not isinstance(expression, ConditionBase):
        return expression, {}, {}
    custom_builder = custom_builder or CustomConditionExpressionBuilder(model)
    built = custom_builder.build_expression(expression, is_key_condition=is_key_condition)
    return (
        built.condition_expression,
        built.attribute_name_placeholders,
        built.attribute_value_placeholders,
    )


def _condition_expressions(
    model: type[DynamoModel],
    expression: ConditionBase | None,
    *,
    builder: CustomConditionExpressionBuilder | None = None,
) -> ConditionExpression:
    """Build a request fragment for write-style condition expressions."""
    dynamo_expression: ConditionExpression = {}
    condition, names, values = _build_condition_expression(
        model, expression, is_key_condition=False, custom_builder=builder
    )
    if condition is not None:
        dynamo_expression["ConditionExpression"] = condition
        if names:
            dynamo_expression["ExpressionAttributeNames"] = names
        if values:
            dynamo_expression["ExpressionAttributeValues"] = values
    return dynamo_expression


def _key_condition_expressions(
    model: type[DynamoModel],
    expression: ConditionBase | None,
    *,
    builder: CustomConditionExpressionBuilder | None = None,
) -> KeyConditionExpression:
    """Build a request fragment for query key conditions."""
    dynamo_expression: KeyConditionExpression = {}
    condition, names, values = _build_condition_expression(
        model,
        expression,
        is_key_condition=True,
        custom_builder=builder,
    )
    if condition is not None:
        dynamo_expression["KeyConditionExpression"] = condition
        if names:
            dynamo_expression["ExpressionAttributeNames"] = names
        if values:
            dynamo_expression["ExpressionAttributeValues"] = values
    return dynamo_expression


def _add_filter_expressions(
    model: type[DynamoModel],
    expression: ConditionBase | None,
    *,
    query_args: dict[str, Any],
    builder: CustomConditionExpressionBuilder | None = None,
) -> None:
    """Merge filter expressions into an existing query argument payload.

    This mutates ``query_args`` so callers can share placeholder state across
    key conditions, filters, and projections.
    """
    condition, names, values = _build_condition_expression(
        model, expression, is_key_condition=False, custom_builder=builder
    )
    if condition is not None:
        query_args["FilterExpression"] = condition
        if names:
            existing_names = query_args.get("ExpressionAttributeNames", {})
            existing_names.update(names)
            query_args["ExpressionAttributeNames"] = existing_names
        if values:
            existing_values = query_args.get("ExpressionAttributeValues", {})
            existing_values.update(values)
            query_args["ExpressionAttributeValues"] = existing_values


def _projection_expression(
    model: type[DynamoModel],
    projection_expression: ProjectionExpressionArg | None,
) -> ProjectionExpression:
    """Build a request fragment for projection expressions."""
    if projection_expression is None:
        return {}

    built = ProjectionExpressionBuilder(model).build_projection_expression(projection_expression)

    payload: ProjectionExpression = {
        "ProjectionExpression": built.projection_expression,
    }
    if built.expression_attribute_names:
        payload["ExpressionAttributeNames"] = built.expression_attribute_names
    return payload
