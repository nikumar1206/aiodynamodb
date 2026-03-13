from __future__ import annotations

from typing import Any, TypedDict

from boto3.dynamodb.conditions import ConditionBase
from pydantic import BaseModel
from types_aiobotocore_dynamodb.type_defs import UniversalAttributeValueTypeDef

from aiodynamodb.conditions import CustomConditionExpressionBuilder
from aiodynamodb.models import DynamoModel

type ConditionExpressionT = str | None
type ExpressionAttributeNamesT = dict[str, str] | None
type ExpressionAttributeValuesT = dict[str, UniversalAttributeValueTypeDef] | None



class ConditionExpression(TypedDict):
    ConditionExpression: ConditionExpressionT
    ExpressionAttributeNamesT: ExpressionAttributeNamesT
    ExpressionAttributeValuesT: ExpressionAttributeValuesT


class KeyConditionExpression(TypedDict):
    KeyConditionExpression: ConditionExpressionT
    ExpressionAttributeNamesT: ExpressionAttributeNamesT
    ExpressionAttributeValuesT: ExpressionAttributeValuesT


class FilterExpression(TypedDict):
    FilterExpression: ConditionExpressionT
    ExpressionAttributeNamesT: ExpressionAttributeNamesT
    ExpressionAttributeValuesT: ExpressionAttributeValuesT


def _build_condition_expression(
        model: type[BaseModel],
        expression: ConditionBase | None,
        *,
        is_key_condition: bool = False,
        builder: CustomConditionExpressionBuilder | None = None,
        # needed as this is a statefiul object with autoincrementing values!
) -> tuple[ConditionExpression, ExpressionAttributeNamesT, ExpressionAttributeValuesT]:
    if expression is None:
        return None, None, None
    if not isinstance(expression, ConditionBase):
        return expression, {}, {}
    builder = builder or CustomConditionExpressionBuilder(model)
    built = builder.build_expression(expression, is_key_condition=is_key_condition)
    return (
        built.condition_expression,
        built.attribute_name_placeholders,
        built.attribute_value_placeholders,
    )


def _condition_expressions(
        model: type[DynamoModel],
        expression: ConditionBase | None,
        *,
        builder: CustomConditionExpressionBuilder | None = None
) -> ConditionExpression:
    dynamo_expression = {}
    condition, names, values = _build_condition_expression(
        model,
        expression,
        is_key_condition=False,
        builder=builder
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
        builder: CustomConditionExpressionBuilder | None = None
) -> KeyConditionExpression:
    dynamo_expression = {}
    condition, names, values = _build_condition_expression(
        model,
        expression,
        is_key_condition=True,
        builder=builder
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
) -> FilterExpression:
    condition, names, values = _build_condition_expression(
        model,
        expression,
        is_key_condition=False,
        builder=builder
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
    return query_args
