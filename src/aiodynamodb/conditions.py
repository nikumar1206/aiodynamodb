from __future__ import annotations

import re
from typing import Any

from boto3.dynamodb.conditions import AttributeBase, BuiltConditionExpression, ConditionBase, Key
from boto3.exceptions import (
    DynamoDBNeedsConditionError,
    DynamoDBNeedsKeyConditionError,
)
from pydantic import BaseModel

from aiodynamodb._serializers import _serialize_custom_attribute

ATTR_NAME_REGEX = re.compile(r"[^.\[\]]+(?![^\[]*\])")


class CustomConditionExpressionBuilder[T: BaseModel]:
    """Build condition expressions and serialize placeholders for model keys."""

    def __init__(
        self,
        model: type[T]
    ):
        self._name_count = 0
        self._value_count = 0
        self._name_placeholder = "n"
        self._value_placeholder = "v"
        self._model = model

    def _get_name_placeholder(self):
        return "#" + self._name_placeholder + str(self._name_count)

    def _get_value_placeholder(self):
        return ":" + self._value_placeholder + str(self._value_count)

    def reset(self):
        self._name_count = 0
        self._value_count = 0

    def build_expression(self, condition, is_key_condition=False):
        if not isinstance(condition, ConditionBase):
            raise DynamoDBNeedsConditionError(condition)
        attribute_name_placeholders = {}
        attribute_value_placeholders = {}
        condition_expression = self._build_expression(
            condition,
            attribute_name_placeholders,
            attribute_value_placeholders,
            is_key_condition=is_key_condition,
        )
        return BuiltConditionExpression(
            condition_expression=condition_expression,
            attribute_name_placeholders=attribute_name_placeholders,
            attribute_value_placeholders=attribute_value_placeholders,
        )

    def _build_expression(
        self,
        condition,
        attribute_name_placeholders,
        attribute_value_placeholders,
        is_key_condition,
    ):
        expression_dict = condition.get_expression()
        replaced_values = []
        current_attribute_name: str | None = None
        for value in expression_dict["values"]:
            if isinstance(value, AttributeBase):
                current_attribute_name = value.name
            replaced_value = self._build_expression_component(
                value,
                attribute_name_placeholders,
                attribute_value_placeholders,
                condition.has_grouped_values,
                is_key_condition,
                attribute_name=current_attribute_name,
            )
            replaced_values.append(replaced_value)
        return expression_dict["format"].format(
            *replaced_values, operator=expression_dict["operator"]
        )

    def _build_expression_component(
        self,
        value,
        attribute_name_placeholders,
        attribute_value_placeholders,
        has_grouped_values,
        is_key_condition,
        attribute_name: str | None = None,
    ):
        if isinstance(value, ConditionBase):
            return self._build_expression(
                value,
                attribute_name_placeholders,
                attribute_value_placeholders,
                is_key_condition,
            )
        if isinstance(value, AttributeBase):
            if is_key_condition and not isinstance(value, Key):
                raise DynamoDBNeedsKeyConditionError(
                    f"Attribute object {value.name} is of type {type(value)}. "
                    "KeyConditionExpression only supports Attribute objects "
                    "of type Key"
                )
            return self._build_name_placeholder(
                value, attribute_name_placeholders
            )
        return self._build_value_placeholder(
            value,
            attribute_value_placeholders,
            has_grouped_values,
            attribute_name=attribute_name,
        )

    def _build_name_placeholder(self, value, attribute_name_placeholders):
        attribute_name = value.name
        attribute_name_parts = ATTR_NAME_REGEX.findall(attribute_name)

        placeholder_format = ATTR_NAME_REGEX.sub("%s", attribute_name)
        str_format_args = []
        for part in attribute_name_parts:
            name_placeholder = self._get_name_placeholder()
            self._name_count += 1
            str_format_args.append(name_placeholder)
            attribute_name_placeholders[name_placeholder] = part
        return placeholder_format % tuple(str_format_args)

    def _serialize_value(self, value: Any, attribute_name: str | None) -> Any:
        root_attr = ATTR_NAME_REGEX.findall(attribute_name)
        if not root_attr:
            return value
        field_name = root_attr[0]
        if field_name not in self._model.model_fields:
            return value
        return _serialize_custom_attribute(self._model, field_name, value)

    def _build_value_placeholder(
        self,
        value,
        attribute_value_placeholders,
        has_grouped_values=False,
        attribute_name: str | None = None,
    ):
        if has_grouped_values:
            placeholder_list = []
            for v in value:
                value_placeholder = self._get_value_placeholder()
                self._value_count += 1
                placeholder_list.append(value_placeholder)
                attribute_value_placeholders[value_placeholder] = self._serialize_value(v, attribute_name)
            return "(" + ", ".join(placeholder_list) + ")"
        value_placeholder = self._get_value_placeholder()
        self._value_count += 1
        attribute_value_placeholders[value_placeholder] = self._serialize_value(value, attribute_name)
        return value_placeholder
