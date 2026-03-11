from __future__ import annotations

import re
from typing import Any

from boto3.dynamodb.conditions import AttributeBase, ConditionExpressionBuilder
from pydantic import BaseModel

from aiodynamodb._serializers import _serialize_custom_attribute

ATTR_NAME_REGEX = re.compile(r"[^.\[\]]+(?![^\[]*\])")


class CustomConditionExpressionBuilder[T: BaseModel](ConditionExpressionBuilder):
    """Build condition expressions and serialize placeholders for model keys."""

    def __init__(self, model: type[T]):
        super().__init__()
        self._model = model

    def _build_expression_component(
        self,
        value,
        attribute_name_placeholders,
        attribute_value_placeholders,
        has_grouped_values,
        is_key_condition,
    ):
        if isinstance(value, AttributeBase):
            self._current_attribute_name = value.name
        return super()._build_expression_component(
            value,
            attribute_name_placeholders,
            attribute_value_placeholders,
            has_grouped_values,
            is_key_condition,
        )

    def _serialize_value(self, value: Any) -> Any:
        attribute_name = getattr(self, "_current_attribute_name", None)
        root_attr = ATTR_NAME_REGEX.findall(attribute_name or "")
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
    ):
        if has_grouped_values:
            placeholder_list = []
            for v in value:
                value_placeholder = self._get_value_placeholder()
                self._value_count += 1
                placeholder_list.append(value_placeholder)
                attribute_value_placeholders[value_placeholder] = self._serialize_value(v)
            return "(" + ", ".join(placeholder_list) + ")"
        value_placeholder = self._get_value_placeholder()
        self._value_count += 1
        attribute_value_placeholders[value_placeholder] = self._serialize_value(value)
        return value_placeholder
