import typing
from typing import Any, get_args, get_origin

from boto3.dynamodb.conditions import ATTR_NAME_REGEX, AttributeBase, ConditionExpressionBuilder
from pydantic import BaseModel

from aiodynamodb._serializers import _serialize_custom_attribute


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
        if not attribute_name:
            return value
        try:
            return _serialize_custom_attribute(self._model, attribute_name, value)
        except (KeyError, TypeError, ValueError):
            return value

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

    def _build_name_placeholder(self, value, attribute_name_placeholders):
        attribute_name = self._normalize_attribute_name(value.name)
        attribute_name_parts = ATTR_NAME_REGEX.findall(attribute_name)

        placeholder_format = ATTR_NAME_REGEX.sub("%s", attribute_name)
        str_format_args = []
        for part in attribute_name_parts:
            name_placeholder = self._get_name_placeholder()
            self._name_count += 1
            str_format_args.append(name_placeholder)
            attribute_name_placeholders[name_placeholder] = part
        return placeholder_format % tuple(str_format_args)

    def _normalize_attribute_name(self, attribute_name: str) -> str:
        parts = attribute_name.split(".")
        if len(parts) <= 1:
            return attribute_name

        current_model: type[BaseModel] | None = self._model
        normalized_parts: list[str] = []

        for index, part in enumerate(parts):
            if current_model is None:
                normalized_parts.append(part)
                continue

            field_name = part.split("[", 1)[0]
            if field_name not in current_model.model_fields:
                normalized_parts.append(part)
                current_model = None
                continue

            field = current_model.model_fields[field_name]
            annotation = self._unwrap_optional_annotation(field.annotation)
            has_explicit_index = "[" in part
            has_next = index < len(parts) - 1
            if has_next and self._is_sequence_annotation(annotation) and not has_explicit_index:
                normalized_parts.append(f"{field_name}[0]")
            else:
                normalized_parts.append(part)
            current_model = self._extract_nested_model(annotation)

        return ".".join(normalized_parts)

    def _is_sequence_annotation(self, annotation: Any) -> bool:
        resolved = self._unwrap_optional_annotation(annotation)
        origin = get_origin(resolved)
        if origin in (list, set, tuple):
            return True
        if origin is dict:
            return False
        if origin and str(origin) == "typing.Annotated":
            args = get_args(resolved)
            if args:
                return self._is_sequence_annotation(args[0])
        return False

    def _extract_nested_model(self, annotation: Any) -> type[BaseModel] | None:
        resolved = self._unwrap_optional_annotation(annotation)
        origin = get_origin(resolved)

        if origin is None:
            if isinstance(resolved, type) and issubclass(resolved, BaseModel):
                return resolved
            return None

        if origin in (list, set, tuple):
            args = [arg for arg in get_args(resolved) if arg is not Ellipsis]
            if args:
                return self._extract_nested_model(args[0])
            return None

        if origin is dict:
            args = get_args(resolved)
            if len(args) == 2:
                return self._extract_nested_model(args[1])
            return None

        if origin == typing.Annotated:
            args = get_args(resolved)
            if args:
                return self._extract_nested_model(args[0])
            return None

        return None

    def _unwrap_optional_annotation(self, annotation: Any) -> Any:
        origin = get_origin(annotation)
        if origin is None:
            return annotation
        args = get_args(annotation)
        non_none = [arg for arg in args if arg is not type(None)]
        if len(non_none) == 1 and len(non_none) != len(args):
            return self._unwrap_optional_annotation(non_none[0])
        return annotation
