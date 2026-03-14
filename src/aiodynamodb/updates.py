from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from boto3.dynamodb.conditions import AttributeBase
from pydantic import BaseModel

from aiodynamodb.conditions import CustomConditionExpressionBuilder


@dataclass(frozen=True)
class _SetAction:
    attribute: UpdateAttr
    value: Any


@dataclass(frozen=True)
class _RemoveAction:
    attribute: UpdateAttr


@dataclass(frozen=True)
class _AddAction:
    attribute: UpdateAttr
    value: Any


@dataclass(frozen=True)
class _DeleteAction:
    attribute: UpdateAttr
    value: Any


class Action(Enum):
    SET = "SET"
    REMOVE = "REMOVE"
    ADD = "ADD"
    DELETE = "DELETE"


class UpdateAttr(AttributeBase):
    """DynamoDB update attribute path.

    This inherits from boto3 ``AttributeBase`` so placeholder handling can reuse
    the same builder machinery as condition expressions.
    """

    type: Action
    value: Any

    def set(self, value: Any) -> UpdateAttr:
        self.value = value
        self.type = Action.SET
        return self

    def remove(self) -> UpdateAttr:
        self.type = Action.REMOVE
        return self

    def add(self, value: Any) -> UpdateAttr:
        self.value = value
        self.type = Action.ADD
        return self

    def delete(self, value: Any) -> UpdateAttr:
        self.value = value
        self.type = Action.DELETE
        return self

    def __hash__(self) -> int:
        return hash((self.type, self.name, _freeze_hashable(getattr(self, "value", None))))


def _freeze_hashable(value: Any) -> Any:
    # UpdateAttr instances live in sets, so nested mutable values must be
    # converted into deterministic hashable shapes before hashing.
    if isinstance(value, dict):
        return tuple(sorted((k, _freeze_hashable(v)) for k, v in value.items()))
    if isinstance(value, list | tuple):
        return tuple(_freeze_hashable(v) for v in value)
    if isinstance(value, set):
        return tuple(sorted(_freeze_hashable(v) for v in value))
    return value


@dataclass
class BuiltUpdateExpression:
    update_expression: str
    expression_attribute_names: dict[str, str]
    expression_attribute_values: dict[str, Any]


class UpdateExpressionBuilder[T: BaseModel](CustomConditionExpressionBuilder[T]):
    """Build DynamoDB update expression with placeholders."""

    def build_update_expression(self, expression: set[UpdateAttr]) -> BuiltUpdateExpression:
        names: dict[str, str] = {}
        values: dict[str, Any] = {}

        set_parts = [
            self._build_set_action(action, names, values) for action in expression if action.type == Action.SET
        ]
        remove_parts = [
            self._build_name_placeholder(action, names) for action in expression if action.type == Action.REMOVE
        ]
        add_parts = [
            self._build_add_delete_action(action, names, values) for action in expression if action.type == Action.ADD
        ]
        delete_parts = [
            self._build_add_delete_action(action, names, values)
            for action in expression
            if action.type == Action.DELETE
        ]

        clauses: list[str] = []
        if set_parts:
            clauses.append("SET " + ", ".join(set_parts))
        if remove_parts:
            clauses.append("REMOVE " + ", ".join(remove_parts))
        if add_parts:
            clauses.append("ADD " + ", ".join(add_parts))
        if delete_parts:
            clauses.append("DELETE " + ", ".join(delete_parts))

        return BuiltUpdateExpression(
            update_expression=" ".join(clauses),
            expression_attribute_names=names,
            expression_attribute_values=values,
        )

    def _build_set_action(
        self,
        action: UpdateAttr,
        names: dict[str, str],
        values: dict[str, Any],
    ) -> str:
        name_placeholder = self._build_name_placeholder(action, names)
        self._current_attribute_name = action.name
        value_placeholder = self._build_value_placeholder(action.value, values)
        return f"{name_placeholder} = {value_placeholder}"

    def _build_add_delete_action(
        self,
        action: UpdateAttr,
        names: dict[str, str],
        values: dict[str, Any],
    ) -> str:
        name_placeholder = self._build_name_placeholder(action, names)
        self._current_attribute_name = action.name
        value_placeholder = self._build_value_placeholder(action.value, values)
        return f"{name_placeholder} {value_placeholder}"
