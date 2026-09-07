from collections.abc import Set as AbstractSet
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any, Self

from boto3.dynamodb.conditions import AttributeBase
from pydantic import BaseModel

from aiodynamodb.conditions import CustomConditionExpressionBuilder


class Action(Enum):
    """DynamoDB update action keywords. Each value maps 1:1 to an update expression clause."""

    SET = "SET"
    REMOVE = "REMOVE"
    ADD = "ADD"
    DELETE = "DELETE"


class _ListOp(Enum):
    """Internal marker for ``SET`` actions that use ``list_append``."""

    APPEND = "append"
    PREPEND = "prepend"


class UpdateAttr(AttributeBase):
    """DynamoDB update attribute path.

    This inherits from boto3 ``AttributeBase`` so placeholder handling can reuse
    the same builder machinery as condition expressions.
    """

    type: Action
    value: Any
    _list_op: _ListOp | None = None
    _if_not_exists: bool = False

    def set(self, value: Any) -> Self:
        if value is None:
            self.type = Action.REMOVE
        else:
            self.value = value
            self.type = Action.SET
        return self

    def remove(self, index: int | None = None) -> Self:
        """Remove the whole attribute, or a list element at a zero-based index."""
        if index is not None:
            if isinstance(index, bool) or not isinstance(index, int):
                raise TypeError("List index must be an integer")
            if index < 0:
                raise ValueError("List index must be non-negative")
            if self.name.endswith("]"):
                raise ValueError(
                    f"Path '{self.name}' already ends with a list index; "
                    "call remove() without an index or drop the index from the path"
                )
            self.name = f"{self.name}[{index}]"
        self.type = Action.REMOVE
        return self

    def append(self, value: list[Any], *, if_not_exists: bool = True) -> Self:
        """Append elements to a list attribute (``SET path = list_append(path, value)``).

        By default a missing list is treated as empty so the first append creates it.
        Pass ``if_not_exists=False`` to require the list to already exist.
        """
        return self._list_append(value, _ListOp.APPEND, if_not_exists)

    def prepend(self, value: list[Any], *, if_not_exists: bool = True) -> Self:
        """Prepend elements to a list attribute (``SET path = list_append(value, path)``).

        By default a missing list is treated as empty so the first prepend creates it.
        Pass ``if_not_exists=False`` to require the list to already exist.
        """
        return self._list_append(value, _ListOp.PREPEND, if_not_exists)

    def _list_append(self, value: list[Any], op: _ListOp, if_not_exists: bool) -> Self:
        if not isinstance(value, list):
            raise TypeError(f"{op.value}() requires a list of elements")
        self.value = value
        self.type = Action.SET
        self._list_op = op
        self._if_not_exists = if_not_exists
        return self

    def add(self, value: int | float | Decimal | AbstractSet[Any]) -> Self:
        """Add a number to a numeric attribute, or members to a set; lists require append()."""
        if isinstance(value, bool) or not isinstance(value, int | float | Decimal | AbstractSet):
            raise TypeError("ADD requires a number or set; use append() for lists")
        self.value = value
        self.type = Action.ADD
        return self

    def delete(self, value: AbstractSet[Any]) -> Self:
        """Delete members from a set attribute; list elements must be removed by index."""
        if not isinstance(value, AbstractSet):
            raise TypeError("DELETE requires a set; use remove(index) for list elements")
        self.value = value
        self.type = Action.DELETE
        return self

    def __hash__(self) -> int:
        return hash((
            self.type,
            self.name,
            self._list_op,
            self._if_not_exists,
            _freeze_hashable(getattr(self, "value", None)),
        ))


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


def _freeze_hashable(value: Any) -> Any:
    # UpdateAttr instances live in sets, so nested mutable values must be
    # converted into deterministic hashable shapes before hashing.
    if isinstance(value, BaseModel):
        return _freeze_hashable(value.model_dump())
    if isinstance(value, dict):
        return tuple(sorted((k, _freeze_hashable(v)) for k, v in value.items()))
    if isinstance(value, list | tuple):
        return tuple(_freeze_hashable(v) for v in value)
    if isinstance(value, AbstractSet):
        return tuple(sorted((repr(v), _freeze_hashable(v)) for v in value))
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
        if action._list_op is None:
            return f"{name_placeholder} = {value_placeholder}"

        existing = name_placeholder
        if action._if_not_exists:
            empty_placeholder = self._build_value_placeholder([], values)
            existing = f"if_not_exists({name_placeholder}, {empty_placeholder})"
        if action._list_op is _ListOp.APPEND:
            return f"{name_placeholder} = list_append({existing}, {value_placeholder})"
        return f"{name_placeholder} = list_append({value_placeholder}, {existing})"

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
