from __future__ import annotations

from dataclasses import dataclass

from boto3.dynamodb.conditions import AttributeBase
from pydantic import BaseModel

from aiodynamodb.conditions import CustomConditionExpressionBuilder


class ProjectionAttr(AttributeBase):
    """DynamoDB projection attribute path."""


type ProjectionExpressionArg = list[ProjectionAttr]


@dataclass
class BuiltProjectionExpression:
    projection_expression: str
    expression_attribute_names: dict[str, str]


class ProjectionExpressionBuilder[T: BaseModel](CustomConditionExpressionBuilder[T]):
    """Build DynamoDB projection expressions with placeholders."""

    def build_projection_expression(self, expression: ProjectionExpressionArg) -> BuiltProjectionExpression:
        names: dict[str, str] = {}
        parts = [self._build_projection_part(attribute, names) for attribute in expression]
        return BuiltProjectionExpression(
            projection_expression=", ".join(parts),
            expression_attribute_names=names,
        )

    def _build_projection_part(self, attribute: ProjectionAttr, names: dict[str, str]) -> str:
        return self._build_name_placeholder(attribute, names)
