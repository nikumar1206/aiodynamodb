from aiodynamodb.projection import ProjectionAttr, ProjectionExpressionBuilder
from tests.unit.entities import ComplexOrder, User, UserVersion


def test_projection_builder_serializes_top_level_attributes():
    built = ProjectionExpressionBuilder(User).build_projection_expression([
        ProjectionAttr("user_id"),
        ProjectionAttr("name"),
    ])

    assert built.projection_expression == "#n0, #n1"
    assert built.expression_attribute_names == {
        "#n0": "user_id",
        "#n1": "name",
    }


def test_projection_builder_serializes_nested_list_paths():
    built = ProjectionExpressionBuilder(ComplexOrder).build_projection_expression([ProjectionAttr("basket.items.qty")])

    assert built.projection_expression == "#n0.#n1[0].#n2"
    assert built.expression_attribute_names == {
        "#n0": "basket",
        "#n1": "items",
        "#n2": "qty",
    }


def test_projection_builder_serializes_enum_key_attributes():
    built = ProjectionExpressionBuilder(UserVersion).build_projection_expression([
        ProjectionAttr("user_id"),
        ProjectionAttr("user_type"),
    ])

    assert built.projection_expression == "#n0, #n1"
    assert built.expression_attribute_names == {
        "#n0": "user_id",
        "#n1": "user_type",
    }
