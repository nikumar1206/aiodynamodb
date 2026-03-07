from decimal import Decimal

from aiodynamodb._serializers import DESERIALIZER, SERIALIZER


def test_serializer_casts_float_to_decimal():
    serialized = SERIALIZER._to_dynamo(12.34)
    assert serialized == {"N": "12.34"}
    assert DESERIALIZER._to_dynamo(serialized) == Decimal("12.34")


def test_serializer_casts_nested_floats_to_decimal():
    payload = {"amount": 1.5, "items": [2.25, {"tax": 0.1}]}
    serialized = SERIALIZER._to_dynamo(payload)
    assert serialized["M"]["amount"] == {"N": "1.5"}
    assert serialized["M"]["items"]["L"][0] == {"N": "2.25"}
    assert serialized["M"]["items"]["L"][1]["M"]["tax"] == {"N": "0.1"}
