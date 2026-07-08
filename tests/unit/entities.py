from enum import IntEnum, StrEnum

from pydantic import BaseModel

from aiodynamodb import DynamoModel, HashKey, RangeKey, table
from aiodynamodb.custom_types import Timestamp
from aiodynamodb.models import GSI, LSI


class UserTypeT(IntEnum):
    foo = 1
    bar = 2


class UserStatusT(StrEnum):
    active = "active"
    inactive = "inactive"


@table("it_users_type")
class UserType(DynamoModel):
    user_type: HashKey[UserTypeT]
    name: str
    active: bool = True


@table("it_user_versions")
class UserVersion(DynamoModel):
    user_id: HashKey[str]
    user_type: RangeKey[UserTypeT]
    name: str


@table("it_status_users")
class StatusUser(DynamoModel):
    status: HashKey[UserStatusT]
    name: str


@table("users")
class User(DynamoModel):
    user_id: HashKey[str]
    name: str
    email: str | None = None


order_gsi = GSI(
    name="order_gsi",
    hash_key="order_id",
    range_key="total",
)

order_lsi = LSI(
    name="order_lsi",
    range_key="total",
)


@table("orders", indexes=[order_gsi, order_lsi])
class Order(DynamoModel):
    order_id: HashKey[str]
    created_at: RangeKey[str]
    total: int


class Item(BaseModel):
    qty: int
    price: float
    name: str


class Basket(BaseModel):
    items: list[Item]


@table("complex_orders", indexes=[order_gsi, order_lsi])
class ComplexOrder(DynamoModel):
    order_id: HashKey[str]
    created_at: RangeKey[Timestamp]
    total: int
    basket: Basket
