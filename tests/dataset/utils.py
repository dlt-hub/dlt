from __future__ import annotations

from collections.abc import Sequence
from typing import TypedDict

import dlt


class ItemRow(TypedDict):
    item: str


class OrderRow(TypedDict):
    order_id: int
    amount: float
    items: list[ItemRow]


class UserRow(TypedDict):
    id: int  # noqa: A003
    name: str
    orders: list[OrderRow]


class ProductRow(TypedDict):
    product_id: int
    name: str


class AnnotatedUserRow(TypedDict):
    id: int  # noqa: A003
    name: str


class UserSessionRow(TypedDict):
    session_id: str
    user_id: int
    device: str


class AccountRow(TypedDict):
    account_id: int
    tenant_id: int
    name: str


class AccountMembershipRow(TypedDict):
    membership_id: int
    account_id: int
    tenant_id: int
    user_name: str


TLoadStats = dict[str, int]
TLoadsFixture = tuple[dlt.Dataset, tuple[str, str], tuple[TLoadStats, TLoadStats]]


USERS_DATA_0: list[UserRow] = [
    {
        "id": 1,
        "name": "Alice",
        "orders": [
            {"order_id": 101, "amount": 100.0, "items": [{"item": "A"}, {"item": "B"}]},
            {"order_id": 102, "amount": 200.0, "items": [{"item": "C"}]},
        ],
    },
    {
        "id": 2,
        "name": "Bob",
        "orders": [{"order_id": 103, "amount": 150.0, "items": [{"item": "D"}]}],
    },
]

USERS_DATA_1: list[UserRow] = [
    {
        "id": 3,
        "name": "Charlie",
        "orders": [{"order_id": 104, "amount": 300.0, "items": [{"item": "E"}]}],
    }
]

PRODUCTS_DATA_0: list[ProductRow] = [
    {"product_id": 1, "name": "Widget"},
    {"product_id": 2, "name": "Gadget"},
]
PRODUCTS_DATA_1: list[ProductRow] = [{"product_id": 3, "name": "Doohickey"}]

ANNOTATED_USERS: list[AnnotatedUserRow] = [
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"},
    {"id": 3, "name": "Charlie"},
]

USER_SESSIONS: list[UserSessionRow] = [
    {"session_id": "s1", "user_id": 1, "device": "web"},
    {"session_id": "s2", "user_id": 1, "device": "mobile"},
    {"session_id": "s3", "user_id": 2, "device": "tablet"},
]

ACCOUNTS: list[AccountRow] = [
    {"account_id": 1, "tenant_id": 10, "name": "Acme"},
    {"account_id": 1, "tenant_id": 20, "name": "Globex"},
    {"account_id": 2, "tenant_id": 10, "name": "Initech"},
]

ACCOUNT_MEMBERSHIPS: list[AccountMembershipRow] = [
    {"membership_id": 100, "account_id": 1, "tenant_id": 10, "user_name": "Alice"},
    {"membership_id": 101, "account_id": 1, "tenant_id": 20, "user_name": "Bob"},
    {"membership_id": 102, "account_id": 2, "tenant_id": 10, "user_name": "Charlie"},
]


def _build_load_stats(users: Sequence[UserRow], products: Sequence[ProductRow]) -> TLoadStats:
    return {
        "users": len(users),
        "products": len(products),
        "users__orders": sum(len(user["orders"]) for user in users),
        "users__orders__items": sum(
            len(order["items"]) for user in users for order in user["orders"]
        ),
    }


LOAD_0_STATS = _build_load_stats(USERS_DATA_0, PRODUCTS_DATA_0)
LOAD_1_STATS = _build_load_stats(USERS_DATA_1, PRODUCTS_DATA_1)


@dlt.source(root_key=False)
def crm(i: int = 0):
    @dlt.resource
    def users(batch_idx: int):
        if batch_idx == 0:
            yield USERS_DATA_0
        elif batch_idx == 1:
            yield USERS_DATA_1

    @dlt.resource
    def products(batch_idx: int):
        if batch_idx == 0:
            yield PRODUCTS_DATA_0
        elif batch_idx == 1:
            yield PRODUCTS_DATA_1

    return [users(i), products(i)]


@dlt.source
def annotated_references():
    @dlt.resource(name="users")
    def users():
        yield ANNOTATED_USERS

    @dlt.resource(
        name="user_sessions",
        references=[
            {
                "referenced_table": "users",
                "columns": ["user_id"],
                "referenced_columns": ["id"],
            }
        ],
    )
    def user_sessions():
        yield USER_SESSIONS

    @dlt.resource(name="accounts")
    def accounts():
        yield ACCOUNTS

    @dlt.resource(
        name="account_memberships",
        references=[
            {
                "referenced_table": "accounts",
                "columns": ["account_id", "tenant_id"],
                "referenced_columns": ["account_id", "tenant_id"],
            }
        ],
    )
    def account_memberships():
        yield ACCOUNT_MEMBERSHIPS

    return [users(), user_sessions(), accounts(), account_memberships()]
