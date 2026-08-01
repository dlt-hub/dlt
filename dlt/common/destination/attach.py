from typing import List, Literal

from dlt.common.typing import TypedDict
from dlt.common.utils import digest128, merge_keyed_groups

TAttachType = Literal["duckdb", "motherduck"]
"""What a set of attach statements requires of the connection that runs it. The connection
runs all sets the same way. This type only tells a primary which sets it accepts
(see `can_attach`)."""


class TAttachStatement(TypedDict):
    """A single statement that the primary runs to attach a foreign dataset."""

    sql: str
    """The SQL of the statement. When `secret` is True, the persisted model holds ciphertext."""
    secret: bool
    """True when `sql` carries credentials. dlt then encrypts `sql` in the persisted model."""
    key: str
    """What the statement configures. Statements that share a key form one group, and
    `merge_attach` replaces the full group."""


def attach_statement(sql: str, secret: bool = False, key: str = None) -> TAttachStatement:
    """Describes one attach statement. `key` identifies what a later statement redefines, such as
    a view over data that grows, or credentials that rotate. Without `key`, the SQL of the
    statement supplies the key"""
    # dlt persists the key in clear text, so the SQL of a secret cannot be its own key
    return {"sql": sql, "secret": secret, "key": key or (digest128(sql) if secret else sql)}


class TAttachInfo(TypedDict):
    """Serializable attach info that attaches a foreign dataset into a primary SQL client."""

    attach_type: TAttachType
    alias: str
    """`ATTACH` catalog name. It is also the catalog qualifier that a bound query resolves to."""
    statements: List[TAttachStatement]
    """Statements that the primary connection runs, in order. dlt encrypts the secret ones in
    the persisted model."""


def merge_attach(into: TAttachInfo, info: TAttachInfo) -> TAttachInfo:
    """Merges the statements of `info` into `into` and keeps one group for each key"""
    statements, _ = merge_keyed_groups(into["statements"], info["statements"], lambda s: s["key"])
    return {**into, "statements": statements}
