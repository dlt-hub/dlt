from typing import List, Literal

from dlt.common.typing import TypedDict
from dlt.common.utils import digest128, merge_keyed_groups

TAttachType = Literal["duckdb", "motherduck"]
"""What a set of attach statements requires of the connection running it. All sets are executed
the same way, this only tells a primary which ones it can accept, see `can_attach`."""


class TAttachStatement(TypedDict):
    """A single statement run to attach a foreign dataset."""

    sql: str
    """The statement SQL; replaced by ciphertext in the persisted model when `secret` is True."""
    secret: bool
    """Whether `sql` carries credentials and must be encrypted when the model is persisted."""
    key: str
    """What the statement configures. Statements sharing a key form a group replaced as a whole."""


def attach_statement(sql: str, secret: bool = False, key: str = None) -> TAttachStatement:
    """Describes one attach statement, keyed by `key` or by the statement itself"""
    # pass `key` for what gets redefined later - a view over growing data, rotating credentials.
    # it is persisted in clear text, so a secret may not be identified by its own SQL
    return {"sql": sql, "secret": secret, "key": key or (digest128(sql) if secret else sql)}


class TAttachInfo(TypedDict):
    """Serializable descriptor to attach a foreign dataset into a primary SQL client."""

    attach_type: TAttachType
    alias: str
    """ATTACH catalog name, also the catalog qualifier a bound query resolves to."""
    statements: List[TAttachStatement]
    """Ordered statements run on the primary connection; secret ones are encrypted when persisted."""


def merge_attach(into: TAttachInfo, info: TAttachInfo) -> TAttachInfo:
    """Merges the statements of `info` into `into`, keeping one group per key"""
    statements, _ = merge_keyed_groups(into["statements"], info["statements"], lambda s: s["key"])
    return {**into, "statements": statements}
