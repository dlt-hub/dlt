import base64
from abc import abstractmethod, ABC
from functools import lru_cache
import math
import hashlib
from typing import Any, List, Protocol, Sequence, Type


class NamingConvention(ABC):

    _TR_TABLE = bytes.maketrans(b"/+", b"ab")
    _DEFAULT_COLLISION_PROB = 0.001

    def __init__(self, max_length: int = None) -> None:
        self.max_length = max_length

    @abstractmethod
    def normalize_identifier(self, identifier: str) -> str:
        """Normalizes and shortens the identifier according to naming convention in this function code"""
        if identifier is None:
            raise ValueError("name is None")
        identifier = identifier.strip()
        if not identifier:
            raise ValueError(identifier)
        return identifier

    def normalize_table_identifier(self, identifier: str) -> str:
        """Normalizes and shortens identifier that will function as a dataset, table or schema name, defaults to `normalize_identifier`"""
        return self.normalize_identifier(identifier)

    @abstractmethod
    def make_path(self, *identifiers: str) -> str:
        """Builds path out of identifiers. Identifiers are neither normalized nor shortened"""
        pass

    @abstractmethod
    def break_path(self, path: str) -> Sequence[str]:
        """Breaks path into sequence of identifiers"""
        pass

    def normalize_path(self, path: str) -> str:
        """Breaks path into identifiers, normalizes components, reconstitutes and shortens the path"""
        normalized_idents = [self.normalize_identifier(ident) for ident in self.break_path(path)]
        # shorten the whole path
        return self.shorten_identifier(self.make_path(*normalized_idents), path, self.max_length)

    def normalize_tables_path(self, path: str) -> str:
        """Breaks path of table identifiers, normalizes components, reconstitutes and shortens the path"""
        normalized_idents = [self.normalize_table_identifier(ident) for ident in self.break_path(path)]
        # shorten the whole path
        return self.shorten_identifier(self.make_path(*normalized_idents), path, self.max_length)

    def shorten_fragments(self, *normalized_idents: str) -> str:
        """Reconstitutes and shortens the path of normalized identifiers"""
        if not normalized_idents:
            return None
        path_str = self.make_path(*normalized_idents)
        return self.shorten_identifier(path_str, path_str, self.max_length)

    @staticmethod
    @lru_cache(maxsize=None)
    def shorten_identifier(normalized_ident: str, identifier: str, max_length: int, collision_prob: float = _DEFAULT_COLLISION_PROB) -> str:
        """Shortens the `name` to `max_length` and adds a tag to it to make it unique. Tag may be placed in the middle or at the end"""
        if max_length and len(normalized_ident) > max_length:
            # use original identifier to compute tag
            tag = NamingConvention._compute_tag(identifier, collision_prob)
            normalized_ident = NamingConvention._trim_and_tag(normalized_ident, tag, max_length)

        return normalized_ident

    @staticmethod
    def _compute_tag(identifier: str, collision_prob: float) -> str:
        # assume that shake_128 has perfect collision resistance 2^N/2 then collision prob is 1/resistance: prob = 1/2^N/2, solving for prob
        # take into account that we are case insensitive in base64 so we need ~1.5x more bits (2+1)
        tl_bytes = int(((2+1)*math.log2(1/(collision_prob)) // 8) + 1)
        tag = base64.b64encode(hashlib.shake_128(identifier.encode("utf-8")).digest(tl_bytes)
                               ).rstrip(b"=").translate(NamingConvention._TR_TABLE).lower().decode("ascii")
        return tag

    @staticmethod
    def _trim_and_tag(identifier: str, tag: str, max_length: int) -> str:
        assert len(tag) <= max_length
        remaining_length = max_length - len(tag)
        remaining_overflow = remaining_length % 2
        identifier = identifier[:remaining_length // 2 + remaining_overflow] + tag + identifier[len(identifier) - remaining_length // 2:]
        assert len(identifier) == max_length
        return identifier


class SupportsNamingConvention(Protocol):
    """Expected of modules defining naming convention"""

    NamingConvention: Type[NamingConvention]
    """A class with a name NamingConvention deriving from normalizers.naming.NamingConvention"""
