from typing import (
    TYPE_CHECKING,
    Union,
    cast,
    overload,
)

if TYPE_CHECKING:
    from typing import (
        SupportsIndex,
    )

BytesLike = Union[bytearray, bytes, str, memoryview]

HEX_PREFIX_LOWER = "0x"
HEX_PREFIX_UPPER = "0X"


class HexBytes(bytes):
    """
    HexBytes is a custom library that replaces the hexbytes library to ensure compatibility with the rest of the codebase.
    It has these changes:
        1. It always appends 0x prefix to the hex string.
        2. The representation at console (__repr__) is 0x-prefixed
    """

    def __new__(cls, val: BytesLike) -> "HexBytes":
        bytesval = HexBytes._to_bytes(val)
        return cast(HexBytes, super().__new__(cls, bytesval))  # type: ignore  # https://github.com/python/typeshed/issues/2630  # noqa: E501

    def hex(  # noqa: A003
        self, sep: Union[str, bytes] = None, bytes_per_sep: "SupportsIndex" = 1
    ) -> str:
        """
        Output hex-encoded bytes, with an "0x" prefix.
        Everything following the "0x" is output exactly like :meth:`bytes.hex`.
        """
        return HEX_PREFIX_LOWER + (
            super().hex() if sep is None else super().hex(sep, bytes_per_sep)
        )

    @overload
    def __getitem__(self, key: "SupportsIndex") -> int:  # noqa: F811
        ...

    @overload  # noqa: F811
    def __getitem__(self, key: slice) -> "HexBytes":  # noqa: F811
        ...

    def __getitem__(  # noqa: F811
        self, key: Union["SupportsIndex", slice]
    ) -> Union[int, "HexBytes"]:
        result = super().__getitem__(key)
        return cast(int, result) if isinstance(key, int) else self.__class__(cast(bytes, result))

    def __repr__(self) -> str:
        return f"HexBytes({self.hex()!r})"

    @staticmethod
    def _to_bytes(val: BytesLike) -> bytes:
        """
        Convert BytesLike input to bytes representation.

        Args:
            val: bytes, str (hex), bytearray, or memoryview

        Returns:
            bytes representation of the input
        """
        if isinstance(val, bytes):
            return val
        if isinstance(val, str):
            return HexBytes.fromhex(val)
        return bytes(val)

    @classmethod
    def fromhex(cls, hexstr: str) -> "HexBytes":
        """
        Create HexBytes from hex string, handling optional 0x prefix.

        Args:
            hexstr: Hex string with or without 0x/0X prefix

        Returns:
            HexBytes instance
        """
        cleaned_hex = hexstr.removeprefix(HEX_PREFIX_LOWER).removeprefix(HEX_PREFIX_UPPER)
        return super(HexBytes, cls).__new__(cls, bytes.fromhex(cleaned_hex))
