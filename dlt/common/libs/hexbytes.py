import binascii
from typing import (
    TYPE_CHECKING,
    Type,
    Union,
    cast,
    overload,
)

if TYPE_CHECKING:
    from typing import (
        SupportsIndex,
    )

BytesLike = Union[bool, bytearray, bytes, int, str, memoryview]


class HexBytes(bytes):
    """
    HexBytes is a custom library that replaces the hexbytes library to ensure compatibility with the rest of the codebase.
    It has these changes:
        1. It always appends 0x prefix to the hex string.
        2. The representation at console (__repr__) is 0x-prefixed
    """

    def __new__(cls: Type[bytes], val: BytesLike) -> "HexBytes":
        bytesval = HexBytes.to_bytes(val)
        return cast(HexBytes, super().__new__(cls, bytesval))  # type: ignore  # https://github.com/python/typeshed/issues/2630  # noqa: E501

    def hex(
        self, sep: Union[str, bytes] = None, bytes_per_sep: "SupportsIndex" = 1
    ) -> str:  # noqa: A003
        """
        Output hex-encoded bytes, with an "0x" prefix.

        Everything following the "0x" is output exactly like :meth:`bytes.hex`.
        """
        # Get raw hex without any prefix
        raw_hex = super().hex()
        # Only add 0x if it's not already there
        if raw_hex.startswith("0x"):
            return raw_hex
        return "0x" + raw_hex

    @overload
    def __getitem__(self, key: "SupportsIndex") -> int:  # noqa: F811
        ...

    @overload  # noqa: F811
    def __getitem__(self, key: slice) -> "HexBytes":  # noqa: F811
        ...

    def __getitem__(  # noqa: F811
        self, key: Union["SupportsIndex", slice]
    ) -> Union[int, bytes, "HexBytes"]:
        result = super().__getitem__(key)
        if hasattr(result, "hex"):
            return type(self)(result)
        else:
            return result

    def __repr__(self) -> str:
        return f"HexBytes({self.hex()!r})"

    @staticmethod
    def to_bytes(val: Union[bool, bytearray, bytes, int, str, memoryview]) -> bytes:
        """
        Equivalent to: `eth_utils.hexstr_if_str(eth_utils.to_bytes, val)` .

        Convert a hex string, integer, or bool, to a bytes representation.
        Alternatively, pass through bytes or bytearray as a bytes value.
        """
        if isinstance(val, bytes):
            return val
        elif isinstance(val, str):
            return HexBytes.hexstr_to_bytes(val)
        elif isinstance(val, bytearray):
            return bytes(val)
        elif isinstance(val, bool):
            return b"\x01" if val else b"\x00"
        elif isinstance(val, int):
            # Note that this int check must come after the bool check, because
            #   isinstance(True, int) is True
            if val < 0:
                raise ValueError(f"Cannot convert negative integer {val} to bytes")
            else:
                return HexBytes.to_bytes(hex(val))
        elif isinstance(val, memoryview):
            return bytes(val)
        else:
            raise TypeError(f"Cannot convert {val!r} of type {type(val)} to bytes")

    @staticmethod
    def hexstr_to_bytes(hexstr: str) -> bytes:
        if hexstr.startswith(("0x", "0X")):
            non_prefixed_hex = hexstr[2:]
        else:
            non_prefixed_hex = hexstr

        # if the hex string is odd-length, then left-pad it to an even length
        if len(hexstr) % 2:
            padded_hex = "0" + non_prefixed_hex
        else:
            padded_hex = non_prefixed_hex

        try:
            ascii_hex = padded_hex.encode("ascii")
        except UnicodeDecodeError:
            raise ValueError(f"hex string {padded_hex} may only contain [0-9a-fA-F] characters")
        else:
            return binascii.unhexlify(ascii_hex)
