import pytest

from dlt.common.libs.hexbytes import HexBytes


def test_hexbytes_from_bytes():
    # Test creation from bytes
    binary_string = HexBytes(b"binary string")
    assert isinstance(binary_string, HexBytes)
    assert isinstance(binary_string, bytes)
    assert binary_string == b"binary string"
    assert binary_string.hex() == "0x62696e61727920737472696e67"


def test_hexbytes_from_hex_string():
    # Test creation from hex string with and without 0x prefix
    hex_with_prefix = "0x62696e61727920737472696e67"
    hex_without_prefix = "62696e61727920737472696e67"

    hex_bytes_with_prefix = HexBytes(hex_with_prefix)
    hex_bytes_without_prefix = HexBytes(hex_without_prefix)

    assert hex_bytes_with_prefix == hex_bytes_without_prefix
    assert hex_bytes_with_prefix == b"binary string"
    assert hex_bytes_with_prefix.hex() == "0x62696e61727920737472696e67"


def test_hexbytes_indexing():
    # Test indexing behavior
    test_bytes = HexBytes(b"binary")

    # Test single item access
    first_byte = test_bytes[0]
    last_byte = test_bytes[-1]
    assert first_byte == ord("b")  # should return int
    assert last_byte == ord("y")

    # Test slicing
    middle_slice = test_bytes[1:3]
    assert isinstance(middle_slice, HexBytes)  # slices should return HexBytes
    assert middle_slice == b"in"
    assert test_bytes[:2] == b"bi"  # prefix slice
    assert test_bytes[-2:] == b"ry"  # suffix slice


def test_hexbytes_representation():
    # Test string representation
    test_bytes = HexBytes(b"test")
    assert repr(test_bytes) == "HexBytes('0x74657374')"

    # Test actual bytes content
    assert bytes(test_bytes) == b"test"


def test_hexbytes_comparison():
    # Test equality comparisons
    first_hex = HexBytes(b"test")
    same_as_first = HexBytes(b"test")
    different_hex = HexBytes(b"different")

    assert first_hex == same_as_first
    assert first_hex != different_hex
    assert first_hex == b"test"  # Compare with bytes
    assert first_hex != b"different"


def test_hexbytes_hex_method():
    # Test hex() method specifically
    single_char = HexBytes(b"A")
    assert single_char.hex() == "0x41"  # Should always include 0x prefix

    # Test with empty bytes
    empty_bytes = HexBytes(b"")
    assert empty_bytes.hex() == "0x"
