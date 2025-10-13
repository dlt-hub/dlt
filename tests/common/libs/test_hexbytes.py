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


def test_hexbytes_from_int():
    # Test creation from integers
    single_byte = HexBytes(255)  # 0xff
    assert single_byte == b"\xff"
    assert single_byte.hex() == "0xff"

    # Test with larger number
    two_bytes = HexBytes(65535)  # 0xffff
    assert two_bytes == b"\xff\xff"
    assert two_bytes.hex() == "0xffff"


def test_hexbytes_from_bool():
    # Test creation from booleans
    hex_from_true = HexBytes(True)
    hex_from_false = HexBytes(False)

    assert hex_from_true == b"\x01"
    assert hex_from_false == b"\x00"
    assert hex_from_true.hex() == "0x01"
    assert hex_from_false.hex() == "0x00"


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


def test_hexbytes_errors():
    # Test error cases
    with pytest.raises(ValueError):
        HexBytes(-1)  # Negative integers not allowed

    with pytest.raises(ValueError):
        HexBytes("not a hex string")  # Invalid hex string

    with pytest.raises(TypeError):
        HexBytes(3.14)  # Floats not supported


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
