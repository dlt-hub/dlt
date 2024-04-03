from dlt.common.typing import SupportsVariant
from dlt.common.wei import Wei, Decimal


def test_init() -> None:
    assert Wei(1) == 1
    assert Wei.from_int256(10**18, decimals=18) == 1
    # make sure the wei scale is supported
    assert Wei.from_int256(1, decimals=18) == Decimal("0.000000000000000001")
    assert Wei.from_int256(2**256 - 1) == 2**256 - 1
    assert (
        str(Wei.from_int256(2**256 - 1, decimals=18))
        == "115792089237316195423570985008687907853269984665640564039457.584007913129639935"
    )
    assert str(Wei.from_int256(2**256 - 1)) == str(2**256 - 1)
    assert type(Wei.from_int256(1)) is Wei


def test_wei_decimal() -> None:
    assert Wei(1) == 1
    assert Wei(2) > 1
    assert Wei("1213.11") == Decimal("1213.11")
    assert Wei(1276.37).quantize(Decimal("1.00")) == Wei("1276.37")


def test_wei_variant() -> None:
    assert issubclass(Wei, SupportsVariant)
    # but Wei is not SupportsVariant
    assert Wei is not SupportsVariant
    assert isinstance(Wei(1), SupportsVariant)
    # SupportsVariant is callable
    assert callable(Wei(1))

    # we get variant value when we call Wei
    assert (
        Wei(578960446186580977117854925043439539266)()
        == 578960446186580977117854925043439539266
    )
    assert Wei(578960446186580977117854925043439539267)() == (
        "str",
        "578960446186580977117854925043439539267",
    )
    assert (
        Wei(-578960446186580977117854925043439539267)()
        == -578960446186580977117854925043439539267
    )
    assert Wei(-578960446186580977117854925043439539268)() == (
        "str",
        "-578960446186580977117854925043439539268",
    )
