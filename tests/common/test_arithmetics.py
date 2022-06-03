from dlt.common import Decimal
from dlt.common.arithmetics import numeric_default_context, numeric_default_quantize


def test_default_numeric_quantize() -> None:
    scale_18 = Decimal("0.532701078956217708")
    # 9 digits scale and round up
    assert numeric_default_quantize(scale_18) == Decimal("0.532701079")
    assert str(numeric_default_quantize(scale_18)) == "0.532701079"

    # 9 digits and round up (HALF UP is default)
    scale_18 = Decimal("0.5327010785")
    assert str(numeric_default_quantize(scale_18)) == "0.532701079"

    # 9 digits and round down
    scale_18 = Decimal("0.5327010784")
    assert str(numeric_default_quantize(scale_18)) == "0.532701078"


    # less than 0 digits
    scale_5 = Decimal("0.4")
    assert str(numeric_default_quantize(scale_5)) == "0.400000000"


def test_numeric_default_context() -> None:
    # we reach (38,9) numeric
    with numeric_default_context():
        v = Decimal(10**29-1) + Decimal("0.532701079")
        assert str(v) == "99999999999999999999999999999.532701079"
        assert numeric_default_quantize(v) == v

    # without context this is 28
    v = Decimal(10**27 - 1) + Decimal("0.5432701079")
    assert str(v) == "999999999999999999999999999.5"
