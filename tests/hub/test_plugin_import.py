import pytest


def test_import_props() -> None:
    import dlt.hub

    # hub plugin found
    assert dlt.hub.__found__
    assert len(dlt.hub.__all__) > 0

    # no exception
    assert dlt.hub.__exception__ is None

    # regular attribute error raised

    with pytest.raises(AttributeError) as attr_err:
        dlt.hub._unknown_feature

    assert attr_err.value.name == "_unknown_feature"
