def test_simple_feature():
    from dlt.feature import get_feature

    feature = get_feature()
    assert feature.calculate(3, 3) == 6
