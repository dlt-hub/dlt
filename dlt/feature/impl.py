from dlt.feature.reference import SupportsSimpleFeature


class SimpleFeature(SupportsSimpleFeature):
    def calculate(self, a: int, b: int) -> int:
        return a + b
