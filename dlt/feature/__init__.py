try:
    from dlt_example_plugin.feature import SupportsExtendedFeature as SupportsFeature
    from dlt_example_plugin.feature import ExtendedFeature as Feature
except ImportError:
    from dlt.feature.reference import SupportsSimpleFeature as SupportsFeature
    from dlt.feature.impl import SimpleFeature as Feature


def get_feature() -> SupportsFeature:
    return Feature()
