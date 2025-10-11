from dlt.common.destination.reference import DestinationReference


def test_iceberg_destination() -> None:
    # check that iceberg destination is available
    assert DestinationReference.find("iceberg") is not None


def test_delta_destination() -> None:
    # check that delta destination is available
    assert DestinationReference.find("delta") is not None


def test_adapters_importable() -> None:
    from dlthub.destinations.adapters import iceberg_adapter, iceberg_partition
