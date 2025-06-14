from dlt._dataset.factory import Dataset


def is_same_physical_destination(dataset1: Dataset, dataset2: Dataset) -> bool:
    """
    Returns true if the other dataset is on the same physical destination
    helpful if we want to run sql queries without extracting the data
    """
    return str(dataset1.destination_client.config) == str(dataset2.destination_client.config)