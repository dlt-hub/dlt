from typing import Any

from dlt.common.destination.dataset import SupportsReadableDataset


# helpers
def row_counts(dataset: SupportsReadableDataset[Any], tables: list[str] = None) -> dict[str, int]:
    counts = dataset.row_counts(table_names=tables).arrow().to_pydict()
    return {t: c for t, c in zip(counts["table_name"], counts["row_count"])}
