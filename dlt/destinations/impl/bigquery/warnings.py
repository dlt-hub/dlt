import warnings
from dlt.common.warnings import Dlt100DeprecationWarning

def per_column_cluster_hint_deprecated(clustered_columns: list[str]) -> None:
    columns_str = ", ".join(f"'{col}'" for col in clustered_columns)
    example = (
        f"\n\nTo migrate, use the 'cluster' parameter in the `bigquery_adapter` function, for example:\n"
        f"    bigquery_adapter(resource, cluster=[{columns_str}])\n"
        "This ensures the order of clustered columns is preserved.\n"
    )
    warnings.warn(
        f"Defining clustered tables in BigQuery using per-column 'cluster' hints is deprecated and will be removed in a future release. "
        f"Clustered columns detected: [{columns_str}]."
        f"{example}",
        Dlt100DeprecationWarning,
        stacklevel=2,
    )
