import warnings
from dlt.common.warnings import Dlt100DeprecationWarning


def per_column_cluster_hint_deprecated(clustered_columns: list[str]) -> None:
    columns_str = ", ".join(f"'{col}'" for col in clustered_columns)
    example = (
        "\n\nTo migrate, use the 'cluster' parameter in the `bigquery_adapter` function, for"
        f" example:\n    bigquery_adapter(resource, cluster=[{columns_str}])\nThis ensures the"
        " order of clustered columns is preserved.\n"
    )
    warnings.warn(
        "Defining clustered tables in BigQuery using per-column 'cluster' hints is deprecated and"
        " will be removed in a future release. Clustered columns detected:"
        f" [{columns_str}].{example}",
        Dlt100DeprecationWarning,
        stacklevel=2,
    )
