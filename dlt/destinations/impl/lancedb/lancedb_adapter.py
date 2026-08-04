import time
from typing import TYPE_CHECKING, Any, Dict, List

from dlt.common import logger
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.common.typing import TColumnNames
from dlt.destinations.impl.lance.lance_adapter import DEFAULT_REMOVE_ORPHANS, lance_adapter
from dlt.extract import DltResource

if TYPE_CHECKING:
    import dlt
    from dlt.destinations.impl.lancedb.lancedb_client import LanceDBClient


ROLLBACK_TIMEOUT_SECONDS = 120.0
"""How long to wait for a restore to become visible to the managed client."""
ROLLBACK_POLL_SECONDS = 2.0


def lancedb_adapter(
    data: Any,
    embed: TColumnNames = None,
    merge_key: TColumnNames = None,
    remove_orphans: bool = DEFAULT_REMOVE_ORPHANS,
) -> DltResource:
    """Prepares data for the LanceDB destination by specifying which columns should be embedded.

    Args:
        data (Any): The data to be transformed. It can be raw data or an instance
            of DltResource. If raw data, the function wraps it into a DltResource
            object.
        embed (TColumnNames, optional): Specify columns to generate embeddings for.
            It can be a single column name as a string, or a list of column names.
        merge_key (TColumnNames, optional): Specify columns to merge on.
            It can be a single column name as a string, or a list of column names.
        remove_orphans (bool): Whether a merge removes records of nested tables whose parent
            record is gone. Off by default.

    Returns:
        DltResource: A resource with applied LanceDB-specific hints.

    Raises:
        ValueError: If input for `embed` invalid or empty.

    Examples:
        >>> data = [{"name": "Marcel", "description": "Moonbase Engineer"}]
        >>> lancedb_adapter(data, embed="description")
        [DltResource with hints applied]
    """
    # both destinations read the same hints, so the `lance` adapter applies them for this one too
    return lance_adapter(data, embed=embed, merge_key=merge_key, remove_orphans=remove_orphans)


def rollback_to_commit_tag(
    dataset: "dlt.Dataset", tag: str, timeout: float = ROLLBACK_TIMEOUT_SECONDS
) -> List[str]:
    """Rolls every table of a LanceDB dataset back to the version named by `tag`.

    A rollback appends a new version holding the tagged contents, so nothing is destroyed and the
    rollback itself can be undone. It waits for the managed client to see the restored version,
    because a load started inside that window fails.

    Warning:
        LanceDB has no transaction spanning tables, so a failure part way through leaves the dataset
        partly rolled back. The names of the tables already restored are logged and returned, and
        running this again is safe.

    Args:
        dataset (dlt.Dataset): Dataset to roll back, from `pipeline.dataset()` or `dlt.dataset(...)`.
        tag (str): Commit tag naming the version to restore, as written by `commit_tag`.
        timeout (float): Seconds to wait for the restore to become visible, per table.

    Returns:
        List[str]: Names of the tables that were restored.

    Raises:
        DestinationTerminalException: If no table of the dataset carries `tag`.

    Example:
        >>> import dlt
        >>> from dlt.destinations.impl.lancedb.lancedb_adapter import rollback_to_commit_tag
        >>> pipeline = dlt.pipeline("movies", destination="lancedb", dataset_name="analytics")
        >>> rollback_to_commit_tag(pipeline.dataset(), "nightly")
        ['movies', '_dlt_loads']
    """
    from dlt.destinations.impl.lancedb.lancedb_client import LanceDBClient

    client = dataset.destination_client
    if not isinstance(client, LanceDBClient):
        raise ValueError(
            "`rollback_to_commit_tag` works on a LanceDB dataset, got"
            f" `{type(client).__name__}`. Open the dataset on a `lancedb` destination."
        )

    skipped: List[str] = []
    # a restore appends a new version, so a current version past the one recorded here publishes it
    heads_before: Dict[str, int] = {}
    for table_name in client.list_owned_table_names():
        table = client.open_table(table_name)
        if tag not in table.tags.list():
            skipped.append(table_name)
            continue
        version = table.tags.get_version(tag)
        heads_before[table_name] = table.version
        table.restore(tag)
        logger.info(f"Restored `{table_name}` of `{client.dataset_name}` to `{tag}` (v{version})")

    if not heads_before:
        raise DestinationTerminalException(
            f"No table of dataset `{client.dataset_name}` carries the commit tag `{tag}`, so there"
            " is nothing to roll back to. List the tags of a table to see which are available."
        )
    if skipped:
        logger.warning(
            f"Tables {skipped} of `{client.dataset_name}` do not carry `{tag}` and were left as"
            " they are, so the dataset mixes versions. They were most likely created after the"
            " tag."
        )

    # every table is restored before anything is awaited, so the propagation windows overlap
    deadline = time.monotonic() + timeout
    for table_name, head_before in heads_before.items():
        _wait_for_restore(client, table_name, head_before, deadline)
    return list(heads_before)


def _wait_for_restore(
    client: "LanceDBClient", table_name: str, head_before: int, deadline: float
) -> None:
    """Waits until the managed client shows the version the restore of `table_name` appended."""
    # the managed client lags the SQL endpoint by tens of seconds and a write in between fails
    while time.monotonic() < deadline:
        if client.open_table(table_name).version > head_before:
            return
        time.sleep(ROLLBACK_POLL_SECONDS)
    logger.warning(
        f"The managed client still does not show the restore of `{table_name}`. A load started now"
        " can fail, so retry it or wait longer."
    )
