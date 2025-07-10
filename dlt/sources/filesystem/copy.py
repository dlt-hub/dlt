# implementation of a file copy command
import tempfile, os
from urllib.parse import urlparse
import dlt

from typing import Union, List, Any, overload, Iterator

from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.common.pipeline import SupportsPipeline
from dlt.common.utils import custom_environ

from dlt.extract import DltResource


@dlt.transformer()
def import_transformer(items: Iterator[FileItemDict], temp_dir: str) -> Any:
    for item in items:
        if isinstance(item, FileItemDict):
            url = urlparse(item["file_url"])
            # local files can be imported directly
            if url.scheme == "file":
                local_file_path = item.local_file_path
            else:
                # remote files need to be downloaded to a temp directory
                local_file_path = os.path.join(temp_dir, os.path.basename(item["file_url"]))
                with open(local_file_path, "wb") as f:
                    if file_content := item.get("file_content", None):
                        f.write(file_content)
                    else:
                        f.write(item.read_bytes())
        elif isinstance(item, str):
            local_file_path = item

        print("ITERATE")
        print(local_file_path)

        ext = os.path.splitext(local_file_path)[1][1:]
        if ext not in [
            "jsonl",
            "typed-jsonl",
            "insert_values",
            "parquet",
            "csv",
            "reference",
            "model",
        ]:
            continue

        print(local_file_path)

        yield dlt.mark.with_file_import(local_file_path, ext, 0)  # type: ignore[arg-type]


@overload
def copy_files(
    items: DltResource,
    pipeline: SupportsPipeline,
    run_kwargs: Any = None,
) -> None: ...


@overload
def copy_files(
    items: List[str],
    pipeline: SupportsPipeline,
    table_name: str = None,
    run_kwargs: Any = None,
) -> None: ...


def copy_files(  # type: ignore[misc]
    items: Union[DltResource, List[str]],
    pipeline: SupportsPipeline,
    table_name: str = None,
    run_kwargs: Any = None,
) -> None:
    """Copy a file from the source to the destination.

    Args:
        resource: The source to copy from. May also be an list of local file paths
        pipeline: The pipeline to run. Must have a filesystem destination attached
        table_name: The name of the table to copy the files to, only used if items is a list of local file paths.
        run_kwargs: The kwargs to pass to the pipeline.run method.
    """

    if pipeline.destination.destination_type != "dlt.destinations.filesystem":
        raise ValueError(
            "Filecopy pipeline must have a filesystem destination attached. Found: "
            + pipeline.destination.destination_type
        )

    temp_dir = os.path.join(tempfile.gettempdir(), "dlt")

    # Wrap list of item in resource
    if isinstance(items, list):
        items = dlt.resource(items, table_name=table_name)

    # add import transformer to the resource
    if isinstance(items, DltResource):
        # pipe through transformer and forward table name
        table_name = items.table_name
        items = items | import_transformer(temp_dir)
        items = items.with_name(table_name)
    else:
        raise ValueError("Invalid resource parameter type: " + type(items))

    with custom_environ({"_experimental_exclude_dlt_tables": "true"}):
        pipeline.run(items, **(run_kwargs or {}))
