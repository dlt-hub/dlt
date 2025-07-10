# implementation of a file copy command
import tempfile, os
from urllib.parse import urlparse
import dlt

from typing import Union, List, Any, overload, Iterator

from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.extract import DltResource
from dlt.common.pipeline import SupportsPipeline
from dlt.common.utils import custom_environ


@dlt.transformer()
def import_transformer(items: Iterator[FileItemDict], temp_dir: str) -> Any:
    for item in items:
        if isinstance(item, FileItemDict):
            url = urlparse(item["file_url"])
            ext = url.path.split(".")[-1]
            # local files can be imported directly
            if url.scheme == "file":
                yield dlt.mark.with_file_import(item.local_file_path, ext, 0)  # type: ignore[arg-type]
            else:
                # remote files need to be downloaded to a temp directory
                local_path = os.path.join(temp_dir, os.path.basename(item["file_url"]))
                with open(local_path, "wb") as f:
                    f.write(item.read_bytes())
                yield dlt.mark.with_file_import(local_path, ext, 0)  # type: ignore[arg-type]
        elif isinstance(item, str):
            ext = os.path.splitext(item)[1][1:]
            yield dlt.mark.with_file_import(item, ext, 0)


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

    if not pipeline.destination.destination_type != "dlt.destinations.filesystem":
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
        items = items | import_transformer(temp_dir)
    else:
        raise ValueError("Invalid resource parameter type: " + type(items))

    with custom_environ({"_experimental_exclude_dlt_tables": "true"}):
        pipeline.run(items, **(run_kwargs or {}))
