# implementation of a file copy command
import tempfile, os
from urllib.parse import urlparse
import dlt

from typing import Any, Iterator

from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.common.pipeline import SupportsPipeline
from dlt.common.utils import custom_environ

from dlt.common.storages.configuration import FilesystemConfiguration

from dlt.extract import DltResource

__source_name__ = "filesystem"


def _import_files(items: Iterator[FileItemDict], temp_dir: str) -> Any:
    for item in items:
        if isinstance(item, FileItemDict):
            # local files can be imported directly
            if FilesystemConfiguration.is_local_path(item["file_url"]):
                local_file_path = item.local_file_path
            else:
                url = urlparse(item["file_url"])
                base_name = os.path.basename(url.path)
                remote_dir_name = os.path.dirname(url.path)

                # remote files need to be downloaded to a temp directory with the remote structure in tact
                local_file_dir = os.path.join(temp_dir, remote_dir_name[1:])
                base_name = os.path.basename(item["file_url"])
                local_file_path = os.path.join(local_file_dir, base_name)

                # copy file to the local filesystem
                os.makedirs(local_file_dir, exist_ok=True)
                with open(local_file_path, "wb") as f:
                    if file_content := item.get("file_content", None):
                        f.write(file_content)
                    else:
                        f.write(item.read_bytes())
        # for string we assume it is a local file path
        # TODO: does this make sense?
        elif isinstance(item, str):
            local_file_path = item

        ext = os.path.splitext(local_file_path)[1][1:]

        # TODO: should we raise? Should it be configurable?
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

        yield dlt.mark.with_file_import(local_file_path, ext, 0)  # type: ignore[arg-type]


def copy_files(
    items: DltResource,
    pipeline: SupportsPipeline,
    run_kwargs: Any = None,
) -> None:
    """Copy a file from the source to the destination.

    Args:
        resource: The source to copy from.
        pipeline: The pipeline to run. Must have a filesystem destination attached
        run_kwargs: The kwargs to pass to the pipeline.run method.
    """

    if pipeline.destination.destination_type != "dlt.destinations.filesystem":
        raise ValueError(
            "Filecopy pipeline must have a filesystem destination attached. Found: "
            + pipeline.destination.destination_type
        )

    temp_dir = os.path.join(tempfile.gettempdir(), pipeline.dataset_name)

    # add import transformer to the resource
    if isinstance(items, DltResource):
        # pipe through transformer and forward table name
        table_name = items.table_name
        items = items | dlt.transformer(table_name=table_name)(_import_files).bind(
            temp_dir=temp_dir
        )
    else:
        raise ValueError("Invalid resource parameter type: " + type(items))

    with custom_environ({"DESTINATION__EXPERIMENTAL_EXCLUDE_DLT_TABLES": "True"}):
        pipeline.run(items, **(run_kwargs or {}))
