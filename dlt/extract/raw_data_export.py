import inspect
import posixpath
import logging
from functools import wraps
from typing import (
    Any,
    ClassVar,
    Dict,
    Optional,
)

from dlt.common.typing import (
    TDataItem,
    TDataItems,
    TFun,
    TLoaderFileFormat,
    extract_inner_type,
    is_subclass,
    resolve_single_annotation,
)
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.storages.configuration import FileSystemCredentials

from dlt.extract.items import SupportsPipe
from dlt.extract.items_transform import ItemTransform


logger = logging.getLogger(__name__)


@configspec
class RawDataExporter(ItemTransform[TDataItem, Dict[str, Any]], BaseConfiguration):
    """Exports extracted data to object storage as a side-effect in the pipe chain.

    Supports all item types:
    - FileItemDict: raw byte copy (preserves original file format)
    - dict: buffered and serialized as JSONL (or configurable format)
    - Arrow Table / RecordBatch / DataFrame: written as Parquet

    Used as a resource function argument, like dlt.sources.incremental:

        @dlt.resource
        def my_files(export=dlt.sources.raw_export('s3://bucket/raw', 'relative_path')):
            yield from filesystem("s3://source/", file_glob="**/*")

    Credentials are resolved from secrets.toml / env vars via @configspec,
    same pattern as Incremental.
    """

    placement_affinity: ClassVar[float] = 1.2

    # configspec-resolvable fields (resolved from TOML/env vars)
    bucket_url: str = None
    name_path: str = None
    file_format: Optional[TLoaderFileFormat] = None
    credentials: Optional[FileSystemCredentials] = None
    enable_in_dev_mode: bool = False
    export_only: bool = False

    def __init__(
        self,
        bucket_url: str,
        name_path: str,
        file_format: Optional[TLoaderFileFormat] = None,
        credentials: Optional[FileSystemCredentials] = None,
        enable_in_dev_mode: bool = False,
        export_only: bool = False,
    ) -> None:
        super().__init__(lambda item: item)
        self.bucket_url = bucket_url
        self.name_path = name_path
        self.file_format = file_format
        self.credentials = credentials
        self.enable_in_dev_mode = enable_in_dev_mode
        self.export_only = export_only

        # set on bind()
        self.fs_client: Optional[Any] = None
        self._resource_name: str = ""
        self._export_path: str = ""
        self._disabled: bool = False
        self._warned: bool = False
        self._load_id: str = ""

        # per-name_path buffers for dict items
        self._writers: Dict[str, Any] = {}

    def copy(self) -> "RawDataExporter":
        """Create a copy of this exporter. Required when the exporter is used as a
        default argument value — each resource call needs its own instance.
        """
        c = RawDataExporter(
            bucket_url=self.bucket_url,
            name_path=self.name_path,
            file_format=self.file_format,
            credentials=self.credentials,
            enable_in_dev_mode=self.enable_in_dev_mode,
            export_only=self.export_only,
        )
        # preserve pre-configured filesystem (set directly for testing or custom setups)
        c.fs_client = self.fs_client
        return c

    @property
    def export_path(self) -> str:
        """Resolved export path for this resource. Available after bind()."""
        return self._export_path

    def bind(self, pipe: SupportsPipe) -> "RawDataExporter":
        """Called when pipe evaluation starts."""
        self._resource_name = pipe.name
        self._warned = False
        self._disabled = False

        # reset buffers from previous run
        self._writers = {}

        from dlt.common.pipeline import current_pipeline

        active_pipeline = current_pipeline()
        is_dev_mode = active_pipeline and getattr(active_pipeline, "dev_mode", False)

        # disable in dev_mode unless explicitly opted in
        if is_dev_mode and not self.enable_in_dev_mode:
            logger.info(
                f"Raw data export disabled for resource '{pipe.name}': "
                "pipeline is running in dev_mode. "
                "Set enable_in_dev_mode=True to export during dev_mode."
            )
            self._disabled = True
            return self

        # require active pipeline for meaningful path and credential resolution
        if not active_pipeline:
            from dlt.extract.exceptions import PipeException

            raise PipeException(
                pipe.name,
                "RawDataExporter requires an active pipeline context. "
                "Use pipeline.run() or pipeline.extract().",
            )

        # build path: {bucket_url}/{pipeline_name}/{source_name}/{resource_name}
        bucket_url = self.bucket_url.rstrip("/")
        pipeline_name = active_pipeline.pipeline_name

        from dlt.common.configuration.container import Container
        from dlt.common.configuration.specs import ConfigSectionContext

        try:
            source_name = Container()[ConfigSectionContext].source_state_key
        except Exception:
            source_name = "unknown"

        # in dev_mode with enable_in_dev_mode=True, isolate under _dev{instance_id}/
        if is_dev_mode:
            instance_id = getattr(active_pipeline, "_pipeline_instance_id", "")
            self._export_path = (
                f"{bucket_url}/{pipeline_name}/_dev{instance_id}"
                f"/{source_name}/{self._resource_name}"
            )
        else:
            self._export_path = f"{bucket_url}/{pipeline_name}/{source_name}/{self._resource_name}"

        # create fsspec filesystem from bucket_url + credentials
        # credentials are resolved by @configspec from secrets.toml / env vars
        if not self.fs_client:
            from dlt.common.storages.fsspec_filesystem import fsspec_filesystem

            self.fs_client, _ = fsspec_filesystem(
                self.bucket_url.split("://")[0],
                self.credentials,
            )

        # ensure base directory exists
        self.fs_client.makedirs(self._export_path, exist_ok=True)

        # capture load_id for run-scoped file naming (dict/Arrow exports)
        try:
            from dlt.common.storages.load_package import load_package_state

            self._load_id = load_package_state()["load_id"]
        except Exception:
            from dlt.common.storages.load_package import create_load_id

            self._load_id = create_load_id()

        logger.info(
            f"Bind raw data export on {self._resource_name} with "
            f"export_path: {self._export_path}, name_path: {self.name_path}, "
            f"export_only: {self.export_only}"
        )

        return self

    def _sanitize_name_value(self, name_value: str) -> str:
        """Sanitize a name_path value to prevent path traversal (CWE-22)."""
        if not name_value:
            raise ValueError(
                f"name_path '{self.name_path}' resolved to empty string "
                f"for resource '{self._resource_name}'"
            )
        name_value = posixpath.normpath(name_value).lstrip("/")
        if name_value.startswith(".."):
            raise ValueError(
                f"name_path value '{name_value}' resolves outside export path "
                f"'{self._export_path}' for resource '{self._resource_name}'"
            )
        return name_value

    def _resolve_file_path(self, item: TDataItem) -> str:
        """Extract name_path value from a dict item and build the full file path.

        Used by FileItemDict copy (flat deterministic path).
        """
        try:
            name_value = str(item[self.name_path])
        except (KeyError, TypeError) as e:
            raise KeyError(
                f"name_path field '{self.name_path}' not found in item "
                f"for resource '{self._resource_name}'. "
                f"Available keys: {list(item.keys()) if isinstance(item, dict) else 'N/A'}"
            ) from e
        name_value = self._sanitize_name_value(name_value)
        return f"{self._export_path}/{name_value}"

    def _resolve_name_value(self, item: TDataItem) -> str:
        """Extract the name_path field value from any item type.

        Returns the sanitized string value for use in file path construction.
        """
        if isinstance(item, dict):
            try:
                raw = str(item[self.name_path])
            except (KeyError, TypeError) as e:
                raise KeyError(
                    f"name_path field '{self.name_path}' not found in item "
                    f"for resource '{self._resource_name}'. "
                    f"Available keys: {list(item.keys()) if isinstance(item, dict) else 'N/A'}"
                ) from e
        elif hasattr(item, "column"):
            # Arrow Table / RecordBatch
            raw = str(item.column(self.name_path)[0].as_py())
        elif hasattr(item, "iloc"):
            # pandas DataFrame
            raw = str(item[self.name_path].iloc[0])
        else:
            raw = str(getattr(item, self.name_path))
        return self._sanitize_name_value(raw)

    def _copy_file(self, item: TDataItem) -> None:
        """Copy a single file from source to destination. Raw byte copy."""
        file_path = self._resolve_file_path(item)
        parent = posixpath.dirname(file_path)
        self.fs_client.makedirs(parent, exist_ok=True)

        with item.open(mode="rb") as src, self.fs_client.open(file_path, "wb") as dst:
            while chunk := src.read(8 * 1024 * 1024):
                dst.write(chunk)

    def _buffer_dict_item(self, item: TDataItem) -> None:
        """Buffer a dict item per name_path value.

        Items are collected in memory and flushed on teardown() or when
        the buffer exceeds the threshold.
        """
        name_value = self._resolve_name_value(item)
        self._writers.setdefault(name_value, []).append(item)
        # flush if buffer is large enough
        total = sum(len(v) for v in self._writers.values() if isinstance(v, list))
        if total >= 5000:
            self._flush_dict_buffer(name_value)

    def _flush_dict_buffer(self, name_value: str) -> None:
        """Flush buffered dict items for a name_path to a file via fsspec."""
        items = self._writers.pop(name_value, [])
        if not items or not isinstance(items, list):
            return

        from dlt.common.data_writers.writers import DataWriter
        from dlt.common.data_writers.buffered import new_file_id

        effective_format = self.file_format or "jsonl"
        file_id = new_file_id()
        dir_path = f"{self._export_path}/{name_value}"
        self.fs_client.makedirs(dir_path, exist_ok=True)

        if effective_format == "parquet":
            import pyarrow as pa

            file_path = f"{dir_path}/{self._load_id}.{file_id}.parquet"
            arrow_table = pa.Table.from_pylist(items)
            f = self.fs_client.open(file_path, "wb")
            writer = DataWriter.from_file_format("parquet", "arrow", f)
        else:
            spec = DataWriter.writer_spec_from_file_format(effective_format, "object")
            file_path = f"{dir_path}/{self._load_id}.{file_id}.{spec.file_extension}"
            mode = "wb" if spec.is_binary_format else "w"
            f = self.fs_client.open(file_path, mode)
            writer = DataWriter.from_file_format(effective_format, "object", f)
            arrow_table = None

        try:
            writer.write_header({})
            writer.write_data([arrow_table] if arrow_table is not None else items)
            writer.write_footer()
            f.flush()
        finally:
            writer.close()
            f.close()

    def _write_arrow_item(self, item: TDataItem) -> None:
        """Write an Arrow table or DataFrame as a single file.

        Uses load_id-scoped path for run isolation. Follows BufferedDataWriter's
        lifecycle pattern: write_header -> write_data -> write_footer -> close.
        """
        from dlt.common.data_writers.writers import DataWriter
        from dlt.common.data_writers.buffered import new_file_id

        name_value = self._resolve_name_value(item)
        effective_format = self.file_format or "parquet"
        spec = DataWriter.writer_spec_from_file_format(effective_format, "arrow")
        file_id = new_file_id()
        dir_path = f"{self._export_path}/{name_value}"
        file_path = f"{dir_path}/{self._load_id}.{file_id}.{spec.file_extension}"
        self.fs_client.makedirs(dir_path, exist_ok=True)

        f = self.fs_client.open(file_path, "wb")
        writer = DataWriter.from_file_format(effective_format, "arrow", f)
        try:
            writer.write_header({})
            writer.write_data([item])
            writer.write_footer()
            f.flush()
        finally:
            writer.close()
            f.close()

    def _handle_item(self, item: TDataItem) -> None:
        """Route item to the correct write strategy based on type."""
        if item is None:
            return

        from dlt.common.storages.fsspec_filesystem import FileItemDict

        # FileItemDict: raw byte copy
        if isinstance(item, FileItemDict):
            self._copy_file(item)
            return

        from dlt.extract.utils import get_data_item_format

        item_format = get_data_item_format(item)

        # "model" format (ReadableDBAPIRelation): skip
        if item_format == "model":
            return

        # Arrow / DataFrame: 1:1, write immediately with load_id-scoped path
        if item_format == "arrow":
            self._write_arrow_item(item)
            return

        # dict: buffer, flush on threshold or teardown
        if isinstance(item, dict):
            self._buffer_dict_item(item)
            return

        if not self._warned:
            logger.warning(
                "Raw data export for resource '%s' received "
                "unrecognized item type (%s). Item will pass through "
                "without being exported.",
                self._resource_name,
                type(item).__name__,
            )
            self._warned = True

    def teardown(self) -> None:
        """Flush all remaining dict buffers to storage."""
        for name_value in list(self._writers.keys()):
            if isinstance(self._writers.get(name_value), list):
                self._flush_dict_buffer(name_value)
        self._writers.clear()

    def __call__(self, item: TDataItems, meta: Any = None) -> Optional[TDataItems]:
        """Export item to storage, pass through unchanged (or consume if export_only)."""
        if item is None:
            return None
        if self._disabled:
            if self.export_only and not self._warned:
                logger.warning(
                    f"Raw data export for resource '{self._resource_name}' is disabled "
                    "(dev_mode) AND export_only=True. No data will be exported or "
                    "passed to the destination. This is a no-op pipeline."
                )
                self._warned = True
            return None if self.export_only else item

        if isinstance(item, list):
            for row in item:
                self._handle_item(row)
        else:
            self._handle_item(item)

        return None if self.export_only else item


class RawDataExportWrapper(ItemTransform[TDataItem, Dict[str, Any]]):
    """Wraps a resource function that accepts a RawDataExporter argument.

    Same pattern as IncrementalResourceWrapper:
    - Detects export argument via signature inspection
    - Wraps the resource function to inject the exporter at call time
    - Delegates __call__ to the inner RawDataExporter
    """

    placement_affinity: ClassVar[float] = 1.2

    def __init__(self) -> None:
        super().__init__(lambda item: item)
        self._exporter: Optional[RawDataExporter] = None

    @staticmethod
    def should_wrap(sig: inspect.Signature) -> bool:
        return RawDataExportWrapper.get_export_arg(sig) is not None

    @staticmethod
    def get_export_arg(sig: inspect.Signature) -> Optional[inspect.Parameter]:
        for p in sig.parameters.values():
            annotation = extract_inner_type(
                resolve_single_annotation(p.annotation, globalns=globals())
            )
            if is_subclass(annotation, RawDataExporter) or isinstance(p.default, RawDataExporter):
                return p
        return None

    def wrap(self, sig: inspect.Signature, func: TFun) -> TFun:
        export_param = self.get_export_arg(sig)
        assert export_param

        @wraps(func)
        def _wrap(*args: Any, **kwargs: Any) -> Any:
            # bind without defaults first to detect explicit vs default args
            try:
                bound_args = sig.bind(*args, **kwargs)
            except TypeError:
                bound_args = sig.bind_partial(*args, **kwargs)

            if export_param.name in bound_args.arguments:
                # user passed an explicit value
                explicit_value = bound_args.arguments[export_param.name]
                if isinstance(explicit_value, RawDataExporter):
                    self._exporter = explicit_value
                elif explicit_value is None:
                    self._exporter = None
                else:
                    self._exporter = None
            elif isinstance(export_param.default, RawDataExporter):
                # no explicit value — copy the default
                self._exporter = export_param.default.copy()
            else:
                self._exporter = None

            bound_args.apply_defaults()
            bound_args.arguments[export_param.name] = self._exporter
            return func(*bound_args.args, **bound_args.kwargs)

        return _wrap  # type: ignore[return-value]

    def bind(self, pipe: SupportsPipe) -> "RawDataExportWrapper":
        if self._exporter:
            self._exporter.bind(pipe)
        return self

    def teardown(self) -> None:
        """Delegates teardown to the inner exporter."""
        if self._exporter:
            self._exporter.teardown()

    @property
    def export_path(self) -> str:
        """Delegates to the inner exporter's export_path."""
        if self._exporter:
            return self._exporter.export_path
        return ""

    def __call__(self, item: TDataItems, meta: Any = None) -> Optional[TDataItems]:
        if not self._exporter:
            return item
        return self._exporter(item, meta)
