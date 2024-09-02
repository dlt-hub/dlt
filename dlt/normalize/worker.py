from typing import Callable, List, Dict, NamedTuple, Sequence, Set, Optional, Type

from dlt.common import logger
from dlt.common.configuration.container import Container
from dlt.common.data_writers import (
    DataWriter,
    create_import_spec,
    resolve_best_writer_spec,
    get_best_writer_spec,
    is_native_writer,
)
from dlt.common.metrics import DataWriterMetrics
from dlt.common.utils import chunks
from dlt.common.schema.typing import TStoredSchema, TTableSchema
from dlt.common.storages import (
    NormalizeStorage,
    LoadStorage,
    LoadStorageConfiguration,
    NormalizeStorageConfiguration,
    ParsedLoadJobFileName,
)
from dlt.common.schema import TSchemaUpdate, Schema

from dlt.normalize.configuration import NormalizeConfiguration
from dlt.normalize.exceptions import NormalizeJobFailed
from dlt.normalize.items_normalizers import (
    ArrowItemsNormalizer,
    FileImportNormalizer,
    JsonLItemsNormalizer,
    ItemsNormalizer,
)


class TWorkerRV(NamedTuple):
    schema_updates: List[TSchemaUpdate]
    file_metrics: List[DataWriterMetrics]


def group_worker_files(files: Sequence[str], no_groups: int) -> List[Sequence[str]]:
    # sort files so the same tables are in the same worker
    files = list(sorted(files))

    chunk_size = max(len(files) // no_groups, 1)
    chunk_files = list(chunks(files, chunk_size))
    # distribute the remainder files to existing groups starting from the end
    remainder_l = len(chunk_files) - no_groups
    l_idx = 0
    while remainder_l > 0:
        idx = 0
        for idx, file in enumerate(reversed(chunk_files.pop())):
            chunk_files[-l_idx - idx - remainder_l].append(file)  # type: ignore
        remainder_l -= 1
        l_idx = idx + 1
    return chunk_files


def w_normalize_files(
    config: NormalizeConfiguration,
    normalize_storage_config: NormalizeStorageConfiguration,
    loader_storage_config: LoadStorageConfiguration,
    stored_schema: TStoredSchema,
    load_id: str,
    extracted_items_files: Sequence[str],
) -> TWorkerRV:
    destination_caps = config.destination_capabilities
    schema_updates: List[TSchemaUpdate] = []
    # normalizers are cached per table name
    item_normalizers: Dict[str, ItemsNormalizer] = {}

    preferred_file_format = (
        destination_caps.preferred_loader_file_format
        or destination_caps.preferred_staging_file_format
    )
    # TODO: capabilities.supported_*_formats can be None, it should have defaults
    supported_file_formats = destination_caps.supported_loader_file_formats or []
    supported_table_formats = destination_caps.supported_table_formats or []

    # process all files with data items and write to buffered item storage
    with Container().injectable_context(destination_caps):
        schema = Schema.from_stored_schema(stored_schema)
        normalize_storage = NormalizeStorage(False, normalize_storage_config)
        load_storage = LoadStorage(False, supported_file_formats, loader_storage_config)

        def _get_items_normalizer(
            parsed_file_name: ParsedLoadJobFileName, table_schema: TTableSchema
        ) -> ItemsNormalizer:
            item_format = DataWriter.item_format_from_file_extension(parsed_file_name.file_format)

            table_name = table_schema["name"]
            if table_name in item_normalizers:
                return item_normalizers[table_name]

            if (
                "table_format" in table_schema
                and table_schema["table_format"] not in supported_table_formats
            ):
                logger.warning(
                    "Destination does not support the configured `table_format` value "
                    f"`{table_schema['table_format']}` for table `{table_schema['name']}`. "
                    "The setting will probably be ignored."
                )

            items_preferred_file_format = preferred_file_format
            items_supported_file_formats = supported_file_formats
            if destination_caps.loader_file_format_adapter is not None:
                items_preferred_file_format, items_supported_file_formats = (
                    destination_caps.loader_file_format_adapter(
                        preferred_file_format,
                        (
                            supported_file_formats.copy()
                            if isinstance(supported_file_formats, list)
                            else supported_file_formats
                        ),
                        table_schema=table_schema,
                    )
                )

            best_writer_spec = None
            if item_format == "file":
                # if we want to import file, create a spec that may be used only for importing
                best_writer_spec = create_import_spec(
                    parsed_file_name.file_format, items_supported_file_formats  # type: ignore[arg-type]
                )

            config_loader_file_format = config.loader_file_format
            if file_format := table_schema.get("file_format"):
                # resource has a file format defined so use it
                if file_format == "preferred":
                    # use destination preferred
                    config_loader_file_format = items_preferred_file_format
                else:
                    # use resource format
                    config_loader_file_format = file_format
                logger.info(
                    f"A file format for table {table_name} was specified to {file_format} in the"
                    f" resource so {config_loader_file_format} format being used."
                )

            if config_loader_file_format and best_writer_spec is None:
                # force file format
                if config_loader_file_format in items_supported_file_formats:
                    # TODO: pass supported_file_formats, when used in pipeline we already checked that
                    # but if normalize is used standalone `supported_loader_file_formats` may be unresolved
                    best_writer_spec = get_best_writer_spec(item_format, config_loader_file_format)
                else:
                    logger.warning(
                        f"The configured value `{config_loader_file_format}` "
                        "for `loader_file_format` is not supported for table "
                        f"`{table_name}` and will be ignored. Dlt "
                        "will use a supported format instead."
                    )

            if best_writer_spec is None:
                # find best spec among possible formats taking into account destination preference
                best_writer_spec = resolve_best_writer_spec(
                    item_format, items_supported_file_formats, items_preferred_file_format
                )
                # if best_writer_spec.file_format != preferred_file_format:
                #     logger.warning(
                #         f"For data items yielded as {item_format} jobs in file format"
                #         f" {preferred_file_format} cannot be created."
                #         f" {best_writer_spec.file_format} jobs will be used instead."
                #         " This may decrease the performance."
                #     )
            item_storage = load_storage.create_item_storage(best_writer_spec)
            if not is_native_writer(item_storage.writer_cls):
                logger.warning(
                    f"For data items in `{table_name}` yielded as {item_format} and job file format"
                    f" {best_writer_spec.file_format} native writer could not be found. A"
                    f" {item_storage.writer_cls.__name__} writer is used that internally"
                    f" converts {item_format}. This will degrade performance."
                )
            cls: Type[ItemsNormalizer]
            if item_format == "arrow":
                cls = ArrowItemsNormalizer
            elif item_format == "object":
                cls = JsonLItemsNormalizer
            else:
                cls = FileImportNormalizer
            logger.info(
                f"Created items normalizer {cls.__name__} with writer"
                f" {item_storage.writer_cls.__name__} for item format {item_format} and file"
                f" format {item_storage.writer_spec.file_format}"
            )
            norm = item_normalizers[table_name] = cls(
                item_storage,
                normalize_storage,
                schema,
                load_id,
                config,
            )
            return norm

        def _gather_metrics_and_close(
            parsed_fn: ParsedLoadJobFileName, in_exception: bool
        ) -> List[DataWriterMetrics]:
            writer_metrics: List[DataWriterMetrics] = []
            try:
                try:
                    for normalizer in item_normalizers.values():
                        normalizer.item_storage.close_writers(load_id, skip_flush=in_exception)
                except Exception:
                    # if we had exception during flushing the writers, close them without flushing
                    if not in_exception:
                        for normalizer in item_normalizers.values():
                            normalizer.item_storage.close_writers(load_id, skip_flush=True)
                    raise
                finally:
                    # always gather metrics
                    for normalizer in item_normalizers.values():
                        norm_metrics = normalizer.item_storage.closed_files(load_id)
                        writer_metrics.extend(norm_metrics)
                    for normalizer in item_normalizers.values():
                        normalizer.item_storage.remove_closed_files(load_id)
            except Exception as exc:
                if in_exception:
                    # swallow exception if we already handle exceptions
                    return writer_metrics
                else:
                    # enclose the exception during the closing in job failed exception
                    job_id = parsed_fn.job_id() if parsed_fn else ""
                    raise NormalizeJobFailed(load_id, job_id, str(exc), writer_metrics)
            return writer_metrics

        parsed_file_name: ParsedLoadJobFileName = None
        try:
            root_tables: Set[str] = set()
            for extracted_items_file in extracted_items_files:
                parsed_file_name = ParsedLoadJobFileName.parse(extracted_items_file)
                # normalize table name in case the normalization changed
                # NOTE: this is the best we can do, until a full lineage information is in the schema
                root_table_name = schema.naming.normalize_table_identifier(
                    parsed_file_name.table_name
                )
                root_tables.add(root_table_name)
                normalizer = _get_items_normalizer(
                    parsed_file_name,
                    stored_schema["tables"].get(root_table_name, {"name": root_table_name}),
                )
                logger.debug(
                    f"Processing extracted items in {extracted_items_file} in load_id"
                    f" {load_id} with table name {root_table_name} and schema {schema.name}"
                )
                partial_updates = normalizer(extracted_items_file, root_table_name)
                schema_updates.extend(partial_updates)
                logger.debug(f"Processed file {extracted_items_file}")
        except Exception as exc:
            job_id = parsed_file_name.job_id() if parsed_file_name else ""
            writer_metrics = _gather_metrics_and_close(parsed_file_name, in_exception=True)
            raise NormalizeJobFailed(load_id, job_id, str(exc), writer_metrics) from exc
        else:
            writer_metrics = _gather_metrics_and_close(parsed_file_name, in_exception=False)

        logger.info(f"Processed all items in {len(extracted_items_files)} files")
        return TWorkerRV(schema_updates, writer_metrics)
