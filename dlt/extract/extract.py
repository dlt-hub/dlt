import contextlib
from typing import Set, Dict, Optional, Set

from dlt.common.configuration.container import Container
from dlt.common.configuration.resolve import inject_section
from dlt.common.configuration.specs import ConfigSectionContext, known_sections
from dlt.common.pipeline import reset_resource_state
from dlt.common.data_writers import TLoaderFileFormat

from dlt.common.runtime import signals
from dlt.common.runtime.collector import Collector, NULL_COLLECTOR
from dlt.common.schema import utils

from dlt.extract.decorators import SourceSchemaInjectableContext
from dlt.extract.exceptions import DataItemRequiredForDynamicTableHints
from dlt.extract.pipe import PipeIterator
from dlt.extract.source import DltSource
from dlt.extract.storage import ExtractorStorage
from dlt.extract.extractors import JsonLExtractor, ArrowExtractor, Extractor


def extract(
    extract_id: str,
    source: DltSource,
    storage: ExtractorStorage,
    collector: Collector = NULL_COLLECTOR,
    *,
    max_parallel_items: int = None,
    workers: int = None,
    futures_poll_interval: float = None
) -> None:
    schema = source.schema
    resources_with_items: Set[str] = set()
    extractors: Dict[TLoaderFileFormat, Extractor] = {
        "puae-jsonl": JsonLExtractor(
            extract_id, storage, schema, resources_with_items, collector=collector
        ),
        "arrow": ArrowExtractor(
            extract_id, storage, schema, resources_with_items, collector=collector
        )
    }
    last_item_format: Optional[TLoaderFileFormat] = None

    with collector(f"Extract {source.name}"):
        # yield from all selected pipes
        with PipeIterator.from_pipes(source.resources.selected_pipes, max_parallel_items=max_parallel_items, workers=workers, futures_poll_interval=futures_poll_interval) as pipes:
            left_gens = total_gens = len(pipes._sources)
            collector.update("Resources", 0, total_gens)
            for pipe_item in pipes:

                curr_gens = len(pipes._sources)
                if left_gens > curr_gens:
                    delta = left_gens - curr_gens
                    left_gens -= delta
                    collector.update("Resources", delta)

                signals.raise_if_signalled()

                resource = source.resources[pipe_item.pipe.name]
                # Fallback to last item's format or default (puae-jsonl) if the current item is an empty list
                item_format = Extractor.item_format(pipe_item.item) or last_item_format or "puae-jsonl"
                extractors[item_format].write_items(resource, pipe_item.item, pipe_item.meta)
                last_item_format = item_format

            # find defined resources that did not yield any pipeitems and create empty jobs for them
            data_tables = {t["name"]: t for t in schema.data_tables()}
            tables_by_resources = utils.group_tables_by_resource(data_tables)
            for resource in source.resources.selected.values():
                if resource.write_disposition != "replace" or resource.name in resources_with_items:
                    continue
                if resource.name not in tables_by_resources:
                    continue
                for table in tables_by_resources[resource.name]:
                    # we only need to write empty files for the top tables
                    if not table.get("parent", None):
                        extractors["puae-jsonl"].write_empty_file(table["name"])

            if left_gens > 0:
                # go to 100%
                collector.update("Resources", left_gens)

        # flush all buffered writers
        storage.close_writers(extract_id)


def extract_with_schema(
    storage: ExtractorStorage,
    source: DltSource,
    collector: Collector,
    max_parallel_items: int,
    workers: int,
) -> str:
    # generate extract_id to be able to commit all the sources together later
    extract_id = storage.create_extract_id()
    with Container().injectable_context(SourceSchemaInjectableContext(source.schema)):
        # inject the config section with the current source name
        with inject_section(ConfigSectionContext(sections=(known_sections.SOURCES, source.section, source.name), source_state_key=source.name)):
            # reset resource states, the `extracted` list contains all the explicit resources and all their parents
            for resource in source.resources.extracted.values():
                with contextlib.suppress(DataItemRequiredForDynamicTableHints):
                    if resource.write_disposition == "replace":
                        reset_resource_state(resource.name)
            extract(extract_id, source, storage, collector, max_parallel_items=max_parallel_items, workers=workers)

    return extract_id
