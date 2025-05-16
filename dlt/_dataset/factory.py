import pathlib
from typing import Generator, Literal, TypedDict

from dlt import Schema
from dlt.pipeline import get_dlt_pipelines_dir, Pipeline
from dlt.destinations import dataset as destinations_dataset
from dlt.common.pipeline import TPipelineState
from dlt.common.storages.file_storage import FileStorage
from dlt.common.storages.live_schema_storage import LiveSchemaStorage, SchemaStorageConfiguration
from dlt.common.versioned_state import json_decode_state
from dlt.pipeline.state_sync import migrate_pipeline_state, PIPELINE_STATE_ENGINE_VERSION


class TDatasetSpecs(TypedDict):
    dataset_name: str
    destination_type: str
    schema: Schema


def find_dataset(dataset_name: str) -> TDatasetSpecs:
    dataset_count = 0
    for dataset_spec in _find_all_datasets():
        if dataset_spec["dataset_name"] == dataset_name:
            return dataset_spec
        dataset_count += 1

    raise StopIteration(
        f"No dataset named `{dataset_name}` found. "
        f"Searched directory `{get_dlt_pipelines_dir()}` and found {dataset_count} datasets."
    )


# TODO expand search method with filters
def find_all_datasets() -> list[TDatasetSpecs]:
    return list(_find_all_datasets())


def _find_all_datasets() -> Generator[TDatasetSpecs, None, None]:
    pipelines_dir = get_dlt_pipelines_dir()
    pipelines_storage = FileStorage(pipelines_dir, makedirs=False)

    for pipeline_subdir in pipelines_storage.list_folder_dirs("."):
        pipeline_dir = pathlib.Path(pipelines_dir, pipeline_subdir)
        pipeline_state_file_path = pipeline_dir / Pipeline.STATE_FILE

        if pipelines_storage.has_file(str(pipeline_state_file_path)) is False:
            continue

        pipeline_state = _load_pipeline_state(pipelines_storage.load(str(pipeline_state_file_path)))
        schema_storage = LiveSchemaStorage(
            SchemaStorageConfiguration(schema_volume_path=f"{pipeline_dir}/schemas")
        )

        for schema_name in pipeline_state["schema_names"]:
            yield TDatasetSpecs(
                dataset_name=pipeline_state["dataset_name"],
                destination_type=pipeline_state["destination_type"],
                schema=schema_storage.load_schema(schema_name),
            )


def _load_pipeline_state(state_str: str) -> TPipelineState:
    state = json_decode_state(state_str)
    migrated_state = migrate_pipeline_state(
        pipeline_name=state["pipeline_name"],
        state=state,
        from_engine=state["_state_engine_version"],
        to_engine=PIPELINE_STATE_ENGINE_VERSION,
    )
    return migrated_state


def dataset(
    dataset_name: str, *, dataset_type: Literal["auto", "default", "ibis"] = "auto"
) -> destinations_dataset.ReadableDBAPIDataset:
    specs = find_dataset(dataset_name)

    return destinations_dataset.dataset(
        dataset_name=dataset_name,
        destination=specs["destination_type"],
        schema=specs["schema"],
        dataset_type=dataset_type,
    )
