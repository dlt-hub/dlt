"""Implements SupportsTracking"""
from typing import Any, ClassVar, List, Optional, Tuple, Union
import fsspec
import pickle
import os

import dlt
from dlt.common import logger
from dlt.common.configuration.exceptions import ConfigurationException
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.configuration.specs.base_configuration import BaseConfiguration, configspec
from dlt.common.storages.configuration import FilesystemConfiguration
from dlt.common.storages.fsspec_filesystem import FileItemDict, fsspec_from_config, glob_files
from dlt.common.versioned_state import json_encode_state

from dlt.pipeline.trace import PipelineTrace, PipelineStepTrace, TPipelineStep, SupportsPipeline
from dlt._workspace.run_context import DEFAULT_WORKSPACE_WORKING_FOLDER
from dlt._workspace._workspace_context import WorkspaceRunContext


@configspec
class RuntimeArtifactsConfiguration(BaseConfiguration):
    artifacts: FilesystemConfiguration = None


def sync_from_runtime() -> None:
    """Sync the pipeline states and traces from the runtime backup, recursively."""
    from dlt._workspace.helpers.runtime.runtime_artifacts import _get_runtime_artifacts_fs

    def sync_dir(fs: fsspec.AbstractFileSystem, src_root: str, dst_root: str) -> None:
        """Recursively sync src_root on fs into dst_root locally, always using fs.walk."""

        os.makedirs(dst_root, exist_ok=True)

        for file_dict in glob_files(fs, src_root):
            file_item = FileItemDict(file_dict, fs)

            relative_dir = os.path.dirname(file_dict["relative_path"])
            local_dir = dst_root if relative_dir == "." else os.path.join(dst_root, relative_dir)
            os.makedirs(local_dir, exist_ok=True)

            local_file = os.path.join(dst_root, file_dict["relative_path"])

            logger.info(f"Restoring artifact {local_file}")
            with open(local_file, "wb") as lf:
                lf.write(file_item.read_bytes())

            ts = file_dict["modification_date"].timestamp()
            os.utime(local_file, (ts, ts))  # (atime, mtime)

    context = dlt.current.run_context()

    if not context.runtime_config.run_id:
        return

    if not isinstance(context, WorkspaceRunContext):
        return

    fs, config = _get_runtime_artifacts_fs(section="sync")
    if not fs:
        return

    # TODO: there's no good way to get this value on sync.
    data_dir_root = os.path.join(
        context.settings_dir, DEFAULT_WORKSPACE_WORKING_FOLDER
    )  # the local .var folder

    # Just sync the whole base folder into the local pipelines dir
    sync_dir(fs, config.bucket_url, data_dir_root)


def _get_runtime_artifacts_fs(
    section: str,
) -> Tuple[fsspec.AbstractFileSystem, FilesystemConfiguration]:
    try:
        config = resolve_configuration(RuntimeArtifactsConfiguration(), sections=(section,))
    except ConfigurationException:
        logger.info(f"No artifact storage credentials found for {section}")
        return None, None

    return fsspec_from_config(config.artifacts)[0], config.artifacts


def _write_to_bucket(
    fs: fsspec.AbstractFileSystem,
    bucket_url: str,
    pipeline_name: str,
    paths: List[str],
    data: Union[str, bytes],
    mode: str = "w",
) -> None:
    # write to bucket using the config, same object may be written to multiple paths

    logger.info(f"Will send run artifact to {bucket_url}: {paths}")
    for path in paths:
        with fs.open(f"{bucket_url}/{pipeline_name}/{path}", mode=mode) as f:
            f.write(data)


def _send_trace_to_bucket(
    fs: fsspec.AbstractFileSystem, bucket_url: str, trace: PipelineTrace, pipeline: SupportsPipeline
) -> None:
    """
    Send the full trace pickled to the runtime bucket
    """
    pickled_trace = pickle.dumps(trace)
    _write_to_bucket(
        fs,
        bucket_url,
        pipeline.pipeline_name,
        [
            "trace.pickle",
        ],  # save current and by start time
        pickled_trace,
        mode="wb",
    )


def _send_state_to_bucket(
    fs: fsspec.AbstractFileSystem, bucket_url: str, pipeline: SupportsPipeline
) -> None:
    encoded_state = json_encode_state(pipeline.state)
    _write_to_bucket(
        fs,
        bucket_url,
        pipeline.pipeline_name,
        [
            "state.json",
        ],  # save current and by start time
        encoded_state,
        mode="w",
    )


def _send_schemas_to_bucket(
    fs: fsspec.AbstractFileSystem, bucket_url: str, pipeline: SupportsPipeline
) -> None:
    schema_dir = os.path.join(pipeline.working_dir, "schemas")
    for schema_file in os.listdir(schema_dir):
        _write_to_bucket(
            fs,
            bucket_url,
            pipeline.pipeline_name,
            [f"schemas/{schema_file}"],
            open(os.path.join(schema_dir, schema_file), "rb").read(),
            mode="wb",
        )


def on_start_trace(trace: PipelineTrace, step: TPipelineStep, pipeline: SupportsPipeline) -> None:
    pass


def on_start_trace_step(
    trace: PipelineTrace, step: TPipelineStep, pipeline: SupportsPipeline
) -> None:
    pass


def on_end_trace_step(
    trace: PipelineTrace,
    step: PipelineStepTrace,
    pipeline: SupportsPipeline,
    step_info: Any,
    send_state: bool,
) -> None:
    pass


def on_end_trace(trace: PipelineTrace, pipeline: SupportsPipeline, send_state: bool) -> None:
    # skip if runtime not running
    if pipeline.run_context.runtime_config.run_id is None:
        return

    fs, config = _get_runtime_artifacts_fs(section="send")
    if fs:
        logger.info(
            f"Sending run artifacts from pipeline `{pipeline.pipeline_name}` to"
            f" `{config.bucket_url}`"
        )
        try:
            _send_trace_to_bucket(fs, config.bucket_url, trace, pipeline)
            _send_state_to_bucket(fs, config.bucket_url, pipeline)
            _send_schemas_to_bucket(fs, config.bucket_url, pipeline)
        except Exception:
            logger.exception(
                f"Sending run artifacts from pipeline `{pipeline.pipeline_name}` to"
                f" `{config.bucket_url}`"
            )
            raise
        else:
            logger.info("Pipeline results reported to runtime")
