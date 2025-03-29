from dataclasses import dataclass
import os
from typing import Optional
from typing_extensions import Self

import dlt
import dlt.common
from dlt.common.storages.configuration import FilesystemConfiguration
from dlt.common.typing import Annotated
from dlt.common.configuration.specs.base_configuration import NotResolved, configspec
from dlt.common.destination.client import DestinationClientConfiguration
from dlt.common.pipeline import SupportsPipeline


LEGACY_DB_PATH_LOCAL_STATE_KEY = "duckdb_database"


@dataclass
class WithLocalFiles(DestinationClientConfiguration):
    local_dir: Annotated[str, NotResolved()] = None
    # needed by duckdb
    # TODO: deprecate this in 2.0
    pipeline_name: Annotated[Optional[str], NotResolved()] = None
    pipeline_working_dir: Annotated[Optional[str], NotResolved()] = None
    legacy_db_path: Annotated[Optional[str], NotResolved()] = None

    def _bind_local_files(self, pipeline: SupportsPipeline = None) -> Self:
        # get context for local files from pipeline
        if pipeline:
            self.pipeline_working_dir = pipeline.working_dir
            try:
                self.legacy_db_path = pipeline.get_local_state_val(LEGACY_DB_PATH_LOCAL_STATE_KEY)
            except KeyError:
                pass
            self.local_dir = pipeline.get_local_state_val("initial_cwd")
            self.pipeline_name = pipeline.pipeline_name
        return self

    def on_partial(self) -> None:
        if not self.local_dir:
            self.local_dir = os.path.abspath(dlt.current.run_context().local_dir)
            if not self.is_partial():
                self.resolve()

    def make_location(self, configured_location: str, default_location_pat: str) -> str:
        # do not set any paths for external database
        if configured_location == ":external:":
            return configured_location
        # use destination name to create duckdb name
        if self.destination_name:
            default_location = default_location_pat % self.destination_name
        else:
            default_location = default_location_pat % (self.pipeline_name or "")

        if configured_location == ":pipeline:":
            # try the pipeline context
            if self.pipeline_working_dir:
                return os.path.join(self.pipeline_working_dir, default_location)
            raise RuntimeError(
                "Attempting to use special location :pipeline: outside of pipeline context."
            )
        else:
            # if explicit path is absolute, use it
            if configured_location and os.path.isabs(configured_location):
                return configured_location
            # TODO: restore this check for the paths below. we may require that path exists
            #  if pipeline had already run
            # if not self.bound_to_pipeline.first_run:
            #     if not os.path.exists(pipeline_path):
            #         logger.warning(
            #             f"Duckdb attached to pipeline {self.bound_to_pipeline.pipeline_name} in"
            #             f" path {os.path.relpath(pipeline_path)} was could not be found but"
            #             " pipeline has already ran. This may be a result of (1) recreating or"
            #             " attaching pipeline  without or with changed explicit path to database"
            #             " that was used when creating the pipeline. (2) keeping the path to to"
            #             " database in secrets and changing the current working folder so  dlt"
            #             " cannot see them. (3) you deleting the database."
            # use stored path if relpath
            if self.legacy_db_path:
                return self.legacy_db_path
            # use tmp path as root, not cwd
            return os.path.join(self.local_dir, configured_location or default_location)


@configspec
class FilesystemConfigurationWithLocalFiles(FilesystemConfiguration, WithLocalFiles):  # type: ignore[misc]
    def normalize_bucket_url(self) -> None:
        # here we deal with normalized file:// local paths
        if self.is_local_filesystem:
            # convert to native path
            try:
                local_file_path = self.make_local_path(self.bucket_url)
            except ValueError:
                local_file_path = self.bucket_url
            relocated_path = self.make_location(local_file_path, "%s")
            # convert back into file:// schema if relocated
            if local_file_path != relocated_path:
                if self.bucket_url.startswith("file:"):
                    self.bucket_url = self.make_file_url(relocated_path)
                else:
                    self.bucket_url = relocated_path
        # modified local path before it is normalized
        super().normalize_bucket_url()
