from types import TracebackType
from typing import Iterable, Optional, Type

from dlt.version import __version__
from dlt.common.destination import DestinationCapabilitiesContext, PreparedTableSchema
from dlt.common.destination.client import JobClientBase, RunnableLoadJob
from dlt.common.schema import Schema


from dlt.destinations.impl.huggingface.configuration import HfClientConfiguration

from huggingface_hub import HfApi, CommitOperationCopy, CommitOperationDelete, CommitOperation
from huggingface_hub.errors import RepositoryNotFoundError


class HfLoadJob(RunnableLoadJob):

    def __init__(self, file_path: str) -> None:
        super().__init__(file_path)
        self._job_client: "HfClient" = None

    def run(self):
        self._job_client.hf_api.upload_file(
            path_or_fileobj=self._file_path,
            path_in_repo=f".load_id={self._load_id}/{self.file_name()}",
            repo_id=self._job_client.dataset_name,
            repo_type="dataset"
        )


class HfClient(JobClientBase):
    def __init__(
        self,
        schema: Schema,
        config: HfClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(schema, config, capabilities)  # type: ignore
        self.config: HfClientConfiguration = config  # type: ignore
        self.hf_api = HfApi(
            token=self.config.credentials.token,
            endpoint=self.config.credentials.endpoint,
            library_name="dlt",
            library_version=__version__
        )
        self.dataset_name = self.config.dataset_name
        self.split = self.config.split
        self.max_operations_per_commit = 100  # recommended value

    def initialize_storage(self, truncate_tables: Optional[Iterable[str]] = None) -> None:
        """Prepares storage to be used ie. creates database schema or file system folder. Truncates requested tables."""
        if not self.is_storage_initialized():
            self.hf_api.create_repo(self.dataset_name, repo_type="dataset")

    def is_storage_initialized(self) -> bool:
        """Returns if storage is ready to be read/written."""
        try:
            self.hf_api.repo_info(self.dataset_name, repo_type="dataset")
            return True
        except RepositoryNotFoundError:
            return False

    def drop_storage(self) -> None:
        """Brings storage back into not initialized state. Typically data in storage is destroyed."""
        self.hf_api.delete_files(self.dataset_name, delete_patterns=["**/*"], repo_type="dataset")

    def create_load_job(
        self, table: PreparedTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> RunnableLoadJob:
        """Creates a load job for a particular `table` with content in `file_path`. Table is already prepared to be loaded."""
        return HfLoadJob(file_path)

    def complete_load(self, load_id: str) -> None:
        """Marks the load package with `load_id` as completed in the destination. Before such commit is done, the data with `load_id` is invalid."""

        def format_path(i):
            return f"data/{self.split}-{i:05d}-of-{len(files_to_rename):05d}.parquet"

        def rename(old_path, new_path):
            if old_path != new_path:
                yield CommitOperationCopy(
                    src_path_in_repo=old_path, path_in_repo=new_path
                )
                yield CommitOperationDelete(path_in_repo=old_path)

        files_to_delete = list(self.hf_api.list_repo_tree(self.dataset_name, repo_type="dataset", path_in_repo="data"))
        operations = [
            CommitOperationDelete(file.path)
            for file in files_to_delete
            if file.path.startswith(f"data/{self.split}-")
        ]

        files_to_rename = list(self.hf_api.list_repo_tree(self.dataset_name, repo_type="dataset", path_in_repo=f".load_id={load_id}"))
        operations += [
            operation
            for i, file in enumerate(files_to_rename)
            for operation in rename(file.path, format_path(i))
        ]

        self._create_commits(
            operations=operations,
            message="Upload using dlt",
        )

    def _create_commits(
        self, operations: list[CommitOperation], message: str
    ) -> None:
        """
        Split the commit into multiple parts if necessary.
        The HuggingFace API may time out if there are too many operations in a single commit.
        """
        import math

        num_commits = math.ceil(len(operations) / self.max_operations_per_commit)
        for i in range(num_commits):
            begin = i * self.max_operations_per_commit
            end = (i + 1) * self.max_operations_per_commit
            part = operations[begin:end]
            commit_message = message + (
                f" (part {i:05d}-of-{num_commits:05d})" if num_commits > 1 else ""
            )
            self.hf_api.create_commit(
                repo_id=self.dataset_name,
                repo_type="dataset",
                operations=part,
                commit_message=commit_message,
            )

    def __enter__(self) -> "HfClient":
        return self

    def __exit__(
        self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType
    ) -> None:
        pass
