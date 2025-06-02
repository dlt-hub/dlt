"""Reads files in s3, gs or azure buckets using fsspec and provides convenience resources for chunked reading of various file formats"""
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

import dlt
from dlt.extract import decorators
from dlt.common.storages.fsspec_filesystem import (
    FileItem,
    FileItemDict,
    fsspec_filesystem,
    glob_files,
)
from dlt.sources import DltResource
from dlt.sources.credentials import FileSystemCredentials

from dlt.sources.filesystem.helpers import (
    AbstractFileSystem,
    FilesystemConfigurationResource,
)
from dlt.sources.filesystem.readers import (
    ReadersSource,
    _read_csv,
    _read_csv_duckdb,
    _read_jsonl,
    _read_parquet,
)
from dlt.sources.filesystem.settings import DEFAULT_CHUNK_SIZE


@decorators.source(_impl_cls=ReadersSource, spec=FilesystemConfigurationResource)
def readers(  # noqa DOC
    bucket_url: str = dlt.secrets.value,
    credentials: Union[FileSystemCredentials, AbstractFileSystem] = dlt.secrets.value,
    file_glob: str = "*",
    kwargs: Optional[Dict[str, Any]] = None,
    client_kwargs: Optional[Dict[str, Any]] = None,
) -> Tuple[DltResource, ...]:
    """This source provides a few resources that are chunked file readers. Readers can be further parametrized before use
       read_csv(chunksize, **pandas_kwargs)
       read_jsonl(chunksize)
       read_parquet(chunksize)

    Args:
        bucket_url (str): The url to the bucket.
        credentials (Union[FileSystemCredentials, AbstractFileSystem]): The credentials to the filesystem of fsspec `AbstractFilesystem` instance.
        file_glob (str, optional): The filter to apply to the files in glob format. by default lists all files in bucket_url non-recursively
        kwargs: (Optional[Dict[str, Any]], optional): Additional arguments passed to fsspec constructor ie. dict(use_ssl=True) for s3fs
        client_kwargs: (Optional[Dict[str, Any]], optional): Additional arguments passed to underlying fsspec native client ie. dict(verify="public.crt) for botocore

    Returns:
        Tuple[DltResource, ...]: A tuple of resources that are chunked file readers.
    """
    return (
        filesystem(
            bucket_url, credentials, file_glob=file_glob, kwargs=kwargs, client_kwargs=client_kwargs
        )
        | dlt.transformer(name="read_csv")(_read_csv),
        filesystem(
            bucket_url, credentials, file_glob=file_glob, kwargs=kwargs, client_kwargs=client_kwargs
        )
        | dlt.transformer(name="read_jsonl")(_read_jsonl),
        filesystem(
            bucket_url, credentials, file_glob=file_glob, kwargs=kwargs, client_kwargs=client_kwargs
        )
        | dlt.transformer(name="read_parquet")(_read_parquet),
        filesystem(
            bucket_url, credentials, file_glob=file_glob, kwargs=kwargs, client_kwargs=client_kwargs
        )
        | dlt.transformer(name="read_csv_duckdb")(_read_csv_duckdb),
    )


@decorators.resource(primary_key="file_url", spec=FilesystemConfigurationResource)
def filesystem(  # noqa DOC
    bucket_url: str = dlt.secrets.value,
    credentials: Union[FileSystemCredentials, AbstractFileSystem] = dlt.secrets.value,
    file_glob: str = "*",
    files_per_page: int = DEFAULT_CHUNK_SIZE,
    extract_content: bool = False,
    kwargs: Optional[Dict[str, Any]] = None,
    client_kwargs: Optional[Dict[str, Any]] = None,
) -> Iterator[List[FileItem]]:
    """This resource lists files in `bucket_url` using `file_glob` pattern. The files are yielded as FileItem which also
    provide methods to open and read file data. It should be combined with transformers that further process (ie. load files)

    Args:
        bucket_url (str): The url to the bucket.
        credentials (Union[FileSystemCredentials, AbstractFileSystem]): The credentials to the filesystem of fsspec `AbstractFilesystem` instance.
        file_glob (str): The filter to apply to the files in glob format. by default lists all files in bucket_url non-recursively
        files_per_page (int, optional): The number of files to process at once, defaults to 100.
        extract_content (bool, optional): If true, the content of the file will be extracted if
            false it will return a fsspec file, defaults to False.
        kwargs: (Optional[Dict[str, Any]]): Additional arguments passed to fsspec constructor ie. dict(use_ssl=True) for s3fs
        client_kwargs: (Optional[Dict[str, Any]]): Additional arguments passed to underlying fsspec native client ie. dict(verify="public.crt) for botocore

    Yields:
        List[FileItem]: The list of files.
    """
    if isinstance(credentials, AbstractFileSystem):
        fs_client = credentials
    else:
        fs_client = fsspec_filesystem(
            bucket_url, credentials, kwargs=kwargs, client_kwargs=client_kwargs
        )[0]

    files_chunk: List[FileItem] = []
    for file_model in glob_files(fs_client, bucket_url, file_glob):
        file_dict = FileItemDict(file_model, fs_client)
        if extract_content:
            file_dict["file_content"] = file_dict.read_bytes()
        files_chunk.append(file_dict)  # type: ignore

        # wait for the chunk to be full
        if len(files_chunk) >= files_per_page:
            yield files_chunk
            files_chunk = []
    if files_chunk:
        yield files_chunk


read_csv = decorators.transformer()(_read_csv)
read_jsonl = decorators.transformer()(_read_jsonl)
read_parquet = decorators.transformer()(_read_parquet)
read_csv_duckdb = decorators.transformer()(_read_csv_duckdb)
