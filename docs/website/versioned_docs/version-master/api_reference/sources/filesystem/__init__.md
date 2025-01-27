---
sidebar_label: filesystem
title: sources.filesystem
---

Reads files in s3, gs or azure buckets using fsspec and provides convenience resources for chunked reading of various file formats

## readers

```python
@decorators.source(_impl_cls=ReadersSource,
                   spec=FilesystemConfigurationResource)
def readers(bucket_url: str = dlt.secrets.value,
            credentials: Union[FileSystemCredentials,
                               AbstractFileSystem] = dlt.secrets.value,
            file_glob: Optional[str] = "*") -> Tuple[DltResource, ...]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/filesystem/__init__.py#L30)

This source provides a few resources that are chunked file readers. Readers can be further parametrized before use
read_csv(chunksize, **pandas_kwargs)
read_jsonl(chunksize)
read_parquet(chunksize)

**Arguments**:

- `bucket_url` _str_ - The url to the bucket.
- `credentials` _FileSystemCredentials | AbstractFilesystem_ - The credentials to the filesystem of fsspec `AbstractFilesystem` instance.
- `file_glob` _str, optional_ - The filter to apply to the files in glob format. by default lists all files in bucket_url non-recursively

## filesystem

```python
@decorators.resource(primary_key="file_url",
                     spec=FilesystemConfigurationResource,
                     standalone=True)
def filesystem(bucket_url: str = dlt.secrets.value,
               credentials: Union[FileSystemCredentials,
                                  AbstractFileSystem] = dlt.secrets.value,
               file_glob: Optional[str] = "*",
               files_per_page: int = DEFAULT_CHUNK_SIZE,
               extract_content: bool = False) -> Iterator[List[FileItem]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/filesystem/__init__.py#L59)

This resource lists files in `bucket_url` using `file_glob` pattern. The files are yielded as FileItem which also
provide methods to open and read file data. It should be combined with transformers that further process (ie. load files)

**Arguments**:

- `bucket_url` _str_ - The url to the bucket.
- `credentials` _FileSystemCredentials | AbstractFilesystem_ - The credentials to the filesystem of fsspec `AbstractFilesystem` instance.
- `file_glob` _str, optional_ - The filter to apply to the files in glob format. by default lists all files in bucket_url non-recursively
- `files_per_page` _int, optional_ - The number of files to process at once, defaults to 100.
- `extract_content` _bool, optional_ - If true, the content of the file will be extracted if
  false it will return a fsspec file, defaults to False.
  

**Returns**:

- `Iterator[List[FileItem]]` - The list of files.

