from typing import Union, Optional, Literal
from urllib.parse import urlparse, ParseResult, urlunparse

from jinja2 import Template


S3_TABLE_FUNCTION_FILE_FORMATS = Literal["jsonl", "parquet"]


def convert_storage_to_http_scheme(
    url: Union[str, ParseResult], use_https: bool = False, endpoint: str = None, region: str = None
) -> str:
    try:
        if isinstance(url, str):
            parsed_url = urlparse(url)
        elif isinstance(url, ParseResult):
            parsed_url = url
        else:
            raise TypeError("Invalid URL type. Expected str or ParseResult.")

        bucket_name = parsed_url.netloc
        object_key = parsed_url.path.lstrip("/")
        protocol = "https" if use_https else "http"

        if endpoint:
            domain = endpoint
        elif region and parsed_url.scheme == "s3":
            domain = f"s3-{region}.amazonaws.com"
        else:
            # TODO: Incorporate dlt.config endpoint.
            storage_domains = {
                "s3": "s3.amazonaws.com",
                "gs": "storage.googleapis.com",
                "gcs": "storage.googleapis.com",
            }
            domain = storage_domains[parsed_url.scheme]

        return f"{protocol}://{bucket_name}.{domain}/{object_key}"
    except Exception as e:
        raise Exception(f"Error converting storage URL to HTTP protocol: '{url}'") from e


def render_s3_table_function(
    url: str,
    access_key_id: Optional[str] = None,
    secret_access_key: Optional[str] = None,
    file_format: Optional[S3_TABLE_FUNCTION_FILE_FORMATS] = "jsonl",
) -> str:
    if file_format not in ["parquet", "jsonl"]:
        raise ValueError("Clickhouse s3/gcs staging only supports 'parquet' and 'jsonl'.")

    format_mapping = {"jsonl": "JSONEachRow", "parquet": "Parquet"}
    clickhouse_format = format_mapping[file_format]

    template = Template(
        """s3('{{ url }}'{% if access_key_id and secret_access_key %},'{{ access_key_id }}','{{ secret_access_key }}'{% else %},NOSIGN{% endif %},'{{ clickhouse_format }}')"""
    )

    return template.render(
        url=url,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        clickhouse_format=clickhouse_format,
    ).strip()
