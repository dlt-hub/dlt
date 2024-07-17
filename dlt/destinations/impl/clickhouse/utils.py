from typing import Union
from urllib.parse import urlparse, ParseResult


def convert_storage_to_http_scheme(
    url: Union[str, ParseResult],
    use_https: bool = False,
    endpoint: str = None,
    region: str = None,
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
            domain = endpoint.replace("https://", "").replace("http://", "")
        elif region and parsed_url.scheme == "s3":
            domain = f"s3-{region}.amazonaws.com"
        else:
            storage_domains = {
                "s3": "s3.amazonaws.com",
                "gs": "storage.googleapis.com",
                "gcs": "storage.googleapis.com",
            }
            domain = storage_domains[parsed_url.scheme]

        return f"{protocol}://{bucket_name}.{domain}/{object_key}"
    except Exception as e:
        raise Exception(f"Error converting storage URL to HTTP protocol: '{url}'") from e
