from urllib.parse import urlparse


def convert_storage_url_to_http(
    url: str, use_https: bool = False, endpoint: str = None, region: str = None
) -> str:
    try:
        parsed_url = urlparse(url)

        bucket_name = parsed_url.netloc
        object_key = parsed_url.path.lstrip("/")
        protocol = "https" if use_https else "http"

        if endpoint:
            domain = endpoint
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
