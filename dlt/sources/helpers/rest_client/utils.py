def join_url(base_url: str, path: str) -> str:
    if base_url is None:
        raise ValueError("Base URL must be provided or set to an empty string.")

    if base_url == "":
        return path

    if path == "":
        return base_url

    # Normalize the base URL
    base_url = base_url.rstrip("/")
    if not base_url.endswith("/"):
        base_url += "/"

    return base_url + path.lstrip("/")
