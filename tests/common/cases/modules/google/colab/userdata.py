"""Mocked colab userdata"""


class SecretNotFoundError(Exception):
    pass


class NotebookAccessError(Exception):
    pass


def get(secret_name: str) -> str:
    if secret_name == "secrets.toml":
        return 'api_key="api"'

    raise SecretNotFoundError()
