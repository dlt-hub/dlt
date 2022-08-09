
from typing import Any, Sequence, Type

# gets credentials in namespace (ie pipeline name), grouped under key with spec
# spec can be a class, TypedDict or dataclass. overwrites initial_values
def get_credentials(spec: Type[Any] = None, key: str = None, namespace: str = None, initial_values: Any = None) -> Any:
    # will use registered credential providers for all values in spec or return all values under key
    pass


def get_config(spec: Type[Any], key: str = None, namespace: str = None, initial_values: Any = None) -> Any:
    # uses config providers (env, .dlt/config.toml)
    # in case of TSecretValues fallbacks to using credential providers
    pass


class ConfigProvider:
    def get(name: str) -> Any:
        pass

    def list(prefix: str = None) -> Sequence[str]:
        pass
