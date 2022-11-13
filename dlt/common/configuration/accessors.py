from typing import ClassVar

from dlt.common.typing import ConfigValue


class _ConfigAccessor:
    """Configuration accessor class"""

    value: ClassVar[None] = ConfigValue
    "A placeholder value that represents any argument that should be injected from the available configuration"

class _SecretsAccessor:
    value: ClassVar[None] = ConfigValue
    "A placeholder value that represents any secret argument that should be injected from the available secrets"

config = _ConfigAccessor()
"""Configuration with dictionary like access to available keys and groups"""

secrets = _SecretsAccessor()
"""Secrets with dictionary like access to available keys and groups"""
