import inspect
import re
from typing import Any, Callable, Dict, Set, cast

from weaviate.classes.config import Configure

from dlt.common import logger
from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.common.exceptions import ValueErrorWithKnownValues

from dlt.destinations.impl.weaviate.warnings import vectorizer_renamed_deprecated

_CAMEL_CASE = re.compile(r"(?<!^)(?=[A-Z])")

"""Vectorizers Weaviate renamed. The old name still resolves, so only the user is nudged."""
VECTORIZER_RENAMES = {
    "text2vec-palm": "text2vec-google",
    "multi2vec-palm": "multi2vec-google",
}

"""v3 module config keys whose v4 counterpart is not a simple snake_case rename."""
MODULE_CONFIG_RENAMES = {
    "vectorizeClassName": "vectorize_collection_name",
    "type": "type_",
}

"""v3 collection-level keys that became per-property settings in v4."""
MODULE_CONFIG_MOVED_TO_PROPERTY = {"vectorizePropertyName"}


def supported_vectorizers() -> Set[str]:
    """Vectorizer module names accepted by `[destination.weaviate] vectorizer`."""
    return {
        name.replace("_", "-")
        for name in dir(Configure.Vectors)
        if not name.startswith("_") and name != "self_provided"
    }


def get_vector_factory(vectorizer: str) -> Callable[..., Any]:
    """Resolves a Weaviate vectorizer module name to its `Configure.Vectors` factory.

    Args:
        vectorizer (str): Module name as written in config, e.g. `text2vec-openai`.

    Returns:
        Callable[..., Any]: The factory building a named vector config.

    Raises:
        ValueErrorWithKnownValues: If `vectorizer` is not a known Weaviate module.
    """
    name = VECTORIZER_RENAMES.get(vectorizer, vectorizer)
    if name != vectorizer:
        vectorizer_renamed_deprecated(vectorizer, name)
    factory = getattr(Configure.Vectors, name.replace("-", "_"), None)
    if factory is None:
        raise ValueErrorWithKnownValues("vectorizer", vectorizer, supported_vectorizers())
    return cast(Callable[..., Any], factory)


def normalize_module_config(
    vectorizer: str, module_config: Dict[str, Any], factory: Callable[..., Any]
) -> Dict[str, Any]:
    """Translates a `module_config` entry into keyword arguments for a vectorizer factory.

    v3-style camelCase keys are converted to their v4 names. Keys that moved to per-property
    configuration in v4 are dropped with a warning.

    Args:
        vectorizer (str): Module name the config belongs to, used in error messages.
        module_config (Dict[str, Any]): Raw config as given by the user.
        factory (Callable[..., Any]): Factory the arguments are destined for.

    Returns:
        Dict[str, Any]: Keyword arguments accepted by `factory`.

    Raises:
        ValueErrorWithKnownValues: If a key is not accepted by `factory`.
    """
    accepted = set(inspect.signature(factory).parameters)
    normalized: Dict[str, Any] = {}
    for key, value in module_config.items():
        if key in MODULE_CONFIG_MOVED_TO_PROPERTY:
            logger.info(
                f"Weaviate module config `{key}` for `{vectorizer}` is a per-property setting in"
                " the v4 API and is ignored at collection level."
            )
            continue
        name = MODULE_CONFIG_RENAMES.get(key) or _CAMEL_CASE.sub("_", key).lower()
        if name not in accepted:
            raise ValueErrorWithKnownValues(f"module_config[{vectorizer}][{key}]", key, accepted)
        normalized[name] = value
    return normalized


"""`collections.create` arguments dlt derives from the schema and its own config."""
RESERVED_COLLECTION_ARGS = {
    "name",
    "properties",
    "vector_config",
    "vectorizer_config",
    "multi_tenancy_config",
}


def validate_collection_config(
    collection_config: Dict[str, Any], create: Callable[..., Any]
) -> Dict[str, Any]:
    """Checks `collection_config` against the signature of `collections.create`.

    Args:
        collection_config (Dict[str, Any]): Extra arguments the user configured.
        create (Callable[..., Any]): The bound `collections.create` method to validate against.

    Returns:
        Dict[str, Any]: `collection_config` unchanged.

    Raises:
        ConfigurationValueError: If an argument is one dlt derives itself.
        ValueErrorWithKnownValues: If an argument is not accepted by `collections.create`.
    """
    accepted = set(inspect.signature(create).parameters) - {"skip_argument_validation"}
    for name in collection_config:
        if name in RESERVED_COLLECTION_ARGS:
            raise ConfigurationValueError(
                f"collection_config[{name}]",
                f"`{name}` is derived by dlt from the schema and cannot be set through"
                " `collection_config`.",
            )
        if name not in accepted:
            raise ValueErrorWithKnownValues(
                f"collection_config[{name}]", name, accepted - RESERVED_COLLECTION_ARGS
            )
    return collection_config
