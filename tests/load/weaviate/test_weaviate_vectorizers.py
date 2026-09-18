from typing import Any, Dict

import pytest

from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.common.exceptions import ValueErrorWithKnownValues
from dlt.common.warnings import Dlt100DeprecationWarning
from dlt.destinations.impl.weaviate.utils import (
    get_vector_factory,
    normalize_module_config,
    supported_vectorizers,
    validate_collection_config,
)

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.mark.parametrize(
    "vectorizer",
    [
        "text2vec-openai",
        "text2vec-cohere",
        "text2vec-contextionary",
        "text2vec-huggingface",
        # none of these resolved before the destination stopped hardcoding an allowlist
        "text2vec-weaviate",
        "text2vec-ollama",
        "text2vec-transformers",
        "text2vec-mistral",
        "text2vec-voyageai",
        "text2vec-jinaai",
    ],
)
def test_vectorizer_resolves(vectorizer: str) -> None:
    assert vectorizer in supported_vectorizers()
    assert get_vector_factory(vectorizer) is not None


def test_unknown_vectorizer_raises_instead_of_silently_disabling_vectorization() -> None:
    with pytest.raises(ValueErrorWithKnownValues):
        get_vector_factory("text2vec-does-not-exist")


def test_renamed_vectorizer_warns_but_still_resolves() -> None:
    with pytest.warns(Dlt100DeprecationWarning, match="text2vec-google"):
        assert get_vector_factory("text2vec-palm") is not None


@pytest.mark.parametrize(
    "vectorizer,module_config,expected",
    [
        pytest.param(
            "text2vec-openai",
            {"model": "ada", "modelVersion": "002", "type": "text"},
            {"model": "ada", "model_version": "002", "type_": "text"},
            id="dlt_default_openai_config",
        ),
        pytest.param(
            "text2vec-contextionary",
            {"vectorizeClassName": False},
            {"vectorize_collection_name": False},
            id="v3_vectorize_class_name",
        ),
        pytest.param(
            "text2vec-contextionary",
            {"vectorize_collection_name": True},
            {"vectorize_collection_name": True},
            id="v4_key_passes_through",
        ),
        pytest.param("text2vec-openai", {}, {}, id="empty"),
    ],
)
def test_module_config_is_translated_to_v4_kwargs(
    vectorizer: str, module_config: Dict[str, Any], expected: Dict[str, Any]
) -> None:
    factory = get_vector_factory(vectorizer)

    assert normalize_module_config(vectorizer, module_config, factory) == expected
    # the translated kwargs must actually build a config
    assert factory(**normalize_module_config(vectorizer, module_config, factory)) is not None


def test_key_that_moved_to_property_config_is_dropped() -> None:
    """`vectorizePropertyName` is per-property in v4, so it cannot be a collection kwarg."""
    factory = get_vector_factory("text2vec-contextionary")

    normalized = normalize_module_config(
        "text2vec-contextionary",
        {"vectorizeClassName": False, "vectorizePropertyName": True},
        factory,
    )

    assert normalized == {"vectorize_collection_name": False}


def test_unknown_module_config_key_raises() -> None:
    factory = get_vector_factory("text2vec-contextionary")

    with pytest.raises(ValueErrorWithKnownValues):
        normalize_module_config("text2vec-contextionary", {"noSuchOption": 1}, factory)


def _create_signature_stub(
    name=None,
    properties=None,
    vector_config=None,
    multi_tenancy_config=None,
    description=None,
    replication_config=None,
    generative_config=None,
    inverted_index_config=None,
    skip_argument_validation=None,
):
    """Stands in for `collections.create` so validation is tested without a server."""


def test_collection_config_passes_supported_arguments() -> None:
    config = {"description": "created by dlt", "replication_config": object()}

    assert validate_collection_config(config, _create_signature_stub) == config


def test_collection_config_rejects_unknown_argument() -> None:
    with pytest.raises(ValueErrorWithKnownValues):
        validate_collection_config({"no_such_option": 1}, _create_signature_stub)


@pytest.mark.parametrize(
    "reserved",
    ["name", "properties", "vector_config", "vectorizer_config", "multi_tenancy_config"],
)
def test_collection_config_rejects_arguments_dlt_derives(reserved: str) -> None:
    """These come from the dlt schema, so silently overriding them would corrupt the collection."""
    with pytest.raises(ConfigurationValueError, match=reserved):
        validate_collection_config({reserved: "x"}, _create_signature_stub)


def test_empty_collection_config_is_allowed() -> None:
    assert validate_collection_config({}, _create_signature_stub) == {}
