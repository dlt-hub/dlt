import io
from typing import Any, Dict, List

import pytest

import dlt
from dlt.common.configuration.container import Container
from dlt.common.destination.client import SqlModel
from dlt.common.encryption import PipelineEncryptionContext
from dlt.common.utils import uniq_id

SECRET = "CREATE SECRET s (TYPE HUGGINGFACE, TOKEN 'topsecret')"


def _attach() -> List[Dict[str, Any]]:
    return [
        {
            "attach_type": "duckdb",
            "alias": "attach_x",
            "dataset_name": "x",
            "physical_location": "loc",
            "statements": [
                {"sql": SECRET, "secret": True},
                {"sql": "ATTACH IF NOT EXISTS ':memory:' AS attach_x", "secret": False},
            ],
            "detach_statements": ["DETACH attach_x"],
        }
    ]


def _model_text(attach: List[Dict[str, Any]]) -> str:
    return str(SqlModel(query="SELECT 1 AS a", dialect="duckdb", attach=attach))


def _secret_sql(attach: List[Dict[str, Any]]) -> List[str]:
    return [s["sql"] for s in attach[0]["statements"] if s["secret"]]


def test_pipeline_encryption_seed_default_is_random_per_instance() -> None:
    name = "enc_key_" + uniq_id()
    a = dlt.pipeline(name, destination="duckdb")
    b = dlt.pipeline(name, destination="duckdb")
    # default salt (name-derived) yields an ephemeral random seed that differs per instance
    assert a.encryption_seed != b.encryption_seed


def test_pipeline_encryption_seed_from_salt_is_stable() -> None:
    salt = "permanent-" + uniq_id()
    a = dlt.pipeline("enc_key_a_" + uniq_id(), destination="duckdb", pipeline_salt=salt)
    b = dlt.pipeline("enc_key_b_" + uniq_id(), destination="duckdb", pipeline_salt=salt)
    assert a.encryption_seed == b.encryption_seed == salt


def test_encryption_context_never_exposes_key() -> None:
    ctx = PipelineEncryptionContext(secret="master-secret")
    assert not hasattr(ctx, "key")
    # the round-trip works without ever returning the derived key
    token = ctx.encrypt_text("credential")
    assert "credential" not in token
    assert ctx.decrypt_text(token) == "credential"


def test_encryption_context_injected_secret_round_trip() -> None:
    # an injected secret drives encrypt/decrypt regardless of pipeline (e.g. detached/pool load)
    secret = "master-" + uniq_id()
    with Container().injectable_context(PipelineEncryptionContext(secret=secret)):
        text = _model_text(_attach())
        assert "topsecret" not in text
        attach = SqlModel.from_file(io.StringIO(text)).attach
        assert _secret_sql(attach) == [SECRET]
    with Container().injectable_context(PipelineEncryptionContext(secret="wrong-" + uniq_id())):
        with pytest.raises(ValueError, match="does not match"):
            SqlModel.from_file(io.StringIO(text))


def test_model_secret_statements_are_encrypted() -> None:
    dlt.pipeline("enc_model_" + uniq_id(), destination="duckdb")
    text = _model_text(_attach())
    # secret value is hidden, but structure stays human-readable
    assert "topsecret" not in text
    assert "HUGGINGFACE" not in text
    assert "attach_x" in text
    assert "ATTACH IF NOT EXISTS ':memory:' AS attach_x" in text
    # the same instance decrypts back to the original secret
    attach = SqlModel.from_file(io.StringIO(text)).attach
    assert _secret_sql(attach) == [SECRET]


def test_model_secret_undecryptable_after_restart() -> None:
    name = "enc_restart_" + uniq_id()
    dlt.pipeline(name, destination="duckdb")
    text = _model_text(_attach())
    # a fresh instance (new ephemeral key) cannot decrypt; user is told to set a permanent salt
    dlt.pipeline(name, destination="duckdb")
    with pytest.raises(ValueError, match="pipeline_salt"):
        SqlModel.from_file(io.StringIO(text))


def test_model_secret_decryptable_with_permanent_salt() -> None:
    salt = "permanent-" + uniq_id()
    dlt.pipeline("enc_perm_a_" + uniq_id(), destination="duckdb", pipeline_salt=salt)
    text = _model_text(_attach())
    # a different instance sharing the salt derives the same key and decrypts
    dlt.pipeline("enc_perm_b_" + uniq_id(), destination="duckdb", pipeline_salt=salt)
    attach = SqlModel.from_file(io.StringIO(text)).attach
    assert _secret_sql(attach) == [SECRET]
