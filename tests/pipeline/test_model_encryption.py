import io
from typing import List

import pytest

import dlt
from dlt.common.configuration.container import Container
from dlt.common.destination.attach import TAttachInfo, attach_statement
from dlt.common.destination.client import SqlModel
from dlt.common.encryption import PipelineEncryptionContext
from dlt.common.utils import uniq_id

SECRET = "CREATE SECRET s (TYPE HUGGINGFACE, TOKEN 'topsecret')"


def _attach() -> List[TAttachInfo]:
    return [
        TAttachInfo(
            attach_type="duckdb",
            alias="attach_x",
            statements=[
                attach_statement(SECRET, secret=True),
                attach_statement("ATTACH IF NOT EXISTS ':memory:' AS attach_x"),
            ],
        )
    ]


def _model_text(attach: List[TAttachInfo]) -> str:
    return str(SqlModel(query="SELECT 1 AS a", dialect="duckdb", attach=attach))


def _secret_sql(attach: List[TAttachInfo]) -> List[str]:
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
    # the round-trip works and never returns the derived key
    token = ctx.encrypt_text("credential")
    assert "credential" not in token
    assert ctx.decrypt_text(token) == "credential"


def test_encryption_context_injected_secret_round_trip() -> None:
    # an injected secret encrypts and decrypts for any pipeline, for example a detached load
    # or a pool load
    secret = "master-" + uniq_id()
    with Container().injectable_context(PipelineEncryptionContext(secret=secret)):
        text = _model_text(_attach())
        assert "topsecret" not in text
        attach = SqlModel.from_file(io.StringIO(text)).attach
        assert _secret_sql(attach) == [SECRET]
    with Container().injectable_context(PipelineEncryptionContext(secret="wrong-" + uniq_id())):
        with pytest.raises(ValueError, match="does not match"):
            SqlModel.from_file(io.StringIO(text)).attach


def test_model_secret_statements_are_encrypted() -> None:
    dlt.pipeline("enc_model_" + uniq_id(), destination="duckdb")
    text = _model_text(_attach())
    # the model text hides the secret value, but the structure stays human-readable
    assert "topsecret" not in text
    assert "HUGGINGFACE" not in text
    assert "attach_x" in text
    assert "ATTACH IF NOT EXISTS ':memory:' AS attach_x" in text
    # the same instance decrypts the original secret
    attach = SqlModel.from_file(io.StringIO(text)).attach
    assert _secret_sql(attach) == [SECRET]


def test_model_secret_undecryptable_after_restart() -> None:
    name = "enc_restart_" + uniq_id()
    dlt.pipeline(name, destination="duckdb")
    text = _model_text(_attach())
    # a fresh instance (new ephemeral key) cannot decrypt the model. the error message tells the
    # user to set a permanent salt
    dlt.pipeline(name, destination="duckdb")
    with pytest.raises(ValueError, match="pipeline_salt"):
        SqlModel.from_file(io.StringIO(text)).attach


def test_model_rewritten_without_encryption_key() -> None:
    """Normalize rewrites the query of a model that it cannot decrypt. Normalize runs in a
    process worker that has neither the pipeline nor the key that encrypted the attach info."""
    secret = "master-" + uniq_id()
    with Container().injectable_context(PipelineEncryptionContext(secret=secret)):
        text = _model_text(_attach())

    # a key that cannot decrypt this model still rewrites it, so the rewrite needs no key at all
    with Container().injectable_context(PipelineEncryptionContext(secret="other-" + uniq_id())):
        rewritten = str(SqlModel.from_file(io.StringIO(text)).with_query("SELECT 2 AS b", "duckdb"))
    assert "SELECT 2 AS b" in rewritten
    # the rewrite copies the ciphertext exactly. dlt encrypts the same secret to a different
    # ciphertext, even under the original key
    assert rewritten.splitlines()[1] == text.splitlines()[1]

    # the key that wrote the model still decrypts the model that normalize rewrote
    with Container().injectable_context(PipelineEncryptionContext(secret=secret)):
        assert _secret_sql(SqlModel.from_file(io.StringIO(rewritten)).attach) == [SECRET]


def test_model_secret_decryptable_with_permanent_salt() -> None:
    salt = "permanent-" + uniq_id()
    dlt.pipeline("enc_perm_a_" + uniq_id(), destination="duckdb", pipeline_salt=salt)
    text = _model_text(_attach())
    # a different instance that shares the salt derives the same key and decrypts the secret
    dlt.pipeline("enc_perm_b_" + uniq_id(), destination="duckdb", pipeline_salt=salt)
    attach = SqlModel.from_file(io.StringIO(text)).attach
    assert _secret_sql(attach) == [SECRET]
