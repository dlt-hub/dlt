import pytest

from dlt.common.libs.cryptography import (
    generate_secret,
    derive_encryption_key,
    encrypt_text,
    decrypt_text,
)


def test_generated_secret_is_unique() -> None:
    assert generate_secret() != generate_secret()


def test_derived_key_is_deterministic() -> None:
    assert derive_encryption_key("my-secret", "model") == derive_encryption_key(
        "my-secret", "model"
    )


def test_derived_key_depends_on_secret_and_purpose() -> None:
    # a different secret, or a different purpose, gives a key that does not match
    assert derive_encryption_key("secret-a", "model") != derive_encryption_key("secret-b", "model")
    assert derive_encryption_key("secret-a", "model") != derive_encryption_key("secret-a", "state")


def test_encrypt_decrypt_round_trip() -> None:
    key = derive_encryption_key(generate_secret(), "model")
    secret = "CREATE SECRET s (TYPE HUGGINGFACE, TOKEN 'topsecret')"
    token = encrypt_text(key, secret)
    # the ciphertext must not leak the plaintext
    assert "topsecret" not in token
    assert "HUGGINGFACE" not in token
    assert decrypt_text(key, token) == secret


def test_decrypt_with_wrong_key_raises() -> None:
    token = encrypt_text(derive_encryption_key(generate_secret(), "model"), "secret")
    with pytest.raises(ValueError, match="does not match"):
        decrypt_text(derive_encryption_key(generate_secret(), "model"), token)
