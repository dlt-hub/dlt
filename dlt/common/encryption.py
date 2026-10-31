from typing import ClassVar, Optional

from dlt.common.typing import Annotated
from dlt.common.configuration import configspec
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs import ContainerInjectableContext
from dlt.common.configuration.specs.base_configuration import NotResolved

ENCRYPTION_PURPOSE = "dlt-model-secrets"


@configspec
class PipelineEncryptionContext(ContainerInjectableContext):
    """Encrypts and decrypts secrets. The encryption key stays hidden from the caller.

    If `secret` is not set, this context reads the secret of the active pipeline. It then derives
    the key from that secret.
    """

    secret: Annotated[Optional[str], NotResolved()] = None

    can_create_default: ClassVar[bool] = True
    worker_affinity: ClassVar[bool] = True

    def encrypt_text(self, text: str) -> str:
        from dlt.common.libs.cryptography import encrypt_text

        return encrypt_text(self._key(), text)

    def decrypt_text(self, token: str) -> str:
        from dlt.common.libs.cryptography import decrypt_text

        return decrypt_text(self._key(), token)

    def _key(self) -> str:
        from dlt.common.libs.cryptography import derive_encryption_key

        secret = self.secret
        if secret is None:
            # inline: dlt.common.pipeline imports utils/data_writers, a top-level import is circular
            from dlt.common.pipeline import current_pipeline

            pipeline = current_pipeline()
            if pipeline is None:
                raise ValueError(
                    "dlt found no encryption secret. Run this code in an active pipeline. As an"
                    " alternative, inject a `PipelineEncryptionContext`. For a restarted or"
                    " detached load, set a permanent `pipeline_salt`."
                )
            secret = pipeline.encryption_seed
        return derive_encryption_key(secret, ENCRYPTION_PURPOSE)


def pipeline_encryption() -> PipelineEncryptionContext:
    """Returns the active encryption context. The caller can inject this context. Without an
    injected context, the default context uses the secret of the active pipeline.
    """
    return Container()[PipelineEncryptionContext]
