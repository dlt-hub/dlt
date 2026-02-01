import contextlib
from typing import Iterator

from dlt.common.configuration.container import Container

from dlthub.common.license import (
    LicenseContext,
    create_self_signed_license,
    DltLicenseNotFoundException,
    decode_license,
)


def issue_ephemeral_license() -> Iterator[LicenseContext]:
    license_ = create_self_signed_license("dlthub.transformation dlthub.project dlthub.runner")
    try:
        ctx = LicenseContext(decode_license(license_))
        Container()[LicenseContext] = ctx
        yield ctx
    finally:
        del Container()[LicenseContext]
        with contextlib.suppress(DltLicenseNotFoundException):
            Container()[LicenseContext].reload()
