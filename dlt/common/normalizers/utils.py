import os
from typing import List

from dlt.common import known_env
from dlt.common.utils import uniq_id_base64, many_uniq_ids_base64


DLT_ID_LENGTH_BYTES = int(os.environ.get(known_env.DLT_DLT_ID_LENGTH_BYTES, 10))


def generate_dlt_ids(n_ids: int) -> List[str]:
    return many_uniq_ids_base64(n_ids, DLT_ID_LENGTH_BYTES)


def generate_dlt_id() -> str:
    return uniq_id_base64(DLT_ID_LENGTH_BYTES)
