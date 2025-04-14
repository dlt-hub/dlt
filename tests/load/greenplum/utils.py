from typing import Dict, List, Any, Optional
import os
import pytest

from dlt.common.typing import DictStrStr
from dlt.destinations.impl.greenplum.configuration import GreenplumClientConfiguration, GreenplumCredentials


def get_greenplum_test_config() -> Dict[str, Any]:
    """Возвращает тестовую конфигурацию для Greenplum"""
    return {
        "credentials": {
            "host": os.environ.get("GREENPLUM_HOST", "localhost"),
            "port": int(os.environ.get("GREENPLUM_PORT", "5432")),
            "database": os.environ.get("GREENPLUM_DATABASE", "test"),
            "username": os.environ.get("GREENPLUM_USER", "gpadmin"),
            "password": os.environ.get("GREENPLUM_PASSWORD", "pivotal"),
        },
        "appendonly": True,
        "blocksize": 32768,
        "compresstype": "zstd",
        "compresslevel": 4,
        "orientation": "column",
        "distribution_key": "_dlt_id",
    }


def create_greenplum_client_config(
    credentials: Optional[GreenplumCredentials] = None,
    appendonly: bool = True,
    blocksize: int = 32768,
    compresstype: str = "zstd",
    compresslevel: int = 4,
    orientation: str = "column",
    distribution_key: str = "_dlt_id",
) -> GreenplumClientConfiguration:
    """Создает конфигурацию клиента Greenplum с заданными параметрами хранения и дистрибуции"""
    if credentials is None:
        # Создаем строку подключения вместо указания отдельных параметров
        conn_string = f"postgresql://{os.environ.get('GREENPLUM_USER', 'gpadmin')}:{os.environ.get('GREENPLUM_PASSWORD', 'pivotal')}@{os.environ.get('GREENPLUM_HOST', 'localhost')}:{os.environ.get('GREENPLUM_PORT', '5432')}/{os.environ.get('GREENPLUM_DATABASE', 'test')}"
        credentials = GreenplumCredentials(conn_string)
    
    return GreenplumClientConfiguration(
        credentials=credentials,
        appendonly=appendonly,
        blocksize=blocksize,
        compresstype=compresstype,
        compresslevel=compresslevel,
        orientation=orientation,
        distribution_key=distribution_key,
    )


def generate_sample_sql_with_distribution_params(
    table_name: str,
    columns: List[str],
    appendonly: bool = True,
    blocksize: int = 32768,
    compresstype: str = "zstd",
    compresslevel: int = 4,
    orientation: str = "column",
    distribution_key: str = "_dlt_id",
) -> str:
    """Генерирует образец SQL с параметрами хранения и дистрибуции"""
    columns_sql = ",\n    ".join(columns)
    storage_params = []
    
    if appendonly:
        storage_params.append("appendonly=true")
    if blocksize:
        storage_params.append(f"blocksize={blocksize}")
    if compresstype:
        storage_params.append(f"compresstype={compresstype}")
    if compresslevel:
        storage_params.append(f"compresslevel={compresslevel}")
    if orientation:
        storage_params.append(f"orientation={orientation}")
    
    storage_sql = ", ".join(storage_params)
    
    sql = f"""CREATE TABLE {table_name} (
    {columns_sql}
) WITH ({storage_sql}) DISTRIBUTED BY ("{distribution_key}");"""
    
    return sql 