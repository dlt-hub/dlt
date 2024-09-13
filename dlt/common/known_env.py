"""Defines env variables that `dlt` uses independently of its configuration system"""

DLT_PROJECT_DIR = "DLT_PROJECT_DIR"
"""The dlt project dir is the current working directory, '.' (current working dir) by default"""

DLT_DATA_DIR = "DLT_DATA_DIR"
"""Gets default directory where pipelines' data (working directories) will be stored"""

DLT_CONFIG_FOLDER = "DLT_CONFIG_FOLDER"
"""A folder (path relative to DLT_PROJECT_DIR) where config and secrets are stored"""

DLT_DEFAULT_NAMING_NAMESPACE = "DLT_DEFAULT_NAMING_NAMESPACE"
"""Python namespace default where naming modules reside, defaults to dlt.common.normalizers.naming"""

DLT_DEFAULT_NAMING_MODULE = "DLT_DEFAULT_NAMING_MODULE"
"""A module name with the default naming convention, defaults to snake_case"""

DLT_DLT_ID_LENGTH_BYTES = "DLT_DLT_ID_LENGTH_BYTES"
"""The length of the _dlt_id identifier, before base64 encoding"""

DLT_USE_JSON = "DLT_USE_JSON"
"""Type of json parser to use, defaults to orjson, may be simplejson"""

DLT_JSON_TYPED_PUA_START = "DLT_JSON_TYPED_PUA_START"
"""Start of the unicode block within the PUA used to encode types in typed json"""
