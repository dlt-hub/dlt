---
sidebar_label: paths
title: common.configuration.paths
---

#### get\_dlt\_project\_dir

```python
def get_dlt_project_dir() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/paths.py#L11)

The dlt project dir is the current working directory but may be overridden by DLT_PROJECT_DIR env variable.

#### get\_dlt\_settings\_dir

```python
def get_dlt_settings_dir() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/paths.py#L16)

Returns a path to dlt settings directory. If not overridden it resides in current working directory

The name of the setting folder is '.dlt'. The path is current working directory '.' but may be overridden by DLT_PROJECT_DIR env variable.

#### make\_dlt\_settings\_path

```python
def make_dlt_settings_path(path: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/paths.py#L24)

Returns path to file in dlt settings folder.

#### get\_dlt\_data\_dir

```python
def get_dlt_data_dir() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/configuration/paths.py#L29)

Gets default directory where pipelines' data will be stored
1. in user home directory: ~/.dlt/
2. if current user is root: in /var/dlt/
3. if current user does not have a home directory: in /tmp/dlt/
4. if DLT_DATA_DIR is set in env then it is used

