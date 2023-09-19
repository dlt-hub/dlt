---
sidebar_label: exec_info
title: common.runtime.exec_info
---

#### exec\_info\_names

```python
def exec_info_names() -> List[TExecInfoNames]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/runtime/exec_info.py#L31)

Get names of execution environments

#### dlt\_version\_info

```python
def dlt_version_info(pipeline_name: str) -> StrStr
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/runtime/exec_info.py#L108)

Gets dlt version info including commit and image version available in docker

#### kube\_pod\_info

```python
def kube_pod_info() -> StrStr
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/runtime/exec_info.py#L117)

Extracts information on pod name, namespace and node name if running on Kubernetes

#### github\_info

```python
def github_info() -> StrStr
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/runtime/exec_info.py#L122)

Extracts github info

#### in\_continuous\_integration

```python
def in_continuous_integration() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/runtime/exec_info.py#L131)

Returns `True` if currently running inside a continuous integration context.

#### is\_docker

```python
def is_docker() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/runtime/exec_info.py#L136)

Guess if we are running in docker environment.

https://stackoverflow.com/questions/20010199/how-to-determine-if-a-process-runs-inside-lxc-docker

**Returns**:

  `True` if we are running inside docker, `False` otherwise.

#### is\_aws\_lambda

```python
def is_aws_lambda() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/runtime/exec_info.py#L159)

Return True if the process is running in the serverless platform AWS Lambda

#### is\_gcp\_cloud\_function

```python
def is_gcp_cloud_function() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/runtime/exec_info.py#L164)

Return True if the process is running in the serverless platform GCP Cloud Functions

