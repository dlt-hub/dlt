---
sidebar_label: git
title: common.git
---

#### is\_clean\_and\_synced

```python
def is_clean_and_synced(repo: Repo) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/git.py#L34)

Checks if repo is clean and synced with origin

#### force\_clone\_repo

```python
def force_clone_repo(repo_url: str,
                     repo_storage: FileStorage,
                     repo_name: str,
                     branch: Optional[str] = None,
                     with_git_command: Optional[str] = None) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/git.py#L82)

Deletes the working directory repo_storage.root/repo_name and clones the `repo_url` into it. Will checkout `branch` if provided

#### get\_fresh\_repo\_files

```python
def get_fresh_repo_files(
        repo_location: str,
        working_dir: str = None,
        branch: Optional[str] = None,
        with_git_command: Optional[str] = None) -> FileStorage
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/git.py#L101)

Returns a file storage leading to the newest repository files. If `repo_location` is url, file will be checked out into `working_dir/repo_name`

