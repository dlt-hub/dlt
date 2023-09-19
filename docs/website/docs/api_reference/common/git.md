---
sidebar_label: git
title: common.git
---

#### is\_clean\_and\_synced

```python
def is_clean_and_synced(repo: Repo) -> bool
```

Checks if repo is clean and synced with origin

#### force\_clone\_repo

```python
def force_clone_repo(repo_url: str,
                     repo_storage: FileStorage,
                     repo_name: str,
                     branch: Optional[str] = None,
                     with_git_command: Optional[str] = None) -> None
```

Deletes the working directory repo_storage.root/repo_name and clones the `repo_url` into it. Will checkout `branch` if provided

#### get\_fresh\_repo\_files

```python
def get_fresh_repo_files(
        repo_location: str,
        working_dir: str = None,
        branch: Optional[str] = None,
        with_git_command: Optional[str] = None) -> FileStorage
```

Returns a file storage leading to the newest repository files. If `repo_location` is url, file will be checked out into `working_dir/repo_name`

