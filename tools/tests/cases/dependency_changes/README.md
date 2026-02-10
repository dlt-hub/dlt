# Dependency Change Test Cases

Static test fixtures for `test_check_dependency_changes.py`. Each test case directory contains three pyproject.toml files representing the 3-way merge scenario:

- **`ancestor.toml`** - merge-base (common ancestor of base and head)
- **`base.toml`** - target branch (what's being merged INTO)
- **`head.toml`** - PR branch (what's being merged FROM)

## Test Cases

### `sqlglot_revert/`
Based on commit `ccec5c0f8` which reverted sqlglot version constraints.
- **Main dep change**: `sqlglot>=25.4.0,<28` → `sqlglot>=25.4.0,!=28.1`
- **Optional deps**: no changes
- **Dev groups**: no changes

### `faster_testing/`
Based on commit `215888991` which modified both main deps and dev group.
- **Main dep change**: `sqlglot>=25.4.0,!=28.1` → `sqlglot>=25.4.0,<28`
- **Dev group change**: added `pytest-xdist>=3.5,<4`

### `no_pyproject_change/`
Based on commit `ea8b1ae7b` which didn't touch pyproject.toml at all.
- `ancestor.toml` == `head.toml` (no changes in PR branch)
- Used to verify exit code 0 when there are no dependency changes
