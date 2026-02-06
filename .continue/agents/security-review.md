---
name: Security Review
description: Checks for security issues including hardcoded credentials, SQL injection vectors, unsafe deserialization, credential leakage in logs/errors, and proper use of dlt's secrets management system.
---

# Security Review

## Context

dlt handles sensitive data: database credentials, API keys, cloud provider tokens, and user data in transit. The project uses `bandit` for security linting (`make lint-security` with `-lll` level) and has a sophisticated secrets management system. Security issues in a data pipeline library can have outsized impact since dlt runs in production ETL environments.

## What to Check

### 1. Hardcoded Credentials

- No API keys, tokens, passwords, or connection strings in source files
- No credentials in test files (use `tests/.dlt/secrets.toml` which is gitignored)
- Check string literals for patterns like `password=`, `token=`, `key=`, `secret=`

### 2. Secret Handling

- Secrets must flow through dlt's configuration system (`TSecretValue`, `@configspec`)
- Secret values must never be logged, printed, or included in error messages
- Check that `__repr__` and `__str__` of configuration classes don't expose secrets
- The `attrs()` method on exceptions should not include secret fields

### 3. SQL Injection

For code that constructs SQL queries (especially in destinations):

- Are user-provided identifiers (table names, column names) properly escaped?
- Is `sql_client.make_qualified_table_name()` used instead of string formatting?
- Are parameterized queries used for data values?

```python
# GOOD
sql_client.execute_sql(
    f"SELECT * FROM {sql_client.make_qualified_table_name(table_name)} WHERE id = %s",
    params=[user_id]
)

# BAD
sql_client.execute_sql(f"SELECT * FROM {table_name} WHERE id = {user_id}")
```

### 4. Unsafe Deserialization

- Is `pickle` used anywhere? It should be avoided for untrusted data
- JSON deserialization should use `dlt.common.json` (not direct `json` module)
- YAML loading should use `safe_load` not `load`

### 5. File Path Handling

- Are file paths validated to prevent path traversal?
- Is `os.path.join` or `pathlib` used instead of string concatenation for paths?
- For filesystem destinations: are bucket paths properly sanitized?

### 6. Credential Leakage in Errors

- Exception messages should not include credential values
- Stack traces that flow to users should be sanitized
- Log messages at any level should not contain secrets

### 7. Dependency Security

- Are new dependencies from reputable sources?
- Do new dependencies have known vulnerabilities?
- Is the minimum version high enough to avoid known CVEs?

### 8. Environment Variable Safety

- Environment variable names for secrets should follow dlt conventions
- Secrets should not be passed via command-line arguments (visible in process lists)
