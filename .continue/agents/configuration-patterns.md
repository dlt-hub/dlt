---
name: Configuration Patterns Check
description: Ensures new configuration classes use @configspec correctly, credentials are properly typed with TSecretValue, and configuration follows dlt's provider-based resolution system.
---

# Configuration Patterns Check

## Context

dlt has a sophisticated configuration system based on `@configspec`-decorated dataclasses that are automatically resolved from multiple providers (environment variables, TOML files, Google Secrets Manager, etc.). New configuration must follow established patterns to work correctly with this system.

## What to Check

### 1. @configspec Usage

Any new configuration class should:

- Be decorated with `@configspec`
- Extend `BaseConfiguration` or a more specific base (`DestinationClientConfiguration`, `CredentialsConfiguration`, etc.)
- Not use plain `@dataclass` for configuration — `@configspec` adds the necessary resolution behavior

```python
# GOOD
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import BaseConfiguration

@configspec
class MyConfig(BaseConfiguration):
    api_url: str = None
    timeout: int = 30
```

### 2. Secret Fields

Fields containing sensitive data must be typed with `TSecretValue` or `TSecretStrValue`:

```python
from dlt.common.typing import TSecretValue

@configspec
class MyConfig(BaseConfiguration):
    api_key: TSecretValue = None  # Will be resolved from secrets providers
    api_url: str = None           # Will be resolved from config providers
```

### 3. Credential Specs

For authentication credentials:

- Reuse existing credential specs where possible (`AwsCredentials`, `GcpServiceAccountCredentials`, `AzureCredentials`, etc.) from `dlt/common/configuration/specs/`
- New credential types should extend `CredentialsConfiguration`
- Connection strings should extend `ConnectionStringCredentials`

### 4. Configuration Injection

Functions that accept configuration should use `@with_config` or accept config through the decorator system:

```python
# GOOD — config is injected via dlt's resolution
@dlt.source
def my_source(api_key: TSecretValue = dlt.secrets.value):
    ...

# GOOD — explicit with_config
@with_config(sections=("my_section",))
def my_function(config: MyConfig):
    ...
```

### 5. Nested Configuration

Nested configuration should use proper field types, not plain dicts:

```python
# GOOD
@configspec
class ParentConfig(BaseConfiguration):
    credentials: AwsCredentials = None  # Nested configspec

# BAD
@configspec
class ParentConfig(BaseConfiguration):
    credentials: dict = None  # Loses type safety and resolution
```

### 6. No Hardcoded Secrets

Check that no credentials, API keys, tokens, or connection strings are hardcoded in source files. They should always flow through the configuration system.
