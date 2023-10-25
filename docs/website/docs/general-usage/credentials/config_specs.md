---
title: Configuration Specs
description: Overview configuration specs and how to create custom specs
keywords: [credentials, secrets.toml, secrets, config, configuration, environment
      variables, specs]
---

# Configuration Specs

## Working with credentials (and other complex configuration values)

Example:

```python
import dlt
from dlt.sources.credentials import GcpServiceAccountCredentials

@dlt.source
def google_sheets(
    spreadsheet_id: str,
    tab_names: List[str] = dlt.config.value,
    credentials: GcpServiceAccountCredentials = dlt.secrets.value,
    only_strings: bool = False
):
  ...
```

Here, `GcpServiceAccountCredentials` is an example of a **spec**: a Python `dataclass` that describes
the configuration fields, their types and default values. It also allows parsing various native
representations of the configuration. Credentials marked with `WithDefaults` mixin are also to
instantiate itself from the machine/user default environment i.e. Googles `default()` or AWS
`.aws/credentials`.

As an example, let's use `ConnectionStringCredentials` which represents a database connection
string.

```python
from dlt.sources.credentials import ConnectionStringCredentials

@dlt.source
def query(sql: str, dsn: ConnectionStringCredentials = dlt.secrets.value):
  ...
```

The source above executes the `sql` against database defined in `dsn`. `ConnectionStringCredentials`
makes sure you get the correct values with correct types and understands the relevant native form of
the credentials.

Example 1. Use the **dictionary** form.

```toml
[dsn]
database="dlt_data"
password="loader"
username="loader"
host="localhost"
```

Example 2. Use the **native** form.

```toml
dsn="postgres://loader:loader@localhost:5432/dlt_data"
```

Example 3. Use the **mixed** form: the password is missing in explicit dsn and will be taken from the
`secrets.toml`.

```toml
dsn.password="loader
```

You can explicitly provide credentials in various forms:

```python
query("SELECT * FROM customers", "postgres://loader@localhost:5432/dlt_data")
# or
query("SELECT * FROM customers", {"database": "dlt_data", "username": "loader"...})
```

### Built in credentials

@Alena could you update this section? what is in `from dlt.sources.credentials`? just list it below
```
from dlt.sources.credentials import ConnectionStringCredentials
```
We will implement more credentials and let people reuse them when writing pipelines:

- To represent oauth credentials.
- API key + API secret.
- AWS credentials.

## Working with alternatives of credentials (Union types)

If your source/resource allows for many authentication methods, you can support those seamlessly for
your user. The user just passes the right credentials and `dlt` will inject the right type into your
decorated function.

Example:


```python
@dlt.source
def zen_source(credentials: Union[ZenApiKeyCredentials, ZenEmailCredentials, str] = dlt.secrets.value, some_option: bool = False):
  # depending on what the user provides in config, ZenApiKeyCredentials or ZenEmailCredentials will be injected in `credentials` argument
  # both classes implement `auth` so you can always call it
  credentials.auth()
  return dlt.resource([credentials], name="credentials")

# pass native value
os.environ["CREDENTIALS"] = "email:mx:pwd"
assert list(zen_source())[0].email == "mx"

# pass explicit native value
assert list(zen_source("secret:🔑:secret"))[0].api_secret == "secret"

# pass explicit dict
assert list(zen_source(credentials={"email": "emx", "password": "pass"}))[0].email == "emx"

```

> This applies not only to credentials but to all specs (see next chapter).

Read the [whole test](https://github.com/dlt-hub/dlt/blob/devel/tests/common/configuration/test_spec_union.py), it shows how to create unions
of credentials that derive from the common class, so you can handle it seamlessly in your code.

## Writing custom specs

**specs** let you take full control over the function arguments:

- Which values should be injected, the types, default values.
- You can specify optional and final fields.
- Form hierarchical configurations (specs in specs).
- Provide own handlers for `on_partial` (called before failing on missing config key) or `on_resolved`.
- Provide own native value parsers.
- Provide own default credentials logic.
- Adds all Python dataclass goodies to it.
- Adds all Python `dict` goodies to it (`specs` instances can be created from dicts and serialized
  from dicts).

This is used a lot in the `dlt` core and may become useful for complicated sources.

In fact, for each decorated function a spec is synthesized. In case of `google_sheets` following
class is created:

```python
from dlt.sources.config import configspec, with_config

@configspec
class GoogleSheetsConfiguration(BaseConfiguration):
  tab_names: List[str] = None  # manadatory
  credentials: GcpServiceAccountCredentials = None # mandatory secret
  only_strings: Optional[bool] = False
```

- All specs derive from [BaseConfiguration.](https://github.com/dlt-hub/dlt/blob/devel/dlt/common/configuration/specs/base_configuration.py#L170)

- All credentials derive from [CredentialsConfiguration.](https://github.com/dlt-hub/dlt/blob/devel/dlt/common/configuration/specs/base_configuration.py#L307)

> Read the docstrings in the code above.
