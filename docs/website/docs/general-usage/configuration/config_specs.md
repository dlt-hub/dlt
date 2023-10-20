---
title: Configuration specs
description: Configuration specs
keywords: [credentials, secrets.toml, environment variables]
---

## Working with credentials (and other complex configuration values)

`GcpClientCredentialsWithDefault` is an example of a **spec**: a Python `dataclass` that describes the configuration fields, their types and default values. It also allows to parse various native representations of the configuration. Credentials marked with `WithDefaults` mixin are also to instantiate itself from the machine/user default environment ie. googles `default()` or AWS `.aws/credentials`.

As an example, let's use `ConnectionStringCredentials` which represents a database connection string.

```python
@dlt.source
def query(sql: str, dsn: ConnectionStringCredentials = dlt.secrets.value):
  ...
```

The source above executes the `sql` against database defined in `dsn`. `ConnectionStringCredentials` makes sure you get the correct values with correct types and understands the relevant native form of the credentials.


Example 1: use the dictionary form
```toml
[dsn]
database="dlt_data"
password="loader"
username="loader"
host="localhost"
```

Example:2: use the native form
```toml
dsn="postgres://loader:loader@localhost:5432/dlt_data"
```

Example 3: use mixed form: the password is missing in explicit dsn and will be taken from the `secrets.toml`
```toml
dsn.password="loader
```
```python
query("SELECT * FROM customers", "postgres://loader@localhost:5432/dlt_data")
# or
query("SELECT * FROM customers", {"database": "dlt_data", "username": "loader"...})
```

☮️ We will implement more credentials and let people reuse them when writing pipelines:
-  to represent oauth credentials
- api key + api secret
- AWS credentials


### Working with alternatives of credentials (Union types)
If your source/resource allows for many authentication methods you can support those seamlessly for your user. The user just passes the right credentials and `dlt` will inject the right type into your decorated function.

Example:

> read the whole test: /tests/common/configuration/test_spec_union.py, it shows how to create unions of credentials that derive from the common class so you can handle it seamlessly in your code.

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
> This applies not only to credentials but to all specs (see next chapter)

## Writing own specs

**specs** let you take full control over the function arguments:
- which values should be injected, the types, default values.
- you can specify optional and final fields
- form hierarchical configurations (specs in specs).
- provide own handlers for `on_error` or `on_resolved`
- provide own native value parsers
- provide own default credentials logic
- adds all Python dataclass goodies to it
- adds all Python `dict` goodies to it (`specs` instances can be created from dicts and serialized from dicts)

This is used a lot in the `dlt` core and may become useful for complicated sources.

In fact for each decorated function a spec is synthesized. In case of `google_sheets` following class is created.
```python
@configspec
class GoogleSheetsConfiguration:
  tab_names: List[str] = None  # manadatory
  credentials: GcpClientCredentialsWithDefault = None # mandatory secret
  only_strings: Optional[bool] = False
```

> all specs derive from BaseConfiguration: /dlt/common/configuration/specs//base_configuration.py

> all credentials derive from CredentialsConfiguration: /dlt/common/configuration/specs//base_configuration.py

> Read the docstrings in the code above