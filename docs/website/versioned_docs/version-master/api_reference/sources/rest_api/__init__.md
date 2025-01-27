---
sidebar_label: rest_api
title: sources.rest_api
---

Generic API Source

## rest\_api

```python
@decorators.source
def rest_api(
    client: ClientConfig = dlt.config.value,
    resources: List[Union[str, EndpointResource,
                          DltResource]] = dlt.config.value,
    resource_defaults: Optional[EndpointResourceBase] = None
) -> List[DltResource]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/rest_api/__init__.py#L64)

Creates and configures a REST API source with default settings

## rest\_api\_source

```python
def rest_api_source(config: RESTAPIConfig,
                    name: str = None,
                    section: str = None,
                    max_table_nesting: int = None,
                    root_key: bool = False,
                    schema: Schema = None,
                    schema_contract: TSchemaContract = None,
                    parallelized: bool = False) -> DltSource
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/rest_api/__init__.py#L75)

Creates and configures a REST API source for data extraction.

**Arguments**:

- `config` _RESTAPIConfig_ - Configuration for the REST API source.
- `name` _str, optional_ - Name of the source.
- `section` _str, optional_ - Section of the configuration file.
- `max_table_nesting` _int, optional_ - Maximum depth of nested table above which
  the remaining nodes are loaded as structs or JSON.
- `root_key` _bool, optional_ - Enables merging on all resources by propagating
  root foreign key to child tables. This option is most useful if you
  plan to change write disposition of a resource to disable/enable merge.
  Defaults to False.
- `schema` _Schema, optional_ - An explicit `Schema` instance to be associated
  with the source. If not present, `dlt` creates a new `Schema` object
  with provided `name`. If such `Schema` already exists in the same
  folder as the module containing the decorated function, such schema
  will be loaded from file.
- `schema_contract` _TSchemaContract, optional_ - Schema contract settings
  that will be applied to this resource.
- `parallelized` _bool, optional_ - If `True`, resource generators will be
  extracted in parallel with other resources. Transformers that return items are also parallelized.
  Non-eligible resources are ignored. Defaults to `False` which preserves resource settings.
  

**Returns**:

- `DltSource` - A configured dlt source.
  

**Example**:

  pokemon_source = rest_api_source({
- `"client"` - {
- `"base_url"` - "https://pokeapi.co/api/v2/",
- `"paginator"` - "json_link",
  },
- `"endpoints"` - {
- `"pokemon"` - {
- `"params"` - {
- `"limit"` - 100, # Default page size is 20
  },
- `"resource"` - {
- `"primary_key"` - "id",
  }
  },
  },
  })

## rest\_api\_resources

```python
def rest_api_resources(config: RESTAPIConfig) -> List[DltResource]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/rest_api/__init__.py#L145)

Creates a list of resources from a REST API configuration.

**Arguments**:

- `config` _RESTAPIConfig_ - Configuration for the REST API source.
  

**Returns**:

- `List[DltResource]` - List of dlt resources.
  

**Example**:

  github_source = rest_api_resources({
- `"client"` - {
- `"base_url"` - "https://api.github.com/repos/dlt-hub/dlt/",
- `"auth"` - {
- `"token"` - dlt.secrets["token"],
  },
  },
- `"resource_defaults"` - {
- `"primary_key"` - "id",
- `"write_disposition"` - "merge",
- `"endpoint"` - {
- `"params"` - {
- `"per_page"` - 100,
  },
  },
  },
- `"resources"` - [
  {
- `"name"` - "issues",
- `"endpoint"` - {
- `"path"` - "issues",
- `"params"` - {
- `"sort"` - "updated",
- `"direction"` - "desc",
- `"state"` - "open",
- `"since"` - {
- `"type"` - "incremental",
- `"cursor_path"` - "updated_at",
- `"initial_value"` - "2024-01-25T11:21:28Z",
  },
  },
  },
  },
  {
- `"name"` - "issue_comments",
- `"endpoint"` - {
- `"path"` - "issues/{issue_number}/comments",
- `"params"` - {
- `"issue_number"` - {
- `"type"` - "resolve",
- `"resource"` - "issues",
- `"field"` - "number",
  }
  },
  },
  },
  ],
  })

