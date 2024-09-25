---
title: OpenAPI source generator
description: OpenAPI dlt source generator
keywords: [openapi, rest api, swagger, source generator, cli, rest]
---
import Header from './_source-info-header.md';

# OpenAPI source generator

<Header/>

Our OpenAPI source generator - `dlt-init-openapi` - generates [`dlt`](../../intro) data pipelines from [OpenAPI 3.x specs](https://swagger.io/specification/) using the [rest_api verified source](./rest_api) to extract data from any REST API. If you are not familiar with the `rest_api` source, please read [rest_api](./rest_api) to learn how our `rest_api` source works.

:::tip
We also have a cool [Google Colab example](https://colab.research.google.com/drive/1MRZvguOTZj1MlkEGzjiso8lQ_wr1MJRI?usp=sharing#scrollTo=LHGxzf1Ev_yr) that demonstrates this generator. 😎
:::

## Features
`dlt-init-openapi` generates code from an OpenAPI spec that you can use to extract data from a `rest_api` into any [`destination`](../destinations/) (e.g., Postgres, BigQuery, Redshift...) that `dlt` supports. dlt-init-openapi additionally executes a set of heuristics to discover information not explicitly defined in OpenAPI specs.

Features include:

* **[Pagination](./rest_api#pagination) discovery** for each endpoint.
* **Primary key discovery** for each entity.
* **Endpoint relationship mapping** into `dlt` [`transformers`](../../general-usage/resource#process-resources-with-dlttransformer) (e.g., /users/ -> /user/{id}).
* **Payload JSON path [data selector](./rest_api#data-selection) discovery** for results nested in the returned JSON.
* **[Authentication](./rest_api#authentication)** discovery for an API.

## A quick example

You will need Python 3.9 or higher installed, as well as pip. You can run `pip install dlt-init-openapi` to install the current version.

We will create a simple example pipeline from a [PokeAPI spec](https://pokeapi.co/) in our repo. You can point to any other OpenAPI Spec instead if you prefer.


1. Run the generator with a URL:
    ```sh
    dlt-init-openapi pokemon --url https://raw.githubusercontent.com/dlt-hub/dlt-init-openapi/devel/tests/cases/e2e_specs/pokeapi.yml --global-limit 2
    ```

2. Alternatively, if you have a local file, you can use the --path flag:
    ```sh
    dlt-init-openapi pokemon --path ./my_specs/pokeapi.yml
    ```

3. You can now pick both of the endpoints from the popup.

4. After selecting your Pokemon endpoints and hitting Enter, your pipeline will be rendered.

5. If you have any kind of authentication on your pipeline (this example does not), open the `.dlt/secrets.toml` and provide the credentials. You can find further settings in the `.dlt/config.toml`.

6. Go to the created pipeline folder and run your pipeline.
    ```sh
    cd pokemon-pipeline
    PROGRESS=enlighten python pipeline.py # we use enlighten for a nice progress bar :)
    ```

7. Print the pipeline info to the console to see what got loaded.
    ```sh
    dlt pipeline pokemon_pipeline info
    ```

8. You can now also install Streamlit to see a preview of the data; you should have loaded 40 Pokemons and their details.
    ```sh
    pip install pandas streamlit
    dlt pipeline pokemon_pipeline show
    ```

9. You can go to our docs at https://dlthub.com/docs to learn how to modify the generated pipeline to load to many destinations, place schema contracts on your pipeline, and many other things.

:::note
We used the `--global-limit 2` CLI flag to limit the requests to the PokeAPI 
for this example. This way, the Pokemon collection endpoint only gets queried 
twice, resulting in 2 x 20 Pokemon details being rendered.
:::

## What will be created?

When you run the `dlt-init-openapi` command above, the following files will be generated:

```text
pokemon_pipeline/
├── .dlt/
│   ├── config.toml     # dlt config, learn more at dlthub.com/docs
│   └── secrets.toml    # your secrets, only needed for APIs with auth
├── pokemon/
│   └── __init__.py     # your rest_api dictionary, learn more below
├── rest_api/
│   └── ...             # rest_api copied from our verified sources repo
├── .gitignore
├── pokemon_pipeline.py # your pipeline file that you can execute
├── README.md           # a list of your endpoints with some additional info
└── requirements.txt    # the pip requirements for your pipeline
```

:::warning
If you re-generate your pipeline, you will be prompted to continue if this folder exists. If you select yes, all generated files will be overwritten. All other files you may have created will remain in this folder. In non-interactive mode, you will not be asked, and the generated files will be overwritten.
:::

## A closer look at your `rest_api` dictionary in `pokemon/__init__.py`

This file contains the [configuration dictionary](./rest_api#source-configuration) for the rest_api source, which is the main result of running this generator. For our Pokemon example, we have used an OpenAPI 3 spec that works out of the box. The result of this dictionary depends on the quality of the spec you are using, whether the API you are querying actually adheres to this spec, and whether our heuristics manage to find the right values.

The generated dictionary will look something like this:

```py
{
    "client": {
        "base_url": base_url,
        # -> the detected common paginator
        "paginator": {
            ...
        },
    },
    # -> your two endpoints
    "resources": [
        {
            # -> A primary key could not be inferred from
            # the spec; usual suspects such as id, pokemon_id, etc.
            # are not defined. You can add one if you know.
            "name": "pokemon_list",
            "table_name": "pokemon",
            "endpoint": {
                # -> the results seem to be nested in { results: [...] }
                "data_selector": "results",
                "path": "/api/v2/pokemon/",
            },
        },
        {
            "name": "pokemon_read",
            "table_name": "pokemon",
            # -> A primary key *name* is assumed, as it is found in the 
            # url.
            "primary_key": "name",
            "write_disposition": "merge",
            "endpoint": {
                "data_selector": "$",
                "path": "/api/v2/pokemon/{name}/",
                "params": {
                    # -> your detected transformer settings
                    # this is a child endpoint of the pokemon_list
                    "name": {
                        "type": "resolve",
                        "resource": "pokemon_list",
                        "field": "name",
                    },
                },
            },
        },
    ],
}
```

:::info
You can edit this file to adapt the behavior of the dlt rest_api accordingly. Please read our [dlt rest_api](./rest_api) docs to learn how to configure the rest_api source and check out our detailed [Google Colab example](https://colab.research.google.com/drive/1MRZvguOTZj1MlkEGzjiso8lQ_wr1MJRI?usp=sharing#scrollTo=LHGxzf1Ev_yr).
:::

## CLI command

```sh
dlt-init-openapi <source_name> [OPTIONS]
```

### Example:
```sh
dlt-init-openapi pokemon --path ./path/to/my_spec.yml --no-interactive --output-path ./my_pipeline
```

**Options**:

_The only required options are either to supply a path or a URL to a spec._

- `--url URL`: A URL to read the OpenAPI JSON or YAML file from.
- `--path PATH`: A path to read the OpenAPI JSON or YAML file from locally.
- `--output-path PATH`: A path to render the output to.
- `--config PATH`: Path to the config file to use (see below).
- `--no-interactive`: Skip endpoint selection and render all paths of the OpenAPI spec.
- `--log-level`: Set the logging level for stdout output, defaults to 20 (INFO).
- `--global-limit`: Set a global limit on the generated source.
- `--update-rest-api-source`: Update the locally cached rest_api verified source.
- `--allow-openapi-2`: Allows the use of OpenAPI v2 specs. Migration of the spec to 3.0 is recommended for better results, though.
- `--version`: Show the installed version of the generator and exit.
- `--help`: Show this message and exit.

## Config options
You can pass a path to a config file with the `--config PATH` argument. To see available config values, go to https://github.com/dlt-hub/dlt-init-openapi/blob/devel/dlt_init_openapi/config.py and read the information below each field on the `Config` class.

The config file can be supplied as a JSON or YAML dictionary. For example, to change the package name, you can create a YAML file:

```yaml
# config.yml
package_name: "other_package_name"
```

And use it with the config argument:

```sh
$ dlt-init-openapi pokemon --url ... --config config.yml
```

## Telemetry
We track your usage of this tool similarly to how we track other commands in the dlt core library. Read more about this and how to disable it [here](../../reference/telemetry).

## Prior work
This project started as a fork of [openapi-python-client](https://github.com/openapi-generators/openapi-python-client). Pretty much all parts are heavily changed or completely replaced, but some lines of code still exist, and we like to acknowledge the many good ideas we got from the original project :)

## Implementation notes
* OAuth Authentication currently is not natively supported. You can supply your own.
* Per endpoint authentication currently is not supported by the generator. Only the first globally set securityScheme will be applied. You can add your own per endpoint if you need to.
* Basic OpenAPI 2.0 support is implemented. We recommend updating your specs at https://editor.swagger.io before using `dlt-init-openapi`.

