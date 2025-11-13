<h1 align="center">
    <strong>data load tool (dlt) — the open-source Python library that automates all your tedious data loading tasks</strong>
</h1>
<p align="center">
Be it a Google Colab notebook, AWS Lambda function, an Airflow DAG, your local laptop,<br/>or a GPT-4 assisted development playground—<strong>dlt</strong> can be dropped in anywhere.
</p>


<h3 align="center">

🚀 Join our thriving community of likeminded developers and build the future together!

</h3>

<div align="center">
  <a target="_blank" href="https://dlthub.com/community" style="background:none">
    <img src="https://img.shields.io/badge/slack-join-dlt.svg?labelColor=191937&color=6F6FF7&logo=slack" style="width: 260px;"  />
  </a>
</div>
<div align="center">
  <a target="_blank" href="https://pypi.org/project/dlt/" style="background:none">
    <img src="https://img.shields.io/pypi/v/dlt?labelColor=191937&color=6F6FF7">
  </a>
  <a target="_blank" href="https://pypi.org/project/dlt/" style="background:none">
    <img src="https://img.shields.io/pypi/pyversions/dlt?labelColor=191937&color=6F6FF7">
  </a>
  <a target="_blank" href="https://pypi.org/project/dlt/" style="background:none">
    <img src="https://img.shields.io/pypi/dm/dlt?labelColor=191937&color=6F6FF7">
  </a>
</div>

## Installation

dlt supports Python 3.9 through Python 3.14. Note that some optional extras are not yet available for Python 3.14, so support for this version is considered experimental.

```sh
pip install dlt
```

## Quick Start

Load chess game data from chess.com API and save it in DuckDB:

```python
import dlt
from dlt.sources.helpers import requests

# Create a dlt pipeline that will load
# chess player data to the DuckDB destination
pipeline = dlt.pipeline(
    pipeline_name='chess_pipeline',
    destination='duckdb',
    dataset_name='player_data'
)

# Grab some player data from Chess.com API
data = []
for player in ['magnuscarlsen', 'rpragchess']:
    response = requests.get(f'https://api.chess.com/pub/player/{player}')
    response.raise_for_status()
    data.append(response.json())

# Extract, normalize, and load the data
pipeline.run(data, table_name='player')
```


Try it out in our **[Colab Demo](https://colab.research.google.com/drive/1NfSB1DpwbbHX9_t5vlalBTf13utwpMGx?usp=sharing)** or directly on our wasm-based [playground](https://dlthub.com/docs/tutorial/playground) in our docs.

## Features

dlt is an open-source Python library that loads data from various, often messy data sources into well-structured datasets. It provides lightweight Python interfaces to extract, load, inspect, and transform data. dlt and dlt docs are built from the ground up to be used with LLMs: the [LLM-native workflow](https://dlthub.com/docs/dlt-ecosystem/llm-tooling/llm-native-workflow.md) will take your pipeline code to data in a notebook for over [5000 sources](https://dlthub.com/workspace).

dlt is designed to be easy to use, flexible, and scalable:

- dlt extracts data from [REST APIs](https://dlthub.com/docs/tutorial/rest-api), [SQL databases](https://dlthub.com/docs/tutorial/sql-database), [cloud storage](https://dlthub.com/docs/tutorial/filesystem), [Python data structures](https://dlthub.com/docs/tutorial/load-data-from-an-api), and [many more](https://dlthub.com/docs/dlt-ecosystem/verified-sources).
- dlt infers [schemas](https://dlthub.com/docs/general-usage/schema) and [data types](https://dlthub.com/docs/general-usage/schema/#data-types), [normalizes the data](https://dlthub.com/docs/general-usage/schema/#data-normalizer), and handles nested data structures.
- dlt supports a variety of [popular destinations](https://dlthub.com/docs/dlt-ecosystem/destinations/) and has an interface to add [custom destinations](https://dlthub.com/docs/dlt-ecosystem/destinations/destination) to create reverse ETL pipelines.
- dlt automates pipeline maintenance with [incremental loading](https://dlthub.com/docs/general-usage/incremental-loading), [schema evolution](https://dlthub.com/docs/general-usage/schema-evolution), and [schema and data contracts](https://dlthub.com/docs/general-usage/schema-contracts).
- dlt supports [Python and SQL data access](https://dlthub.com/docs/general-usage/dataset-access/), [transformations](https://dlthub.com/docs/dlt-ecosystem/transformations), [pipeline inspection](https://dlthub.com/docs/general-usage/dashboard.md), and [visualizing data in Marimo Notebooks](https://dlthub.com/docs/general-usage/dataset-access/marimo).
- dlt can be deployed anywhere Python runs, be it on [Airflow](https://dlthub.com/docs/walkthroughs/deploy-a-pipeline/deploy-with-airflow-composer), [serverless functions](https://dlthub.com/docs/walkthroughs/deploy-a-pipeline/deploy-with-google-cloud-functions), or any other cloud deployment of your choice.

## Documentation

For detailed usage and configuration, please refer to the [official documentation](https://dlthub.com/docs).

## Examples

You can find examples for various use cases in the [examples](docs/examples) folder, or in the [code examples section](https://dlthub.com/docs/examples) of our docs page.

## Adding as dependency

`dlt` follows the semantic versioning with the [`MAJOR.MINOR.PATCH`](https://peps.python.org/pep-0440/#semantic-versioning) pattern.

* `major` means breaking changes and removed deprecations
* `minor` new features, sometimes automatic migrations
* `patch` bug fixes

We suggest that you allow only `patch` level updates automatically:
* Using the [Compatible Release Specifier](https://packaging.python.org/en/latest/specifications/version-specifiers/#compatible-release). For example **dlt~=1.0** allows only versions **>=1.0** and less than **<1.1**
* Poetry [caret requirements](https://python-poetry.org/docs/dependency-specification/). For example **^1.0** allows only versions **>=1.0** to **<1.0**

Please also see our [release notes](https://github.com/dlt-hub/dlt/releases) for notable changes between versions.

## Get Involved

The dlt project is quickly growing, and we're excited to have you join our community! Here's how you can get involved:

- **Connect with the Community**: Join other dlt users and contributors on our [Slack](https://dlthub.com/community)
- **Report issues and suggest features**: Please use the [GitHub Issues](https://github.com/dlt-hub/dlt/issues) to report bugs or suggest new features. Before creating a new issue, make sure to search the tracker for possible duplicates and add a comment if you find one.
- **Track progress of our work and our plans**: Please check out our [public Github project](https://github.com/orgs/dlt-hub/projects/9)
- **Improve documentation**: Help us enhance the dlt documentation.

## Contribute code
Please read [CONTRIBUTING](CONTRIBUTING.md) before you make a PR.

- 📣 **New destinations are unlikely to be merged** due to high maintenance cost (but we are happy to improve SQLAlchemy destination to handle more dialects)
- Significant changes require tests and docs and in many cases writing tests will be more laborious than writing code
- Bugfixes and improvements are welcome! You'll get help with writing tests and docs + a decent review.

## License

`dlt` is released under the [Apache 2.0 License](LICENSE.txt).
