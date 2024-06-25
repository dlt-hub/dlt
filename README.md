<h1 align="center">
    <strong>data load tool (dlt) — the open-source Python library for data loading</strong>
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
</div>

## Installation

dlt supports Python 3.8+.

```sh
pip install dlt
```

More options: [Install via Conda or Pixi](https://dlthub.com/docs/reference/installation#install-dlt-via-pixi-and-conda)


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


Try it out in our **[Colab Demo](https://colab.research.google.com/drive/1NfSB1DpwbbHX9_t5vlalBTf13utwpMGx?usp=sharing)**

## Features

- **Automatic Schema:** Data structure inspection and schema creation for the destination.
- **Data Normalization:** Consistent and verified data before loading.
- **Seamless Integration:** Colab, AWS Lambda, Airflow, and local environments.
- **Scalable:** Adapts to growing data needs in production.
- **Easy Maintenance:** Clear data pipeline structure for updates.
- **Rapid Exploration:** Quickly explore and gain insights from new data sources.
- **Versatile Usage:** Suitable for ad-hoc exploration to advanced loading infrastructures.
- **Start in Seconds with CLI:** Powerful CLI for managing, deploying and inspecting local pipelines.
- **Incremental Loading:** Load only new or changed data and avoid loading old records again.
- **Open Source:** Free and Apache 2.0 Licensed.

## Ready to use Sources and Destinations

Explore ready to use sources (e.g. Google Sheets) in the [Verified Sources docs](https://dlthub.com/docs/dlt-ecosystem/verified-sources) and supported destinations (e.g. DuckDB) in the [Destinations docs](https://dlthub.com/docs/dlt-ecosystem/destinations).

## Documentation

For detailed usage and configuration, please refer to the [official documentation](https://dlthub.com/docs).

## Examples

You can find examples for various use cases in the [examples](docs/examples) folder.

## Adding as dependency

`dlt` follows the semantic versioning with the [`MAJOR.MINOR.PATCH`](https://peps.python.org/pep-0440/#semantic-versioning) pattern. Currently, we are using **pre-release versioning** with the major version being 0.

- `minor` version change means breaking changes
- `patch` version change means new features that should be backward compatible
- any suffix change, e.g., `post10` -> `post11`, is considered a patch

We suggest that you allow only `patch` level updates automatically:
* Using the [Compatible Release Specifier](https://packaging.python.org/en/latest/specifications/version-specifiers/#compatible-release). For example **dlt~=0.3.10** allows only versions **>=0.3.10** and less than **<0.4**
* Poetry [caret requirements](https://python-poetry.org/docs/dependency-specification/). For example **^0.3.10** allows only versions **>=0.3.10** to **<0.4**
## Get Involved

The dlt project is quickly growing, and we're excited to have you join our community! Here's how you can get involved:

- **Connect with the Community**: Join other dlt users and contributors on our [Slack](https://dlthub.com/community)
- **Report issues and suggest features**: Please use the [GitHub Issues](https://github.com/dlt-hub/dlt/issues) to report bugs or suggest new features. Before creating a new issue, make sure to search the tracker for possible duplicates and add a comment if you find one.
- **Track progress of our work and our plans**: Please check out our [public Github project](https://github.com/orgs/dlt-hub/projects/9)
- **Contribute Verified Sources**: Contribute your custom sources to the [dlt-hub/verified-sources](https://github.com/dlt-hub/verified-sources) to help other folks in handling their data tasks.
- **Contribute code**: Check out our [contributing guidelines](CONTRIBUTING.md) for information on how to make a pull request.
- **Improve documentation**: Help us enhance the dlt documentation.

## License

`dlt` is released under the [Apache 2.0 License](LICENSE.txt).
