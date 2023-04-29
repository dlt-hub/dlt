<h1 align="center">
    <strong>data load tool (dlt) — the open-source Python library for data loading</strong>
</h1>
<p align="center">
Be it a Google Colab notebook, AWS Lambda function, an Airflow DAG, your local laptop,<br/>or a GPT-4 assisted development playground—<strong>dlt</strong> can be dropped in anywhere.
</p>

<div align="center">
  <a target="_blank" href="https://join.slack.com/t/dlthub-community/shared_invite/zt-1slox199h-HAE7EQoXmstkP_bTqal65g" style="background:none">
    <img src="https://img.shields.io/badge/slack-join-dlt.svg?labelColor=191937&color=6F6FF7&logo=slack" />
  </a>
  <a target="_blank" href="https://pypi.org/project/dlt/" style="background:none">
    <img src="https://img.shields.io/pypi/v/dlt?labelColor=191937&color=6F6FF7">
  </a>
  <a target="_blank" href="https://pypi.org/project/dlt/" style="background:none">
    <img src="https://img.shields.io/pypi/pyversions/dlt?labelColor=191937&color=6F6FF7">
  </a>
</div>

## Installation

dlt supports Python 3.8+.

```bash
pip install dlt
```

## Quick Start

Load chess game data from chess.com API and save it in DuckDB:

```python
import dlt
from chess import chess # a utility function that grabs data from the chess.com API

# create a dlt pipeline that will load chess game data to the DuckDB destination
pipeline = dlt.pipeline(
    pipeline_name='chess_pipeline',
    destination='duckdb',
    dataset_name='games_data'
)

# use chess.com API to grab data about a few players
data = chess(['magnuscarlsen', 'rpragchess'], start_month='2022/11', end_month='2022/12')

# extract, normalize, and load the data
pipeline.run(data)
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

## Ready to use Pipelines and Destinations

Explore ready to use pipelines (e.g. Google Sheets) in the [Pipelines docs](https://dlthub.com/docs/pipelines/chess) and supported destinations (e.g. DuckDB) in the [Destinations docs](https://dlthub.com/docs/destinations/bigquery).

## Documentation

For detailed usage and configuration, please refer to the [official documentation](https://dlthub.com/docs).

## Examples

You can find examples for various use cases in the [examples](docs/examples) folder.

## Get Involved

The dlt project is quickly growing, and we're excited to have you join our community! Here's how you can get involved:

- **Connect with the Community**: Join other dlt users and contributors on our [Slack](https://join.slack.com/t/dlthub-community/shared_invite/zt-1slox199h-HAE7EQoXmstkP_bTqal65g)
- **Report issues and suggest features**: Please use the [GitHub Issues](https://github.com/dlt-hub/dlt/issues) to report bugs or suggest new features. Before creating a new issue, make sure to search the tracker for possible duplicates and add a comment if you find one.
- **Contribute Pipelines**: Pipelines are data processing steps that help move and transform data between various sources and destinations. Contribute your custom pipelines to the [dlt-hub/pipelines](https://github.com/dlt-hub/pipelines) to help other folks in handling their data tasks.
- **Contribute code**: Check out our [contributing guidelines](CONTRIBUTING.md) for information on how to make a pull request.
- **Improve documentation**: Help us enhance the dlt documentation.

## License

DLT is released under the [Apache 2.0 License](LICENSE.txt).
