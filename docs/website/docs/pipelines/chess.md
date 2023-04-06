# Chess.com

This pipeline can be used to load player data from the [Chess.com API](https://www.chess.com/news/view/published-data-api) onto a [destination](https://dlthub.com/docs/destinations) of your choice.

## Initialize the pipeline

Initialize the pipeline with the following command:
```
dlt init chess bigquery
```
Here, we chose BigQuery as the destination. To choose a different destination, replace `bigquery` with your choice of destination. 

Running this command will create a directory with the following structure:
```bash
├── .dlt
│   ├── .pipelines
│   ├── config.toml
│   └── secrets.toml
├── chess
│   └── __pycache__
│   └── __init__.py
├── .gitignore
├── chess_pipeline.py
└── requirements.txt
```

## Add credentials

Before running the pipeline you may need to add credentials in the `.dlt/secrets.toml` file for your chosen destination. For instructions on how to do this, follow the steps detailed under the desired destination in the [destinations](https://dlthub.com/docs/destinations) page.

## Run the pipeline

1. Install the necessary dependencies by running the following command:
```
pip install -r requirements.txt
```
2. Now the pipeline can be run by using the command:
```
python3 chess_pipeline.py
```
3. To make sure that everything is loaded as expected, use the command:
```
dlt pipeline chess_pipeline show
```

## Customize parameters

Without any modifications, the chess pipeline will load data for a default list of players over a default period of time. You can change these values in the `chess_pipeline.py` script.

For example, if you wish to load player games for a specific set of players, add the player list to the function `load_player_games_example` as below.
```python
def load_players_games_example(start_month: str, end_month: str):

    pipeline = dlt.pipeline(pipeline_name="chess_pipeline", destination='bigquery', dataset_name="chess_players_games_data")

    data = chess(
        [], # Specify your list of players here
        start_month=start_month,
        end_month=end_month
    )

    info = pipeline.run(data.with_resources("players_games", "players_profiles"))
    print(info)
```
To specify the time period, pass the starting and ending months as parameters when calling the function in the `__main__` block:
```python
if __name__ == "__main__" :
    load_players_games_example("2022/11", "2022/12") # Replace the strings "2022/11" and "2022/12" with different months in the "YYYY/MM" format
```
