# @@@SNIPSTART getting_started_index_snippet_csv
import dlt
import pandas as pd

owid_disasters_csv = "https://raw.githubusercontent.com/owid/owid-datasets/master/datasets/Natural%20disasters%20from%201900%20to%202019%20-%20EMDAT%20(2020)/Natural%20disasters%20from%201900%20to%202019%20-%20EMDAT%20(2020).csv"
df = pd.read_csv(owid_disasters_csv)
data = df.to_dict(orient='records')

pipeline = dlt.pipeline(
    pipeline_name='from_csv',
    destination='duckdb',
    dataset_name='mydata',
)
load_info = pipeline.run(data, table_name="natural_disasters")
print(load_info)
# @@@SNIPEND
