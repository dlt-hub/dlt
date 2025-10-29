import dlt
from dlt._workspace._templates._single_file_templates.fruitshop_pipeline import (
    fruitshop as fruitshop_source,
)

pf = dlt.pipeline(pipeline_name="fruit_pipeline", destination=dlt.destinations.filesystem("_data"))
print(pf.run(fruitshop_source()))
