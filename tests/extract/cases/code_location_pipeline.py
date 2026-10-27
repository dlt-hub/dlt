import dlt
from dlt.common import json
from dlt.common.utils import uniq_id


@dlt.source
def fruitshop():
    @dlt.resource
    def fruits():
        yield [{"name": "apple"}]

    return fruits


if __name__ == "__main__":
    pipeline = dlt.pipeline(pipeline_name="code_location_" + uniq_id())
    info = pipeline.extract(fruitshop())
    print(json.dumps(info.metrics[info.loads_ids[0]][0]["inputs"]))
